"""
risk_detector.py — RiskGuard 风控检测消费者（手动 Offset 提交）
================================================================
消费 trades.raw，对每笔交易执行三条风控规则检测：

  规则1 — 大额交易：单笔交易 > CAD $50,000
  规则2 — 高频交易：同账户 60 秒内超过 5 笔
  规则3 — 价格异常：成交价偏离市场价 > 5%

检测到告警 → 发布到 risk.alerts
处理失败   → 死信队列 trades.dlq
手动提交   → 处理成功后才提交 Offset（At-Least-Once 语义）
优雅关闭   → SIGINT/SIGTERM 触发 graceful shutdown

运行方式：
  python consumer/risk_detector.py
  python consumer/risk_detector.py --log-level DEBUG
"""

import argparse
import json
import logging
import signal
import sys
import time
import uuid
from collections import defaultdict, deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

# 将项目根目录加入 Python 路径
sys.path.insert(0, str(Path(__file__).parent.parent))

from confluent_kafka import Consumer, Producer, KafkaError, KafkaException
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField
from prometheus_client import Counter, Gauge, Histogram, start_http_server

from config.kafka_config import (
    CONSUMER_CONFIG,
    PRODUCER_CONFIG,
    SCHEMA_REGISTRY_CONFIG,
    TOPICS,
    RISK_RULES,
    METRICS_CONFIG,
)
from scripts.market_prices import get_market_price

# =============================================================================
# 日志配置
# =============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("risk-detector")


# =============================================================================
# Prometheus 指标
# =============================================================================

TRADES_CONSUMED = Counter(
    "riskguard_detector_trades_consumed_total",
    "消费者处理的交易总条数",
    ["status"],  # success / failed / dlq
)
ALERTS_GENERATED = Counter(
    "riskguard_alerts_generated_total",
    "生成的风险告警总数",
    ["alert_type", "severity"],
)
PROCESSING_LATENCY = Histogram(
    "riskguard_detection_latency_ms",
    "风控检测处理延迟（毫秒）",
    buckets=[1, 2, 5, 10, 25, 50, 100, 250],
)
CONSUMER_LAG = Gauge(
    "riskguard_consumer_lag",
    "当前消费者 Lag（待处理消息数）",
    ["partition"],
)
ACTIVE_ACCOUNTS_TRACKED = Gauge(
    "riskguard_accounts_tracked",
    "当前正在追踪的账户数（频率检测）",
)


# =============================================================================
# 频率检测窗口管理
# =============================================================================

class FrequencyWindow:
    """
    基于滑动时间窗口的交易频率追踪器。

    实现：使用 collections.deque（双端队列）存储时间戳。
      - 每次检查时，弹出窗口外的旧时间戳：O(1) 均摊复杂度
      - 窗口内的时间戳数量即为近期交易笔数
      - 内存占用：每个账户最多 max_trades + 1 个时间戳

    为何用 deque 而非 list：
      - list.pop(0) 是 O(n)（移动所有元素）
      - deque.popleft() 是 O(1)（直接删除头部节点）
    """

    def __init__(self, window_seconds: int, max_trades: int):
        self.window_seconds = window_seconds
        self.max_trades = max_trades
        # 每个账户维护一个 deque，存储交易时间戳（秒）
        self._windows: dict[str, deque] = defaultdict(deque)

    def add_and_check(self, account_id: str, timestamp_ms: int) -> tuple[bool, int]:
        """
        记录一笔交易，并检查是否触发频率告警。

        Args:
            account_id: 账户 ID
            timestamp_ms: 交易时间戳（毫秒）

        Returns:
            (triggered: bool, count: int)
              triggered — 是否触发告警
              count — 当前窗口内交易笔数
        """
        now_sec = timestamp_ms / 1000.0
        cutoff = now_sec - self.window_seconds
        window = self._windows[account_id]

        # 弹出窗口外的旧时间戳（O(1) 均摊）
        while window and window[0] < cutoff:
            window.popleft()

        # 添加当前时间戳
        window.append(now_sec)

        count = len(window)
        triggered = count > self.max_trades

        return triggered, count

    def cleanup_stale_accounts(self, current_time_sec: float) -> int:
        """
        清理长时间无活动的账户（防止内存无限增长）。

        Returns:
            清理的账户数量
        """
        cutoff = current_time_sec - self.window_seconds * 2
        stale = [
            acc for acc, window in self._windows.items()
            if not window or window[-1] < cutoff
        ]
        for acc in stale:
            del self._windows[acc]
        return len(stale)

    @property
    def tracked_accounts(self) -> int:
        return len(self._windows)


# =============================================================================
# 风控规则实现
# =============================================================================

class RiskRuleEngine:
    """
    风控规则引擎。

    三条规则独立运行，可同时触发（MULTI_RULE 类型）。
    """

    def __init__(self, rules: dict):
        self.large_trade_threshold = rules["large_trade_threshold_cad"]
        self.freq_window = rules["frequency_window_seconds"]
        self.freq_max = rules["frequency_max_trades"]
        self.price_deviation_pct = rules["price_deviation_pct"]

        # 初始化频率窗口
        self._freq_tracker = FrequencyWindow(self.freq_window, self.freq_max)

        logger.info("⚙️  风控规则引擎初始化完成")
        logger.info(f"   规则1 大额阈值:  CAD {self.large_trade_threshold:,.0f}")
        logger.info(f"   规则2 频率窗口:  {self.freq_window}s 内 >{self.freq_max} 笔")
        logger.info(f"   规则3 价格偏差:  >{self.price_deviation_pct}%")

    def check(self, trade: dict) -> list[dict]:
        """
        对单笔交易执行所有风控规则检查。

        Args:
            trade: 交易数据字典

        Returns:
            告警列表（空列表 = 无风险）
        """
        alerts = []

        # ── 规则1：大额交易 ──────────────────────────────────────────────────
        alert = self._check_large_trade(trade)
        if alert:
            alerts.append(alert)

        # ── 规则2：高频交易 ──────────────────────────────────────────────────
        alert = self._check_high_frequency(trade)
        if alert:
            alerts.append(alert)

        # ── 规则3：价格异常 ──────────────────────────────────────────────────
        alert = self._check_price_anomaly(trade)
        if alert:
            alerts.append(alert)

        # 多规则同时触发 → 升级为 CRITICAL
        if len(alerts) >= 2:
            for a in alerts:
                a["severity"] = "CRITICAL"
                a["alert_type"] = "MULTI_RULE"
            # 合并为一个综合告警
            combined = alerts[0].copy()
            combined["alert_id"] = str(uuid.uuid4())
            combined["description"] = (
                f"多规则同时触发！账户 {trade['account_id']} 触发 {len(alerts)} 条风控规则: "
                + "; ".join(a["description"] for a in alerts)
            )
            combined["alert_type"] = "MULTI_RULE"
            combined["severity"] = "CRITICAL"
            combined["recommended_action"] = "FREEZE_ACCOUNT"
            return [combined]

        return alerts

    def _check_large_trade(self, trade: dict) -> Optional[dict]:
        """规则1：大额交易告警。"""
        total = trade["total_value_cad"]
        if total <= self.large_trade_threshold:
            return None

        return {
            "alert_id": str(uuid.uuid4()),
            "trade_id": trade["trade_id"],
            "account_id": trade["account_id"],
            "alert_type": "LARGE_TRADE",
            "severity": "HIGH" if total < 200_000 else "CRITICAL",
            "description": (
                f"大额交易告警：账户 {trade['account_id']} 执行了 "
                f"{trade['asset_pair']} {trade['side']} 交易，"
                f"金额 CAD {total:,.2f}，超过阈值 CAD {self.large_trade_threshold:,.0f}"
            ),
            "trade_total_value_cad": total,
            "asset_pair": trade["asset_pair"],
            "alert_timestamp_ms": int(time.time() * 1000),
            "trade_timestamp_ms": trade["timestamp_ms"],
            "rule_details": {
                "threshold_cad": str(self.large_trade_threshold),
                "actual_cad": str(total),
                "excess_cad": str(round(total - self.large_trade_threshold, 2)),
            },
            "recommended_action": "REVIEW" if total < 200_000 else "BLOCK_TRADE",
            "is_resolved": False,
            "resolved_by": None,
        }

    def _check_high_frequency(self, trade: dict) -> Optional[dict]:
        """规则2：高频交易告警（滑动窗口）。"""
        triggered, count = self._freq_tracker.add_and_check(
            trade["account_id"],
            trade["timestamp_ms"],
        )

        if not triggered:
            return None

        return {
            "alert_id": str(uuid.uuid4()),
            "trade_id": trade["trade_id"],
            "account_id": trade["account_id"],
            "alert_type": "HIGH_FREQUENCY",
            "severity": "HIGH",
            "description": (
                f"高频交易告警：账户 {trade['account_id']} 在 {self.freq_window} 秒内"
                f"完成了 {count} 笔交易，超过限制 {self.freq_max} 笔"
            ),
            "trade_total_value_cad": trade["total_value_cad"],
            "asset_pair": trade["asset_pair"],
            "alert_timestamp_ms": int(time.time() * 1000),
            "trade_timestamp_ms": trade["timestamp_ms"],
            "rule_details": {
                "window_seconds": str(self.freq_window),
                "max_trades": str(self.freq_max),
                "actual_count": str(count),
            },
            "recommended_action": "REVIEW",
            "is_resolved": False,
            "resolved_by": None,
        }

    def _check_price_anomaly(self, trade: dict) -> Optional[dict]:
        """规则3：价格异常告警。"""
        market_price = trade.get("market_price_cad")
        if not market_price or market_price <= 0:
            # 没有市场价参考，尝试实时获取
            try:
                market_price = get_market_price(trade["asset_pair"])
            except Exception:
                return None  # 无法获取市场价，跳过检测

        actual_price = trade["price_cad"]
        deviation_pct = abs(actual_price - market_price) / market_price * 100

        if deviation_pct <= self.price_deviation_pct:
            return None

        return {
            "alert_id": str(uuid.uuid4()),
            "trade_id": trade["trade_id"],
            "account_id": trade["account_id"],
            "alert_type": "PRICE_ANOMALY",
            "severity": "MEDIUM" if deviation_pct < 10 else "HIGH",
            "description": (
                f"价格异常告警：{trade['asset_pair']} 成交价 "
                f"CAD {actual_price:,.4f} 偏离市场价 "
                f"CAD {market_price:,.4f} 达 {deviation_pct:.2f}%，"
                f"超过阈值 {self.price_deviation_pct}%"
            ),
            "trade_total_value_cad": trade["total_value_cad"],
            "asset_pair": trade["asset_pair"],
            "alert_timestamp_ms": int(time.time() * 1000),
            "trade_timestamp_ms": trade["timestamp_ms"],
            "rule_details": {
                "market_price_cad": str(market_price),
                "actual_price_cad": str(actual_price),
                "deviation_pct": str(round(deviation_pct, 4)),
                "threshold_pct": str(self.price_deviation_pct),
            },
            "recommended_action": "MONITOR",
            "is_resolved": False,
            "resolved_by": None,
        }

    def cleanup(self) -> None:
        """定期清理内存（防止内存泄漏）。"""
        now = time.time()
        removed = self._freq_tracker.cleanup_stale_accounts(now)
        if removed:
            logger.debug(f"🧹 清理 {removed} 个非活跃账户跟踪记录")
        ACTIVE_ACCOUNTS_TRACKED.set(self._freq_tracker.tracked_accounts)


# =============================================================================
# 风控检测消费者
# =============================================================================

class RiskDetector:
    """
    风控检测消费者主类。

    消费 trades.raw → 执行风控规则 → 发布 risk.alerts / trades.dlq
    """

    def __init__(self):
        self._running = True
        self._rule_engine = RiskRuleEngine(RISK_RULES)

        # 初始化 Schema Registry
        self._schema_registry = SchemaRegistryClient(SCHEMA_REGISTRY_CONFIG)

        # 加载 Avro Schema
        schema_dir = Path(__file__).parent.parent / "config/schemas"

        with open(schema_dir / "trade.avsc") as f:
            trade_schema_str = f.read()
        with open(schema_dir / "risk_alert.avsc") as f:
            alert_schema_str = f.read()

        # 反序列化器（Consumer 端）
        self._trade_deserializer = AvroDeserializer(
            self._schema_registry,
            trade_schema_str,
        )

        # 序列化器（Producer 端，用于发送告警）
        self._alert_serializer = AvroSerializer(
            self._schema_registry,
            alert_schema_str,
            lambda obj, ctx: obj,
        )

        # Consumer 实例
        self._consumer = Consumer(CONSUMER_CONFIG)
        self._consumer.subscribe([TOPICS["raw_trades"]])

        # Alert Producer 实例（重用生产者配置）
        alert_producer_config = {**PRODUCER_CONFIG}
        self._alert_producer = Producer(alert_producer_config)

        # 注册信号处理器
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

        logger.info("✅ RiskDetector 初始化完成")
        logger.info(f"   订阅 Topic: {TOPICS['raw_trades']}")
        logger.info(f"   告警 Topic: {TOPICS['risk_alerts']}")
        logger.info(f"   DLQ Topic:  {TOPICS['dlq']}")

    def _signal_handler(self, signum, frame):
        """优雅关闭信号处理。"""
        logger.info(f"\n📥 收到信号 {signum}，正在关闭...")
        self._running = False

    def _publish_alert(self, alert: dict) -> None:
        """发布风险告警到 risk.alerts Topic。"""
        topic = TOPICS["risk_alerts"]
        key = alert["account_id"]  # 按账户分区（保证同账户告警有序）

        try:
            serialized = self._alert_serializer(
                alert,
                SerializationContext(topic, MessageField.VALUE),
            )
            self._alert_producer.produce(
                topic=topic,
                key=key.encode("utf-8"),
                value=serialized,
            )
            self._alert_producer.poll(0)

            ALERTS_GENERATED.labels(
                alert_type=alert["alert_type"],
                severity=alert["severity"],
            ).inc()

            logger.warning(
                f"🚨 [{alert['severity']}] {alert['alert_type']} | "
                f"账户: {alert['account_id']} | "
                f"描述: {alert['description'][:80]}..."
            )

        except Exception as e:
            logger.error(f"❌ 发布告警失败: {e}")

    def _send_to_dlq(self, raw_message_bytes: bytes, error_reason: str) -> None:
        """将处理失败的消息发送到死信队列（DLQ）。"""
        dlq_topic = TOPICS["dlq"]
        headers = [
            ("error_reason", error_reason.encode("utf-8")),
            ("failed_at", str(int(time.time() * 1000)).encode("utf-8")),
            ("original_topic", TOPICS["raw_trades"].encode("utf-8")),
        ]
        try:
            self._alert_producer.produce(
                topic=dlq_topic,
                value=raw_message_bytes,
                headers=headers,
            )
            self._alert_producer.poll(0)
            logger.warning(f"☠️  消息已发送到 DLQ: {error_reason[:100]}")
        except Exception as e:
            logger.error(f"❌ DLQ 写入失败: {e}")

    def run(self) -> None:
        """主消费循环。"""
        logger.info("=" * 55)
        logger.info("  RiskGuard 风控检测器启动")
        logger.info("=" * 55)

        cleanup_counter = 0
        total_processed = 0
        total_alerts = 0

        try:
            while self._running:
                # poll() 会处理心跳、Rebalance、Offset 提交等
                msg = self._consumer.poll(timeout=1.0)

                if msg is None:
                    # 无新消息，继续等待
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # 已到达分区末尾（正常，不是错误）
                        logger.debug(
                            f"📍 分区 {msg.partition()} 末尾，offset={msg.offset()}"
                        )
                    else:
                        logger.error(f"❌ Kafka 错误: {msg.error()}")
                    continue

                # ── 处理消息 ────────────────────────────────────────────────
                start_time = time.perf_counter()
                success = False
                raw_value = msg.value()

                try:
                    # 反序列化 Avro
                    trade = self._trade_deserializer(
                        raw_value,
                        SerializationContext(msg.topic(), MessageField.VALUE),
                    )

                    # 执行风控检测
                    alerts = self._rule_engine.check(trade)

                    # 发布告警
                    for alert in alerts:
                        self._publish_alert(alert)
                        total_alerts += 1

                    success = True
                    total_processed += 1

                    # 记录延迟
                    elapsed_ms = (time.perf_counter() - start_time) * 1000
                    PROCESSING_LATENCY.observe(elapsed_ms)

                except Exception as e:
                    logger.error(f"❌ 处理消息失败: {e}", exc_info=True)
                    # 发送到 DLQ（死信队列）
                    self._send_to_dlq(raw_value or b"", str(e))
                    TRADES_CONSUMED.labels(status="dlq").inc()

                # ── 手动提交 Offset ──────────────────────────────────────────
                # 关键：只有处理成功才提交
                # 即使有部分失败（已发 DLQ），也提交——因为失败已被安全处理
                if success:
                    self._consumer.commit(message=msg, asynchronous=False)
                    TRADES_CONSUMED.labels(status="success").inc()

                # ── 定期维护 ─────────────────────────────────────────────────
                cleanup_counter += 1
                if cleanup_counter >= 1000:
                    self._rule_engine.cleanup()
                    cleanup_counter = 0
                    logger.info(
                        f"📊 处理统计: 总处理={total_processed}, "
                        f"总告警={total_alerts}, "
                        f"追踪账户={self._rule_engine._freq_tracker.tracked_accounts}"
                    )

        finally:
            self.shutdown(total_processed, total_alerts)

    def shutdown(self, total_processed: int, total_alerts: int) -> None:
        """优雅关闭：刷新 Producer，关闭 Consumer。"""
        logger.info("\n⏳ 正在关闭 RiskDetector...")

        self._alert_producer.flush(timeout=15)
        self._consumer.close()

        logger.info("=" * 55)
        logger.info("  风控检测器关闭统计")
        logger.info("=" * 55)
        logger.info(f"  总处理消息: {total_processed}")
        logger.info(f"  总生成告警: {total_alerts}")
        logger.info("=" * 55)


# =============================================================================
# 主程序入口
# =============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="RiskGuard — Kafka 风控检测消费者",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="日志级别（默认: INFO）",
    )
    parser.add_argument(
        "--metrics-port",
        type=int,
        default=METRICS_CONFIG["consumer_port"],
        help=f"Prometheus 指标端口（默认: {METRICS_CONFIG['consumer_port']}）",
    )
    args = parser.parse_args()

    logging.getLogger().setLevel(getattr(logging, args.log_level))

    # 启动 Prometheus 指标服务器
    start_http_server(args.metrics_port)
    logger.info(f"📊 Prometheus 指标已暴露: http://localhost:{args.metrics_port}/metrics")

    detector = RiskDetector()
    detector.run()


if __name__ == "__main__":
    main()
