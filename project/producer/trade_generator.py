"""
trade_generator.py — RiskGuard 交易生成器（幂等 Producer）
===========================================================
模拟加密货币交易所的实时交易流。

功能：
  - 随机生成真实感的交易数据（10 个账户，6 个资产对）
  - 可选注入异常数据（大额、高频、价格偏差）以触发风控
  - 使用幂等 Producer + Avro 序列化（Schema Registry）
  - Delivery Callback 记录发送成功/失败
  - Prometheus 指标暴露（端口 8000）
  - 优雅关闭（Ctrl+C / SIGTERM）

运行方式：
  python producer/trade_generator.py
  python producer/trade_generator.py --rate 10 --duration 60 --inject-anomalies
  python producer/trade_generator.py --rate 5 --duration 0  # 无限运行
"""

import argparse
import json
import logging
import os
import random
import signal
import sys
import time
import uuid
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

# 将项目根目录加入 Python 路径
sys.path.insert(0, str(Path(__file__).parent.parent))

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField
from prometheus_client import Counter, Gauge, Histogram, start_http_server

from config.kafka_config import (
    PRODUCER_CONFIG,
    SCHEMA_REGISTRY_CONFIG,
    TOPICS,
    METRICS_CONFIG,
)
from scripts.market_prices import get_market_price, get_price_with_anomaly

# =============================================================================
# 日志配置
# =============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("trade-generator")


# =============================================================================
# Prometheus 指标定义
# =============================================================================

TRADES_PRODUCED = Counter(
    "riskguard_trades_produced_total",
    "生产者发出的交易总条数",
    ["asset_pair", "side", "status"],
)
TRADES_IN_FLIGHT = Gauge(
    "riskguard_trades_in_flight",
    "当前正在传输中（尚未收到确认）的消息数",
)
TRADE_VALUE_CAD = Histogram(
    "riskguard_trade_value_cad",
    "交易金额分布（CAD）",
    buckets=[1_000, 5_000, 10_000, 25_000, 50_000, 100_000, 500_000],
)
ANOMALIES_INJECTED = Counter(
    "riskguard_anomalies_injected_total",
    "注入的异常数据条数",
    ["anomaly_type"],
)
PRODUCE_LATENCY = Histogram(
    "riskguard_produce_latency_ms",
    "消息发送延迟（毫秒）",
    buckets=[1, 5, 10, 25, 50, 100, 250, 500],
)


# =============================================================================
# 模拟数据配置
# =============================================================================

# 模拟账户列表
ACCOUNTS = [f"ACC-{str(i).zfill(6)}" for i in range(1, 11)]

# 支持的资产对
ASSET_PAIRS = ["BTC-CAD", "ETH-CAD", "SOL-CAD", "XRP-CAD", "ADA-CAD", "DOGE-CAD"]

# 正常交易金额分布（CAD）
NORMAL_TRADE_RANGES = {
    "BTC-CAD": (0.001, 2.0),      # 0.001 ~ 2 BTC
    "ETH-CAD": (0.01, 20.0),      # 0.01 ~ 20 ETH
    "SOL-CAD": (1.0, 500.0),      # 1 ~ 500 SOL
    "XRP-CAD": (100.0, 50000.0),  # 100 ~ 50,000 XRP
    "ADA-CAD": (100.0, 50000.0),  # 100 ~ 50,000 ADA
    "DOGE-CAD": (1000.0, 200000.0),  # 1,000 ~ 200,000 DOGE
}


# =============================================================================
# 交易生成器
# =============================================================================

class TradeGenerator:
    """
    加密货币交易生成器。

    职责：
      1. 生成随机（或异常）交易数据
      2. 序列化为 Avro 格式
      3. 发布到 Kafka（幂等 Producer）
      4. 记录指标
    """

    def __init__(
        self,
        inject_anomalies: bool = True,
        anomaly_rate: float = 0.1,
    ):
        """
        初始化生成器。

        Args:
            inject_anomalies: 是否注入异常数据
            anomaly_rate: 异常数据比例（0.0 ~ 1.0）
        """
        self.inject_anomalies = inject_anomalies
        self.anomaly_rate = anomaly_rate
        self._running = True
        self._stats = defaultdict(int)

        # 用于高频告警：记录每个账户的最近交易时间
        self._account_trade_times: dict[str, list] = defaultdict(list)

        # 初始化 Schema Registry 客户端
        self._schema_registry = SchemaRegistryClient(SCHEMA_REGISTRY_CONFIG)

        # 加载 Avro Schema
        schema_path = Path(__file__).parent.parent / "config/schemas/trade.avsc"
        with open(schema_path) as f:
            trade_schema_str = f.read()

        # 初始化 Avro 序列化器（自动注册 Schema）
        self._avro_serializer = AvroSerializer(
            self._schema_registry,
            trade_schema_str,
            self._trade_to_dict,
        )

        # 初始化幂等 Producer
        self._producer = Producer(PRODUCER_CONFIG)
        logger.info("✅ Producer 初始化完成（幂等模式）")
        logger.info(f"   Broker: {PRODUCER_CONFIG['bootstrap.servers']}")
        logger.info(f"   Acks: {PRODUCER_CONFIG['acks']}")
        logger.info(f"   幂等: {PRODUCER_CONFIG['enable.idempotence']}")

        # 注册信号处理器（优雅关闭）
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    @staticmethod
    def _trade_to_dict(trade: dict, ctx: SerializationContext) -> dict:
        """将交易字典转换为 Avro 序列化格式（Schema Registry 回调）。"""
        return trade

    def _signal_handler(self, signum, frame):
        """处理 SIGINT/SIGTERM，触发优雅关闭。"""
        logger.info(f"\n📥 收到信号 {signum}，正在关闭...")
        self._running = False

    def _generate_normal_trade(self) -> dict:
        """生成正常交易数据。"""
        asset_pair = random.choice(ASSET_PAIRS)
        account_id = random.choice(ACCOUNTS)
        side = random.choice(["BUY", "SELL"])

        # 获取市场价（含随机波动）
        market_price = get_market_price(asset_pair)

        # 正常成交价：在市场价 ±1% 范围内浮动
        price = market_price * random.uniform(0.99, 1.01)

        # 随机数量
        qty_min, qty_max = NORMAL_TRADE_RANGES[asset_pair]
        quantity = round(random.uniform(qty_min, qty_max), 8)
        total_value = round(quantity * price, 2)

        return {
            "trade_id": str(uuid.uuid4()),
            "account_id": account_id,
            "asset_pair": asset_pair,
            "side": side,
            "quantity": quantity,
            "price_cad": round(price, 4),
            "total_value_cad": total_value,
            "market_price_cad": round(market_price, 4),
            "timestamp_ms": int(time.time() * 1000),
            "exchange_id": "RISKGUARD-EXCHANGE",
            "ip_address": f"192.168.{random.randint(1, 254)}.{random.randint(1, 254)}",
            "user_agent": random.choice([
                "RiskGuard-Mobile/2.1 iOS",
                "RiskGuard-Web/3.0 Chrome",
                "RiskGuard-Desktop/1.5 Windows",
                "RiskGuard-API/1.0 Python",
            ]),
            "metadata": {},
        }

    def _generate_large_trade(self) -> dict:
        """生成大额交易（触发规则1：> CAD $50,000）。"""
        trade = self._generate_normal_trade()
        asset_pair = trade["asset_pair"]
        market_price = trade["market_price_cad"]

        # 强制设置交易金额 > $50,000
        target_value = random.uniform(55_000, 500_000)
        quantity = round(target_value / market_price, 8)
        total_value = round(quantity * trade["price_cad"], 2)

        trade.update({
            "quantity": quantity,
            "total_value_cad": total_value,
        })

        ANOMALIES_INJECTED.labels(anomaly_type="large_trade").inc()
        logger.debug(f"💰 注入大额交易: {trade['account_id']} CAD {total_value:,.2f}")
        return trade

    def _generate_high_frequency_burst(self, account_id: str) -> list[dict]:
        """
        生成高频交易爆发（触发规则2：60秒内 > 5 笔）。
        一次性生成 6 笔同账户交易。
        """
        trades = []
        base_time = int(time.time() * 1000)

        for i in range(6):
            trade = self._generate_normal_trade()
            trade["account_id"] = account_id
            # 时间戳集中在 30 秒内
            trade["timestamp_ms"] = base_time - random.randint(0, 30_000)
            trades.append(trade)

        ANOMALIES_INJECTED.labels(anomaly_type="high_frequency").inc()
        logger.debug(f"⚡ 注入高频交易爆发: {account_id}，共 {len(trades)} 笔")
        return trades

    def _generate_price_anomaly_trade(self) -> dict:
        """生成价格异常交易（触发规则3：偏离市场价 > 5%）。"""
        trade = self._generate_normal_trade()
        asset_pair = trade["asset_pair"]

        # 使用异常价格（偏离 8%）
        anomaly_price = get_price_with_anomaly(asset_pair, anomaly_pct=0.08)
        total_value = round(trade["quantity"] * anomaly_price, 2)

        trade.update({
            "price_cad": round(anomaly_price, 4),
            "total_value_cad": total_value,
        })

        ANOMALIES_INJECTED.labels(anomaly_type="price_anomaly").inc()
        deviation = abs(anomaly_price - trade["market_price_cad"]) / trade["market_price_cad"] * 100
        logger.debug(
            f"📉 注入价格异常: {trade['asset_pair']} "
            f"成交价={anomaly_price:.2f} 市场价={trade['market_price_cad']:.2f} "
            f"偏离={deviation:.1f}%"
        )
        return trade

    def _delivery_callback(self, err, msg):
        """
        Producer 的 Delivery Callback（异步确认回调）。

        每条消息被 Broker 确认（或失败）后调用。
        这是幂等 Producer 的关键监控点。
        """
        TRADES_IN_FLIGHT.dec()

        if err is not None:
            # 发送失败（网络断连、Broker 不可达等）
            logger.error(
                f"❌ 消息发送失败: topic={msg.topic()}, "
                f"partition={msg.partition()}, "
                f"error={err}"
            )
            TRADES_PRODUCED.labels(
                asset_pair="unknown",
                side="unknown",
                status="failed",
            ).inc()
            self._stats["failed"] += 1
        else:
            # 发送成功
            self._stats["success"] += 1
            if self._stats["success"] % 100 == 0:
                logger.info(
                    f"📊 进度: 已发送 {self._stats['success']} 条, "
                    f"失败 {self._stats['failed']} 条"
                )

    def produce(self, trade: dict) -> None:
        """
        将单条交易发布到 Kafka。

        分区策略：按 account_id 哈希分区，确保同账户消息有序。
        这是频率检测的关键——同账户消息落到同一分区，
        消费者只需维护本地状态即可完成检测。
        """
        topic = TOPICS["raw_trades"]
        key = trade["account_id"]  # 按账户 ID 分区

        start_time = time.perf_counter()

        try:
            # Avro 序列化
            serialized_value = self._avro_serializer(
                trade,
                SerializationContext(topic, MessageField.VALUE),
            )

            # 发布到 Kafka（非阻塞）
            self._producer.produce(
                topic=topic,
                key=key.encode("utf-8"),
                value=serialized_value,
                on_delivery=self._delivery_callback,
            )

            TRADES_IN_FLIGHT.inc()

            # 触发 Delivery Callback（非阻塞轮询）
            self._producer.poll(0)

            # 记录指标
            elapsed_ms = (time.perf_counter() - start_time) * 1000
            PRODUCE_LATENCY.observe(elapsed_ms)
            TRADES_PRODUCED.labels(
                asset_pair=trade["asset_pair"],
                side=trade["side"],
                status="sent",
            ).inc()
            TRADE_VALUE_CAD.observe(trade["total_value_cad"])

        except BufferError:
            # Producer 内部队列已满（背压）
            logger.warning("⚠️ Producer 队列已满，等待 1 秒...")
            self._producer.flush(timeout=1)
        except Exception as e:
            logger.error(f"❌ 序列化/发送失败: {e}")
            self._stats["failed"] += 1

    def run(self, rate: float = 5.0, duration: int = 0) -> None:
        """
        主循环：持续生成并发送交易数据。

        Args:
            rate: 每秒发送条数
            duration: 运行秒数（0 = 无限运行）
        """
        interval = 1.0 / max(rate, 0.1)
        start_time = time.time()
        total_sent = 0

        logger.info("=" * 55)
        logger.info("  RiskGuard 交易生成器启动")
        logger.info("=" * 55)
        logger.info(f"  发送速率:   {rate} 条/秒")
        logger.info(f"  运行时长:   {'无限' if duration == 0 else f'{duration} 秒'}")
        logger.info(f"  注入异常:   {self.inject_anomalies}（比例: {self.anomaly_rate:.0%}）")
        logger.info(f"  目标 Topic: {TOPICS['raw_trades']}")
        logger.info("=" * 55)

        # 高频爆发计时器（每 30 秒注入一次）
        last_burst_time = 0

        while self._running:
            # 检查运行时长
            if duration > 0 and (time.time() - start_time) >= duration:
                logger.info(f"⏰ 已达到设定运行时长 {duration} 秒，停止生成")
                break

            loop_start = time.perf_counter()

            # 决定是否注入异常
            if self.inject_anomalies and random.random() < self.anomaly_rate:
                anomaly_type = random.choices(
                    ["large_trade", "high_frequency", "price_anomaly"],
                    weights=[0.4, 0.3, 0.3],
                )[0]

                if anomaly_type == "large_trade":
                    trade = self._generate_large_trade()
                    self.produce(trade)
                    total_sent += 1

                elif anomaly_type == "high_frequency":
                    # 高频爆发：每 30 秒触发一次
                    now = time.time()
                    if now - last_burst_time >= 30:
                        burst_account = random.choice(ACCOUNTS)
                        burst_trades = self._generate_high_frequency_burst(burst_account)
                        for t in burst_trades:
                            self.produce(t)
                        total_sent += len(burst_trades)
                        last_burst_time = now
                    else:
                        # 还未到爆发时间，发正常交易替代
                        trade = self._generate_normal_trade()
                        self.produce(trade)
                        total_sent += 1

                elif anomaly_type == "price_anomaly":
                    trade = self._generate_price_anomaly_trade()
                    self.produce(trade)
                    total_sent += 1
            else:
                # 正常交易
                trade = self._generate_normal_trade()
                self.produce(trade)
                total_sent += 1

            # 速率控制：精确间隔
            elapsed = time.perf_counter() - loop_start
            sleep_time = max(0, interval - elapsed)
            if sleep_time > 0:
                time.sleep(sleep_time)

        # 关闭前刷新所有待发消息
        self.shutdown(total_sent)

    def shutdown(self, total_sent: int) -> None:
        """优雅关闭：等待所有消息发送完毕。"""
        logger.info("\n⏳ 正在刷新剩余消息队列...")
        remaining = self._producer.flush(timeout=30)

        if remaining > 0:
            logger.warning(f"⚠️ 仍有 {remaining} 条消息未发送完成")
        else:
            logger.info("✅ 所有消息已成功发送")

        logger.info("=" * 55)
        logger.info("  生产者关闭统计")
        logger.info("=" * 55)
        logger.info(f"  总发送条数: {total_sent}")
        logger.info(f"  成功确认:   {self._stats['success']}")
        logger.info(f"  发送失败:   {self._stats['failed']}")
        logger.info("=" * 55)


# =============================================================================
# 主程序入口
# =============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="RiskGuard — Kafka 交易生成器（幂等 Producer）",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例：
  # 默认模式：5 条/秒，无限运行，注入异常
  python producer/trade_generator.py

  # 高速模式：20 条/秒，运行 60 秒
  python producer/trade_generator.py --rate 20 --duration 60

  # 纯正常模式：不注入异常
  python producer/trade_generator.py --no-anomalies

  # 自定义异常比例
  python producer/trade_generator.py --anomaly-rate 0.3
        """,
    )
    parser.add_argument(
        "--rate",
        type=float,
        default=5.0,
        help="每秒发送条数（默认: 5）",
    )
    parser.add_argument(
        "--duration",
        type=int,
        default=0,
        help="运行秒数（0 = 无限运行，默认: 0）",
    )
    parser.add_argument(
        "--inject-anomalies",
        action="store_true",
        default=True,
        help="注入异常数据（默认启用）",
    )
    parser.add_argument(
        "--no-anomalies",
        action="store_true",
        help="禁用异常数据注入",
    )
    parser.add_argument(
        "--anomaly-rate",
        type=float,
        default=0.1,
        help="异常数据比例（0.0 ~ 1.0，默认: 0.1）",
    )
    parser.add_argument(
        "--metrics-port",
        type=int,
        default=METRICS_CONFIG["producer_port"],
        help=f"Prometheus 指标端口（默认: {METRICS_CONFIG['producer_port']}）",
    )
    args = parser.parse_args()

    inject = not args.no_anomalies

    # 启动 Prometheus HTTP 服务器
    start_http_server(args.metrics_port)
    logger.info(f"📊 Prometheus 指标已暴露: http://localhost:{args.metrics_port}/metrics")

    # 启动生成器
    generator = TradeGenerator(
        inject_anomalies=inject,
        anomaly_rate=args.anomaly_rate,
    )
    generator.run(rate=args.rate, duration=args.duration)


if __name__ == "__main__":
    main()
