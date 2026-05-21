"""
test_risk_rules.py — RiskGuard 风控规则单元测试
================================================
使用 pytest 测试三条风控规则，无需真实 Kafka 连接。

运行方式：
  pytest tests/test_risk_rules.py -v
  pytest tests/test_risk_rules.py -v --tb=short
  pytest tests/test_risk_rules.py -v -k "test_large_trade"
"""

import time
import uuid
import sys
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

# 将项目根目录加入路径
sys.path.insert(0, str(Path(__file__).parent.parent))

# 我们直接导入规则引擎（不涉及 Kafka/Schema Registry）
from consumer.risk_detector import RiskRuleEngine, FrequencyWindow


# =============================================================================
# Fixtures（测试夹具）
# =============================================================================

@pytest.fixture
def default_rules() -> dict:
    """标准风控规则配置。"""
    return {
        "large_trade_threshold_cad": 50_000.0,
        "frequency_window_seconds": 60,
        "frequency_max_trades": 5,
        "price_deviation_pct": 5.0,
    }


@pytest.fixture
def engine(default_rules) -> RiskRuleEngine:
    """初始化风控规则引擎（mock 市场价格获取）。"""
    with patch("consumer.risk_detector.get_market_price", return_value=85_000.0):
        eng = RiskRuleEngine(default_rules)
    return eng


def make_trade(
    account_id: str = "ACC-000001",
    asset_pair: str = "BTC-CAD",
    side: str = "BUY",
    quantity: float = 0.1,
    price_cad: float = 85_000.0,
    total_value_cad: float = 8_500.0,
    market_price_cad: float = 85_000.0,
    timestamp_ms: int = None,
) -> dict:
    """构造测试用交易数据。"""
    return {
        "trade_id": str(uuid.uuid4()),
        "account_id": account_id,
        "asset_pair": asset_pair,
        "side": side,
        "quantity": quantity,
        "price_cad": price_cad,
        "total_value_cad": total_value_cad,
        "market_price_cad": market_price_cad,
        "timestamp_ms": timestamp_ms or int(time.time() * 1000),
        "exchange_id": "RISKGUARD-EXCHANGE",
        "ip_address": "192.168.1.1",
        "user_agent": "test-client",
        "metadata": {},
    }


# =============================================================================
# 测试套件 1：大额交易检测（规则1）
# =============================================================================

class TestLargeTradeDetection:
    """规则1：大额交易告警（> CAD $50,000）。"""

    def test_large_trade_triggers_alert(self, engine):
        """超过阈值的交易应触发告警。"""
        trade = make_trade(total_value_cad=75_000.0)
        alerts = engine.check(trade)
        assert len(alerts) == 1
        assert alerts[0]["alert_type"] == "LARGE_TRADE"

    def test_large_trade_severity_high(self, engine):
        """75,000 < 200,000 → 严重程度应为 HIGH。"""
        trade = make_trade(total_value_cad=75_000.0)
        alerts = engine.check(trade)
        assert alerts[0]["severity"] == "HIGH"

    def test_large_trade_severity_critical(self, engine):
        """超过 200,000 → 严重程度应为 CRITICAL。"""
        trade = make_trade(total_value_cad=250_000.0)
        alerts = engine.check(trade)
        # 单规则触发，severity = CRITICAL
        assert alerts[0]["severity"] == "CRITICAL"

    def test_large_trade_exactly_at_threshold_no_alert(self, engine):
        """恰好等于阈值（50,000）不应触发告警（规则是 >，非 >=）。"""
        trade = make_trade(total_value_cad=50_000.0)
        alerts = engine.check(trade)
        # 无大额告警（50000 <= 50000）
        assert not any(a["alert_type"] == "LARGE_TRADE" for a in alerts)

    def test_large_trade_below_threshold_no_alert(self, engine):
        """低于阈值的正常交易不应触发告警。"""
        trade = make_trade(total_value_cad=1_000.0)
        alerts = engine.check(trade)
        assert not any(a["alert_type"] == "LARGE_TRADE" for a in alerts)

    def test_large_trade_alert_contains_account_id(self, engine):
        """告警应包含正确的账户 ID。"""
        trade = make_trade(account_id="ACC-999999", total_value_cad=60_000.0)
        alerts = engine.check(trade)
        assert alerts[0]["account_id"] == "ACC-999999"

    def test_large_trade_rule_details(self, engine):
        """告警 rule_details 应包含阈值和实际金额。"""
        trade = make_trade(total_value_cad=80_000.0)
        alerts = engine.check(trade)
        details = alerts[0]["rule_details"]
        assert "threshold_cad" in details
        assert "actual_cad" in details
        assert "excess_cad" in details
        assert float(details["excess_cad"]) == pytest.approx(30_000.0)

    def test_large_trade_recommended_action_review(self, engine):
        """75,000 → 建议动作应为 REVIEW。"""
        trade = make_trade(total_value_cad=75_000.0)
        alerts = engine.check(trade)
        assert alerts[0]["recommended_action"] == "REVIEW"

    def test_large_trade_recommended_action_block(self, engine):
        """超过 200,000 → 建议动作应为 BLOCK_TRADE。"""
        trade = make_trade(total_value_cad=300_000.0)
        alerts = engine.check(trade)
        assert alerts[0]["recommended_action"] == "BLOCK_TRADE"


# =============================================================================
# 测试套件 2：高频交易检测（规则2）
# =============================================================================

class TestFrequencyAlert:
    """规则2：高频交易告警（60s 内 > 5 笔）。"""

    def test_frequency_alert_triggers_on_sixth_trade(self, engine):
        """第 6 笔交易应触发频率告警。"""
        account = "ACC-FREQ-001"
        now_ms = int(time.time() * 1000)

        # 发送 5 笔（不应触发）
        for i in range(5):
            trade = make_trade(account_id=account, timestamp_ms=now_ms - (4 - i) * 5000)
            alerts = engine.check(trade)
            freq_alerts = [a for a in alerts if a["alert_type"] == "HIGH_FREQUENCY"]
            assert len(freq_alerts) == 0, f"第 {i+1} 笔不应触发频率告警"

        # 第 6 笔（应触发）
        trade = make_trade(account_id=account, timestamp_ms=now_ms)
        alerts = engine.check(trade)
        freq_alerts = [a for a in alerts if a["alert_type"] == "HIGH_FREQUENCY"]
        assert len(freq_alerts) == 1

    def test_frequency_window_resets_after_expiry(self, engine):
        """窗口过期后，计数应重置，不再触发告警。"""
        account = "ACC-FREQ-002"
        past_ms = int(time.time() * 1000) - 120_000  # 2 分钟前（超出 60s 窗口）

        # 在 2 分钟前发送 6 笔
        for i in range(6):
            trade = make_trade(account_id=account, timestamp_ms=past_ms + i * 1000)
            engine.check(trade)

        # 当前发送 1 笔（旧记录应已过期）
        now_ms = int(time.time() * 1000)
        trade = make_trade(account_id=account, timestamp_ms=now_ms)
        alerts = engine.check(trade)
        freq_alerts = [a for a in alerts if a["alert_type"] == "HIGH_FREQUENCY"]
        assert len(freq_alerts) == 0, "过期记录不应影响新窗口"

    def test_frequency_independent_per_account(self, engine):
        """不同账户的频率计数应相互独立。"""
        now_ms = int(time.time() * 1000)

        # 账户 A 发 6 笔
        for i in range(6):
            trade = make_trade(account_id="ACC-A", timestamp_ms=now_ms - i * 1000)
            engine.check(trade)

        # 账户 B 只发 1 笔，不应触发
        trade = make_trade(account_id="ACC-B", timestamp_ms=now_ms)
        alerts = engine.check(trade)
        freq_alerts = [a for a in alerts if a["alert_type"] == "HIGH_FREQUENCY"]
        assert len(freq_alerts) == 0

    def test_frequency_alert_contains_count(self, engine):
        """告警 rule_details 应包含实际交易笔数。"""
        account = "ACC-FREQ-003"
        now_ms = int(time.time() * 1000)

        for i in range(6):
            trade = make_trade(account_id=account, timestamp_ms=now_ms - i * 2000)
            alerts = engine.check(trade)

        freq_alerts = [a for a in alerts if a["alert_type"] == "HIGH_FREQUENCY"]
        assert "actual_count" in freq_alerts[0]["rule_details"]


# =============================================================================
# 测试套件 3：价格异常检测（规则3）
# =============================================================================

class TestPriceAnomalyDetection:
    """规则3：价格异常告警（偏离市场价 > 5%）。"""

    def test_price_anomaly_above_threshold_triggers_alert(self, engine):
        """成交价高于市场价 8% → 应触发告警。"""
        market_price = 85_000.0
        anomaly_price = market_price * 1.08  # 偏离 +8%
        trade = make_trade(
            price_cad=anomaly_price,
            market_price_cad=market_price,
            total_value_cad=anomaly_price * 0.1,
        )
        with patch("consumer.risk_detector.get_market_price", return_value=market_price):
            alerts = engine.check(trade)
        price_alerts = [a for a in alerts if a["alert_type"] == "PRICE_ANOMALY"]
        assert len(price_alerts) == 1

    def test_price_anomaly_below_threshold_triggers_alert(self, engine):
        """成交价低于市场价 7% → 应触发告警。"""
        market_price = 85_000.0
        anomaly_price = market_price * 0.93  # 偏离 -7%
        trade = make_trade(
            price_cad=anomaly_price,
            market_price_cad=market_price,
            total_value_cad=anomaly_price * 0.1,
        )
        with patch("consumer.risk_detector.get_market_price", return_value=market_price):
            alerts = engine.check(trade)
        price_alerts = [a for a in alerts if a["alert_type"] == "PRICE_ANOMALY"]
        assert len(price_alerts) == 1

    def test_normal_price_no_alert(self, engine):
        """偏离 2%（< 5% 阈值）→ 不应触发告警。"""
        market_price = 85_000.0
        normal_price = market_price * 1.02
        trade = make_trade(
            price_cad=normal_price,
            market_price_cad=market_price,
            total_value_cad=normal_price * 0.1,
        )
        with patch("consumer.risk_detector.get_market_price", return_value=market_price):
            alerts = engine.check(trade)
        price_alerts = [a for a in alerts if a["alert_type"] == "PRICE_ANOMALY"]
        assert len(price_alerts) == 0

    def test_price_anomaly_severity_medium_for_small_deviation(self, engine):
        """偏离 7%（< 10%）→ 严重程度应为 MEDIUM。"""
        market_price = 85_000.0
        trade = make_trade(
            price_cad=market_price * 1.07,
            market_price_cad=market_price,
        )
        with patch("consumer.risk_detector.get_market_price", return_value=market_price):
            alerts = engine.check(trade)
        price_alerts = [a for a in alerts if a["alert_type"] == "PRICE_ANOMALY"]
        assert price_alerts[0]["severity"] == "MEDIUM"

    def test_price_anomaly_severity_high_for_large_deviation(self, engine):
        """偏离 15%（> 10%）→ 严重程度应为 HIGH。"""
        market_price = 85_000.0
        trade = make_trade(
            price_cad=market_price * 1.15,
            market_price_cad=market_price,
        )
        with patch("consumer.risk_detector.get_market_price", return_value=market_price):
            alerts = engine.check(trade)
        price_alerts = [a for a in alerts if a["alert_type"] == "PRICE_ANOMALY"]
        assert price_alerts[0]["severity"] == "HIGH"

    def test_price_anomaly_uses_market_price_from_trade(self, engine):
        """应优先使用 trade 中的 market_price_cad（而非实时获取）。"""
        market_price = 4_500.0  # ETH-CAD
        trade = make_trade(
            asset_pair="ETH-CAD",
            price_cad=market_price * 1.1,  # 偏离 10%
            market_price_cad=market_price,
        )
        # 即使 mock 返回不同的价格，应使用 trade 中的 market_price_cad
        with patch("consumer.risk_detector.get_market_price", return_value=999_999):
            alerts = engine.check(trade)
        price_alerts = [a for a in alerts if a["alert_type"] == "PRICE_ANOMALY"]
        assert len(price_alerts) == 1


# =============================================================================
# 测试套件 4：正常交易无告警
# =============================================================================

class TestNormalTradeNoAlert:
    """正常交易不应触发任何告警。"""

    def test_normal_small_trade(self, engine):
        """小额正常交易：无告警。"""
        trade = make_trade(
            total_value_cad=500.0,
            price_cad=85_000.0,
            market_price_cad=85_000.0,
        )
        with patch("consumer.risk_detector.get_market_price", return_value=85_000.0):
            alerts = engine.check(trade)
        assert len(alerts) == 0

    def test_normal_medium_trade(self, engine):
        """中等金额正常交易（$10,000）：无告警。"""
        trade = make_trade(
            total_value_cad=10_000.0,
            price_cad=85_000.0,
            market_price_cad=85_000.0,
        )
        with patch("consumer.risk_detector.get_market_price", return_value=85_000.0):
            alerts = engine.check(trade)
        assert len(alerts) == 0

    def test_different_assets_no_interference(self, engine):
        """不同资产对的交易不应相互干扰。"""
        assets = ["BTC-CAD", "ETH-CAD", "SOL-CAD"]
        for asset in assets:
            trade = make_trade(
                asset_pair=asset,
                total_value_cad=5_000.0,
                price_cad=100.0,
                market_price_cad=100.0,
            )
            with patch("consumer.risk_detector.get_market_price", return_value=100.0):
                alerts = engine.check(trade)
            assert len(alerts) == 0, f"资产 {asset} 不应触发告警"

    def test_sell_trade_normal(self, engine):
        """正常卖出交易：无告警。"""
        trade = make_trade(
            side="SELL",
            total_value_cad=8_000.0,
            price_cad=85_000.0,
            market_price_cad=85_000.0,
        )
        with patch("consumer.risk_detector.get_market_price", return_value=85_000.0):
            alerts = engine.check(trade)
        assert len(alerts) == 0


# =============================================================================
# 测试套件 5：多规则同时触发
# =============================================================================

class TestMultiRuleTrigger:
    """多条规则同时触发时应升级为 MULTI_RULE CRITICAL。"""

    def test_large_trade_and_price_anomaly_combined(self, engine):
        """同时触发大额 + 价格异常 → MULTI_RULE CRITICAL。"""
        market_price = 85_000.0
        trade = make_trade(
            total_value_cad=150_000.0,  # 大额
            price_cad=market_price * 1.10,  # 价格偏离 10%
            market_price_cad=market_price,
        )
        with patch("consumer.risk_detector.get_market_price", return_value=market_price):
            alerts = engine.check(trade)

        assert len(alerts) == 1
        assert alerts[0]["alert_type"] == "MULTI_RULE"
        assert alerts[0]["severity"] == "CRITICAL"
        assert alerts[0]["recommended_action"] == "FREEZE_ACCOUNT"


# =============================================================================
# 测试套件 6：频率窗口内部逻辑
# =============================================================================

class TestFrequencyWindow:
    """FrequencyWindow 滑动窗口实现的单元测试。"""

    def test_deque_o1_operations(self):
        """验证 deque 的 O(1) 操作（内部使用正确的数据结构）。"""
        from collections import deque
        window = FrequencyWindow(window_seconds=60, max_trades=5)
        # 应使用 deque，而非 list
        account = "test-acc"
        window.add_and_check(account, int(time.time() * 1000))
        assert isinstance(window._windows[account], deque)

    def test_cleanup_removes_inactive_accounts(self):
        """清理函数应删除长时间无活动的账户。"""
        window = FrequencyWindow(window_seconds=10, max_trades=5)
        old_time_ms = int((time.time() - 100) * 1000)  # 100 秒前
        window.add_and_check("ACC-OLD", old_time_ms)
        assert window.tracked_accounts == 1

        removed = window.cleanup_stale_accounts(time.time())
        assert removed == 1
        assert window.tracked_accounts == 0

    def test_window_count_accuracy(self):
        """窗口计数应准确反映时间窗口内的交易数。"""
        window = FrequencyWindow(window_seconds=60, max_trades=5)
        now_ms = int(time.time() * 1000)

        for i in range(3):
            triggered, count = window.add_and_check("ACC-X", now_ms - i * 10_000)

        # 第 3 次后计数应为 3
        assert count == 3
        assert not triggered
