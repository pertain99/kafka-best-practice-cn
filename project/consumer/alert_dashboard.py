"""
alert_dashboard.py — RiskGuard 实时告警仪表盘（控制台）
=========================================================
消费 risk.alerts Topic，实时展示风险告警信息。

功能：
  - 彩色控制台输出（使用 ANSI 转义码，无额外依赖）
  - 统计：总告警数、各类型分布、各严重级别分布
  - 实时展示最近 10 条告警
  - 每秒自动刷新
  - 可选 rich 库（更美观的表格），回退到 ANSI 原生实现

运行方式：
  python consumer/alert_dashboard.py
  python consumer/alert_dashboard.py --no-clear  # 不清屏（适合日志模式）
"""

import argparse
import signal
import sys
import time
import logging
from collections import defaultdict, deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).parent.parent))

from confluent_kafka import Consumer, KafkaError
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField

from config.kafka_config import (
    DASHBOARD_CONSUMER_CONFIG,
    SCHEMA_REGISTRY_CONFIG,
    TOPICS,
)

# 尝试导入 rich（可选依赖）
try:
    from rich.console import Console
    from rich.table import Table
    from rich.live import Live
    from rich.panel import Panel
    from rich.columns import Columns
    from rich import box
    HAS_RICH = True
except ImportError:
    HAS_RICH = False

logging.basicConfig(
    level=logging.WARNING,  # 仪表盘模式下减少日志噪声
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("alert-dashboard")

# =============================================================================
# ANSI 颜色常量
# =============================================================================

class Color:
    RESET   = "\033[0m"
    BOLD    = "\033[1m"
    RED     = "\033[91m"
    YELLOW  = "\033[93m"
    GREEN   = "\033[92m"
    BLUE    = "\033[94m"
    MAGENTA = "\033[95m"
    CYAN    = "\033[96m"
    WHITE   = "\033[97m"
    GRAY    = "\033[90m"
    BG_RED  = "\033[41m"
    BG_DARK = "\033[40m"

SEVERITY_COLORS = {
    "CRITICAL": Color.BG_RED + Color.WHITE + Color.BOLD,
    "HIGH":     Color.RED + Color.BOLD,
    "MEDIUM":   Color.YELLOW + Color.BOLD,
    "LOW":      Color.GREEN,
}

ALERT_TYPE_ICONS = {
    "LARGE_TRADE":    "💰",
    "HIGH_FREQUENCY": "⚡",
    "PRICE_ANOMALY":  "📉",
    "MULTI_RULE":     "🚨",
}

SEVERITY_ICONS = {
    "CRITICAL": "🔴",
    "HIGH":     "🟠",
    "MEDIUM":   "🟡",
    "LOW":      "🟢",
}


# =============================================================================
# 统计数据模型
# =============================================================================

class AlertStats:
    """告警统计数据。"""

    def __init__(self, max_recent: int = 10):
        self.total = 0
        self.by_type: dict[str, int] = defaultdict(int)
        self.by_severity: dict[str, int] = defaultdict(int)
        self.by_account: dict[str, int] = defaultdict(int)
        self.recent: deque = deque(maxlen=max_recent)
        self.start_time = time.time()

    def add(self, alert: dict) -> None:
        self.total += 1
        self.by_type[alert.get("alert_type", "UNKNOWN")] += 1
        self.by_severity[alert.get("severity", "UNKNOWN")] += 1
        self.by_account[alert.get("account_id", "UNKNOWN")] += 1
        self.recent.append(alert)

    @property
    def uptime_str(self) -> str:
        elapsed = int(time.time() - self.start_time)
        h, m, s = elapsed // 3600, (elapsed % 3600) // 60, elapsed % 60
        return f"{h:02d}:{m:02d}:{s:02d}"

    @property
    def alerts_per_minute(self) -> float:
        elapsed = max(time.time() - self.start_time, 1)
        return self.total / elapsed * 60

    def top_accounts(self, n: int = 3) -> list[tuple[str, int]]:
        return sorted(self.by_account.items(), key=lambda x: -x[1])[:n]


# =============================================================================
# ANSI 原生仪表盘渲染器
# =============================================================================

class AnsiDashboard:
    """
    纯 ANSI 转义码仪表盘渲染器（无外部依赖）。
    支持所有终端。
    """

    def __init__(self, clear_screen: bool = True):
        self.clear_screen = clear_screen

    def render(self, stats: AlertStats) -> None:
        if self.clear_screen:
            print("\033[2J\033[H", end="")  # 清屏 + 光标归位

        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        width = 72

        # ── 标题栏 ──────────────────────────────────────────────────────────
        print(f"{Color.CYAN}{Color.BOLD}{'=' * width}")
        print(f"  🛡️  RiskGuard — 实时风险告警仪表盘   {now:>28}")
        print(f"{'=' * width}{Color.RESET}")

        # ── 汇总统计 ─────────────────────────────────────────────────────────
        print(f"\n{Color.BOLD}  📊 汇总统计{Color.RESET}")
        print(f"  {'─' * (width - 4)}")
        print(
            f"  总告警数: {Color.BOLD}{Color.RED}{stats.total:>6}{Color.RESET}   "
            f"运行时长: {Color.CYAN}{stats.uptime_str}{Color.RESET}   "
            f"速率: {Color.YELLOW}{stats.alerts_per_minute:.1f} 条/分钟{Color.RESET}"
        )

        # ── 按类型统计 ────────────────────────────────────────────────────────
        print(f"\n{Color.BOLD}  🏷  按告警类型{Color.RESET}")
        for atype in ["LARGE_TRADE", "HIGH_FREQUENCY", "PRICE_ANOMALY", "MULTI_RULE"]:
            count = stats.by_type.get(atype, 0)
            icon = ALERT_TYPE_ICONS.get(atype, "❓")
            bar_len = min(count, 30)
            bar = "█" * bar_len + "░" * (30 - bar_len)
            print(f"    {icon} {atype:<18}  {Color.BLUE}{bar}{Color.RESET}  {count:>4}")

        # ── 按严重程度统计 ────────────────────────────────────────────────────
        print(f"\n{Color.BOLD}  🔥 按严重程度{Color.RESET}")
        for sev in ["CRITICAL", "HIGH", "MEDIUM", "LOW"]:
            count = stats.by_severity.get(sev, 0)
            icon = SEVERITY_ICONS.get(sev, "⚪")
            color = SEVERITY_COLORS.get(sev, Color.WHITE)
            print(f"    {icon} {color}{sev:<10}{Color.RESET}  {count:>4} 条")

        # ── 高危账户 Top 3 ────────────────────────────────────────────────────
        top = stats.top_accounts(3)
        if top:
            print(f"\n{Color.BOLD}  👤 高危账户 Top {min(3, len(top))}{Color.RESET}")
            for rank, (account, count) in enumerate(top, 1):
                medal = ["🥇", "🥈", "🥉"][rank - 1]
                print(f"    {medal} {account}  →  {Color.RED}{count} 次告警{Color.RESET}")

        # ── 最近告警列表 ──────────────────────────────────────────────────────
        print(f"\n{Color.BOLD}  📋 最近 {min(len(stats.recent), 10)} 条告警{Color.RESET}")
        print(f"  {'─' * (width - 4)}")

        if not stats.recent:
            print(f"  {Color.GRAY}  暂无告警...{Color.RESET}")
        else:
            for alert in reversed(list(stats.recent)):
                ts_ms = alert.get("alert_timestamp_ms", 0)
                ts_str = datetime.fromtimestamp(ts_ms / 1000).strftime("%H:%M:%S")
                sev = alert.get("severity", "UNKNOWN")
                atype = alert.get("alert_type", "UNKNOWN")
                account = alert.get("account_id", "UNKNOWN")
                asset = alert.get("asset_pair", "UNKNOWN")
                value = alert.get("trade_total_value_cad", 0)
                icon = ALERT_TYPE_ICONS.get(atype, "❓")
                sev_icon = SEVERITY_ICONS.get(sev, "⚪")
                color = SEVERITY_COLORS.get(sev, Color.WHITE)

                print(
                    f"  {Color.GRAY}{ts_str}{Color.RESET} "
                    f"{sev_icon} {color}{sev:<10}{Color.RESET} "
                    f"{icon} {atype:<18} "
                    f"{Color.CYAN}{account}{Color.RESET} "
                    f"{asset:<10} "
                    f"{Color.YELLOW}CAD {value:>12,.2f}{Color.RESET}"
                )

        print(f"\n{Color.GRAY}  按 Ctrl+C 退出 | 每秒自动刷新{Color.RESET}")
        print(f"{Color.CYAN}{'─' * width}{Color.RESET}")


# =============================================================================
# Rich 仪表盘渲染器（可选，更美观）
# =============================================================================

class RichDashboard:
    """基于 rich 库的高级仪表盘渲染器。"""

    def __init__(self):
        self.console = Console()

    def build_table(self, stats: AlertStats) -> Table:
        """构建最近告警表格。"""
        table = Table(
            title="📋 最近 10 条告警",
            box=box.ROUNDED,
            show_header=True,
            header_style="bold cyan",
        )
        table.add_column("时间", style="dim", width=10)
        table.add_column("严重程度", width=10)
        table.add_column("类型", width=18)
        table.add_column("账户", width=14)
        table.add_column("资产对", width=10)
        table.add_column("金额 (CAD)", justify="right", width=16)

        severity_styles = {
            "CRITICAL": "bold red on red",
            "HIGH":     "bold red",
            "MEDIUM":   "bold yellow",
            "LOW":      "green",
        }

        for alert in reversed(list(stats.recent)):
            ts_ms = alert.get("alert_timestamp_ms", 0)
            ts_str = datetime.fromtimestamp(ts_ms / 1000).strftime("%H:%M:%S")
            sev = alert.get("severity", "?")
            atype = alert.get("alert_type", "?")
            account = alert.get("account_id", "?")
            asset = alert.get("asset_pair", "?")
            value = alert.get("trade_total_value_cad", 0)
            icon = ALERT_TYPE_ICONS.get(atype, "❓")
            sev_icon = SEVERITY_ICONS.get(sev, "⚪")

            table.add_row(
                ts_str,
                f"{sev_icon} {sev}",
                f"{icon} {atype}",
                account,
                asset,
                f"$ {value:>12,.2f}",
                style=severity_styles.get(sev, ""),
            )

        return table

    def render(self, stats: AlertStats) -> None:
        self.console.clear()
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        self.console.print(
            Panel(
                f"[bold cyan]🛡️  RiskGuard 实时风险告警仪表盘[/bold cyan]   "
                f"[dim]{now}[/dim]   "
                f"[yellow]运行 {stats.uptime_str}[/yellow]",
                box=box.DOUBLE,
            )
        )

        # 汇总
        self.console.print(
            f"[bold]总告警: [red]{stats.total}[/red]  "
            f"速率: [yellow]{stats.alerts_per_minute:.1f}/min[/yellow][/bold]"
        )

        # 告警表格
        table = self.build_table(stats)
        self.console.print(table)

        self.console.print("[dim]按 Ctrl+C 退出 | 每秒自动刷新[/dim]")


# =============================================================================
# 告警仪表盘主类
# =============================================================================

class AlertDashboard:
    """实时告警消费者 + 仪表盘展示。"""

    def __init__(self, clear_screen: bool = True, use_rich: bool = True):
        self._running = True
        self._stats = AlertStats(max_recent=10)

        # 选择渲染器
        if use_rich and HAS_RICH:
            self._renderer = RichDashboard()
            logger.warning("使用 rich 渲染器")
        else:
            self._renderer = AnsiDashboard(clear_screen=clear_screen)
            if use_rich and not HAS_RICH:
                print("提示：安装 rich 可获得更美观的界面：pip install rich")

        # 初始化 Schema Registry
        schema_registry = SchemaRegistryClient(SCHEMA_REGISTRY_CONFIG)
        schema_path = Path(__file__).parent.parent / "config/schemas/risk_alert.avsc"
        with open(schema_path) as f:
            alert_schema_str = f.read()

        self._deserializer = AvroDeserializer(schema_registry, alert_schema_str)
        self._consumer = Consumer(DASHBOARD_CONSUMER_CONFIG)
        self._consumer.subscribe([TOPICS["risk_alerts"]])

        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, signum, frame):
        print(f"\n\n👋 关闭仪表盘...")
        self._running = False

    def run(self, refresh_interval: float = 1.0) -> None:
        """主循环：消费告警并定时刷新仪表盘。"""
        last_render = 0

        try:
            while self._running:
                # 非阻塞 poll
                msg = self._consumer.poll(timeout=0.1)

                if msg is not None and not msg.error():
                    try:
                        alert = self._deserializer(
                            msg.value(),
                            SerializationContext(msg.topic(), MessageField.VALUE),
                        )
                        self._stats.add(alert)
                        # 仪表盘模式下不手动提交（自动提交 latest offset）
                    except Exception as e:
                        logger.error(f"反序列化失败: {e}")

                # 定时刷新界面
                now = time.time()
                if now - last_render >= refresh_interval:
                    self._renderer.render(self._stats)
                    last_render = now

        finally:
            self._consumer.close()
            print(f"\n📊 本次会话共展示 {self._stats.total} 条告警")


# =============================================================================
# 主程序入口
# =============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="RiskGuard — 实时告警仪表盘",
    )
    parser.add_argument(
        "--no-clear",
        action="store_true",
        help="禁用清屏（适合日志/管道模式）",
    )
    parser.add_argument(
        "--no-rich",
        action="store_true",
        help="强制使用 ANSI 模式（即使 rich 可用）",
    )
    parser.add_argument(
        "--refresh",
        type=float,
        default=1.0,
        help="刷新间隔（秒，默认: 1.0）",
    )
    args = parser.parse_args()

    dashboard = AlertDashboard(
        clear_screen=not args.no_clear,
        use_rich=not args.no_rich,
    )
    dashboard.run(refresh_interval=args.refresh)


if __name__ == "__main__":
    main()
