"""
market_prices.py — 模拟市场价格数据
=====================================
提供各加密资产对的基准市场价格，用于风控检测器的价格异常检测。

设计说明：
  - 生产环境中，市场价格应从实时行情 API（如 CoinGecko、Binance）获取
  - 开发/测试环境使用此文件中的静态基准价格 + 随机波动模拟
  - 价格单位：CAD（加元）
"""

import random
import time
import math

# =============================================================================
# 基准价格（CAD，参考 2024 年 Q1 市场均价）
# =============================================================================

BASE_PRICES_CAD: dict[str, float] = {
    "BTC-CAD": 85_000.00,   # 比特币
    "ETH-CAD": 4_500.00,    # 以太坊
    "SOL-CAD": 195.00,      # Solana
    "XRP-CAD": 0.82,        # Ripple
    "ADA-CAD": 0.75,        # Cardano
    "DOGE-CAD": 0.22,       # Dogecoin
}

# =============================================================================
# 价格波动模拟（正弦波 + 随机噪声）
# =============================================================================

# 每个资产的波动参数
_VOLATILITY: dict[str, float] = {
    "BTC-CAD": 0.015,    # 1.5% 日波动率
    "ETH-CAD": 0.020,    # 2.0%
    "SOL-CAD": 0.035,    # 3.5%（高波动）
    "XRP-CAD": 0.025,    # 2.5%
    "ADA-CAD": 0.030,    # 3.0%
    "DOGE-CAD": 0.050,   # 5.0%（最高波动）
}

# 程序启动时间（用于计算正弦波相位）
_START_TIME = time.time()


def get_market_price(asset_pair: str) -> float:
    """
    获取指定资产对的当前市场价格（含模拟波动）。

    实现原理：
      - 以 BASE_PRICES_CAD 为基准
      - 叠加正弦波模拟日内价格周期（涨跌循环）
      - 叠加高斯噪声模拟随机扰动
      - 价格始终为正

    Args:
        asset_pair: 资产对名称，例如 "BTC-CAD"

    Returns:
        模拟市场价格（CAD，精确到 4 位小数）

    Raises:
        ValueError: 不支持的资产对
    """
    if asset_pair not in BASE_PRICES_CAD:
        raise ValueError(
            f"不支持的资产对: {asset_pair}。"
            f"支持列表: {list(BASE_PRICES_CAD.keys())}"
        )

    base = BASE_PRICES_CAD[asset_pair]
    vol = _VOLATILITY.get(asset_pair, 0.02)
    elapsed = time.time() - _START_TIME

    # 正弦波：模拟 1 小时为一个周期的价格波动
    cycle_period = 3600  # 1 小时
    sine_component = math.sin(2 * math.pi * elapsed / cycle_period) * vol * base * 0.5

    # 高斯噪声：标准差 = 波动率 × 基准价
    noise = random.gauss(0, vol * base * 0.3)

    price = base + sine_component + noise

    # 确保价格为正（极端情况保护）
    price = max(price, base * 0.5)

    return round(price, 4)


def get_all_prices() -> dict[str, float]:
    """获取所有资产对的当前市场价格。"""
    return {pair: get_market_price(pair) for pair in BASE_PRICES_CAD}


def get_price_with_anomaly(asset_pair: str, anomaly_pct: float = 0.08) -> float:
    """
    生成带价格异常的测试价格（用于注入测试数据）。

    Args:
        asset_pair: 资产对名称
        anomaly_pct: 偏离幅度（默认 8%，超过 5% 风控阈值）

    Returns:
        异常价格（偏离市场价 anomaly_pct）
    """
    market_price = get_market_price(asset_pair)
    # 随机选择偏高或偏低
    direction = random.choice([1, -1])
    anomaly_price = market_price * (1 + direction * anomaly_pct)
    return round(max(anomaly_price, 0.0001), 4)


# =============================================================================
# 命令行工具：直接运行查看当前价格
# =============================================================================

if __name__ == "__main__":
    print("=" * 55)
    print("  RiskGuard 模拟市场价格（CAD）")
    print("=" * 55)

    prices = get_all_prices()
    for pair, price in prices.items():
        bar = "█" * min(int(price / BASE_PRICES_CAD[pair] * 20), 40)
        change_pct = (price - BASE_PRICES_CAD[pair]) / BASE_PRICES_CAD[pair] * 100
        sign = "+" if change_pct >= 0 else ""
        print(f"  {pair:<10}  CAD {price:>12,.4f}  ({sign}{change_pct:.2f}%)")

    print("=" * 55)
    print(f"  更新时间: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 55)
