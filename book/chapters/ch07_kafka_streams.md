# 第 7 章：Kafka Streams 流处理

## 本章你将学到

- 流处理与批处理的本质区别，以及如何选型
- Kafka Streams 的核心概念：KStream、KTable、Topology、State Store
- 用 Python faust 库实现生产级流处理应用
- 四种窗口类型的原理与适用场景
- Stream-Table Join（流表关联）实现实时数据丰富
- Exactly-Once 语义在 Streams 中的落地方案
- 动手实现交易量滚动统计系统

---

## 7.1 流处理 vs 批处理：什么时候用 Kafka Streams？

### 7.1.1 两种计算范式的本质

批处理（Batch Processing）和流处理（Stream Processing）的根本区别不在于数据量，而在于**时间维度**。

```
批处理思维：
  数据是静止的 → 等数据积累 → 触发计算 → 得到结果
  
  [数据积累区] --（每小时）--> [计算引擎] --> [结果]
  
流处理思维：
  数据是流动的 → 数据到达即计算 → 结果持续更新
  
  [数据流] --> [计算引擎（常驻）] --> [实时结果]
```

**一个直观的类比**：

批处理像银行月结账单——月底汇总所有交易，生成报告。准确、完整，但你要等一个月才能看到。

流处理像实时银行余额显示——每笔交易后立即更新。你刷卡的瞬间余额就变了。

### 7.1.2 框架选型指南

在 Kafka Streams、Apache Spark Streaming、Apache Flink 之间如何选择？

| 维度 | Kafka Streams | Apache Flink | Spark Structured Streaming |
|------|--------------|--------------|---------------------------|
| **部署复杂度** | 极低（嵌入应用） | 高（独立集群） | 中（需要 Spark 集群） |
| **状态管理** | RocksDB，本地 | 远程状态，强大 | 内存/外部存储 |
| **延迟** | 毫秒级 | 毫秒级 | 秒级（微批） |
| **吞吐量** | 中高 | 极高 | 极高 |
| **学习曲线** | 低 | 高 | 中 |
| **数据源** | 仅 Kafka | 多种 | 多种 |
| **语言** | Java/Scala（Python via faust） | Java/Python/SQL | Python/Scala/SQL |

**选 Kafka Streams / faust 的场景**：
- 数据源就是 Kafka，不需要其他数据源
- 应用规模中等，不需要独立流处理集群
- 希望流处理逻辑嵌入微服务，随服务一起部署
- 团队熟悉 Python，想快速上手

**选 Flink 的场景**：
- 需要复杂的事件时间处理和精确的水位线（Watermark）控制
- 超高吞吐（每秒百万级消息）
- 需要有状态的 exactly-once，且状态超大（TB 级）

**选 Spark Streaming 的场景**：
- 团队已有 Spark 基础设施
- 流批统一处理（同一套代码处理流数据和历史数据）
- 与 Hive、Delta Lake 深度集成

> **本书的选择**：我们使用 **faust**（Python 的 Kafka Streams 实现），原因是 John 的技术栈以 Python 为主，faust 的 API 设计优雅，适合快速落地。

### 7.1.3 实时交易风控：为什么批处理不够用

```
场景：检测同一账户 5 分钟内超过 3 笔交易（疑似盗刷）

批处理方案的问题：
  - 每 5 分钟跑一次 Spark Job
  - 检测到风险时，账户已被盗刷 5 分钟了
  - 延迟 = 处理时间 + 批次等待时间 = 5min + 数分钟 = 太晚了

流处理方案：
  - 每笔交易到达时立即检查窗口内计数
  - 第 4 笔交易触达时，毫秒级告警
  - 实时冻结账户，损失最小化
```

这就是我们这章要构建的系统。

---

## 7.2 Kafka Streams 核心概念

### 7.2.1 KStream：无界的事件流

KStream（流，Stream）表示一系列**无界的、仅追加的**事件记录。每条记录代表一个独立事件。

```
KStream 示意：
时间轴 ──────────────────────────────────────────→
       │                │              │
    交易A            交易B           交易C
  (acc_001,100)   (acc_002,50)   (acc_001,200)

特点：
  - 每条记录都是新事件，不会替换旧记录
  - 同一个 key 可以出现多次（账户 acc_001 出现了两次）
  - 流是无限的，永远不会"结束"
```

**类比**：KStream 就像银行的流水账——每一笔交易都记录下来，历史记录永不删除。

### 7.2.2 KTable：变化日志，当前状态

KTable（表，Table）表示每个 key 的**最新状态**。当同一个 key 出现新记录时，旧记录被替换。

```
KTable 示意（用户账户余额）：
时间轴 ──────────────────────────────────────────→
       acc_001=1000    acc_001=1100    acc_001=900
       acc_002=500     acc_002=500     acc_002=700

KTable 的当前状态（快照）：
  时刻1: {acc_001: 1000, acc_002: 500}
  时刻2: {acc_001: 1100, acc_002: 500}  ← acc_001 更新了
  时刻3: {acc_001: 900,  acc_002: 700}  ← 两个都更新了
```

**类比**：KTable 就像数据库中的用户表——记录的是每个用户的当前状态，不保留历史。

### 7.2.3 KStream vs KTable 的选择

```
交易事件（每笔都重要，不能覆盖）     → KStream
用户余额（只关心最新值）             → KTable
订单状态变更（每次变更都是事件）     → KStream
商品库存（只关心当前库存数量）       → KTable
用户行为日志（每次点击都要分析）     → KStream
用户画像（只关心最新画像）           → KTable
```

### 7.2.4 Topology：处理拓扑

Topology（拓扑）是流处理的**计算图**，描述数据如何从 Source（源）经过各种处理节点，流向 Sink（汇）。

```
Topology 示意图：

  [Kafka Topic: raw-trades]
          │
          ▼
    [Source Node]          ← 从 Kafka 读取
          │
          ▼
    [Filter Node]          ← 过滤无效交易
          │
          ▼
    [Map Node]             ← 转换数据格式
          │
          ▼
    [Aggregate Node]       ← 按账户聚合
          │
          ▼
    [Sink Node]            ← 写入 Kafka Topic: trade-stats
```

在 faust 中，Topology 通过 `@app.agent()` 装饰器和流操作链式调用来定义。

### 7.2.5 State Store：状态存储

State Store（状态存储）是流处理的关键——它保存了流处理过程中的**中间状态**（比如窗口内的计数）。

**Kafka Streams** 使用 **RocksDB**（一种嵌入式 KV 数据库，由 Facebook 开源）作为默认状态存储：

```
为什么需要 State Store？

场景：统计每个账户过去 5 分钟的交易次数

方案A（无状态）：每次新交易到达，查数据库看历史
  缺点：高延迟、数据库压力大

方案B（有状态，本地 State Store）：
  - 把过去 5 分钟的计数存在本地 RocksDB
  - 新交易到达：直接在本地读写，微秒级操作
  - 定期把状态变更同步到 Kafka（称为 changelog topic）
  优点：极低延迟，故障后可从 Kafka 恢复状态
```

**faust** 使用自己的 Table 实现状态存储，底层支持 RocksDB 和内存模式。

---

## 7.3 Python 实现流处理（faust 库）

### 7.3.1 安装与环境配置

```bash
# 安装 faust 及依赖
pip install faust-streaming  # faust 的活跃维护版本
pip install aiokafka         # 异步 Kafka 客户端（faust 依赖）

# 验证安装
python -c "import faust; print(faust.__version__)"
```

> **注意**：原始的 `faust` 包已停止维护，请使用 `faust-streaming`（社区维护版本，API 兼容）。

### 7.3.2 faust 应用基础结构

```python
# app_base.py - faust 应用基础结构
import faust
from datetime import datetime

# ============================================================
# 应用初始化
# broker: Kafka 集群地址
# value_serializer: 消息序列化方式（json/raw/pickle）
# ============================================================
app = faust.App(
    'trade-processor',           # 应用名称（也是 Consumer Group ID 的前缀）
    broker='kafka://localhost:9092',
    value_serializer='json',
    # 生产环境建议配置
    producer_max_request_size=1048576,   # 1MB，生产者最大消息大小
    consumer_max_fetch_size=1048576,     # 1MB，消费者单次最大拉取
    processing_guarantee='exactly_once', # 精确一次语义（需要 Kafka ≥ 2.5）
)

# ============================================================
# 定义消息模型（Kafka 消息的结构）
# faust 使用 Record 类来定义和验证消息结构
# ============================================================
class TradeEvent(faust.Record, serializer='json'):
    """交易事件消息模型"""
    trade_id: str          # 交易 ID
    account_id: str        # 账户 ID
    symbol: str            # 交易标的（如 AAPL, BTC-USD）
    quantity: float        # 交易数量
    price: float           # 交易价格
    trade_type: str        # 买入/卖出 (BUY/SELL)
    timestamp: float       # 时间戳（Unix 毫秒）

class TradeStat(faust.Record, serializer='json'):
    """交易统计结果模型"""
    symbol: str            # 交易标的
    count: int             # 交易笔数
    total_volume: float    # 总成交量
    window_start: float    # 窗口开始时间
    window_end: float      # 窗口结束时间

# ============================================================
# 定义 Kafka Topics
# ============================================================
raw_trades_topic = app.topic(
    'raw-trades',
    value_type=TradeEvent,
    partitions=12,         # 分区数（建议 = Broker数 * 2~3）
    retention=604800.0,    # 数据保留时间（秒），7天
)

trade_stats_topic = app.topic(
    'trade-stats',
    value_type=TradeStat,
    partitions=12,
)

trade_alerts_topic = app.topic(
    'trade-alerts',
    partitions=4,
)
```

### 7.3.3 实现一：统计每分钟各资产对的交易笔数

```python
# feature1_per_minute_count.py - 每分钟交易笔数统计
import faust
from datetime import datetime, timezone
import logging

logger = logging.getLogger(__name__)

app = faust.App(
    'trade-minute-counter',
    broker='kafka://localhost:9092',
    value_serializer='json',
)

class TradeEvent(faust.Record, serializer='json'):
    trade_id: str
    account_id: str
    symbol: str
    quantity: float
    price: float
    trade_type: str
    timestamp: float

raw_trades_topic = app.topic('raw-trades', value_type=TradeEvent)

# ============================================================
# State Table：存储每分钟每个 symbol 的交易计数
# 
# Table 的 key 格式："{symbol}:{minute_bucket}"
# 例如："AAPL:2024-01-15T10:30"
#
# default=int 表示不存在的 key 默认值为 0（int() = 0）
# ============================================================
trade_count_table = app.Table(
    'trade-count-per-minute',
    default=int,
    help='每分钟每个交易标的的交易计数',
)

def get_minute_bucket(timestamp_ms: float) -> str:
    """
    将时间戳转换为分钟桶（minute bucket）
    
    Args:
        timestamp_ms: Unix 时间戳（毫秒）
    
    Returns:
        格式为 "YYYY-MM-DDTHH:MM" 的分钟字符串
    
    Example:
        1705312234567 → "2024-01-15T10:30"
    """
    dt = datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)
    return dt.strftime('%Y-%m-%dT%H:%M')

@app.agent(raw_trades_topic)
async def count_trades_per_minute(trades):
    """
    Agent：实时统计每分钟每个资产对的交易笔数
    
    处理逻辑：
    1. 消费 raw-trades topic 中的交易事件
    2. 提取 symbol 和分钟桶
    3. 在 State Table 中累加计数
    4. 记录日志（生产环境可改为发送到 trade-stats topic）
    """
    async for trade in trades:
        # 构造 table key：symbol + 分钟桶
        minute_bucket = get_minute_bucket(trade.timestamp)
        table_key = f"{trade.symbol}:{minute_bucket}"
        
        # 原子性地在状态表中累加计数
        # 注意：faust Table 不是线程安全的，但 agent 是单线程协程，无需加锁
        trade_count_table[table_key] += 1
        
        current_count = trade_count_table[table_key]
        
        logger.info(
            f"[{minute_bucket}] {trade.symbol} 交易笔数: {current_count} "
            f"（本次交易ID: {trade.trade_id}）"
        )
        
        # 每 100 笔交易发送一次统计快照到下游 topic
        if current_count % 100 == 0:
            await trade_stats_topic.send(
                key=trade.symbol,
                value={
                    'symbol': trade.symbol,
                    'count': current_count,
                    'minute_bucket': minute_bucket,
                    'snapshot_type': 'per_100_trades',
                }
            )

# ============================================================
# 定时任务：每分钟整点输出完整统计报告
# @app.timer(interval) 会每隔 interval 秒执行一次
# ============================================================
@app.timer(interval=60.0)
async def report_minute_stats():
    """每分钟输出统计汇总报告"""
    now = datetime.now(tz=timezone.utc)
    # 获取上一分钟的桶（刚刚完成的分钟窗口）
    last_minute = now.strftime('%Y-%m-%dT%H:%M')
    
    stats = {}
    # 遍历 Table，找出上一分钟的所有统计
    for key, count in trade_count_table.items():
        if last_minute in key:
            symbol = key.split(':')[0]
            stats[symbol] = count
    
    if stats:
        logger.info(f"=== 分钟统计报告 [{last_minute}] ===")
        for symbol, count in sorted(stats.items(), key=lambda x: -x[1]):
            logger.info(f"  {symbol}: {count} 笔交易")

if __name__ == '__main__':
    app.main()
```

### 7.3.4 实现二：滑动窗口检测高频交易告警

```python
# feature2_sliding_window_alert.py - 滑动窗口风控告警
import faust
from datetime import datetime, timezone
import asyncio
import logging
from collections import deque
from typing import Deque, List

logger = logging.getLogger(__name__)

app = faust.App(
    'trade-risk-monitor',
    broker='kafka://localhost:9092',
    value_serializer='json',
    # 生产环境开启 exactly-once（需要 Kafka 支持事务）
    # processing_guarantee='exactly_once',
)

class TradeEvent(faust.Record, serializer='json'):
    trade_id: str
    account_id: str
    symbol: str
    quantity: float
    price: float
    trade_type: str
    timestamp: float  # Unix 时间戳（毫秒）

class RiskAlert(faust.Record, serializer='json'):
    """风控告警消息模型"""
    alert_id: str
    account_id: str
    alert_type: str       # 告警类型
    trade_count: int      # 窗口内交易笔数
    window_seconds: int   # 检测窗口（秒）
    triggered_at: float   # 告警触发时间
    trade_ids: List[str]  # 触发告警的交易 ID 列表

raw_trades_topic = app.topic('raw-trades', value_type=TradeEvent)
risk_alerts_topic = app.topic('risk-alerts', value_type=RiskAlert)

# ============================================================
# 滑动窗口实现：使用 Table 存储每个账户的最近交易时间戳队列
#
# 设计说明：
# - key: account_id
# - value: 最近 N 笔交易的 [timestamp_ms, trade_id] 列表
#          存储为列表是因为 faust Table 的 value 必须可序列化
#
# 滑动窗口逻辑：
#   新交易到达 → 加入队列 → 清除窗口外的旧记录 → 检查队列长度
# ============================================================

# 告警阈值配置
WINDOW_SECONDS = 300      # 5 分钟滑动窗口
ALERT_THRESHOLD = 3       # 窗口内超过 3 笔交易触发告警

# 存储每个账户的交易时间戳队列
# value 格式: [[timestamp_ms, trade_id], ...]
account_trade_history = app.Table(
    'account-trade-history',
    default=list,
    help='每个账户在滑动窗口内的交易历史',
)

# 存储已告警的账户（避免重复告警）
alerted_accounts = app.Table(
    'alerted-accounts',
    default=float,
    help='账户最后一次告警时间戳（防止告警风暴）',
)

@app.agent(raw_trades_topic)
async def detect_high_frequency_trading(trades):
    """
    滑动窗口高频交易检测 Agent
    
    检测逻辑：
    - 维护每个账户过去 5 分钟内的交易记录
    - 每次新交易到达时：
      1. 将新交易加入账户历史队列
      2. 清除超过 5 分钟的历史记录
      3. 如果窗口内交易数 > 阈值，触发告警
    """
    async for trade in trades:
        account_id = trade.account_id
        current_time_ms = trade.timestamp
        window_start_ms = current_time_ms - (WINDOW_SECONDS * 1000)  # 5分钟前的时间戳
        
        # ——— Step 1: 获取该账户的历史队列 ———
        # 注意：faust Table 返回的列表是共享对象，直接修改会有问题
        # 必须创建新列表再赋值回去
        history = list(account_trade_history[account_id])
        
        # ——— Step 2: 加入新交易记录 ———
        history.append([current_time_ms, trade.trade_id])
        
        # ——— Step 3: 清除窗口外的旧记录（滑动窗口的核心） ———
        history = [
            record for record in history
            if record[0] >= window_start_ms  # 只保留窗口内的记录
        ]
        
        # ——— Step 4: 写回 State Table ———
        account_trade_history[account_id] = history
        
        # ——— Step 5: 检查是否超过告警阈值 ———
        trade_count = len(history)
        
        logger.debug(
            f"账户 {account_id}: 过去 {WINDOW_SECONDS}s 内 "
            f"{trade_count} 笔交易（阈值: {ALERT_THRESHOLD}）"
        )
        
        if trade_count > ALERT_THRESHOLD:
            await _trigger_alert(trade, history, trade_count)

async def _trigger_alert(
    trade: TradeEvent,
    history: list,
    trade_count: int
):
    """
    触发风控告警
    
    包含告警去重逻辑：同一账户在 60 秒内不重复告警
    （避免一次高频交易事件产生大量重复告警）
    """
    account_id = trade.account_id
    current_time_ms = trade.timestamp
    
    # 告警冷却：60 秒内同一账户不重复告警
    last_alert_time = alerted_accounts[account_id]
    cooldown_ms = 60 * 1000  # 60 秒冷却期
    
    if last_alert_time and (current_time_ms - last_alert_time) < cooldown_ms:
        logger.debug(f"账户 {account_id} 处于告警冷却期，跳过")
        return
    
    # 更新最后告警时间
    alerted_accounts[account_id] = current_time_ms
    
    # 构造告警消息
    import uuid
    alert = RiskAlert(
        alert_id=str(uuid.uuid4()),
        account_id=account_id,
        alert_type='HIGH_FREQUENCY_TRADING',
        trade_count=trade_count,
        window_seconds=WINDOW_SECONDS,
        triggered_at=current_time_ms,
        trade_ids=[record[1] for record in history],  # 提取 trade_id 列表
    )
    
    # 发送告警到专用 topic
    await risk_alerts_topic.send(
        key=account_id,  # 以 account_id 为 key，确保同一账户的告警有序
        value=alert,
    )
    
    logger.warning(
        f"⚠️  风控告警！账户 {account_id} 在过去 {WINDOW_SECONDS}s 内 "
        f"发生 {trade_count} 笔交易，超过阈值 {ALERT_THRESHOLD}！"
        f"告警 ID: {alert.alert_id}"
    )

if __name__ == '__main__':
    app.main()
```

**启动 faust 应用**：

```bash
# 启动 faust worker（类似启动一个消费者进程）
faust -A feature2_sliding_window_alert worker -l info

# 多实例启动（自动分配分区，水平扩展）
faust -A feature2_sliding_window_alert worker -l info &
faust -A feature2_sliding_window_alert worker -l info &
faust -A feature2_sliding_window_alert worker -l info &

# 查看应用状态（faust 内置 Web UI）
faust -A feature2_sliding_window_alert web
# 然后访问 http://localhost:6066
```

---

## 7.4 窗口类型详解

### 7.4.1 Tumbling Window（滚动窗口）

滚动窗口将时间轴切成**等长、不重叠**的片段。每个事件只属于一个窗口。

```
滚动窗口示意（窗口大小 = 1 分钟）：

时间轴: 10:00  10:01  10:02  10:03  10:04
         │──────│──────│──────│──────│
         
窗口1    [─────]
窗口2           [─────]
窗口3                  [─────]

事件 A（10:00:30）→ 属于窗口1
事件 B（10:00:55）→ 属于窗口1
事件 C（10:01:10）→ 属于窗口2（不属于窗口1！）
```

**faust 实现**：

```python
# tumbling_window.py - 滚动窗口统计
import faust

app = faust.App('tumbling-window-demo', broker='kafka://localhost:9092')

class TradeEvent(faust.Record, serializer='json'):
    symbol: str
    quantity: float
    price: float
    timestamp: float

raw_trades_topic = app.topic('raw-trades', value_type=TradeEvent)

# ============================================================
# Tumbling Window Table
# tumbling(60) 表示 60 秒滚动窗口
# expires=3600 表示窗口数据保留 1 小时后过期（释放内存）
# ============================================================
symbol_volume = app.Table(
    'symbol-volume-tumbling',
    default=float,
).tumbling(
    60,           # 窗口大小：60 秒
    expires=3600, # 过期时间：1 小时
)

@app.agent(raw_trades_topic)
async def aggregate_by_symbol(trades):
    """使用滚动窗口统计每 60 秒每个 symbol 的成交量"""
    async for trade in trades:
        # .current() 返回当前时间窗口的值
        # .delta(n) 返回 n 个窗口前的值（用于环比对比）
        symbol_volume[trade.symbol].current() + trade.quantity * trade.price
        
        # 正确的写法（faust Table 窗口操作）
        symbol_volume[trade.symbol] = (
            symbol_volume[trade.symbol].current() + trade.quantity * trade.price
        )
```

**适用场景**：
- 每小时/每天的交易统计报表
- 固定时间段的指标汇总（每 5 分钟 K 线）
- 数据管道中的批次聚合（每 10 分钟写入一次数据库）

### 7.4.2 Hopping Window（跳跃窗口）

跳跃窗口的大小固定，但**以较小的步长向前移动**，导致窗口之间有重叠。每个事件可能属于多个窗口。

```
跳跃窗口示意（窗口大小 = 10 分钟，步长 = 5 分钟）：

时间轴: 10:00  10:05  10:10  10:15
         │──────│──────│──────│

窗口1    [──────────]
窗口2           [──────────]
窗口3                  [──────────]

事件 A（10:02）→ 属于窗口1
事件 B（10:07）→ 属于窗口1 + 窗口2（重复计数！）
事件 C（10:12）→ 属于窗口2 + 窗口3
```

**faust 实现**：

```python
# hopping_window.py - 跳跃窗口
import faust

app = faust.App('hopping-window-demo', broker='kafka://localhost:9092')

class TradeEvent(faust.Record, serializer='json'):
    symbol: str
    quantity: float
    price: float

raw_trades_topic = app.topic('raw-trades', value_type=TradeEvent)

# ============================================================
# Hopping Window Table
# hopping(size, step) 表示窗口大小为 size 秒，步长为 step 秒
# 窗口大小=600秒（10分钟），步长=300秒（5分钟）
# ============================================================
symbol_trades_hopping = app.Table(
    'symbol-trades-hopping',
    default=int,
).hopping(
    600,          # 窗口大小：10 分钟
    300,          # 步长：5 分钟
    expires=7200, # 过期：2 小时
)

@app.agent(raw_trades_topic)
async def count_with_hopping(trades):
    """使用跳跃窗口统计 10 分钟内交易笔数（每 5 分钟更新一次）"""
    async for trade in trades:
        symbol_trades_hopping[trade.symbol] += 1
```

**适用场景**：
- 移动平均计算（5 分钟更新一次，但看的是 10 分钟数据）
- 风控检测（持续评估最近 N 分钟的行为，而非等窗口结束）
- 滚动指标仪表盘（实时 K 线，每 1 分钟刷新但显示 5 分钟数据）

### 7.4.3 Session Window（会话窗口）

会话窗口基于**用户活跃度**动态划分，不固定大小。当两个相邻事件的时间间隔超过"会话超时"（Session Gap）时，旧会话关闭，新会话开始。

```
会话窗口示意（Session Gap = 30 分钟）：

用户 A 的事件：
时间:  10:00  10:05  10:15  10:50  11:00
事件:    e1     e2     e3     e4     e5
         │──────────────│       │──────│
         会话1（15分钟）  ↑间隔35分>30分  会话2
         
会话1 包含 e1, e2, e3
会话2 包含 e4, e5
```

**注意**：faust 原生不支持 Session Window，但可以手动实现：

```python
# session_window_manual.py - 手动实现会话窗口
import faust
import asyncio
from datetime import datetime

app = faust.App('session-window-demo', broker='kafka://localhost:9092')

class UserEvent(faust.Record, serializer='json'):
    user_id: str
    event_type: str
    timestamp: float  # 毫秒

events_topic = app.topic('user-events', value_type=UserEvent)

SESSION_GAP_MS = 30 * 60 * 1000  # 30 分钟会话超时

# 存储每个用户的会话状态
# value: {"session_start": ms, "event_count": n, "last_event_time": ms}
user_sessions = app.Table(
    'user-sessions',
    default=dict,
)

@app.agent(events_topic)
async def track_user_sessions(events):
    """手动实现会话窗口逻辑"""
    async for event in events:
        user_id = event.user_id
        current_time = event.timestamp
        
        session = dict(user_sessions[user_id])  # 复制避免引用问题
        
        if not session:
            # 新用户或会话已过期：开启新会话
            session = {
                'session_start': current_time,
                'event_count': 0,
                'last_event_time': current_time,
            }
        elif current_time - session['last_event_time'] > SESSION_GAP_MS:
            # 距离上次事件超过 30 分钟：结束旧会话，开启新会话
            session_duration = session['last_event_time'] - session['session_start']
            print(
                f"会话结束 | 用户: {user_id} | "
                f"时长: {session_duration/1000:.0f}秒 | "
                f"事件数: {session['event_count']}"
            )
            # 开启新会话
            session = {
                'session_start': current_time,
                'event_count': 0,
                'last_event_time': current_time,
            }
        
        # 更新当前会话
        session['event_count'] += 1
        session['last_event_time'] = current_time
        user_sessions[user_id] = session
```

**适用场景**：
- 用户行为分析（一次交易会话有多少步骤）
- 客服系统（一次对话包含多少轮）
- 游戏玩家会话统计（每次上线时长）

### 7.4.4 窗口类型选型总结

```
我需要固定时间段的统计（每小时报表）          → Tumbling Window
我需要平滑的滚动指标（移动平均）              → Hopping Window
我的业务有"会话"概念（用户活跃期）            → Session Window
我需要实时检测近期行为（过去 N 分钟内…）       → Hopping Window
```

---

## 7.5 Stream-Table Join（流表关联）

### 7.5.1 什么是流表关联

流表关联（Stream-Table Join）是指用一个流（KStream）中的事件，去关联一个表（KTable）中的静态/缓慢变化数据，从而"丰富"流中的事件。

**典型场景**：交易流 + 用户信息表 → 带用户完整信息的交易流

```
交易流（KStream）:
  {trade_id: T001, account_id: ACC-001, symbol: AAPL, amount: 5000}
  
用户信息表（KTable，从数据库/Kafka 同步）:
  {account_id: ACC-001, name: "张三", risk_level: "高", country: "CN"}
  
Join 后的丰富数据：
  {
    trade_id: T001,
    account_id: ACC-001,
    symbol: AAPL,
    amount: 5000,
    user_name: "张三",       ← 来自 KTable
    risk_level: "高",        ← 来自 KTable
    country: "CN",           ← 来自 KTable
  }
```

### 7.5.2 faust 实现流表关联

```python
# stream_table_join.py - 实时丰富交易数据
import faust
import logging

logger = logging.getLogger(__name__)

app = faust.App(
    'trade-enricher',
    broker='kafka://localhost:9092',
    value_serializer='json',
)

# ——— 消息模型 ———

class TradeEvent(faust.Record, serializer='json'):
    """原始交易事件（来自交易系统）"""
    trade_id: str
    account_id: str
    symbol: str
    quantity: float
    price: float
    trade_type: str
    timestamp: float

class UserProfile(faust.Record, serializer='json'):
    """用户画像（来自用户服务）"""
    account_id: str
    name: str
    risk_level: str     # LOW / MEDIUM / HIGH
    country: str
    kyc_status: str     # VERIFIED / PENDING / REJECTED
    daily_limit: float  # 每日交易限额

class EnrichedTrade(faust.Record, serializer='json'):
    """丰富后的交易事件（含用户信息）"""
    trade_id: str
    account_id: str
    symbol: str
    quantity: float
    price: float
    trade_type: str
    timestamp: float
    # 来自用户画像的字段
    user_name: str
    risk_level: str
    country: str
    kyc_status: str
    daily_limit: float

# ——— Topics ———
raw_trades_topic = app.topic('raw-trades', value_type=TradeEvent)
user_profiles_topic = app.topic('user-profiles', value_type=UserProfile)
enriched_trades_topic = app.topic('enriched-trades', value_type=EnrichedTrade)

# ============================================================
# KTable：存储最新的用户画像
# 当 user-profiles topic 有更新时，自动更新这个 Table
#
# 关键设计：
# - user-profiles topic 的消息 key 必须是 account_id
# - 这样 Table 才能正确地以 account_id 为键存储
# ============================================================
user_profile_table = app.GlobalTable(
    # GlobalTable vs Table:
    # - Table: 分区化，每个 worker 只保存部分数据，JOIN 时需要 co-partitioning
    # - GlobalTable: 全量复制到每个 worker，任意 JOIN，但内存占用更大
    # 用户画像数据量通常可接受，所以用 GlobalTable
    'user-profiles-cache',
    default=None,
    help='用户画像缓存，从 user-profiles topic 实时同步',
)

# 持续更新用户画像 Table
@app.agent(user_profiles_topic)
async def sync_user_profiles(profiles):
    """
    持续消费 user-profiles topic，保持 GlobalTable 最新
    当用户风险等级变化、KYC 状态变化时，Table 会自动更新
    """
    async for profile in profiles:
        user_profile_table[profile.account_id] = profile
        logger.info(f"用户画像已更新: {profile.account_id} ({profile.name})")

# Stream-Table JOIN：丰富交易数据
@app.agent(raw_trades_topic)
async def enrich_trades(trades):
    """
    流表关联：将原始交易事件与用户画像关联，生成丰富的交易记录
    
    JOIN 语义：LEFT JOIN（无论用户画像是否存在，交易记录都会处理）
    """
    async for trade in trades:
        # 在 GlobalTable 中查找用户画像（O(1) 本地查找，无网络开销）
        profile = user_profile_table[trade.account_id]
        
        if profile is None:
            # 用户画像不存在（可能是新用户，画像还未同步）
            logger.warning(
                f"用户画像缺失: account_id={trade.account_id}, "
                f"trade_id={trade.trade_id}。使用默认值。"
            )
            # 使用默认值，确保交易记录不丢失（宁可信息不完整，不能丢交易）
            enriched = EnrichedTrade(
                trade_id=trade.trade_id,
                account_id=trade.account_id,
                symbol=trade.symbol,
                quantity=trade.quantity,
                price=trade.price,
                trade_type=trade.trade_type,
                timestamp=trade.timestamp,
                user_name='UNKNOWN',
                risk_level='UNKNOWN',
                country='UNKNOWN',
                kyc_status='UNKNOWN',
                daily_limit=0.0,
            )
        else:
            # 正常情况：将交易数据与用户画像合并
            enriched = EnrichedTrade(
                trade_id=trade.trade_id,
                account_id=trade.account_id,
                symbol=trade.symbol,
                quantity=trade.quantity,
                price=trade.price,
                trade_type=trade.trade_type,
                timestamp=trade.timestamp,
                user_name=profile.name,
                risk_level=profile.risk_level,
                country=profile.country,
                kyc_status=profile.kyc_status,
                daily_limit=profile.daily_limit,
            )
            
            # 额外：KYC 未通过的用户发出大额交易，触发审核
            trade_amount = trade.quantity * trade.price
            if profile.kyc_status != 'VERIFIED' and trade_amount > 10000:
                logger.warning(
                    f"KYC 合规告警: 账户 {trade.account_id} (KYC: {profile.kyc_status}) "
                    f"发起 {trade_amount:.2f} 大额交易 {trade.trade_id}"
                )
        
        # 发送丰富后的交易记录到下游
        await enriched_trades_topic.send(
            key=trade.account_id,
            value=enriched,
        )

if __name__ == '__main__':
    app.main()
```

---

## 7.6 Exactly-Once 语义在 Streams 中的实现

### 7.6.1 三种处理语义

```
At-Most-Once（最多一次）：
  → 宁可丢消息，不重复处理
  → 实现简单：消费后立即提交 offset，处理失败不重试
  → 适用：非关键日志、可以接受少量丢失的指标

At-Least-Once（至少一次）：
  → 宁可重复，不丢消息
  → 实现简单：处理成功后才提交 offset
  → 问题：网络故障可能导致重复处理
  → 适用：大多数业务场景（幂等处理即可）

Exactly-Once（精确一次）：
  → 每条消息恰好处理一次，既不丢，也不重复
  → 实现复杂：需要事务支持
  → 适用：金融交易、计费系统等对准确性要求极高的场景
```

### 7.6.2 Kafka 如何实现 Exactly-Once

Kafka 的 Exactly-Once 基于**事务（Transaction）**机制，核心思想是：

```
原子写入（Atomic Write）：
  "消费消息 + 处理 + 写入结果 + 提交 Offset" 作为一个原子操作
  
  要么全部成功：结果写入了，Offset 提交了
  要么全部回滚：结果没写，Offset 没提交（下次重试）
  
  → 无论网络怎么抖动，结果只会写入一次！
```

**faust 开启 Exactly-Once**：

```python
# exactly_once_demo.py - Exactly-Once 配置
import faust

app = faust.App(
    'exactly-once-processor',
    broker='kafka://localhost:9092',
    value_serializer='json',
    
    # ============================================================
    # 开启 Exactly-Once 的关键配置
    # ============================================================
    
    # 处理保证级别
    processing_guarantee='exactly_once',
    
    # 事务 ID 前缀（每个 worker 实例会自动追加唯一后缀）
    # 确保重启后事务能正确识别和恢复
    producer_transactional_id='trade-processor-tx',
    
    # 事务超时：超过此时间未提交的事务自动回滚
    producer_transaction_timeout_ms=60000,  # 60 秒
    
    # Consumer 隔离级别：只读已提交的消息（Exactly-Once 必须）
    consumer_isolation_level='read_committed',
)

# 配置完成后，agent 的写入操作自动包裹在事务中
# 无需修改业务代码！

class TradeEvent(faust.Record, serializer='json'):
    trade_id: str
    account_id: str
    amount: float

class ProcessedTrade(faust.Record, serializer='json'):
    trade_id: str
    account_id: str
    amount: float
    fee: float  # 手续费（处理结果）

input_topic = app.topic('raw-trades', value_type=TradeEvent)
output_topic = app.topic('processed-trades', value_type=ProcessedTrade)

@app.agent(input_topic)
async def process_trades(trades):
    """
    在 Exactly-Once 模式下，这个 agent 的每次处理都是事务性的：
    - 如果 send() 失败 → 整个事务回滚，offset 不提交 → 下次重试
    - 如果成功 → send() 和 offset 提交一起原子提交
    - 结果：无论何种故障，每笔交易恰好处理一次
    """
    async for trade in trades:
        # 计算手续费（业务逻辑）
        fee = trade.amount * 0.001  # 0.1% 手续费
        
        processed = ProcessedTrade(
            trade_id=trade.trade_id,
            account_id=trade.account_id,
            amount=trade.amount,
            fee=fee,
        )
        
        # 这个 send() 操作会包裹在事务中
        # 与 offset 提交一起原子提交
        await output_topic.send(key=trade.account_id, value=processed)

if __name__ == '__main__':
    app.main()
```

### 7.6.3 Exactly-Once 的代价

```
性能开销：
  - 事务协调需要额外的 RPC 请求
  - 吞吐量大约降低 10-30%
  - 延迟略有增加

适用建议：
  ✅ 金融交易处理（手续费计算、余额变更）
  ✅ 计费系统（每笔费用只收一次）
  ✅ 数据库同步（避免重复写入）
  ❌ 高吞吐日志处理（At-Least-Once + 幂等性更合适）
  ❌ 指标统计（允许少量误差的场景）
```

---

## 7.7 动手练习：实现交易量滚动统计

### 目标

构建一个完整的流处理应用，实现：
1. **滚动窗口**：每 60 秒统计每个 symbol 的总成交额
2. **实时排行**：每 30 秒输出成交额 Top 5 的 symbol
3. **写入结果**：将统计结果写入 `trade-volume-stats` topic

### 步骤一：准备测试数据生产者

```python
# mock_producer.py - 生成模拟交易数据
import json
import time
import random
import uuid
from kafka import KafkaProducer

SYMBOLS = ['AAPL', 'TSLA', 'BTC-USD', 'ETH-USD', 'SPY', 'QQQ', 'NVDA', 'MSFT']
ACCOUNTS = [f'ACC-{i:04d}' for i in range(1, 101)]  # 100 个模拟账户

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: k.encode('utf-8'),
)

def generate_trade():
    symbol = random.choice(SYMBOLS)
    price = random.uniform(10, 500)
    return {
        'trade_id': str(uuid.uuid4()),
        'account_id': random.choice(ACCOUNTS),
        'symbol': symbol,
        'quantity': random.uniform(1, 100),
        'price': round(price, 2),
        'trade_type': random.choice(['BUY', 'SELL']),
        'timestamp': time.time() * 1000,  # 毫秒时间戳
    }

print("开始生成模拟交易数据（每秒 10 笔）...")
try:
    while True:
        for _ in range(10):
            trade = generate_trade()
            producer.send(
                'raw-trades',
                key=trade['account_id'],  # 以 account_id 为 key
                value=trade,
            )
        producer.flush()
        time.sleep(1)
except KeyboardInterrupt:
    print("停止生成数据")
    producer.close()
```

### 步骤二：实现滚动统计 Agent

```python
# rolling_volume_stats.py - 完整的滚动统计应用
import faust
import asyncio
from datetime import datetime, timezone

app = faust.App(
    'rolling-volume-stats',
    broker='kafka://localhost:9092',
    value_serializer='json',
)

class TradeEvent(faust.Record, serializer='json'):
    trade_id: str
    account_id: str
    symbol: str
    quantity: float
    price: float
    trade_type: str
    timestamp: float

raw_trades_topic = app.topic('raw-trades', value_type=TradeEvent)
volume_stats_topic = app.topic('trade-volume-stats')

# 60 秒滚动窗口：统计每个 symbol 的成交额
symbol_volume_table = app.Table(
    'symbol-volume-60s',
    default=float,
).tumbling(
    60,
    expires=3600,
)

# 内存中维护排行榜（简化实现）
leaderboard = {}

@app.agent(raw_trades_topic)
async def compute_volume(trades):
    """统计每个 symbol 在 60 秒窗口内的总成交额"""
    async for trade in trades:
        trade_value = trade.quantity * trade.price
        
        # 累加到窗口中
        symbol_volume_table[trade.symbol] += trade_value
        
        # 更新内存排行榜
        leaderboard[trade.symbol] = symbol_volume_table[trade.symbol].current()

@app.timer(interval=30.0)
async def print_top5():
    """每 30 秒输出 Top 5 成交额排行"""
    if not leaderboard:
        return
    
    now = datetime.now(tz=timezone.utc).strftime('%H:%M:%S')
    sorted_symbols = sorted(leaderboard.items(), key=lambda x: -x[1])
    
    print(f"\n=== [{now}] 成交额 Top 5（60秒窗口）===")
    for i, (symbol, volume) in enumerate(sorted_symbols[:5], 1):
        print(f"  {i}. {symbol}: ${volume:,.2f}")
    
    # 将统计结果写入 topic
    await volume_stats_topic.send(
        key='top5',
        value={
            'timestamp': datetime.now(tz=timezone.utc).isoformat(),
            'top5': [
                {'symbol': s, 'volume': v}
                for s, v in sorted_symbols[:5]
            ]
        }
    )

if __name__ == '__main__':
    app.main()
```

### 步骤三：运行与验证

```bash
# 终端 1：启动 Kafka（如果还没启动）
docker-compose up -d kafka

# 终端 2：启动流处理 worker
faust -A rolling_volume_stats worker -l info

# 终端 3：启动模拟数据生产者
python mock_producer.py

# 终端 4：消费统计结果，验证输出
kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic trade-volume-stats \
  --from-beginning

# 预期输出（每 30 秒更新一次）：
# {"timestamp": "2024-01-15T10:30:00+00:00", "top5": [{"symbol": "NVDA", "volume": 485234.5}, ...]}
```

### 练习扩展挑战

1. **基础**：增加 `trade_type` 维度，分别统计 BUY 和 SELL 的成交额
2. **进阶**：添加 Hopping Window（10 分钟窗口，5 分钟步长），与 Tumbling Window 对比结果差异
3. **挑战**：实现成交量异常检测——当某 symbol 的成交量超过过去 1 小时均值的 3 倍时，发出告警

---

## 本章小结

| 概念 | 核心要点 |
|------|---------|
| KStream vs KTable | KStream = 事件流；KTable = 当前状态 |
| Tumbling Window | 等长不重叠，每个事件只属于一个窗口 |
| Hopping Window | 等长有重叠，适合移动平均 |
| Session Window | 按活跃度动态划分，无固定大小 |
| Stream-Table Join | 用 GlobalTable 实时丰富流数据 |
| Exactly-Once | 事务机制保证，有约 10-30% 性能开销 |
| faust | Python Kafka Streams 实现，`faust-streaming` 包 |

下一章，我们将深入 Kafka 的**监控与可观测性**——如何知道你的 Kafka 集群是否健康？
