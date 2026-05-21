# 第 11 章：End-to-End 项目 — 实时交易风控系统 RiskGuard

> "理论是地图，项目是旅途本身。"

前十章我们系统地学习了 Kafka 的核心概念——幂等 Producer、手动 Offset 提交、Schema Registry、Consumer Group Rebalance、监控告警……本章将这一切融合成一个完整的端到端项目：**RiskGuard**，一套模拟加密货币交易所的实时交易风控监控系统。

读完本章，你将能够：

- 独立搭建一套完整的 Kafka 事件驱动管道
- 理解幂等生产、Avro 序列化、手动提交在真实场景的协同工作
- 掌握滑动窗口等有状态流处理技术
- 构建可观测、可扩展的 Kafka 应用程序

---

## 11.1 项目概览

### 11.1.1 业务场景

加密货币交易所每天处理数百万笔交易。监管合规要求交易所必须具备实时风控能力，对以下异常行为立即响应：

| 风险类型 | 业务背景 | 监管要求 |
|---------|---------|---------|
| 大额交易 | 洗钱、资本外逃 | FINTRAC 报告阈值（CAD $10,000+） |
| 高频交易 | 账户被盗、刷量操纵 | FINRA 规则 5310 |
| 价格异常 | 内幕交易、市场操纵 | IIROC 市场监察规则 |

RiskGuard 的核心目标是：**从交易发生到风险告警，延迟 < 100ms**。

### 11.1.2 项目架构

```
╔══════════════════════════════════════════════════════════════════════╗
║                    RiskGuard 数据流架构                               ║
╠══════════════════════════════════════════════════════════════════════╣
║                                                                      ║
║  ┌─────────────────────┐                                             ║
║  │  trade_generator.py │  ← 模拟交易所，注入正常/异常交易             ║
║  │  （幂等 Producer）   │                                             ║
║  └──────────┬──────────┘                                             ║
║             │  Avro 序列化                                            ║
║             ▼                                                        ║
║  ┌──────────────────────────────────┐                                ║
║  │   Kafka Topic: trades.raw        │                                ║
║  │   6 分区 | 7 天保留 | snappy 压缩 │                               ║
║  └──────────┬───────────────────────┘                                ║
║             │  按 account_id 分区                                     ║
║             ▼                                                        ║
║  ┌─────────────────────┐                                             ║
║  │  risk_detector.py   │  ← 三条风控规则检测                          ║
║  │  （手动 Offset 提交）│    规则1: 大额 > CAD $50,000                ║
║  └──────┬──────────────┘    规则2: 60s 内 > 5 笔                    ║
║         │                   规则3: 价格偏离 > 5%                      ║
║         │ 告警              ╔═════════════════╗                      ║
║         ▼                  ║ 失败 → DLQ       ║                      ║
║  ┌──────────────────────┐  ║ trades.dlq       ║                      ║
║  │  Topic: risk.alerts  │  ╚═════════════════╝                      ║
║  │  3 分区 | 30天 | Log  │                                           ║
║  │  Compaction          │                                            ║
║  └──────────┬───────────┘                                            ║
║             │  实时消费                                               ║
║             ▼                                                        ║
║  ┌──────────────────────┐   ┌──────────────────┐                    ║
║  │  alert_dashboard.py  │   │  Grafana + Prom   │                   ║
║  │  （控制台实时展示）   │   │  （指标可视化）    │                   ║
║  └──────────────────────┘   └──────────────────┘                    ║
║                                                                      ║
╚══════════════════════════════════════════════════════════════════════╝

基础设施（Docker Compose）：
  Kafka (KRaft) :9092  |  Schema Registry :8081
  Kafka UI      :8080  |  Prometheus :9090  |  Grafana :3000
```

### 11.1.3 技术选型

| 组件 | 技术 | 选型理由 |
|------|------|---------|
| 消息队列 | Confluent Kafka 7.x (KRaft) | 无 ZooKeeper 依赖，运维简单 |
| 序列化 | Apache Avro + Schema Registry | Schema 演进保障，编解码高效 |
| Python 客户端 | confluent-kafka | 官方维护，性能最优 |
| 监控 | Prometheus + Grafana | 行业标准，生态成熟 |
| 终端 UI | rich（可选）/ ANSI | 零依赖回退方案 |

---

## 11.2 目录结构

```
kafka-best-practice-cn/project/
├── docker-compose.yml          # 完整基础设施配置
├── Makefile                    # 一键操作命令集
├── requirements.txt            # Python 依赖清单
│
├── config/
│   ├── kafka_config.py         # 集中配置管理（Broker、Topics、规则阈值）
│   ├── prometheus.yml          # Prometheus 抓取配置
│   ├── grafana/
│   │   ├── datasources/        # Grafana 数据源自动注入
│   │   └── dashboards/         # Grafana 仪表盘 JSON
│   └── schemas/
│       ├── trade.avsc          # Trade 事件 Avro Schema
│       └── risk_alert.avsc     # RiskAlert 事件 Avro Schema
│
├── producer/
│   └── trade_generator.py      # 交易生成器（幂等 Producer）
│
├── consumer/
│   ├── risk_detector.py        # 风控检测消费者（手动 Offset）
│   └── alert_dashboard.py      # 实时告警仪表盘
│
├── scripts/
│   ├── setup_topics.py         # Topic 初始化脚本
│   └── market_prices.py        # 模拟市场价格数据
│
└── tests/
    └── test_risk_rules.py      # 单元测试（pytest）
```

**设计原则**：

- **关注点分离**：每个文件只做一件事
- **配置集中**：所有 Kafka 参数在 `kafka_config.py` 统一管理
- **Schema 先行**：先定义 `.avsc`，再写业务代码
- **可测试性**：规则引擎与 Kafka 解耦，可独立单元测试

---

## 11.3 数据模型设计（Avro Schema）

Schema 是事件驱动系统的"契约"。在写任何生产者/消费者代码之前，我们先定义数据结构。

### 11.3.1 Trade 事件（trades.raw）

```json
{
  "type": "record",
  "name": "Trade",
  "namespace": "com.riskguard.events",
  "fields": [
    {"name": "trade_id",          "type": "string"},
    {"name": "account_id",        "type": "string"},
    {"name": "asset_pair",        "type": "string"},
    {"name": "side",              "type": {"type": "enum", "symbols": ["BUY", "SELL"]}},
    {"name": "quantity",          "type": "double"},
    {"name": "price_cad",         "type": "double"},
    {"name": "total_value_cad",   "type": "double"},
    {"name": "market_price_cad",  "type": "double"},
    {"name": "timestamp_ms",      "type": "long", "logicalType": "timestamp-millis"},
    {"name": "ip_address",        "type": ["null", "string"], "default": null},
    {"name": "metadata",          "type": {"type": "map", "values": "string"}, "default": {}}
  ]
}
```

**字段设计要点**：

- `market_price_cad`：交易提交时的市场参考价，存入消息本身。这样消费者做价格偏差检测时不需要再查询外部数据源，大幅降低延迟和复杂度。
- `timestamp_ms`：使用 `logicalType: timestamp-millis`，Avro 将其视为有语义的时间戳（而非普通 long），方便下游工具自动解析。
- `metadata`：通用扩展字段，避免频繁修改 Schema。

### 11.3.2 RiskAlert 事件（risk.alerts）

```json
{
  "type": "record",
  "name": "RiskAlert",
  "fields": [
    {"name": "alert_id",          "type": "string"},
    {"name": "trade_id",          "type": "string"},
    {"name": "alert_type",        "type": {"type": "enum",
                                   "symbols": ["LARGE_TRADE", "HIGH_FREQUENCY",
                                               "PRICE_ANOMALY", "MULTI_RULE"]}},
    {"name": "severity",          "type": {"type": "enum",
                                   "symbols": ["LOW", "MEDIUM", "HIGH", "CRITICAL"]}},
    {"name": "description",       "type": "string"},
    {"name": "recommended_action","type": {"type": "enum",
                                   "symbols": ["MONITOR", "REVIEW",
                                               "FREEZE_ACCOUNT", "BLOCK_TRADE"]}},
    {"name": "is_resolved",       "type": "boolean", "default": false},
    {"name": "rule_details",      "type": {"type": "map", "values": "string"}}
  ]
}
```

**`risk.alerts` 使用 Log Compaction**：`is_resolved` 字段配合 Log Compaction 实现"告警状态最终一致"——当风控人员处置告警后，更新同一 Key（account_id）的消息，旧的未处置记录将被压缩清除，只保留最新状态。

---

## 11.4 组件深度解析

### 11.4.1 幂等生产者（trade_generator.py）

#### 核心配置

```python
PRODUCER_CONFIG = {
    "acks": "all",                        # ISR 全部确认
    "enable.idempotence": True,           # 幂等模式
    "compression.type": "snappy",         # 低 CPU 压缩
    "batch.size": 65536,                  # 64KB 批量
    "linger.ms": 5,                       # 5ms 等待凑批
    "retries": 2147483647,                # 无限重试
    "max.in.flight.requests.per.connection": 5,
}
```

**幂等 Producer 工作原理**：

```
Producer                    Broker
   │                           │
   │─── 消息 seq=1, PID=100 ──▶│
   │                           │ 写入日志
   │◀── ACK ───────────────────│
   │                           │
   │─── 消息 seq=2, PID=100 ──▶│
   │     （网络超时）           │ 写入日志
   │                           │
   │─── 重试 seq=2, PID=100 ──▶│ Broker 发现 seq=2 已存在
   │                           │ 拒绝重复写入（返回 DUP）
   │◀── ACK (DUP) ─────────────│
   │                           │
   ✅ 消息精确一次，不重复
```

#### 分区策略

```python
# 按 account_id 作为消息 Key，相同账户的交易落到同一分区
self._producer.produce(
    topic=topic,
    key=trade["account_id"].encode("utf-8"),  # 关键！
    value=serialized_value,
    on_delivery=self._delivery_callback,
)
```

这是频率检测的基础——同账户消息有序地落到同一分区，消费者只需维护本地内存状态即可完成检测，无需跨分区协调。

#### 异常注入机制

生产者支持三种异常模式：

```python
anomaly_type = random.choices(
    ["large_trade", "high_frequency", "price_anomaly"],
    weights=[0.4, 0.3, 0.3],  # 大额最常见
)[0]
```

| 异常类型 | 注入逻辑 | 触发规则 |
|---------|---------|---------|
| `large_trade` | 强制 total_value > $55,000 | 规则1 |
| `high_frequency` | 同账户 30s 内连发 6 笔 | 规则2 |
| `price_anomaly` | 价格偏离市场价 8% | 规则3 |

#### Delivery Callback（异步确认）

```python
def _delivery_callback(self, err, msg):
    TRADES_IN_FLIGHT.dec()   # Prometheus 指标
    if err is not None:
        logger.error(f"❌ 发送失败: {err}")
        self._stats["failed"] += 1
    else:
        self._stats["success"] += 1
```

Delivery Callback 是 Kafka 生产者的核心监控点。每条消息被 Broker 确认或失败后触发，确保我们能感知每条消息的命运。

---

### 11.4.2 风控检测消费者（risk_detector.py）

#### 手动 Offset 提交模式

```python
# 配置关键项
CONSUMER_CONFIG = {
    "enable.auto.commit": False,   # 禁用自动提交！
    ...
}

# 处理流程
msg = consumer.poll(timeout=1.0)        # 1. 拉取消息
trade = deserialize(msg.value())        # 2. 反序列化
alerts = rule_engine.check(trade)       # 3. 执行风控规则
for alert in alerts:
    publish_alert(alert)                # 4. 发布告警
consumer.commit(message=msg)            # 5. 提交 Offset（最后执行！）
```

**为什么必须手动提交？**

```
自动提交（auto.commit）的问题：
  ┌──────────┐   poll()   ┌──────────┐
  │ Consumer │──────────▶│  Kafka   │
  │          │◀──────────│  Broker  │
  │          │  10条消息  │          │
  │          │            │          │
  │ 定时器到 │─── commit ▶│ offset=10│  ← 已提交！
  │          │            │          │
  │ 处理失败 │            │          │  ← 消息丢失！
  └──────────┘            └──────────┘

手动提交（manual commit）：
  ┌──────────┐
  │ Consumer │  poll → 处理成功 → commit（保证处理后才提交）
  │          │  poll → 处理失败 → 发 DLQ → commit（失败也提交，但消息已保存到 DLQ）
  └──────────┘  ✅ At-Least-Once 语义
```

#### 三条风控规则实现

**规则1：大额交易（O(1) 复杂度）**

```python
def _check_large_trade(self, trade: dict) -> Optional[dict]:
    total = trade["total_value_cad"]
    if total <= self.large_trade_threshold:  # $50,000
        return None
    return {
        "alert_type": "LARGE_TRADE",
        "severity": "HIGH" if total < 200_000 else "CRITICAL",
        "recommended_action": "REVIEW" if total < 200_000 else "BLOCK_TRADE",
        ...
    }
```

**规则2：高频检测（滑动窗口，O(1) 均摊）**

这是三条规则中最有技术含量的一条。

```python
class FrequencyWindow:
    def __init__(self, window_seconds=60, max_trades=5):
        # 每个账户一个 deque（双端队列）
        self._windows: dict[str, deque] = defaultdict(deque)

    def add_and_check(self, account_id, timestamp_ms):
        now_sec = timestamp_ms / 1000.0
        cutoff = now_sec - self.window_seconds  # 60s 前的时间点
        window = self._windows[account_id]

        # 弹出窗口外的旧记录（O(1) 均摊）
        while window and window[0] < cutoff:
            window.popleft()

        # 添加当前时间戳
        window.append(now_sec)
        count = len(window)
        return count > self.max_trades, count
```

**为什么用 `deque` 而非 `list`？**

| 操作 | list | deque |
|------|------|-------|
| `append(x)` 末尾添加 | O(1) | O(1) |
| `pop(0)` 头部删除 | **O(n)**（移动所有元素）| **O(1)**（直接删节点）|
| 内存分配 | 连续内存 | 链表结构 |

每次 `add_and_check` 最多删除「超出窗口的旧记录」，平均每次只删 0~1 个，所以总复杂度是 O(1) 均摊。

**规则3：价格异常（偏差计算）**

```python
def _check_price_anomaly(self, trade):
    market_price = trade.get("market_price_cad")
    actual_price = trade["price_cad"]
    deviation_pct = abs(actual_price - market_price) / market_price * 100

    if deviation_pct <= 5.0:   # 5% 阈值
        return None

    return {
        "alert_type": "PRICE_ANOMALY",
        "severity": "MEDIUM" if deviation_pct < 10 else "HIGH",
        "rule_details": {
            "deviation_pct": str(round(deviation_pct, 4)),
            "threshold_pct": "5.0",
        },
        ...
    }
```

**多规则联合告警**：

```python
alerts = []
alerts.extend(check_large_trade(trade))
alerts.extend(check_high_frequency(trade))
alerts.extend(check_price_anomaly(trade))

if len(alerts) >= 2:
    # 多规则同时触发 → 升级为 CRITICAL，建议冻结账户
    combined = merge_alerts(alerts)
    combined["alert_type"] = "MULTI_RULE"
    combined["severity"] = "CRITICAL"
    combined["recommended_action"] = "FREEZE_ACCOUNT"
    return [combined]
```

#### 死信队列（DLQ）

处理失败的消息不能丢弃，必须保存到 DLQ 供人工审查：

```python
def _send_to_dlq(self, raw_bytes, error_reason):
    self._alert_producer.produce(
        topic="trades.dlq",
        value=raw_bytes,
        headers=[
            ("error_reason", error_reason.encode()),
            ("failed_at", str(int(time.time() * 1000)).encode()),
            ("original_topic", "trades.raw".encode()),
        ],
    )
```

DLQ 消息保留原始字节（未解码），附带 Headers 说明失败原因。这样即使原始消息格式损坏，也能从 Headers 中了解上下文。

#### 优雅关闭

```python
def _signal_handler(self, signum, frame):
    logger.info(f"收到信号 {signum}，正在关闭...")
    self._running = False

# 主循环退出后
def shutdown(self):
    self._alert_producer.flush(timeout=15)  # 等待告警全部发出
    self._consumer.close()                  # 触发最终 Rebalance
```

优雅关闭的重要性：
- `consumer.close()` 会向 GroupCoordinator 发送 LeaveGroup 请求
- Broker 立即触发 Rebalance，将该 Consumer 的分区分配给其他实例
- 这避免了等待 `session.timeout.ms`（30s）才发现 Consumer 离线

---

### 11.4.3 实时告警仪表盘（alert_dashboard.py）

仪表盘消费 `risk.alerts`，在控制台实时展示：

```
========================================================================
  🛡️  RiskGuard — 实时风险告警仪表盘     2024-03-15 14:23:07
========================================================================

  📊 汇总统计
  ────────────────────────────────────────────────────────────────
  总告警数:     47   运行时长: 00:08:32   速率: 5.5 条/分钟

  🏷  按告警类型
    💰 LARGE_TRADE          ██████████████░░░░░░░░░░░░░░░░   19
    ⚡ HIGH_FREQUENCY       ████████░░░░░░░░░░░░░░░░░░░░░░   14
    📉 PRICE_ANOMALY        ██████░░░░░░░░░░░░░░░░░░░░░░░░   11
    🚨 MULTI_RULE           ███░░░░░░░░░░░░░░░░░░░░░░░░░░░    3

  🔥 按严重程度
    🔴 CRITICAL       3 条
    🟠 HIGH          31 条
    🟡 MEDIUM        11 条
    🟢 LOW            2 条

  👤 高危账户 Top 3
    🥇 ACC-000007  →  12 次告警
    🥈 ACC-000003  →   9 次告警
    🥉 ACC-000001  →   7 次告警

  📋 最近 10 条告警
  ────────────────────────────────────────────────────────────────
  14:23:06 🟠 HIGH       💰 LARGE_TRADE     ACC-000007  BTC-CAD  CAD   87,432.00
  14:23:04 🔴 CRITICAL   🚨 MULTI_RULE      ACC-000003  ETH-CAD  CAD  142,600.00
  14:23:01 🟡 MEDIUM     📉 PRICE_ANOMALY   ACC-000005  SOL-CAD  CAD    3,890.00
  14:22:58 🟠 HIGH       ⚡ HIGH_FREQUENCY  ACC-000007  BTC-CAD  CAD   12,000.00
  ...

  按 Ctrl+C 退出 | 每秒自动刷新
```

仪表盘使用**独立的消费者组**（`alert-dashboard-group`），且 `auto.offset.reset=latest`——它只关心最新告警，不需要重放历史数据。这是与风控检测器（`earliest`）的关键区别。

---

## 11.5 基础设施配置

### 11.5.1 Docker Compose（KRaft 模式）

```yaml
kafka:
  image: confluentinc/cp-kafka:7.6.0
  environment:
    # KRaft：无需 ZooKeeper
    KAFKA_PROCESS_ROLES: "broker,controller"
    KAFKA_CONTROLLER_QUORUM_VOTERS: "1@kafka:29093"
    CLUSTER_ID: "MkU3OEVBNTcwNTJENDM2Qk"

    # 监听地址（容器内 + 宿主机）
    KAFKA_LISTENERS: >
      PLAINTEXT://kafka:29092,
      PLAINTEXT_HOST://0.0.0.0:9092,
      CONTROLLER://kafka:29093
    KAFKA_ADVERTISED_LISTENERS: >
      PLAINTEXT://kafka:29092,
      PLAINTEXT_HOST://localhost:9092
```

**KRaft 模式的优势**：

| 对比项 | ZooKeeper 模式 | KRaft 模式 |
|-------|--------------|-----------|
| 组件数 | Kafka + ZooKeeper | 仅 Kafka |
| 元数据存储 | ZooKeeper | Kafka 自身 |
| Controller 切换 | 依赖 ZooKeeper | 内置 Raft 协议 |
| 最大分区数 | ~200,000 | ~数百万 |
| 开发环境复杂度 | 需同时维护两套 | 只需一套 |

### 11.5.2 Topic 配置策略

```
┌──────────────┬───────────┬──────────────┬────────────────────┐
│ Topic 名称   │ 分区数    │ 保留策略     │ 清理策略           │
├──────────────┼───────────┼──────────────┼────────────────────┤
│ trades.raw   │ 6         │ 7 天         │ delete（时间驱动）  │
│ risk.alerts  │ 3         │ 30 天        │ compact（最新状态） │
│ trades.dlq   │ 3         │ 30 天        │ delete             │
└──────────────┴───────────┴──────────────┴────────────────────┘
```

`trades.raw` 使用 6 个分区（覆盖 10 个账户），支持最多 6 个并发消费者实例水平扩展。

`risk.alerts` 使用 Log Compaction，只保留每个 Key（account_id）的最新告警状态，天然实现"告警状态表"语义。

---

## 11.6 一键启动指南

### 前置准备

```bash
# 确认 Docker Compose 已安装
docker compose version  # 需要 v2.x+

# 克隆项目
cd kafka-best-practice-cn/project

# 安装 Python 依赖
pip install -r requirements.txt
```

### 第一步：启动基础设施

```bash
make start
# 等效于：docker-compose up -d

# 等待约 30 秒，检查健康状态
make ps
```

预期输出：

```
NAME                       STATUS          PORTS
riskguard-kafka            healthy         0.0.0.0:9092->9092/tcp
riskguard-schema-registry  healthy         0.0.0.0:8081->8081/tcp
riskguard-kafka-ui         running         0.0.0.0:8080->8080/tcp
riskguard-prometheus       healthy         0.0.0.0:9090->9090/tcp
riskguard-grafana          healthy         0.0.0.0:3000->3000/tcp
```

### 第二步：初始化 Topics

```bash
make setup
# 等效于：python scripts/setup_topics.py
```

预期输出：

```
2024-03-15 14:00:01 [INFO] setup-topics — ✅ 已成功连接到 Kafka Broker
2024-03-15 14:00:02 [INFO] setup-topics — 🚀 开始创建 3 个 Topic...
2024-03-15 14:00:02 [INFO] setup-topics — ✅ Topic 创建成功: trades.raw
2024-03-15 14:00:02 [INFO] setup-topics — ✅ Topic 创建成功: risk.alerts
2024-03-15 14:00:02 [INFO] setup-topics — ✅ Topic 创建成功: trades.dlq
2024-03-15 14:00:04 [INFO] setup-topics — 🎉 所有 Topic 初始化完成！
```

### 第三步：启动风控检测器（终端 A）

```bash
make consume
```

预期输出：

```
2024-03-15 14:00:10 [INFO] risk-detector — ✅ RiskDetector 初始化完成
2024-03-15 14:00:10 [INFO] risk-detector — ⚙️  风控规则引擎初始化完成
2024-03-15 14:00:10 [INFO] risk-detector —    规则1 大额阈值:  CAD 50,000
2024-03-15 14:00:10 [INFO] risk-detector —    规则2 频率窗口:  60s 内 >5 笔
2024-03-15 14:00:10 [INFO] risk-detector —    规则3 价格偏差:  >5.0%
```

### 第四步：启动告警仪表盘（终端 B）

```bash
make dashboard
```

### 第五步：启动交易生成器（终端 C）

```bash
make produce
# 等效于：python producer/trade_generator.py --rate 5 --inject-anomalies
```

### 运行效果验证

约 30 秒后，你会在终端 A（检测器）看到：

```
2024-03-15 14:00:45 [WARNING] risk-detector — 🚨 [HIGH] LARGE_TRADE | 账户: ACC-000007 | 描述: 大额交易告警：账户 ACC-000007 执行了 BTC-CAD BUY 交易，金额 CAD 87,432.00，超过阈值 CAD 50,000
2024-03-15 14:01:15 [WARNING] risk-detector — 🚨 [HIGH] HIGH_FREQUENCY | 账户: ACC-000003 | 描述: 高频交易告警：账户 ACC-000003 在 60 秒内完成了 6 笔交易，超过限制 5 笔
```

在仪表盘（终端 B）看到实时刷新的告警统计表。

在 Kafka UI（http://localhost:8080）可以看到：
- `trades.raw` 的消息积累和消费进度
- `risk.alerts` 的告警条数
- Consumer Group `risk-detector-group` 的 Lag 值

---

## 11.7 监控与可观测性

### 11.7.1 Prometheus 指标

生产者暴露（端口 8000）：

| 指标名称 | 类型 | 含义 |
|---------|------|------|
| `riskguard_trades_produced_total` | Counter | 发送总条数（按状态标签） |
| `riskguard_trades_in_flight` | Gauge | 当前在途消息数 |
| `riskguard_produce_latency_ms` | Histogram | 发送延迟分布 |
| `riskguard_anomalies_injected_total` | Counter | 注入异常条数 |

消费者暴露（端口 8001）：

| 指标名称 | 类型 | 含义 |
|---------|------|------|
| `riskguard_detector_trades_consumed_total` | Counter | 消费总条数 |
| `riskguard_alerts_generated_total` | Counter | 告警总数（按类型和严重度） |
| `riskguard_detection_latency_ms` | Histogram | 检测延迟分布 |
| `riskguard_consumer_lag` | Gauge | Consumer Lag |
| `riskguard_accounts_tracked` | Gauge | 当前追踪账户数 |

### 11.7.2 关键 SLO 指标

| SLO | 目标值 | 告警阈值 |
|-----|-------|---------|
| 检测延迟 P99 | < 50ms | > 100ms |
| Consumer Lag | < 1000 条 | > 5000 条 |
| 消息发送失败率 | < 0.1% | > 1% |
| DLQ 积累速率 | 0 条/分钟 | > 10 条/分钟 |

---

## 11.8 扩展挑战

以下是留给读者的进阶练习，难度从低到高：

### 🟢 初级

1. **调整风控阈值**：修改 `kafka_config.py` 中的 `RISK_RULES`，将大额阈值从 $50,000 改为 $20,000，观察告警数量变化。

2. **添加新资产对**：在 `market_prices.py` 中添加 `MATIC-CAD`，并在生产者中启用它。

3. **增加统计维度**：在仪表盘中添加"按资产对统计"功能，展示哪种加密货币最常触发告警。

### 🟡 中级

4. **规则4：地理位置异常**：同一账户在 10 分钟内从两个不同 IP（不同地区）发起交易，触发"不可能的旅行"告警。实现提示：在 `FrequencyWindow` 基础上扩展，追踪每个账户的最近 IP 和时间。

5. **多 Consumer 实例扩展**：同时启动 2 个 `risk_detector.py` 实例，观察 Rebalance 过程，验证两个实例是否正确分配分区（每个处理 3 个分区）。

6. **Exactly-Once 改造**：将风控检测器升级为 Kafka 事务模式，实现 `consume-transform-produce` 的 Exactly-Once 语义。参考第 5 章的事务 API。

### 🔴 高级

7. **持久化状态存储**：当前频率检测状态存在内存中，进程重启后状态丢失。将 `FrequencyWindow` 的状态持久化到 Redis（使用 `ZADD` + `ZREMRANGEBYSCORE` 实现滑动窗口），使检测器支持无状态水平扩展。

8. **机器学习集成**：使用 Isolation Forest 算法对 `trades.raw` 进行无监督异常检测，将 ML 模型的预测结果作为第四条风控规则。

9. **Kafka Streams 重构**：将三条规则的检测逻辑迁移到 Kafka Streams（Java），对比与 Python Consumer 在性能、状态管理方面的差异。

10. **告警聚合与抑制**：同一账户在 5 分钟内多次触发相同类型告警时，聚合为一条告警，避免告警风暴（Alert Storm）。参考 PagerDuty 的告警聚合设计。

---

## 11.9 本章总结

恭喜你完成了 RiskGuard 端到端项目！让我们回顾本章涉及的核心知识点：

| 知识点 | 在本项目中的体现 |
|-------|---------------|
| 幂等 Producer | `enable.idempotence=True`，防止网络重试产生重复交易 |
| Avro Schema | `trade.avsc` / `risk_alert.avsc`，类型安全的事件契约 |
| Schema Registry | 自动注册/拉取 Schema，消费者无感知升级 |
| 按 Key 分区 | `account_id` 作为 Key，保证同账户消息有序 |
| 手动 Offset 提交 | 处理成功后才 `commitSync()`，At-Least-Once 保证 |
| 滑动时间窗口 | `collections.deque` 实现 O(1) 频率检测 |
| 死信队列 | `trades.dlq` 保存失败消息，不丢数据 |
| Log Compaction | `risk.alerts` 保留最新告警状态 |
| 优雅关闭 | SIGINT/SIGTERM → `flush()` + `close()` |
| Prometheus 指标 | 延迟、吞吐、Lag 全方位可观测 |
| KRaft 模式 | 无 ZooKeeper，简化运维 |

**最重要的一课**：Kafka 的价值不在于它是消息队列，而在于它是一个**分布式的、可重放的事件日志**。`trades.raw` 保留 7 天数据，意味着：

- 风控规则可以**随时回放历史数据**，测试新规则的效果
- 新接入的消费者可以从头消费，无需数据迁移
- 系统出问题时，可以**精确回放**到故障发生时刻

这正是事件驱动架构最核心的优势：**事件是不可变的历史，而不是一次性的通知**。

---

*下一步：附录 A 提供完整的 Kafka 命令速查表，附录 B 整理了 20 道高频面试题，助你在技术面试中胸有成竹。*
