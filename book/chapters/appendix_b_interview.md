# 附录 B：Kafka 面试题精选 20 道

> 覆盖高频考点，每题含完整答案和实战要点。

---

## 本章你将学到

- 20 道 Kafka 高频面试题及标准答案
- 每题的核心考察点和答题思路
- 结合 RiskGuard 项目的实战例子

---

## 基础原理类

---

### 1. Kafka 如何保证消息不丢失？

**考察点**：Producer → Broker → Consumer 全链路可靠性

**答**：

消息不丢失需要三端协同：

**Producer 端**：
```python
# 关键配置
producer_config = {
    "acks": "all",                    # 等待所有 ISR 副本确认
    "enable.idempotence": True,       # 幂等，防止重试导致重复
    "retries": 2147483647,            # 无限重试
    "retry.backoff.ms": 100,
}
```

**Broker 端**：
```properties
# server.properties
min.insync.replicas=2        # 至少 2 个副本在 ISR 中
unclean.leader.election.enable=false  # 禁止不洁选举
```

**Consumer 端**：
```python
# 手动提交 Offset，处理成功后才提交
consumer.poll(timeout_ms=1000)
# ... 处理消息 ...
consumer.commit()  # 处理完成后才提交，不用自动提交
```

**一句话总结**：`acks=all` + `min.insync.replicas=2` + 手动 Offset 提交 = 消息零丢失。

---

### 2. Kafka 如何保证消息顺序？

**考察点**：Partition 内有序，全局无序

**答**：

Kafka 只保证**同一 Partition 内**的消息有序，不保证跨 Partition 有序。

实现有序的关键：**相同 Key 的消息路由到同一 Partition**。

```python
# 同一账户的所有交易，按 account_id 路由到同一分区
producer.produce(
    topic="trades.raw",
    key=trade["account_id"],   # Key 决定分区，同 Key → 同分区 → 有序
    value=serialize(trade),
)
```

**注意事项**：
- 分区数变化后，同一 Key 可能路由到不同分区，顺序保证失效
- 如需严格全局有序，使用单 Partition（牺牲吞吐量）
- `max.in.flight.requests.per.connection=1` + `enable.idempotence=True` 保证单分区严格有序

**RiskGuard 应用**：按 `account_id` 做 Key，同一账户的交易按时间顺序处理，频率风控才准确。

---

### 3. Kafka 如何实现 Exactly-Once 语义？

**考察点**：幂等 Producer + 事务 Producer + Streams EOS

**答**：

| 语义 | 配置 | 适用场景 |
|------|------|---------|
| At-most-once | `acks=0`，自动 Offset 提交 | 日志，允许丢失 |
| At-least-once | `acks=all`，手动 Offset 提交 | 大多数业务场景 |
| Exactly-once | 事务 Producer + `isolation.level=read_committed` | 金融，不允许重复 |

**Exactly-Once 实现**：

```python
# 开启事务
producer = Producer({
    "enable.idempotence": True,
    "transactional.id": "risk-detector-txn-001",  # 唯一事务 ID
})
producer.init_transactions()

try:
    producer.begin_transaction()
    # 消费消息 + 生产告警，在同一个事务中
    producer.produce("risk.alerts", value=alert)
    producer.send_offsets_to_transaction(offsets, consumer_group)
    producer.commit_transaction()
except Exception:
    producer.abort_transaction()
```

**Consumer 端**：
```python
consumer_config = {
    "isolation.level": "read_committed",  # 只读已提交的事务消息
}
```

**面试加分点**：事务 Producer 有性能开销（约 20-30%），除非业务强需求，否则 At-least-once + 业务层幂等（数据库唯一键）是更实用的方案。

---

### 4. Consumer Lag 是什么？如何处理 Lag 增长？

**考察点**：监控理解 + 问题排查能力

**答**：

**Consumer Lag = Latest Offset - Consumer Committed Offset**

即消费者落后于最新消息的条数。

```bash
# 查看 Lag
kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe --group risk-detector-group

# 输出示例
TOPIC       PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
trades.raw  0          10000           10500           500  ← Lag
trades.raw  1          9800            10500           700
```

**Lag 增长的原因和处理**：

| 原因 | 诊断 | 解决 |
|------|------|------|
| 消费速度 < 生产速度 | CPU/内存利用率高 | 增加 Consumer 实例（不超过分区数） |
| 处理逻辑太慢 | 处理耗时监控 | 异步处理，或拆分为多个 Consumer |
| 分区数不够 | 单分区 Lag 高 | 增加分区数 + Consumer 实例 |
| Rebalance 频繁 | 频繁 rebalance 日志 | 调整 `session.timeout.ms`，增大 `max.poll.interval.ms` |

**告警规则**：Lag > 10,000 且持续 5 分钟 → PagerDuty 告警。

---

### 5. Rebalance 是什么？如何减少其影响？

**考察点**：Consumer Group 机制理解

**答**：

**Rebalance** 是 Consumer Group 中分区分配的重新调整过程。触发条件：
1. Consumer 加入或离开 Group
2. Topic 分区数变化
3. Consumer 心跳超时（`session.timeout.ms` 到期）

**Rebalance 期间**：所有消费者**停止消费**，等待重新分配完成。这是 Kafka 的"Stop-The-World"事件。

**减少影响的策略**：

```python
# 1. 使用 StickyAssignor — 尽量保持原分配，减少迁移
consumer_config = {
    "partition.assignment.strategy": "cooperative-sticky",  # Kafka 2.4+
}

# 2. 调大心跳超时，防止误触发
consumer_config = {
    "session.timeout.ms": 45000,       # 默认 45s
    "heartbeat.interval.ms": 3000,     # 心跳间隔（建议为 session 的 1/3）
    "max.poll.interval.ms": 300000,    # 两次 poll 之间最长间隔
}

# 3. 优雅关闭，让 Broker 立即知道 Consumer 下线
consumer.close()  # 主动触发 LeaveGroup，快速 Rebalance
```

**面试加分点**：Kafka 2.4+ 引入 Incremental Cooperative Rebalancing，不再需要"停止全部消费者"，显著减少停顿时间。

---

### 6. ZooKeeper 和 KRaft 的区别是什么？

**考察点**：Kafka 架构演进理解

**答**：

| 对比项 | ZooKeeper 模式（旧） | KRaft 模式（新，Kafka 3.x） |
|--------|---------------------|--------------------------|
| 元数据存储 | 外部 ZooKeeper 集群 | Kafka 自身（内置 Raft） |
| 运维复杂度 | 需维护两套集群 | 只需维护 Kafka |
| Controller 选举 | 依赖 ZooKeeper | 内置 Raft 共识算法 |
| 启动速度 | 慢（需连接 ZK） | 快 |
| 扩展性 | Controller 是瓶颈 | 更好（多 Controller） |
| Kafka 版本 | < 3.x | 3.x+ 推荐，3.7+ 强制 |

**KRaft 配置关键**：
```properties
# server.properties (KRaft 模式)
process.roles=broker,controller    # 可以同时承担两个角色
node.id=1
controller.quorum.voters=1@kafka:29093
```

**结论**：新项目直接用 KRaft，2025 年后 ZooKeeper 模式已废弃。

---

### 7. Kafka 和 RabbitMQ 的核心区别是什么？

**考察点**：消息队列选型判断力

**答**：

| 对比项 | Kafka | RabbitMQ |
|--------|-------|---------|
| 设计定位 | 分布式流平台（持久化日志） | 传统消息队列（路由/推送） |
| 消息存储 | 持久化到磁盘，可回放 | 消费后删除（默认） |
| 吞吐量 | 百万级/秒 | 万级/秒 |
| 延迟 | 毫秒级（批量） | 微秒级（单条） |
| 消费模式 | Pull（Consumer 拉取） | Push（Broker 推送） |
| 路由能力 | 简单（Topic/Partition） | 丰富（Exchange/Queue/Binding） |
| 回放能力 | ✅ 可回放历史消息 | ❌ 不支持 |
| 适用场景 | 日志、事件流、大数据 | 任务队列、RPC、微服务通知 |

**选型原则**：
- 需要**回放历史**、**高吞吐**、**流处理** → Kafka
- 需要**复杂路由**、**低延迟单条**、**任务队列** → RabbitMQ

---

### 8. Log Compaction（日志压缩）是什么？

**考察点**：Topic 保留策略理解

**答**：

Log Compaction 是 Kafka 的一种数据保留策略：**对于相同 Key，只保留最新的一条消息**，旧值被删除。

**对比时间保留**：

| 策略 | 保留方式 | 适用场景 |
|------|---------|---------|
| `retention.ms`（默认） | 按时间删除全部旧消息 | 事件流、日志 |
| Log Compaction | 同 Key 只保留最新值 | 状态快照、变更数据捕获（CDC） |

**配置**：
```bash
kafka-topics.sh --create \
  --topic account-balances \
  --config cleanup.policy=compact \     # 开启 Log Compaction
  --config min.cleanable.dirty.ratio=0.5 \
  --config segment.ms=3600000           # 每小时触发一次压缩
```

**RiskGuard 应用**：账户最新余额表用 Log Compaction，Key=account_id，始终保留每个账户的最新余额，Consumer 重启后不需要重放全部历史。

---

### 9. 分区数应该如何选择？

**考察点**：容量规划能力

**答**：

**公式**：
```
分区数 = max(
    目标写入吞吐量 / 单分区写入能力,
    目标读取吞吐量 / 单分区读取能力,
    最大 Consumer 并发数
)
```

**经验值**：
- 单分区写入：10-100 MB/s（取决于消息大小和硬件）
- 单分区读取：同上
- 生产环境最小副本：3

**实战案例**：
```
目标：处理 60 MB/s 的交易数据
单分区能力：20 MB/s
需要分区数：60 / 20 = 3
Consumer 实例：6（每实例处理 2 个分区）
副本因子：3

→ 分区数 = max(3, 6) = 6，取整到 2 的幂次 → 8 分区
```

**关键提醒**：
- 分区数只能增加，不能减少
- 增加分区会破坏 Key 到 Partition 的路由一致性
- 不要过度分区（每个分区在 Broker 上有文件句柄开销）

---

### 10. acks=0 / acks=1 / acks=all 分别适用什么场景？

**考察点**：可靠性 vs 性能权衡

**答**：

| acks 值 | 含义 | 吞吐量 | 可靠性 | 适用场景 |
|---------|------|--------|--------|---------|
| `0` | 不等待确认，发完即忘 | 最高 | 最低（可能丢失） | 指标采集、可丢弃日志 |
| `1` | 等待 Leader 写入确认 | 中 | 中（Leader 宕机可能丢失） | 非关键业务日志 |
| `all`（`-1`） | 等待所有 ISR 副本确认 | 较低 | 最高（零丢失） | 金融交易、审计日志 |

**RiskGuard 选择**：`acks=all`，交易数据不允许丢失。

---

## 进阶机制类

---

### 11. 什么是幂等 Producer？如何开启？

**考察点**：重试与去重机制

**答**：

幂等 Producer 保证：**即使因网络抖动触发重试，同一条消息只写入 Broker 一次**。

**实现原理**：
- Broker 为每个 Producer 分配一个 `PID`（Producer ID）
- 每条消息附带 `sequence number`（序列号）
- Broker 检测到重复序列号时，直接丢弃，返回成功

**开启方式**：
```python
producer_config = {
    "enable.idempotence": True,                        # 开启幂等
    "acks": "all",                                     # 必须配合 acks=all
    "max.in.flight.requests.per.connection": 5,        # 必须 ≤ 5
    "retries": 2147483647,                             # 无限重试
}
```

**注意**：幂等 Producer 只保证单 Partition 内的幂等。跨 Partition 或跨 Session 的幂等需要事务 Producer。

---

### 12. Kafka 消息大小有限制吗？大消息如何处理？

**考察点**：实际工程问题处理能力

**答**：

**默认限制**：
```properties
# Broker 配置
message.max.bytes=1048576      # 默认 1MB（含消息头）

# Consumer 配置
fetch.max.bytes=52428800       # 默认 50MB（单次 fetch 上限）
max.partition.fetch.bytes=1048576  # 默认 1MB（单分区）
```

**大消息处理方案（Claim-Check 模式）**：

```
Producer:
1. 将大文件（图片、视频）上传到 S3/GCS
2. 只发送引用（S3 URL + 元数据）到 Kafka

Consumer:
1. 从 Kafka 消费引用消息
2. 按需从 S3 拉取实际内容
```

**优点**：Kafka 保持高吞吐，大文件存储交给对象存储，成本更低。

---

### 13. Kafka 如何水平扩展？

**考察点**：分布式系统扩展能力

**答**：

**扩 Broker**（写入扩展）：
```bash
# 1. 启动新 Broker（增加 node.id）
# 2. 将现有分区副本迁移到新 Broker（再平衡）
kafka-reassign-partitions.sh \
  --bootstrap-server localhost:9092 \
  --reassignment-json-file expand-cluster.json \
  --execute
```

**扩 Consumer**（读取扩展）：
```bash
# 直接增加 Consumer 实例即可，触发 Rebalance 自动重分配
# 最大并发数 = 分区数（多余的 Consumer 会闲置）
```

**扩分区**（当单分区是瓶颈时）：
```bash
kafka-topics.sh --alter \
  --topic trades.raw \
  --partitions 12   # 从 6 增加到 12
# ⚠️ 警告：会破坏 Key 路由一致性，需要迁移策略
```

---

### 14. Schema Registry 的作用是什么？

**考察点**：数据契约和演化能力

**答**：

Schema Registry 解决三个问题：
1. **类型安全**：JSON 无类型，Avro/Protobuf 有严格类型，防止数据错误
2. **Schema 演化**：新版 Consumer 能读旧消息（BACKWARD 兼容），或旧 Consumer 能读新消息（FORWARD 兼容）
3. **存储效率**：Schema 只注册一次，消息中只存 Schema ID（4 字节），而不是完整 Schema

**工作原理**：
```
Producer:
1. 向 Schema Registry 注册 Schema → 获得 Schema ID（如 42）
2. 消息格式：[0x00][Schema ID 4字节][Avro 序列化数据]

Consumer:
1. 读取消息中的 Schema ID（42）
2. 从 Schema Registry 拉取 Schema（缓存）
3. 用 Schema 反序列化数据
```

---

### 15. Kafka Streams vs Flink vs Spark Streaming 怎么选？

**考察点**：流处理技术选型

**答**：

| 对比项 | Kafka Streams | Apache Flink | Spark Streaming |
|--------|--------------|-------------|----------------|
| 部署方式 | 嵌入应用（无集群） | 独立集群 | 独立集群 |
| 学习曲线 | 低 | 高 | 中 |
| 延迟 | 毫秒级 | 毫秒级 | 秒级（微批） |
| 状态管理 | RocksDB | RocksDB | 内存/磁盘 |
| Exactly-Once | ✅ | ✅ | ✅ |
| SQL 支持 | ksqlDB | Flink SQL | Spark SQL |
| 适用规模 | 中小型 | 超大规模 | 大规模批+流 |

**选型指导**：
- 简单流处理、Kafka 生态内 → **Kafka Streams / faust（Python）**
- 超大规模、复杂 CEP、跨数据源 → **Flink**
- 已有 Spark 团队、批流一体 → **Spark Structured Streaming**

---

## 监控与运维类

---

### 16. 如何监控 Kafka 集群健康？关键指标有哪些？

**考察点**：生产运维能力

**答**：

**必须监控的 5 个关键指标**：

| 指标 | 正常值 | 告警条件 |
|------|--------|---------|
| `UnderReplicatedPartitions` | 0 | > 0（有分区副本不足） |
| `ActiveControllerCount` | 1 | ≠ 1（无 Controller 或多 Controller） |
| `consumer-lag`（各 Group） | 趋于 0 或稳定 | 持续增长 |
| `BytesInPerSec` | 正常范围 | 突然暴增或骤降 |
| `RequestHandlerAvgIdlePercent` | > 30% | < 10%（Broker 过载） |

**监控栈**：JMX Exporter → Prometheus → Grafana Dashboard。

---

### 17. Kafka 的 ISR 副本机制是什么？

**考察点**：副本同步机制深度理解

**答**：

**ISR（In-Sync Replicas）**：与 Leader 保持同步的副本集合。

**同步判断标准**：Follower 在 `replica.lag.time.max.ms`（默认 30s）内向 Leader 发送过 Fetch 请求，则认为同步。

```
Leader: Partition 0, Broker 1
ISR: [Broker 1, Broker 2, Broker 3]  ← 全部同步，健康

Broker 3 宕机 30s 后：
ISR: [Broker 1, Broker 2]  ← Broker 3 移出 ISR

配合 acks=all + min.insync.replicas=2：
→ Leader + Broker 2 确认即可，不等 Broker 3
→ 消息安全写入，系统继续运行
```

**关键配置**：
```properties
min.insync.replicas=2       # ISR 中至少 2 个副本，否则 Producer 报错
unclean.leader.election.enable=false  # 禁止 ISR 以外的副本成为 Leader（防数据丢失）
```

---

### 18. Sticky Assignor 为什么比 RoundRobin 好？

**考察点**：Consumer 分区分配策略理解

**答**：

**RoundRobin Assignor**：
- Rebalance 时完全重新分配所有分区
- Consumer 1 原来处理 P0,P1，Rebalance 后可能变成 P2,P3
- 所有 Consumer 都要重新建立 Partition 连接、重新初始化缓存

**Sticky Assignor**：
- Rebalance 时尽量保持原有分配不变
- 只重新分配必须变更的分区（如某 Consumer 下线的那些）
- 减少不必要的状态迁移和连接重建

```python
# 配置 Sticky Assignor（推荐用 cooperative-sticky，支持增量 Rebalance）
consumer_config = {
    "partition.assignment.strategy": "cooperative-sticky",
}
```

**性能对比**：在 100 个分区、10 个 Consumer 场景下，Sticky 比 RoundRobin 减少约 80% 的分区迁移量。

---

### 19. 如何处理"毒丸消息"（Poison Pill）？

**考察点**：消费者错误处理和 DLQ 设计

**答**：

**毒丸消息**：Consumer 无法处理的消息（格式错误、业务校验失败），如果不处理会导致 Consumer 卡在同一条消息无法前进。

**处理策略（DLQ 模式）**：

```python
def process_with_dlq(consumer, dlq_producer, msg):
    max_retries = 3
    for attempt in range(max_retries):
        try:
            process_message(msg)
            consumer.commit()          # 成功，提交 Offset
            return
        except ValidationError as e:
            # 业务错误，不重试，直接进 DLQ
            send_to_dlq(dlq_producer, msg, error=str(e))
            consumer.commit()          # 提交 Offset，跳过该消息
            return
        except Exception as e:
            if attempt == max_retries - 1:
                # 重试耗尽，进 DLQ
                send_to_dlq(dlq_producer, msg, error=str(e))
                consumer.commit()
                return
            time.sleep(2 ** attempt)   # 指数退避

def send_to_dlq(producer, original_msg, error):
    dlq_payload = {
        "original_topic": original_msg.topic(),
        "original_partition": original_msg.partition(),
        "original_offset": original_msg.offset(),
        "error": error,
        "failed_at": datetime.utcnow().isoformat(),
        "payload": original_msg.value(),
    }
    producer.produce("trades.dlq", value=json.dumps(dlq_payload))
    producer.flush()
```

**RiskGuard 实现**：`risk_detector.py` 中三次重试失败后写入 `trades.dlq`，运营人员定期检查 DLQ，修复后手动重放。

---

### 20. Kafka 在微服务中的常见设计模式？

**考察点**：系统设计和架构能力

**答**：

**1. Event Sourcing（事件溯源）**
```
所有状态变更都作为不可变事件写入 Kafka
账户余额 = 所有历史交易事件的 Replay 结果
优点：完整审计日志，可回放到任意历史状态
```

**2. CQRS（命令查询职责分离）**
```
写操作 → 发布事件到 Kafka
读操作 → 从物化视图（由 Consumer 维护）查询
RiskGuard 应用：写交易 → Kafka → Consumer 更新账户余额视图
```

**3. Saga 模式（分布式事务）**
```
Choreography-based Saga:
下单服务 → ORDER_CREATED
→ 库存服务消费 → INVENTORY_RESERVED
→ 支付服务消费 → PAYMENT_COMPLETED
→ 配送服务消费 → SHIPMENT_STARTED
任一步骤失败 → 发布补偿事件回滚
```

**4. Outbox 模式（保证事务性发布）**
```python
# 数据库事务内：
# 1. 写业务数据
# 2. 写 outbox 表（同一事务）
# 独立进程（CDC）：
# 3. 读 outbox 表 → 发布到 Kafka → 标记已发送
# 保证：数据库写成功 ↔ Kafka 消息发布成功
```

**5. Fan-out 模式**
```
一个 Topic → 多个 Consumer Group 独立消费
trades.raw → risk-detector-group（风控）
           → analytics-group（统计）
           → audit-group（审计）
每个 Group 有独立的 Offset，互不影响
```

---

## 附录 B 速查索引

| 题号 | 主题 | 难度 |
|------|------|------|
| 1 | 消息不丢失 | ⭐⭐ |
| 2 | 消息顺序 | ⭐⭐ |
| 3 | Exactly-Once | ⭐⭐⭐ |
| 4 | Consumer Lag | ⭐⭐ |
| 5 | Rebalance | ⭐⭐⭐ |
| 6 | ZooKeeper vs KRaft | ⭐⭐ |
| 7 | Kafka vs RabbitMQ | ⭐⭐ |
| 8 | Log Compaction | ⭐⭐ |
| 9 | 分区数选择 | ⭐⭐⭐ |
| 10 | acks 配置 | ⭐ |
| 11 | 幂等 Producer | ⭐⭐ |
| 12 | 消息大小限制 | ⭐⭐ |
| 13 | 水平扩展 | ⭐⭐⭐ |
| 14 | Schema Registry | ⭐⭐ |
| 15 | 流处理框架选型 | ⭐⭐⭐ |
| 16 | 集群监控指标 | ⭐⭐ |
| 17 | ISR 机制 | ⭐⭐⭐ |
| 18 | Sticky Assignor | ⭐⭐ |
| 19 | 毒丸消息 / DLQ | ⭐⭐⭐ |
| 20 | 微服务设计模式 | ⭐⭐⭐ |

---

## 动手练习

1. **复盘 RiskGuard**：阅读 `project/consumer/risk_detector.py`，找出它处理毒丸消息的代码，理解 DLQ 实现。
2. **模拟 Exactly-Once**：修改 `trade_generator.py`，开启事务 Producer，观察 `risk_detector.py` 的 `isolation.level=read_committed` 效果。
3. **压测**：用 `make produce -- --rate 5000` 大量发送消息，观察 Consumer Lag 变化。

---

*全书完 · RiskGuard 项目源码见 `project/` 目录*
