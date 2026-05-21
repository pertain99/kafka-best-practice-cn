# 第 5 章：Topic 设计与分区策略

## 本章你将学到

- Topic 命名规范及其背后的设计逻辑
- 如何科学计算分区数（不靠直觉，靠公式）
- Replication Factor（副本因子）的选择原则
- 消息保留策略：时间保留、大小保留、Log Compaction 的适用场景
- 大消息的处理方案（Claim-Check 模式）
- Topic 配置速查表（生产就绪的参数清单）

---

## 5.1 Topic 命名规范

命名是工程文化的体现。随着系统规模增长，一个规范的 Topic 命名体系能让你在几百个 Topic 中迅速定位目标，也能帮助新人快速理解数据架构。

### 推荐格式

```
{env}.{domain}.{entity}.{version}
```

| 字段 | 含义 | 示例值 |
|------|------|--------|
| `env` | 环境标识 | `prod`、`staging`、`dev`、`qa` |
| `domain` | 业务域 | `trading`、`risk`、`auth`、`payment` |
| `entity` | 数据实体（名词，复数） | `trades`、`alerts`、`orders`、`users` |
| `version` | Schema 版本 | `v1`、`v2` |

### 真实示例

```
# 生产环境交易数据
prod.trading.trades.v1

# 生产环境风险告警
prod.risk.alerts.v1

# 开发环境风险告警
dev.risk.alerts.v1

# Staging 环境用户认证事件
staging.auth.login-events.v1

# 死信队列（DLQ 后缀）
prod.trading.trades.v1.dlq

# 内部重试 Topic（RETRY 后缀，可选）
prod.trading.trades.v1.retry
```

### 命名规则细则

```
✅ 推荐：
  - 全部小写（避免大小写混淆问题）
  - 使用 . 或 - 作为分隔符（选一个，全项目统一）
  - 实体用名词复数（trades, alerts, users）
  - 版本号从 v1 开始

❌ 避免：
  - 使用下划线 _ 作为分隔符（与某些监控系统的指标名冲突）
  - 在 Topic 名中包含时间戳或日期（数据时间属于消息体，不属于 Topic 名）
  - 太模糊的名字（如 data, events, messages）
  - 太长的名字（Kafka Topic 名最大 249 字符，但实践中 < 80 字符为佳）
```

### 如何做 Topic 版本升级

```
prod.trading.trades.v1  →  prod.trading.trades.v2

升级策略（双写过渡）:
1. 保持 v1 Topic 继续运行（旧 Consumer 不受影响）
2. 修改 Producer，同时写入 v1 和 v2 Topic
3. 逐步将 Consumer 迁移到 v2
4. 所有 Consumer 迁移完成后，停止 v1 Topic 的写入
5. 等待 v1 Topic 的消息过期后，删除 v1 Topic

迁移期间的双写配置示例:
```

```python
def produce_trade(producer, trade_data, enable_v2=True):
    """双写：同时发送到 v1 和 v2 Topic（迁移期间使用）"""
    value = json.dumps(trade_data).encode()
    key = trade_data['trade_id'].encode()
    
    # 始终写入 v1（保持向后兼容）
    producer.produce(topic='prod.trading.trades.v1', key=key, value=value)
    
    # 同时写入 v2（新格式，含额外字段）
    if enable_v2:
        v2_data = {**trade_data, 'schema_version': '2', 'new_field': 'new_value'}
        producer.produce(
            topic='prod.trading.trades.v2',
            key=key,
            value=json.dumps(v2_data).encode()
        )
    
    producer.flush()
```

---

## 5.2 分区数如何计算

### 分区的本质

每个分区是 Kafka 并行处理的基本单元：
- 写入：同一 Key 的消息总是路由到同一分区（顺序保证）
- 读取：一个分区只能被同一 Consumer Group 内的一个 Consumer 消费

**分区数 = 最大并行度上限。** 超过分区数的 Consumer 实例永远空转。

### 计算公式

```
分区数 = max(目标吞吐量 / 单分区吞吐量, Consumer 实例数)
```

**拆解：**

```
假设场景：
  目标吞吐量（Producer）: 300 MB/s
  单分区写入吞吐量: 30 MB/s（取决于消息大小、网络、存储）
  Consumer 实例数: 20

计算：
  基于吞吐量: 300 / 30 = 10 个分区
  基于 Consumer: 20 个实例

结论: max(10, 20) = 20 个分区
（如果用 10 个分区，10 个 Consumer 会空转浪费）
```

### 单分区典型吞吐量参考

| 消息大小 | 单分区写入吞吐 | 单分区读取吞吐 |
|---------|-------------|-------------|
| 1 KB    | 10 MB/s     | 30 MB/s     |
| 10 KB   | 30 MB/s     | 60 MB/s     |
| 100 KB  | 60 MB/s     | 100 MB/s    |
| 1 MB    | 80 MB/s     | 120 MB/s    |

> 读取比写入快，因为读取可以利用操作系统的 Page Cache（零拷贝技术）。

### 实战计算示例：RiskGuard 项目

```
需求：
  每秒产生 50,000 笔交易
  每条交易消息约 500 字节
  每天消息量：50,000 × 86,400 = 43.2 亿条
  
计算吞吐量：
  写入速率 = 50,000 条/秒 × 500 字节 = 25 MB/s
  
单分区吞吐（500B 消息，约 10-15 MB/s）:
  保守估计 10 MB/s
  
基于吞吐量的分区数：
  25 / 10 = 2.5 → 向上取整 = 3 个分区
  
Consumer 实例数规划：
  每个 Consumer 处理 5,000 条/秒
  需要 Consumer 数 = 50,000 / 5,000 = 10 个实例
  
最终分区数：
  max(3, 10) = 10 个分区
  
加上扩容余量（2x）：
  10 × 2 = 20 个分区（推荐）
```

### 分区数的黄金法则

```
1. 宁多勿少：分区数只能增加，不能减少！
   （减少分区会导致 Key 路由关系改变，破坏顺序保证）

2. 不要盲目设大：
   每个分区在 Broker 上是一个独立的文件系统目录
   100 个 Topic × 1000 分区 × 3 副本 = 300,000 个目录！
   过多分区会增加 Broker 内存压力和 Failover 时间

3. 初始值推荐：
   小系统（< 100 MB/s）：6-12 个分区
   中等系统（100-500 MB/s）：20-50 个分区
   大型系统（> 500 MB/s）：50-200 个分区

4. 分区增加后 Key 路由会变：
   原来 key="user-123" → 路由到 P0
   增加分区后 key="user-123" → 可能路由到 P7
   → 同一 Key 的历史消息和新消息在不同分区，顺序无法保证！
   → 需要通知所有相关方，或使用自定义分区器
```

### 增加分区的操作

```bash
# 查看当前分区数
kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --topic prod.trading.trades.v1

# 增加分区（从 6 增加到 20）
kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --alter \
  --topic prod.trading.trades.v1 \
  --partitions 20

# 验证
kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --topic prod.trading.trades.v1
```

---

## 5.3 Replication Factor（副本因子）选择

### 副本机制

每个分区有一个 Leader 副本和多个 Follower 副本。

```
Topic: prod.trading.trades.v1（3 副本，6 分区）

Broker-1: P0-Leader, P1-Follower, P5-Follower
Broker-2: P1-Leader, P2-Follower, P0-Follower
Broker-3: P2-Leader, P3-Follower, P1-Follower
Broker-4: P3-Leader, P4-Follower, P2-Follower
Broker-5: P4-Leader, P5-Follower, P3-Follower
Broker-6: P5-Leader, P0-Follower, P4-Follower

Producer 写入 → Leader（同步到 Follower）
Consumer 读取 → Leader（或启用 Follower 读取）
```

### 副本因子的选择原则

| 环境 | 副本因子 | 理由 |
|------|---------|------|
| 开发 (dev) | 1 | 节省资源，数据丢了重来 |
| 测试 (staging/qa) | 2 | 容忍 1 个 Broker 故障 |
| **生产 (prod)** | **3（最小值）** | 容忍 1 个 Broker 故障同时进行维护 |
| 关键金融数据 | 3-5 | 高可用要求极高 |

> **为什么生产至少需要 3 个副本，而不是 2 个？**
>
> 假设有 2 个副本，Broker-1（Leader）故障时，Broker-2（Follower）变成新 Leader。此时如果需要对 Broker-2 进行滚动升级或维护，整个系统只剩 1 个副本——任何故障都会导致数据丢失。
>
> 3 个副本提供了"在一个 Broker 正在故障时，还能容忍另一个 Broker 进行计划维护"的能力。

### `min.insync.replicas` 与 `acks=all` 配合

这是 Kafka 数据安全的黄金组合：

```python
# Producer 侧：acks=all
producer_config = {
    'acks': 'all',   # 等待所有 ISR（In-Sync Replicas）确认
    # 或等价写法：
    # 'acks': '-1',
}

# Topic 侧：min.insync.replicas
kafka-configs.sh \
  --bootstrap-server localhost:9092 \
  --entity-type topics \
  --entity-name prod.trading.trades.v1 \
  --alter \
  --add-config min.insync.replicas=2
```

**工作原理：**

```
replication.factor = 3（3 个副本）
min.insync.replicas = 2（至少 2 个副本同步完成才算写入成功）

正常情况（3 个 Broker 都在线）：
  Producer 写入 → Leader + 2 个 Follower 确认 → 成功
  即使 1 个 Broker 故障，仍有 2 个副本 → 系统正常

极端情况（2 个 Broker 故障，只剩 1 个）：
  可用副本 (1) < min.insync.replicas (2)
  → Producer 写入失败，抛出 NotEnoughReplicasException
  → 这是正确行为：宁可拒绝写入，也不接受可能丢失的数据

危险配置（不要这样做）：
  min.insync.replicas = 1 + acks = all
  → 等价于 acks=1，没有额外保护
```

### ISR（In-Sync Replicas）管理

```bash
# 查看 Topic 的 ISR 状态
kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --topic prod.trading.trades.v1

# 输出示例：
# Topic: prod.trading.trades.v1 Partition: 0 Leader: 1 Replicas: 1,2,3 Isr: 1,2,3
# Topic: prod.trading.trades.v1 Partition: 1 Leader: 2 Replicas: 2,3,1 Isr: 2,3
#                                                                           ^^^
#                                           Broker-1 落后太多，被踢出 ISR！

# 如果某分区 ISR < min.insync.replicas，写入会阻塞
# 需要立即排查 Broker-1 的状态
```

---

## 5.4 消息保留策略

### 三种保留模式

Kafka 支持三种消息保留策略，可以组合使用。

#### 模式1：时间保留（`retention.ms`）

```bash
# 默认：保留 7 天（604800000 ms）
# 超过时间的消息，在下次 Segment 清理时被删除

kafka-configs.sh \
  --bootstrap-server localhost:9092 \
  --entity-type topics \
  --entity-name prod.trading.trades.v1 \
  --alter \
  --add-config 'retention.ms=604800000'  # 7 天

# 常用时间配置
retention.ms=3600000    # 1 小时（实时分析 Topic）
retention.ms=86400000   # 1 天
retention.ms=604800000  # 7 天（默认）
retention.ms=2592000000 # 30 天（合规审计）
retention.ms=-1         # 永不删除（Log Compaction Topic）
```

**适用场景：**
- 流式处理 Topic（保留足够的时间窗口供下游处理即可）
- 事件日志（7-30 天）
- 指标数据（1-7 天）

#### 模式2：大小保留（`retention.bytes`）

```bash
# 按存储大小限制，超过后删除最老的 Segment
# 注意：retention.bytes 是 per-partition 的！

# 单分区最大 1 GB
kafka-configs.sh \
  --bootstrap-server localhost:9092 \
  --entity-type topics \
  --entity-name prod.metrics.v1 \
  --alter \
  --add-config 'retention.bytes=1073741824'  # 1 GB per partition

# 如果 Topic 有 20 个分区，总最大存储 = 20 × 1GB = 20 GB
```

**时间 + 大小保留同时配置：** 满足任一条件即触发清理（取先到者）

```bash
# 7 天 OR 超过 10GB（per partition），取先到者
--add-config 'retention.ms=604800000,retention.bytes=10737418240'
```

#### 模式3：Log Compaction（日志压缩，按 Key 保留最新值）

Log Compaction（日志压缩）是 Kafka 最独特的功能之一，适合"状态表"类型的数据。

**核心思想：** 对于同一个 Key，只保留最新的一条消息，删除历史版本。

```
原始消息流:
  Key=user-123, value={"status": "ACTIVE"}     offset=100
  Key=user-456, value={"status": "ACTIVE"}     offset=101
  Key=user-123, value={"status": "SUSPENDED"}  offset=200
  Key=user-789, value={"status": "ACTIVE"}     offset=201
  Key=user-123, value={"status": "ACTIVE"}     offset=350  ← 最新

Log Compaction 后:
  Key=user-456, value={"status": "ACTIVE"}     offset=101  ← 只有 1 条记录的 Key，保留
  Key=user-789, value={"status": "ACTIVE"}     offset=201
  Key=user-123, value={"status": "ACTIVE"}     offset=350  ← 只保留最新值

效果：任何时候从头消费这个 Topic，都能重建出完整的当前状态快照
```

**适用场景：**
- 用户状态表（账户余额、订阅状态）
- 配置变更历史（只关心最新配置）
- 数据库 CDC（Change Data Capture）变更日志
- 任何"最终状态"胜过"完整历史"的场景

**配置 Log Compaction：**

```bash
# 创建 Compacted Topic
kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create \
  --topic prod.trading.account-balances.v1 \
  --partitions 12 \
  --replication-factor 3 \
  --config cleanup.policy=compact \
  --config min.cleanable.dirty.ratio=0.5 \
  --config segment.ms=86400000  # 每天滚动一个 Segment

# 或者修改现有 Topic
kafka-configs.sh \
  --bootstrap-server localhost:9092 \
  --entity-type topics \
  --entity-name prod.trading.account-balances.v1 \
  --alter \
  --add-config 'cleanup.policy=compact'
```

**删除墓碑（Tombstone）：** 发送 value=null 的消息表示删除某个 Key

```python
# 删除 user-123 的账户余额记录
producer.produce(
    topic='prod.trading.account-balances.v1',
    key='user-123'.encode(),
    value=None,  # value=null → 触发删除（Tombstone 消息）
)
producer.flush()

# Kafka 会先保留这条 null 消息（让 Consumer 知道删除发生了）
# 一段时间后（delete.retention.ms，默认 24 小时）才真正删除
```

---

## 5.5 消息大小最佳实践

### Kafka 的消息大小限制

```
默认最大消息大小：1 MB
  Broker 配置：message.max.bytes=1048576
  Topic 配置：max.message.bytes=1048576
  Producer 配置：max.request.size=1048576

Consumer 配置：fetch.max.bytes=52428800（50 MB，单次 fetch）
              max.partition.fetch.bytes=1048576（1 MB，单分区单次）
```

### 为什么要限制消息大小？

```
1. 内存压力：大消息占用 Broker 和 Consumer 的内存
2. 网络延迟：大消息的传输时间更长，影响 P99 延迟
3. Replication 放大：3 副本 × 10 MB 消息 = 30 MB 网络 I/O
4. GC 压力：JVM 堆中的大对象触发 Full GC
5. 分区不均衡：某些分区因大消息占用更多存储
```

### 处理大消息的方案

#### 方案1：消息压缩（最简单）

```python
# Producer 侧启用压缩
producer_config = {
    'compression.type': 'lz4',   # 推荐：lz4（速度快）或 snappy（压缩率高）
    # 其他选项：gzip（最高压缩率，CPU 开销大）、zstd（均衡）
}

# 压缩效果（JSON 消息）：
# 原始大小：500 KB
# lz4 压缩：~150 KB（70% 压缩率）
# gzip 压缩：~80 KB（84% 压缩率）
```

#### 方案2：Claim-Check 模式（推荐处理超大消息）

Claim-Check 模式（行李牌模式）：将大消息的实际内容存储在外部系统（S3、HDFS、数据库），Kafka 消息只传递一个"提取票据"（Claim-Check）。

```
传统方式（消息太大）:
  Producer → Kafka（消息 = 完整大文件，50 MB）

Claim-Check 模式:
  Producer → S3（上传大文件）→ 获取 S3 URL
           → Kafka（消息 = S3 URL 引用，< 1 KB）
  Consumer ← Kafka（获取 S3 URL）
           → S3（下载大文件，按需读取）
```

```python
import boto3
import uuid
import json

class ClaimCheckProducer:
    """
    Claim-Check 模式 Producer
    
    大消息（> 阈值）存储到 S3，Kafka 消息只包含引用
    小消息直接内嵌到 Kafka 消息中
    """
    
    SIZE_THRESHOLD = 100 * 1024  # 100 KB 以上使用 Claim-Check
    
    def __init__(self, bootstrap_servers, s3_bucket):
        from confluent_kafka import Producer
        self.producer = Producer({'bootstrap.servers': bootstrap_servers})
        self.s3_client = boto3.client('s3')
        self.s3_bucket = s3_bucket
    
    def produce(self, topic: str, key: str, value: dict):
        """智能投递：根据消息大小决定是否使用 Claim-Check"""
        raw_value = json.dumps(value).encode('utf-8')
        
        if len(raw_value) <= self.SIZE_THRESHOLD:
            # 小消息：直接内嵌
            kafka_message = {
                'type': 'inline',
                'payload': value
            }
        else:
            # 大消息：上传到 S3，Kafka 只传引用
            s3_key = f"kafka-payloads/{topic}/{uuid.uuid4()}.json"
            self.s3_client.put_object(
                Bucket=self.s3_bucket,
                Key=s3_key,
                Body=raw_value,
                ContentType='application/json',
            )
            kafka_message = {
                'type': 'claim-check',      # 标记为 Claim-Check 类型
                's3_bucket': self.s3_bucket,
                's3_key': s3_key,
                'size_bytes': len(raw_value),
                'uploaded_at': time.time(),
            }
        
        # Kafka 消息始终 < 1 KB
        self.producer.produce(
            topic=topic,
            key=key.encode(),
            value=json.dumps(kafka_message).encode(),
        )
        self.producer.flush()


class ClaimCheckConsumer:
    """
    Claim-Check 模式 Consumer
    
    自动解引用：收到 claim-check 类型消息时，从 S3 下载实际内容
    """
    
    def __init__(self, bootstrap_servers, group_id, s3_region='us-east-1'):
        from confluent_kafka import Consumer
        self.consumer = Consumer({
            'bootstrap.servers': bootstrap_servers,
            'group.id': group_id,
            'enable.auto.commit': False,
        })
        self.s3_client = boto3.client('s3', region_name=s3_region)
    
    def resolve(self, kafka_message) -> dict:
        """
        解引用：将 Kafka 消息转换为实际内容
        
        Returns:
            实际的消息内容（无论是 inline 还是 claim-check）
        """
        envelope = json.loads(kafka_message.value())
        
        if envelope['type'] == 'inline':
            return envelope['payload']
        
        elif envelope['type'] == 'claim-check':
            # 从 S3 下载实际内容
            response = self.s3_client.get_object(
                Bucket=envelope['s3_bucket'],
                Key=envelope['s3_key'],
            )
            return json.loads(response['Body'].read())
        
        else:
            raise ValueError(f"未知消息类型: {envelope['type']}")
```

**Claim-Check 模式的代价：**
- 额外的 S3 读写延迟（几十毫秒）
- S3 存储成本
- 消息的生命周期管理（何时清理 S3 对象）

**建议：** 消息大小 > 100 KB 再考虑 Claim-Check；< 100 KB 通常用压缩解决。

---

## 5.6 Topic 配置速查表

### 通用参数

| 参数 | 默认值 | 推荐值（生产） | 说明 |
|------|--------|-------------|------|
| `replication.factor` | 1 | **3** | 副本数（生产最小值） |
| `min.insync.replicas` | 1 | **2** | 最小同步副本数（配合 `acks=all`） |
| `num.partitions` | 1 | 按公式计算 | 分区数 |

### 数据保留参数

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `retention.ms` | 604800000（7天） | 按时间保留消息 |
| `retention.bytes` | -1（不限制） | 按大小保留消息（per partition） |
| `cleanup.policy` | delete | `delete`（按时间/大小删除）或 `compact`（Log Compaction） |
| `delete.retention.ms` | 86400000（1天） | Compacted Topic 中 Tombstone 的保留时间 |
| `min.cleanable.dirty.ratio` | 0.5 | Compaction 触发阈值（dirty logs / total logs） |

### 性能参数

| 参数 | 默认值 | 推荐值 | 说明 |
|------|--------|--------|------|
| `segment.bytes` | 1073741824（1GB） | 1GB | 单个 Segment 文件大小 |
| `segment.ms` | 604800000（7天） | 86400000（1天） | Segment 最大时间（超时强制滚动） |
| `max.message.bytes` | 1048576（1MB） | 1MB（或根据需要） | 单条消息最大大小 |
| `compression.type` | producer | lz4 | Broker 存储时的压缩格式 |

### 可靠性参数

| 参数 | 默认值 | 推荐值 | 说明 |
|------|--------|--------|------|
| `unclean.leader.election.enable` | false | **false** | 禁止非 ISR 成员成为 Leader（防数据丢失） |
| `replica.lag.time.max.ms` | 30000 | 30000 | Follower 落后超过此时间被踢出 ISR |

### 按场景的推荐配置

#### 场景A：实时交易事件（高可靠，低延迟）

```bash
kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create \
  --topic prod.trading.trades.v1 \
  --partitions 20 \
  --replication-factor 3 \
  --config min.insync.replicas=2 \
  --config retention.ms=2592000000 \   # 30 天（合规要求）
  --config compression.type=lz4 \
  --config max.message.bytes=1048576 \
  --config unclean.leader.election.enable=false
```

#### 场景B：监控指标 Topic（高吞吐，允许丢失）

```bash
kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create \
  --topic prod.monitoring.metrics.v1 \
  --partitions 50 \
  --replication-factor 2 \             # 监控数据允许较低可靠性
  --config retention.ms=3600000 \      # 1 小时（指标很快过期）
  --config retention.bytes=1073741824 \# 每分区 1GB 上限
  --config compression.type=snappy
```

#### 场景C：账户状态表（Log Compaction）

```bash
kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create \
  --topic prod.trading.account-balances.v1 \
  --partitions 12 \
  --replication-factor 3 \
  --config cleanup.policy=compact \
  --config min.insync.replicas=2 \
  --config min.cleanable.dirty.ratio=0.5 \
  --config segment.ms=86400000 \       # 每天滚动 Segment，加速 Compaction
  --config delete.retention.ms=86400000 # Tombstone 保留 1 天
```

---

## 动手练习

### 练习目标

为 RiskGuard 项目设计完整的 Topic 配置。

**背景：** RiskGuard 是一个加密货币交易风险管理系统，需要以下数据流：

1. **交易事件流** - 实时交易数据，每秒约 50,000 条，约 500 字节/条
2. **风险告警流** - 触发风险规则时产生，峰值约 5,000 条/秒
3. **账户余额快照** - 账户实时余额，按账户 ID 保留最新值
4. **合规审计日志** - 监管要求，保留 90 天，不允许数据丢失

### 设计任务

完成以下 Topic 的配置设计，并给出理由：

```bash
# Topic 1: 交易事件流
# 分区数计算：目标吞吐 = ? MB/s，Consumer 实例数 = ?
# 副本因子选择：?
# 保留策略：?

# Topic 2: 风险告警流
# 分区数计算：...

# Topic 3: 账户余额快照
# cleanup.policy: compact
# 其他参数？

# Topic 4: 合规审计日志
# 特殊要求：不丢数据，保留 90 天
```

### 参考答案

```bash
# Topic 1: 交易事件流
# 吞吐量 = 50,000 × 500B = 25 MB/s
# 单分区 ≈ 10 MB/s → 需要至少 3 个分区
# Consumer 规划：每实例处理 5,000 条/秒 → 10 个实例
# 分区数 = max(3, 10) × 2(余量) = 20
kafka-topics.sh --bootstrap-server localhost:9092 --create \
  --topic prod.trading.trades.v1 \
  --partitions 20 \
  --replication-factor 3 \
  --config min.insync.replicas=2 \
  --config retention.ms=2592000000 \
  --config compression.type=lz4

# Topic 2: 风险告警流
# 吞吐量 = 5,000 × 500B = 2.5 MB/s（较低）
# Consumer = 5 个实例
# 分区数 = max(1, 5) = 6（向上取 2^n 的习惯）
kafka-topics.sh --bootstrap-server localhost:9092 --create \
  --topic prod.risk.alerts.v1 \
  --partitions 6 \
  --replication-factor 3 \
  --config min.insync.replicas=2 \
  --config retention.ms=604800000

# Topic 3: 账户余额快照（Log Compaction）
kafka-topics.sh --bootstrap-server localhost:9092 --create \
  --topic prod.trading.account-balances.v1 \
  --partitions 12 \
  --replication-factor 3 \
  --config cleanup.policy=compact \
  --config min.insync.replicas=2 \
  --config segment.ms=86400000

# Topic 4: 合规审计日志（强一致，90 天保留）
kafka-topics.sh --bootstrap-server localhost:9092 --create \
  --topic prod.compliance.audit-logs.v1 \
  --partitions 6 \
  --replication-factor 3 \
  --config min.insync.replicas=2 \
  --config retention.ms=7776000000 \
  --config unclean.leader.election.enable=false \
  --config compression.type=gzip
```

### 加分挑战

- **分区数影响实验：** 创建一个 6 分区的 Topic，使用固定 Key 发送 1000 条消息，记录每个分区的消息分布。然后增加到 12 个分区，再发送 1000 条，观察 Key 路由的变化。
- **Log Compaction 观察：** 对同一 Key 发送 10 条不同 value 的消息到 Compacted Topic，等待 Compaction 后（触发条件：`min.cleanable.dirty.ratio`），用 `kafka-console-consumer --from-beginning` 观察最终只保留最新值。
- **存储成本计算：** 假设 `prod.trading.trades.v1` 每天产生 50GB 数据，3 副本，保留 30 天，计算总存储成本（AWS S3 价格：$0.023/GB-month）。

---

## 本章小结

| 设计决策 | 推荐方案 |
|---------|---------|
| Topic 命名 | `{env}.{domain}.{entity}.{version}` |
| 分区数计算 | `max(目标吞吐 / 单分区吞吐, Consumer 实例数) × 余量系数` |
| 分区数原则 | 只能增加，不能减少；不要盲目设大 |
| 生产副本因子 | 3（最小值） |
| 数据安全组合 | `replication.factor=3` + `min.insync.replicas=2` + `acks=all` |
| 时间保留 | 根据业务需求（交易: 30天，日志: 7天，指标: 1-24小时） |
| 状态表场景 | `cleanup.policy=compact`（Log Compaction） |
| 大消息处理 | 压缩（< 10MB）或 Claim-Check 模式（> 10MB） |
