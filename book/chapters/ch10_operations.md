# 第 10 章：生产级运维与调优

## 本章你将学到

- Kafka 集群硬件选型与规划方法
- 关键 Broker 配置参数的调优逻辑
- 分区扩容的正确姿势（避免 Key 路由破坏）
- 用官方工具进行性能压测（压测命令详解）
- 10 个常见故障的排查 Playbook（Consumer Lag、Broker 不可达、消息丢失等）
- 磁盘空间管理：Log Compaction 与手动清理
- Kafka 滚动升级最佳实践
- 动手：用压测工具测量本地环境的吞吐量上限

---

## 10.1 Kafka 集群规划

### 10.1.1 硬件选型原则

Kafka 的性能瓶颈通常按优先级排列：**磁盘 I/O → 网络带宽 → 内存 → CPU**。

#### 磁盘（最重要）

```
Kafka 是"磁盘友好型"应用：
  - 所有消息持久化到磁盘（这正是 Kafka 高可靠的基础）
  - 大量顺序读写（比随机读写快 100 倍以上）
  - 建议：NVMe SSD 或 SAS HDD（顺序读写速度高）
  - 避免：网络存储（NAS/SAN）—— 延迟太高

磁盘容量估算公式：
  每日数据量 = 消息速率(msg/s) × 平均消息大小(bytes) × 86400
  总容量 = 每日数据量 × 保留天数 × 副本数 × 1.2（留 20% 余量）

示例：
  消息速率 = 100,000 msg/s
  平均消息大小 = 1 KB
  保留天数 = 7 天
  副本数 = 3
  
  每日数据量 = 100,000 × 1024 × 86400 ÷ 1024^3 ≈ 8.06 TB/天
  总容量 = 8.06 × 7 × 3 × 1.2 ≈ 203 TB（需要分布在多个 Broker）
```

**RAID 配置建议**：

```
Kafka 不建议使用 RAID：
  - Kafka 本身通过副本（replication）提供冗余
  - RAID 1/5/6 会降低写性能
  - 推荐：多块独立磁盘 + num.io.threads 调优
  
  但如果只有少量磁盘：
  → RAID 0（条带化）可提升顺序读写速度，但无冗余
  → 依赖 Kafka 副本来保证数据安全
```

#### 内存

```
Kafka JVM 堆内存（Heap）：
  - 推荐：4 GB ~ 8 GB
  - 过大反而有害（GC 停顿时间增加）
  - Kafka 使用 Page Cache（操作系统文件缓存）加速读写
  - 真正的优化在于给操作系统留足够的内存做 Page Cache
  
  规则：
  总内存 64 GB → JVM Heap 6 GB，其余 58 GB 用于 Page Cache
  
  为什么 Page Cache 很重要？
  Consumer 读取最近的消息时，通常直接从 Page Cache 读取（零拷贝！）
  而不需要磁盘 I/O，速度极快。
```

#### 网络

```
网络带宽估算：
  出站带宽 = 生产速率 × 副本数 × 消费者数量（最坏情况）
  
  示例：
  生产速率 = 1 Gbps
  副本数 = 3（Broker 间复制）
  消费者数量 = 2（两个消费组）
  
  峰值网络需求 = 1 × 3 + 1 × 2 = 5 Gbps
  
  建议：
  - 小规模集群：25 Gbps 网卡（2021年以后的新标准）
  - 大规模集群：100 Gbps 网卡
  - 避免集群 Broker 跨机房（延迟会影响副本同步）
```

#### CPU

```
Kafka 是 I/O 密集型，而非 CPU 密集型：
  - 开启压缩（LZ4/Zstd）时 CPU 使用率会上升
  - 推荐：8~16 核，现代多核处理器即可
  - SSL 加密会增加 CPU 开销（约 10-20%）
  
  如果大量使用压缩：
  - 优先考虑压缩性能好的 CPU
  - 或者让 Producer 负责压缩（Broker 不需要重新压缩）
```

### 10.1.2 集群规模规划

```python
# capacity_planner.py - Kafka 集群容量规划工具
def calculate_cluster_requirements(
    messages_per_second: int,       # 每秒消息数
    avg_message_size_bytes: int,    # 平均消息大小（字节）
    retention_days: int,            # 消息保留天数
    replication_factor: int,        # 副本数
    num_consumers: int,             # 消费者组数量
    disk_per_broker_tb: float,      # 每个 Broker 的磁盘容量（TB）
    network_gbps_per_broker: float, # 每个 Broker 的网络带宽（Gbps）
) -> dict:
    """
    计算 Kafka 集群所需 Broker 数量和配置
    
    Returns:
        包含集群规划建议的字典
    """
    # 每秒数据量（MB/s）
    mb_per_second = messages_per_second * avg_message_size_bytes / 1024 / 1024
    
    # 每日磁盘使用（TB）
    tb_per_day = mb_per_second * 86400 * replication_factor / 1024 / 1024
    
    # 总磁盘需求（含 20% 余量）
    total_disk_tb = tb_per_day * retention_days * 1.2
    
    # 出站网络带宽需求（Gbps）
    # 包括：副本复制 + 消费者读取
    replication_bandwidth = mb_per_second * (replication_factor - 1) * 8 / 1024  # Gbps
    consumer_bandwidth = mb_per_second * num_consumers * 8 / 1024  # Gbps
    total_bandwidth_gbps = replication_bandwidth + consumer_bandwidth
    
    # 基于磁盘计算所需 Broker 数
    brokers_by_disk = max(3, int(total_disk_tb / disk_per_broker_tb) + 1)
    
    # 基于网络计算所需 Broker 数
    brokers_by_network = max(3, int(total_bandwidth_gbps / (network_gbps_per_broker * 0.7)) + 1)
    # 0.7 = 只使用 70% 的网络带宽（留余量）
    
    # 取较大值，并确保是 3 的倍数（便于分区均匀分布）
    min_brokers = max(brokers_by_disk, brokers_by_network)
    recommended_brokers = max(3, min_brokers + (3 - min_brokers % 3) % 3)
    
    return {
        'data_rate_mb_per_second': round(mb_per_second, 2),
        'storage_tb_per_day': round(tb_per_day, 2),
        'total_storage_needed_tb': round(total_disk_tb, 2),
        'network_bandwidth_needed_gbps': round(total_bandwidth_gbps, 2),
        'brokers_needed_by_disk': brokers_by_disk,
        'brokers_needed_by_network': brokers_by_network,
        'recommended_brokers': recommended_brokers,
        'recommended_partitions': recommended_brokers * 4,  # 每个 Broker 4 个分区
    }

# 示例：金融交易系统规划
result = calculate_cluster_requirements(
    messages_per_second=50_000,   # 每秒 5 万条交易
    avg_message_size_bytes=512,   # 每条消息 512 字节
    retention_days=7,
    replication_factor=3,
    num_consumers=5,              # 5 个消费组
    disk_per_broker_tb=10.0,      # 每个 Broker 10 TB 磁盘
    network_gbps_per_broker=25.0, # 25 Gbps 网卡
)

print("=== Kafka 集群规划建议 ===")
for key, value in result.items():
    print(f"  {key}: {value}")
```

---

## 10.2 关键 Broker 配置调优

### 10.2.1 线程配置

```properties
# server.properties - 线程配置

# ——— 网络处理线程 ———
# 负责接收/发送网络请求（不处理业务逻辑）
# 建议：CPU 核心数
num.network.threads=8

# ——— I/O 处理线程 ———
# 负责实际的磁盘 I/O 操作
# 建议：2 × CPU 核心数（I/O 密集型）
num.io.threads=16

# ——— 副本同步线程 ———
# Broker 间同步副本数据的线程数
# 大集群/高吞吐建议调高
num.replica.fetchers=4

# 判断依据：
# num.network.threads 瓶颈：
#   → kafka.network:type=Processor,name=IdlePercent 接近 0%
#   → 增大 num.network.threads
#
# num.io.threads 瓶颈：
#   → kafka.server:type=KafkaRequestHandlerPool,name=RequestHandlerAvgIdlePercent 接近 0%
#   → 增大 num.io.threads
```

### 10.2.2 日志（消息存储）配置

```properties
# server.properties - 消息存储配置

# ——— 日志段（Log Segment）大小 ———
# 每个日志段文件的大小上限
# 较小的值：更频繁的日志段滚动，便于清理，但文件数量更多
# 较大的值：文件数量少，但清理粒度粗
# 推荐：1 GB（默认就是 1 GB）
log.segment.bytes=1073741824

# ——— 消息保留时间 ———
# 超过保留时间的消息将被删除
# 注意：这不是实时删除，Kafka 只在日志段滚动时检查
log.retention.hours=168   # 7 天（默认值，大多数场景合适）
# 或者按字节数限制
# log.retention.bytes=107374182400  # 100 GB per partition

# ——— 日志清理检查间隔 ———
# 后台线程多久检查一次是否有过期的日志段可以删除
log.retention.check.interval.ms=300000  # 每 5 分钟检查

# ——— 日志段提前滚动时间 ———
# 即使日志段未达到 log.segment.bytes，也会在这个时间后强制滚动
log.roll.hours=168  # 7 天后强制滚动
```

### 10.2.3 副本和请求配置

```properties
# server.properties - 副本和请求配置

# ——— 副本同步最大字节数 ———
# Follower 每次从 Leader 同步的最大数据量
# 如果 Producer 发送大消息，需要相应调大
replica.fetch.max.bytes=10485760  # 10 MB（默认 1 MB）
# 注意：这个值必须 >= message.max.bytes

# ——— 最大消息大小 ———
# Broker 接收的单条消息最大大小
# 客户端的 max.request.size 也需要相应调整
message.max.bytes=10485760  # 10 MB

# ——— 副本 Lag 阈值 ———
# Follower 落后 Leader 超过这个时间，会被移出 ISR（同步副本集合）
replica.lag.time.max.ms=10000  # 10 秒（默认值）

# ——— Socket 缓冲区 ———
# 增大 Socket 缓冲区可以提升网络吞吐
socket.send.buffer.bytes=102400    # 100 KB
socket.receive.buffer.bytes=102400 # 100 KB
socket.request.max.bytes=104857600 # 100 MB（最大请求大小）
```

### 10.2.4 Producer 端调优

```python
# producer_tuning.py - 不同场景的 Producer 配置
from kafka import KafkaProducer

# ——— 场景一：高吞吐（批量日志、监控指标）———
high_throughput_producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    # 批次大小：等积累到 64 KB 才发送（减少网络往返）
    batch_size=65536,
    # 等待最多 100ms 让批次积累更多消息
    linger_ms=100,
    # 压缩（减少网络带宽和磁盘占用，CPU 换 I/O）
    compression_type='lz4',  # lz4：速度快，推荐高吞吐场景
    # 发送缓冲区：32 MB
    buffer_memory=33554432,
)

# ——— 场景二：低延迟（金融交易、实时控制）———
low_latency_producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    # 不等待，立即发送
    linger_ms=0,
    # 小批次（减少等待）
    batch_size=16384,
    # 不压缩（压缩会增加 CPU 时间）
    compression_type='none',
    # 发送即确认（acks=1 vs acks=all 的延迟差异在于等待副本同步）
    acks=1,
)

# ——— 场景三：高可靠（金融核心交易，不允许丢失）———
high_reliability_producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    # 等待所有 ISR 副本确认
    acks='all',
    # 开启幂等性（防止重试导致重复消息）
    enable_idempotence=True,
    # 重试次数（幂等性开启后，重试是安全的）
    retries=5,
    # 重试间隔
    retry_backoff_ms=200,
    # 压缩（节省磁盘，对可靠性无影响）
    compression_type='zstd',  # zstd：压缩率高，推荐高可靠场景
)
```

---

## 10.3 分区扩容操作

### 10.3.1 为什么扩分区会破坏 Key 路由

```
问题背景：
  - 消息通过 Key 决定写入哪个分区：partition = hash(key) % num_partitions
  - 如果你用 account_id 作为 Key，同一账户的消息总在同一分区（有序保证）
  
  原始状态（3 个分区）：
    ACC-001 → hash(ACC-001) % 3 = 1 → 分区 1
    ACC-002 → hash(ACC-002) % 3 = 2 → 分区 2
    ACC-003 → hash(ACC-003) % 3 = 0 → 分区 0
  
  扩容后（从 3 → 6 个分区）：
    ACC-001 → hash(ACC-001) % 6 = 4 → 分区 4（!! 路由变了 !!)
    ACC-002 → hash(ACC-002) % 6 = 5 → 分区 5（!! 路由变了 !!)
    ACC-003 → hash(ACC-003) % 6 = 0 → 分区 0（凑巧没变）
  
  后果：
    扩容前的消息在旧分区
    扩容后的消息在新分区
    同一账户的消息分散在不同分区 → 顺序性被破坏！
    Consumer 按分区处理时，同一账户可能被并发处理 → 逻辑错误！
```

### 10.3.2 正确的扩容流程

```
推荐方案：创建新 Topic，数据迁移，切换流量

步骤：
  1. 创建 new-topic（目标分区数）
  2. 启动数据迁移：将 old-topic 中的历史数据写入 new-topic
  3. 将 Producer 切换到写 new-topic（双写过渡期，同时写两个）
  4. 将 Consumer 切换到读 new-topic，等 old-topic 的消费赶上来
  5. 停止写 old-topic，等 old-topic 消费完
  6. 删除 old-topic（或继续保留一段时间作为备份）
```

```bash
# 扩容操作：示例（从 12 分区扩容到 24 分区）

# 如果确实要直接扩分区（接受 Key 路由变化，或没有用 Key）：
kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --alter \
  --topic raw-trades \
  --partitions 24

# 注意：Kafka 只允许增加分区，不允许减少！
# 增加分区后立即查看分区分布
kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --topic raw-trades

# 如果需要重新均衡分区到各 Broker，使用 kafka-reassign-partitions
```

```python
# partition_migration.py - 安全的 Topic 扩容迁移脚本
import json
import time
from kafka import KafkaProducer, KafkaConsumer, TopicPartition
from kafka.admin import KafkaAdminClient, NewTopic

def migrate_topic(
    bootstrap_servers: str,
    old_topic: str,
    new_topic: str,
    new_partitions: int,
    replication_factor: int = 3,
    batch_size: int = 10000,
):
    """
    安全迁移 Topic 数据到新分区数的 Topic
    
    注意：迁移期间需要暂停生产者写入，或接受少量消息在两个 Topic 中
    """
    admin = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
    
    # 创建新 Topic
    print(f"创建新 Topic: {new_topic} ({new_partitions} 分区)")
    admin.create_topics([NewTopic(
        name=new_topic,
        num_partitions=new_partitions,
        replication_factor=replication_factor,
    )])
    
    # 获取旧 Topic 的分区数
    topic_metadata = admin.describe_topics([old_topic])
    old_partitions = len(topic_metadata[0]['partitions'])
    
    # 创建消费者读取旧 Topic 所有分区
    consumer = KafkaConsumer(
        bootstrap_servers=bootstrap_servers,
        group_id=f'migration-{old_topic}-to-{new_topic}',
        auto_offset_reset='earliest',   # 从头开始
        enable_auto_commit=True,
        max_poll_records=batch_size,
    )
    
    # 手动分配所有分区
    partitions = [TopicPartition(old_topic, i) for i in range(old_partitions)]
    consumer.assign(partitions)
    
    # 创建写入新 Topic 的 Producer
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        linger_ms=50,
        batch_size=65536,
        compression_type='lz4',
    )
    
    total_migrated = 0
    
    print(f"开始迁移数据: {old_topic} → {new_topic}")
    
    try:
        while True:
            records = consumer.poll(timeout_ms=5000)
            if not records:
                # 没有新消息，迁移完成
                break
            
            for tp, messages in records.items():
                for msg in messages:
                    # 将消息写入新 Topic（保留原始 key）
                    producer.send(
                        new_topic,
                        key=msg.key,
                        value=msg.value,
                        headers=msg.headers,
                    )
            
            total_migrated += sum(len(msgs) for msgs in records.values())
            
            if total_migrated % 100000 == 0:
                print(f"已迁移 {total_migrated:,} 条消息")
    
    finally:
        producer.flush()
        producer.close()
        consumer.close()
    
    print(f"迁移完成！共迁移 {total_migrated:,} 条消息")
    return total_migrated
```

---

## 10.4 性能压测方法

### 10.4.1 Producer 压测

```bash
# kafka-producer-perf-test.sh 详解

kafka-producer-perf-test.sh \
  --topic perf-test \
  --num-records 1000000 \         # 发送 100 万条消息
  --record-size 1024 \            # 每条消息 1 KB
  --throughput -1 \               # -1 = 不限速（测最大吞吐）
                                  # 或设为正整数限制 msg/s
  --producer-props \
    bootstrap.servers=localhost:9092 \
    acks=1 \                      # acks=1 测写入性能；acks=all 测可靠性+性能
    batch.size=65536 \            # 64 KB 批次
    linger.ms=5 \
    compression.type=lz4

# 输出示例：
# 100000 records sent, 98765.4 records/sec (96.45 MB/sec), 3.2 ms avg latency, 125.0 ms max latency
# ...
# 1000000 records sent, 97654.3 records/sec (95.37 MB/sec), 3.5 ms avg latency
#   Latency distribution:
#   50th percentile latency: 2 ms
#   95th percentile latency: 10 ms  
#   99th percentile latency: 32 ms
#   99.9th percentile latency: 87 ms

# 关键指标解读：
# records/sec    = 吞吐量（越高越好）
# MB/sec         = 带宽利用率
# avg latency    = 平均发送延迟
# max latency    = 最大延迟（峰值）
# P99 latency    = 99% 的消息延迟低于此值（评估尾延迟）
```

**不同场景的压测命令**：

```bash
# ——— 场景 1：测试低延迟配置（金融交易）———
kafka-producer-perf-test.sh \
  --topic perf-latency \
  --num-records 500000 \
  --record-size 512 \       # 小消息（512 字节）
  --throughput 10000 \      # 限速 10,000 msg/s（接近生产速率）
  --producer-props bootstrap.servers=localhost:9092 acks=1 linger.ms=0 batch.size=16384

# 期望结果：avg latency < 5ms，P99 latency < 20ms

# ——— 场景 2：测试最大吞吐（日志收集）———
kafka-producer-perf-test.sh \
  --topic perf-throughput \
  --num-records 5000000 \
  --record-size 1024 \      # 1 KB 消息
  --throughput -1 \         # 不限速
  --producer-props bootstrap.servers=localhost:9092 acks=1 batch.size=65536 linger.ms=20 compression.type=lz4

# 期望结果：吞吐量 > 300 MB/s（现代 NVMe SSD 系统）

# ——— 场景 3：测试高可靠性（核心交易）———
kafka-producer-perf-test.sh \
  --topic perf-reliable \
  --num-records 100000 \
  --record-size 1024 \
  --throughput -1 \
  --producer-props bootstrap.servers=localhost:9092 acks=all min.insync.replicas=2 \
    enable.idempotence=true compression.type=zstd
```

### 10.4.2 Consumer 压测

```bash
# kafka-consumer-perf-test.sh 详解

kafka-consumer-perf-test.sh \
  --bootstrap-server localhost:9092 \
  --topic perf-test \
  --messages 1000000 \        # 消费 100 万条消息
  --fetch-size 1048576 \      # 单次 Fetch 大小：1 MB
  --threads 1 \               # 消费线程数
  --group consumer-perf-group \
  --reporting-interval 5000   # 每 5 秒报告一次进度

# 输出示例：
# start.time, end.time, data.consumed.in.MB, MB.sec, data.consumed.in.nMsg, nMsg.sec, rebalance.time.ms, fetch.time.ms, fetch.MB.sec, fetch.nMsg.sec
# 2024-01-15 10:30:00:000, 2024-01-15 10:30:08:234, 976.56, 118.59, 1000000, 121452.3, 312, 7922, 123.27, 126210.5

# 关键指标：
# MB.sec        = Consumer 吞吐量
# fetch.MB.sec  = Fetch 操作吞吐量（不含 rebalance 时间，更准确）
# rebalance.time.ms = Rebalance 耗时（越短越好）
```

### 10.4.3 端到端延迟测试

```bash
# 测试从 Producer 发送到 Consumer 收到的端到端延迟
kafka-e2e-latency.sh \
  --broker-list localhost:9092 \
  --topic e2e-latency-test \
  --num-messages 10000 \
  --producer-acks 1

# 输出：
# 1000 messages with body size 100 bytes
# Avg latency: 2.47 ms
# 50th percentile latency: 2 ms
# 99th percentile latency: 12 ms
# 99.9th percentile latency: 45 ms
```

---

## 10.5 故障排查 Playbook

### 10.5.1 故障 1：Consumer Lag 持续增长

```
症状：Grafana 显示 Consumer Lag 不断上升，告警触发

排查步骤：

Step 1: 确认 Lag 在增长（而非瞬时尖峰）
────────────────────────────────────────
kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --group your-consumer-group

# 观察 LAG 列：等 30 秒，再次执行，确认 LAG 数字在持续增长

Step 2: 检查 Consumer 是否存活
────────────────────────────────
# 查看 Consumer Group 成员
kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --group your-consumer-group \
  --members

# 如果 MEMBERS 为空 → Consumer 全部宕机
# 如果 MEMBERS 有但 LAG 在增长 → Consumer 处理速度跟不上

Step 3: 检查 Consumer 处理性能
────────────────────────────────
# 查看 Consumer 日志：
# - 有没有大量 WARN/ERROR
# - 处理每条消息是否有明显慢操作（数据库查询、外部 API 调用）
# - 有没有频繁 Rebalance（日志中会有 "Rebalancing..." 字样）

Step 4: 检查生产速率是否突然增加
──────────────────────────────────
# 查看生产速率
kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic raw-trades
# Grafana: kafka_server_brokertopicmetrics_messagesinpersec

Step 5: 解决方案
──────────────────
原因：Consumer 宕机
  → 重启 Consumer 服务
  → 检查 OOM/崩溃原因，修复后重启

原因：处理速度跟不上
  → 增加 Consumer 实例（水平扩展，不超过分区数）
  → 优化 Consumer 处理逻辑（批量处理、异步 I/O）
  → 临时提高处理线程数

原因：消费者频繁 Rebalance
  → 增大 session.timeout.ms（延长心跳超时）
  → 减少每次 poll 的处理量（减小 max.poll.records）
  → 增大 max.poll.interval.ms（允许更长的处理时间）
```

### 10.5.2 故障 2：Broker 不可达

```
症状：Producer/Consumer 报错 "NoBrokersAvailable" 或 "Connection refused"

排查步骤：

Step 1: 检查 Broker 进程是否存活
──────────────────────────────────
# 登录到 Broker 服务器
ssh broker-host
ps aux | grep kafka
# 如果 Kafka 进程不存在 → Broker 崩溃，需要重启

Step 2: 检查 Kafka 端口是否监听
──────────────────────────────────
netstat -tlnp | grep 9092
# 或
ss -tlnp sport = :9092

Step 3: 检查 ZooKeeper 连接（如果使用 ZooKeeper 模式）
──────────────────────────────────────────────────────────
echo "ruok" | nc localhost 2181
# 正常响应：imok
# 无响应：ZooKeeper 故障 → Kafka 无法完成 Leader 选举

Step 4: 检查 Kafka 日志
────────────────────────
tail -f /var/log/kafka/server.log | grep -i "error\|exception\|fatal"
# 常见错误：
# "Not enough replicas in ISR" → ISR 不足，min.insync.replicas 配置问题
# "OutOfMemoryError" → JVM 内存不足
# "No space left on device" → 磁盘满

Step 5: 检查磁盘空间
─────────────────────
df -h /var/kafka/data/
# 如果磁盘使用率 > 95%，Kafka 会拒绝写入！
# 临时解决：删除过期日志段（调小 log.retention.hours 并等待清理）

Step 6: 检查网络连通性
─────────────────────
# 从客户端服务器测试
telnet broker-host 9092
# 或
nc -zv broker-host 9092
```

### 10.5.3 故障 3：消息丢失

```
症状：Producer 报告发送成功，但 Consumer 没有收到某些消息

可能原因和排查：

原因 A：Producer acks 配置不当
──────────────────────────────────
# acks=0：完全不等待确认，发送即忘 → 最易丢消息
# acks=1：只等 Leader 确认，Follower 未同步 Leader 宕机 → 可能丢消息
# acks=all：等 ISR 所有副本确认 → 不丢消息

检查 Producer 配置：
  grep -r "acks" ./config/

解决：改为 acks=all + min.insync.replicas=2 + enable.idempotence=true

原因 B：Consumer 提前提交 Offset
──────────────────────────────────
# enable.auto.commit=true 时，自动定时提交 Offset
# 如果提交后处理失败，消息被标记为已处理但实际未处理
检查 Consumer 配置：
  是否使用了 enable_auto_commit=True？
  处理逻辑是否在 commit 之前已完成？

解决：
  改为手动提交 offset（enable_auto_commit=False）
  确保业务处理成功后才调用 consumer.commit()

原因 C：Topic 数据过期被删除
──────────────────────────────────
kafka-topics.sh --bootstrap-server localhost:9092 \
  --describe --topic raw-trades
# 查看 retention.ms 和 retention.bytes 配置
# 如果消费太慢，消息可能在消费前就被删除了

解决：
  增大 retention.hours（延长保留时间）
  或提高 Consumer 消费速度
```

### 10.5.4 故障 4：消息重复

```
症状：Consumer 处理了相同 trade_id 的消息多次

可能原因和排查：

原因 A：Consumer 重启后重新消费
──────────────────────────────────
# 如果 Consumer 在处理消息后、提交 Offset 前崩溃
# 重启后会从上次提交的 Offset 重新消费 → 重复处理

解决：
  实现幂等消费（Idempotent Consumer）：
  处理前检查 trade_id 是否已处理（用 Redis/数据库记录）

原因 B：Producer 重试导致重复
──────────────────────────────────
检查 Producer 配置：
  retries > 0 且 enable.idempotence=False → 可能重复发送

解决：
  enable_idempotence=True（Producer 幂等性）

Python 实现幂等消费：
```

```python
# idempotent_consumer.py - 幂等消费实现
import redis
from kafka import KafkaConsumer

# 使用 Redis 记录已处理的消息 ID
r = redis.Redis(host='localhost', port=6379)

consumer = KafkaConsumer(
    'raw-trades',
    bootstrap_servers='localhost:9092',
    group_id='idempotent-group',
    enable_auto_commit=False,  # 手动提交
)

for message in consumer:
    trade = message.value
    trade_id = trade.get('trade_id')
    
    # 使用 SETNX（SET if Not eXists）实现幂等检查
    # 如果 trade_id 已存在（处理过），SETNX 返回 False
    if not r.setnx(f"processed:{trade_id}", "1"):
        # 已处理，跳过（但要提交 Offset，避免一直重复）
        consumer.commit()
        continue
    
    # 设置过期时间（避免 Redis 无限增长）
    r.expire(f"processed:{trade_id}", 86400 * 7)  # 7 天过期
    
    try:
        # 处理消息（业务逻辑）
        process_trade(trade)
        # 处理成功后提交 Offset
        consumer.commit()
    except Exception as e:
        # 处理失败，删除 Redis 中的记录（允许重试）
        r.delete(f"processed:{trade_id}")
        raise
```

### 10.5.5 其他常见故障快速参考

```
故障 5：ISR 收缩（ISR Shrinking）
  症状：UnderReplicatedPartitions > 0，Follower 落后
  原因：Follower Broker 过载 / GC 停顿 / 网络抖动
  排查：检查 Follower Broker 的 GC 日志和负载
  解决：增大 replica.lag.time.max.ms 缓解频繁收缩

故障 6：Leader 选举失败
  症状：OfflinePartitionsCount > 0
  原因：所有持有该分区的 Broker 都宕机
  排查：kafka-topics.sh --describe，找 Leader=-1 的分区
  解决：恢复 Broker；或 unclean.leader.election.enable=true（会丢数据！）

故障 7：消费者 Rebalance 风暴
  症状：Consumer 频繁重启，Lag 忽高忽低
  原因：心跳超时 / 处理时间超过 max.poll.interval.ms
  解决：调大 max.poll.interval.ms；减少每次 poll 处理量

故障 8：Producer 发送超时
  症状：producer.send() 阻塞或抛出 TimeoutException
  原因：缓冲区满（生产速率 > 发送速率）
  解决：增大 buffer.memory；调优 batch.size 和 linger.ms

故障 9：ZooKeeper 连接频繁断开
  症状：日志大量 "ZooKeeper session expired"
  原因：ZooKeeper 会话超时，网络问题
  解决：检查网络稳定性；增大 zookeeper.session.timeout.ms

故障 10：磁盘 I/O 成为瓶颈
  症状：Broker 吞吐量上不去，iostat 显示磁盘饱和
  排查：iostat -x 1 | grep -v "^$"，观察 %util > 80%
  解决：升级 SSD；增加数据目录（log.dirs 配置多个磁盘）；减少无关工作负载
```

---

## 10.6 磁盘空间管理

### 10.6.1 Log Compaction（日志压缩）

Log Compaction（日志压缩）是 Kafka 的一种数据保留策略，与基于时间/大小的删除策略不同：

```
删除策略（cleanup.policy=delete）：
  → 超过保留时间或大小的 Segment 被整体删除
  → 消息随时间消失
  
压缩策略（cleanup.policy=compact）：
  → 对于同一个 Key，只保留最新的一条消息
  → 老的相同 Key 的消息被清除（tombstone 机制）
  → 历史数据永久保留（最新状态）
  
压缩示意：
  原始日志：
  [A=1] [B=5] [A=2] [C=3] [A=3] [B=8]
  
  压缩后：
  [A=3] [C=3] [B=8]
  （A 保留最新值 3，B 保留最新值 8，C 只有一条直接保留）
```

**适用 Log Compaction 的场景**：
- KTable 的 changelog topic（Kafka Streams）
- CDC（Change Data Capture）数据库变更日志
- 用户状态 topic（每个用户只需要最新状态）

```properties
# 为 Topic 配置 Log Compaction
kafka-configs.sh --bootstrap-server localhost:9092 \
  --alter \
  --entity-type topics \
  --entity-name user-profiles \
  --add-config 'cleanup.policy=compact,min.cleanable.dirty.ratio=0.1'

# min.cleanable.dirty.ratio：未压缩数据超过 10% 时触发压缩
# 越小 = 越频繁压缩 = 磁盘更干净，但 CPU 开销更大
```

**删除 compacted topic 中的 Key（Tombstone）**：

```python
# tombstone_delete.py - 通过发送 null value 删除 Key
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers='localhost:9092')

# 发送 value=None 的消息作为 tombstone（墓碑消息）
# Compaction 时会删除该 Key 的所有历史记录（包括这条 tombstone）
producer.send(
    'user-profiles',
    key=b'ACC-001',  # 要删除的用户 ID
    value=None,      # None = tombstone，表示删除该 Key
)
producer.flush()
print("已发送 tombstone 消息，ACC-001 将在下次 compaction 后被删除")
```

### 10.6.2 手动磁盘清理

```bash
# 查看各 Topic 的磁盘占用
du -sh /var/kafka/data/*/ | sort -hr | head -20

# 临时快速释放磁盘空间（紧急情况）
# 方法：缩短保留时间触发立即清理
kafka-configs.sh --bootstrap-server localhost:9092 \
  --alter \
  --entity-type topics \
  --entity-name old-logs-topic \
  --add-config 'retention.ms=3600000'  # 改为只保留 1 小时

# 等待 log.retention.check.interval.ms 后（默认 5 分钟），旧数据会被清理
# 清理完成后，恢复原来的保留时间
kafka-configs.sh --bootstrap-server localhost:9092 \
  --alter \
  --entity-type topics \
  --entity-name old-logs-topic \
  --add-config 'retention.ms=604800000'  # 恢复为 7 天

# 彻底删除 Topic（慎用！数据无法恢复！）
kafka-topics.sh --bootstrap-server localhost:9092 \
  --delete \
  --topic unnecessary-old-topic
```

---

## 10.7 Kafka 滚动升级最佳实践

### 10.7.1 滚动升级原则

```
核心原则：一次升级一个 Broker，保证集群始终可用

滚动升级步骤（以 3 个 Broker 集群为例）：

  [B1: old] [B2: old] [B3: old]  ← 升级前
      │
      ▼
  步骤 1: 升级 Broker 1（其余继续服务）
  [B1: new] [B2: old] [B3: old]
      │
      ▼
  步骤 2: 验证 B1 正常，UnderReplicatedPartitions = 0
  步骤 3: 升级 Broker 2
  [B1: new] [B2: new] [B3: old]
      │
      ▼
  步骤 4: 验证 B2 正常
  步骤 5: 升级 Broker 3
  [B1: new] [B2: new] [B3: new]  ← 升级完成
```

### 10.7.2 升级前检查清单

```bash
#!/bin/bash
# pre_upgrade_check.sh - 升级前健康检查

BOOTSTRAP_SERVER="localhost:9092"

echo "=== Kafka 升级前检查 ==="

# 1. 检查 UnderReplicatedPartitions = 0
echo -n "1. 检查 UnderReplicatedPartitions... "
URP=$(kafka-topics.sh --bootstrap-server $BOOTSTRAP_SERVER \
  --describe --under-replicated-partitions 2>/dev/null | wc -l)
if [ "$URP" -eq "0" ]; then
  echo "✅ PASS（0 个分区副本落后）"
else
  echo "❌ FAIL（$URP 个分区有副本落后，请先修复！）"
  exit 1
fi

# 2. 检查 Consumer Lag
echo -n "2. 检查 Consumer Groups... "
LAGGING=$(kafka-consumer-groups.sh --bootstrap-server $BOOTSTRAP_SERVER \
  --list 2>/dev/null | xargs -I{} kafka-consumer-groups.sh \
  --bootstrap-server $BOOTSTRAP_SERVER --describe --group {} 2>/dev/null \
  | awk '$6 > 10000 {print $1, $6}')
if [ -z "$LAGGING" ]; then
  echo "✅ PASS（无高 Lag Consumer Group）"
else
  echo "⚠️  WARNING（以下 Consumer Group Lag > 10000，升级可能加重 Lag）："
  echo "$LAGGING"
fi

# 3. 检查磁盘空间
echo -n "3. 检查磁盘空间... "
DISK_USAGE=$(df -h /var/kafka/data/ | awk 'NR==2{print $5}' | tr -d '%')
if [ "$DISK_USAGE" -lt "80" ]; then
  echo "✅ PASS（磁盘使用率 ${DISK_USAGE}%）"
else
  echo "❌ FAIL（磁盘使用率 ${DISK_USAGE}%，升级期间可能空间不足！）"
  exit 1
fi

echo ""
echo "检查完成！如果全部 PASS，可以开始升级。"
```

### 10.7.3 单个 Broker 升级步骤

```bash
#!/bin/bash
# upgrade_single_broker.sh - 升级单个 Broker 的标准流程
# 用法：./upgrade_single_broker.sh broker-host-1

BROKER_HOST=$1
BOOTSTRAP_SERVER="localhost:9092"

echo "=== 开始升级 Broker: $BROKER_HOST ==="

# Step 1: 优雅关闭 Broker（触发 Leader 迁移）
# controlled.shutdown.enable=true（默认已开启）
echo "Step 1: 优雅关闭 Broker..."
ssh $BROKER_HOST "kafka-server-stop.sh"
echo "等待 Broker 完全关闭（60 秒）..."
sleep 60

# Step 2: 在 Broker 机器上升级软件
echo "Step 2: 升级 Kafka 软件..."
ssh $BROKER_HOST << 'UPGRADE_SCRIPT'
  # 备份旧版本配置
  cp /etc/kafka/server.properties /etc/kafka/server.properties.backup
  
  # 下载新版本（示例：升级到 3.6.1）
  wget https://downloads.apache.org/kafka/3.6.1/kafka_2.13-3.6.1.tgz -O /tmp/kafka.tgz
  tar -xzf /tmp/kafka.tgz -C /opt/
  
  # 更新符号链接
  ln -sfn /opt/kafka_2.13-3.6.1 /opt/kafka
  
  # 恢复配置（重要！）
  cp /etc/kafka/server.properties.backup /etc/kafka/server.properties
UPGRADE_SCRIPT

# Step 3: 启动升级后的 Broker
echo "Step 3: 启动 Broker..."
ssh $BROKER_HOST "kafka-server-start.sh -daemon /etc/kafka/server.properties"
echo "等待 Broker 启动（60 秒）..."
sleep 60

# Step 4: 验证 Broker 加入集群
echo "Step 4: 验证 Broker 状态..."
kafka-broker-api-versions.sh --bootstrap-server $BROKER_HOST:9092

# Step 5: 等待副本同步完成
echo "Step 5: 等待副本同步（UnderReplicatedPartitions 归零）..."
MAX_WAIT=300  # 最多等 5 分钟
WAITED=0
while true; do
  URP=$(kafka-topics.sh --bootstrap-server $BOOTSTRAP_SERVER \
    --describe --under-replicated-partitions 2>/dev/null | wc -l)
  if [ "$URP" -eq "0" ]; then
    echo "✅ Broker $BROKER_HOST 升级完成，副本已全部同步"
    break
  fi
  if [ "$WAITED" -ge "$MAX_WAIT" ]; then
    echo "❌ 等待超时，UnderReplicatedPartitions=$URP，请检查日志"
    exit 1
  fi
  echo "等待中... UnderReplicatedPartitions=$URP（已等待 ${WAITED}s）"
  sleep 10
  WAITED=$((WAITED + 10))
done
```

---

## 10.8 动手练习：压测本地环境

### 目标

测量本地 Docker Kafka 环境的 Producer 和 Consumer 吞吐量上限，理解 Kafka 性能特征。

### 步骤一：创建压测 Topic

```bash
# 创建高分区数的测试 Topic
kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create \
  --topic perf-benchmark \
  --partitions 6 \
  --replication-factor 1  # 本地测试用 1 副本（生产用 3）
```

### 步骤二：Producer 压测系列

```bash
# ——— 测试 1：基准压测（默认配置）———
echo "=== 测试 1: 默认配置基准 ==="
kafka-producer-perf-test.sh \
  --topic perf-benchmark \
  --num-records 500000 \
  --record-size 1024 \
  --throughput -1 \
  --producer-props bootstrap.servers=localhost:9092

# ——— 测试 2：增大批次大小（提升吞吐量）———
echo "=== 测试 2: 大批次配置 ==="
kafka-producer-perf-test.sh \
  --topic perf-benchmark \
  --num-records 500000 \
  --record-size 1024 \
  --throughput -1 \
  --producer-props bootstrap.servers=localhost:9092 batch.size=65536 linger.ms=20

# ——— 测试 3：开启 LZ4 压缩 ———
echo "=== 测试 3: LZ4 压缩 ==="
kafka-producer-perf-test.sh \
  --topic perf-benchmark \
  --num-records 500000 \
  --record-size 1024 \
  --throughput -1 \
  --producer-props bootstrap.servers=localhost:9092 batch.size=65536 linger.ms=20 compression.type=lz4

# ——— 测试 4：高可靠配置（acks=all）———
echo "=== 测试 4: acks=all 可靠性配置 ==="
kafka-producer-perf-test.sh \
  --topic perf-benchmark \
  --num-records 200000 \
  --record-size 1024 \
  --throughput -1 \
  --producer-props bootstrap.servers=localhost:9092 acks=all batch.size=65536 linger.ms=20
```

### 步骤三：Consumer 压测

```bash
# ——— Consumer 单线程压测 ———
echo "=== Consumer 单线程 ==="
kafka-consumer-perf-test.sh \
  --bootstrap-server localhost:9092 \
  --topic perf-benchmark \
  --messages 500000 \
  --group perf-consumer-group-1

# ——— Consumer 多线程压测 ———
echo "=== Consumer 多线程（3 线程）==="
kafka-consumer-perf-test.sh \
  --bootstrap-server localhost:9092 \
  --topic perf-benchmark \
  --messages 500000 \
  --threads 3 \
  --group perf-consumer-group-2
```

### 步骤四：记录结果并分析

```python
# parse_perf_results.py - 解析并汇总压测结果
# 手动将压测输出粘贴到下面的字典中

results = {
    "测试1_默认配置": {
        "records_per_sec": 85432,
        "mb_per_sec": 83.4,
        "avg_latency_ms": 12.3,
        "p99_latency_ms": 45.2,
    },
    "测试2_大批次": {
        "records_per_sec": 156789,
        "mb_per_sec": 153.1,
        "avg_latency_ms": 18.5,
        "p99_latency_ms": 52.1,
    },
    "测试3_LZ4压缩": {
        "records_per_sec": 143210,
        "mb_per_sec": 139.9,
        "avg_latency_ms": 20.1,
        "p99_latency_ms": 58.3,
    },
    "测试4_acks_all": {
        "records_per_sec": 52341,
        "mb_per_sec": 51.1,
        "avg_latency_ms": 35.6,
        "p99_latency_ms": 120.4,
    },
}

print("=" * 60)
print(f"{'配置':<20} {'吞吐(msg/s)':>12} {'吞吐(MB/s)':>10} {'P99延迟(ms)':>12}")
print("-" * 60)
for name, r in results.items():
    print(f"{name:<20} {r['records_per_sec']:>12,} {r['mb_per_sec']:>10.1f} {r['p99_latency_ms']:>12.1f}")
print("=" * 60)

# 分析最优配置
best_throughput = max(results.items(), key=lambda x: x[1]['mb_per_sec'])
best_latency = min(results.items(), key=lambda x: x[1]['p99_latency_ms'])
print(f"\n最高吞吐量: {best_throughput[0]} ({best_throughput[1]['mb_per_sec']} MB/s)")
print(f"最低 P99 延迟: {best_latency[0]} ({best_latency[1]['p99_latency_ms']} ms)")
```

### 练习扩展挑战

1. **基础**：测量不同消息大小（100B / 1KB / 10KB / 1MB）对吞吐量和延迟的影响
2. **进阶**：对比 acks=1 和 acks=all 在不同 linger.ms 设置下的吞吐量曲线，找到最佳平衡点
3. **挑战**：编写自动化压测脚本，测试 1~12 个 Consumer 线程的 Scale-Out 效果（预期：随线程增加，吞吐量线性提升，直到达到分区数限制）

---

## 本章小结

本章涵盖了 Kafka 生产运维的全貌：

| 主题 | 核心要点 |
|------|---------|
| 硬件规划 | 磁盘 I/O 是瓶颈；Page Cache 比 JVM 堆更重要 |
| 线程调优 | num.network.threads ≈ CPU 核数；num.io.threads ≈ 2×CPU 核数 |
| 分区扩容 | 直接增加分区会破坏 Key 路由；推荐创建新 Topic 迁移 |
| 压测 | kafka-producer-perf-test.sh 和 kafka-consumer-perf-test.sh |
| 故障排查 | Consumer Lag 首先检查进程存活和处理速度 |
| Log Compaction | cleanup.policy=compact 用于 KTable changelog |
| 滚动升级 | 一次升级一个 Broker，等 UnderReplicatedPartitions=0 再继续 |

---

## 全书回顾

恭喜你完成了《Kafka 最佳实践实战》的学习！让我们回顾这 10 章覆盖的知识体系：

```
第 1-2 章：Kafka 基础与架构
  → Topic、Partition、Broker、Producer、Consumer 的核心概念
  
第 3-4 章：Producer 与 Consumer 最佳实践
  → acks、幂等性、Consumer Group、Offset 管理
  
第 5-6 章：可靠性与性能调优
  → 消息不丢失的三要素；延迟 vs 吞吐量的平衡
  
第 7 章：Kafka Streams 流处理（本书）
  → KStream/KTable、窗口类型、faust Python 实现
  
第 8 章：监控与可观测性（本书）
  → Prometheus + Grafana、Consumer Lag 告警
  
第 9 章：安全与认证（本书）
  → TLS + SASL + ACL 三层安全体系
  
第 10 章：生产级运维与调优（本书）
  → 硬件规划、故障排查 Playbook、滚动升级
```

**下一步学习建议**：
- 实战：将本书的交易系统示例完整部署到云环境（AWS MSK 或 Confluent Cloud）
- 深入：探索 Kafka 3.x 的 KRaft 模式（去 ZooKeeper 化）
- 拓展：学习 Schema Registry（Avro/Protobuf 消息格式管理）
- 进阶：研究 Kafka Connect 生态（连接各种数据源和目标系统）
