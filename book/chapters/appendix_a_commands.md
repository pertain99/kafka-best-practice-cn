# 附录 A：Kafka 常用命令速查

> 这是你日常运维 Kafka 集群的"瑞士军刀"。所有命令都基于 Confluent Platform 7.x 和原生 Apache Kafka。

**说明**：
- `BROKER` 通常为 `localhost:9092`（单机开发）或 `kafka-1:9092,kafka-2:9092,kafka-3:9092`（生产集群）
- `--bootstrap-server` 是新版推荐参数（替代已废弃的 `--zookeeper`）
- KRaft 模式无需 ZooKeeper 相关命令

---

## A.1 Topic 管理

### 创建 Topic

```bash
# 基础创建
kafka-topics --bootstrap-server BROKER \
  --create \
  --topic my-topic \
  --partitions 6 \
  --replication-factor 3

# 带配置参数（保留 7 天，snappy 压缩）
kafka-topics --bootstrap-server BROKER \
  --create \
  --topic trades.raw \
  --partitions 6 \
  --replication-factor 3 \
  --config retention.ms=604800000 \
  --config compression.type=snappy \
  --config cleanup.policy=delete

# Log Compaction Topic（适用于状态表）
kafka-topics --bootstrap-server BROKER \
  --create \
  --topic risk.alerts \
  --partitions 3 \
  --replication-factor 3 \
  --config cleanup.policy=compact \
  --config min.cleanable.dirty.ratio=0.5 \
  --config retention.ms=2592000000

# 如果 Topic 不存在才创建（幂等创建）
kafka-topics --bootstrap-server BROKER \
  --create \
  --topic my-topic \
  --partitions 6 \
  --replication-factor 3 \
  --if-not-exists
```

### 列出所有 Topic

```bash
# 列出所有 Topic 名称
kafka-topics --bootstrap-server BROKER --list

# 示例输出：
# __consumer_offsets
# risk.alerts
# trades.dlq
# trades.raw

# 排除内部 Topic（过滤双下划线开头）
kafka-topics --bootstrap-server BROKER --list \
  | grep -v '^__'
```

### 查看 Topic 详情

```bash
# 查看单个 Topic
kafka-topics --bootstrap-server BROKER \
  --describe \
  --topic trades.raw

# 示例输出：
# Topic: trades.raw  PartitionCount: 6  ReplicationFactor: 3  Configs: retention.ms=604800000
#   Topic: trades.raw  Partition: 0  Leader: 1  Replicas: 1,2,3  Isr: 1,2,3
#   Topic: trades.raw  Partition: 1  Leader: 2  Replicas: 2,3,1  Isr: 2,3,1
#   ...

# 查看所有 Topic
kafka-topics --bootstrap-server BROKER --describe

# 查看副本不足的 Topic（运维常用）
kafka-topics --bootstrap-server BROKER \
  --describe \
  --under-replicated-partitions

# 查看没有 Leader 的分区（严重故障指标）
kafka-topics --bootstrap-server BROKER \
  --describe \
  --unavailable-partitions
```

### 修改 Topic 配置

```bash
# 修改分区数（只能增加，不能减少！）
kafka-topics --bootstrap-server BROKER \
  --alter \
  --topic trades.raw \
  --partitions 12

# 修改 Topic 配置参数
kafka-configs --bootstrap-server BROKER \
  --entity-type topics \
  --entity-name trades.raw \
  --alter \
  --add-config retention.ms=172800000

# 删除特定配置（恢复为 Broker 默认值）
kafka-configs --bootstrap-server BROKER \
  --entity-type topics \
  --entity-name trades.raw \
  --alter \
  --delete-config retention.ms

# 查看 Topic 所有自定义配置
kafka-configs --bootstrap-server BROKER \
  --entity-type topics \
  --entity-name trades.raw \
  --describe
```

### 删除 Topic

```bash
# 删除 Topic（Broker 需配置 delete.topic.enable=true）
kafka-topics --bootstrap-server BROKER \
  --delete \
  --topic my-old-topic

# 批量删除（谨慎！）
for topic in topic-1 topic-2 topic-3; do
  kafka-topics --bootstrap-server BROKER --delete --topic $topic
done
```

---

## A.2 生产者测试命令

### 控制台生产者（kafka-console-producer）

```bash
# 基础：从标准输入读取，每行一条消息
kafka-console-producer \
  --bootstrap-server BROKER \
  --topic my-topic

# 带 Key 的消息（key:value 格式）
kafka-console-producer \
  --bootstrap-server BROKER \
  --topic my-topic \
  --property "parse.key=true" \
  --property "key.separator=:"

# 示例输入：
# ACC-000001:{"trade_id": "abc", "amount": 1000}

# 指定分区（发到特定分区）
kafka-console-producer \
  --bootstrap-server BROKER \
  --topic my-topic \
  --property "partitioner.class=org.apache.kafka.clients.producer.internals.DefaultPartitioner"

# 发送 JSON 文件（批量导入）
cat trades.json | kafka-console-producer \
  --bootstrap-server BROKER \
  --topic trades.raw
```

### 性能测试（kafka-producer-perf-test）

```bash
# 基础性能测试：100 万条消息，每条 1KB
kafka-producer-perf-test \
  --topic perf-test \
  --num-records 1000000 \
  --record-size 1024 \
  --throughput -1 \
  --producer-props bootstrap.servers=BROKER acks=all

# 示例输出：
# 100000 records sent, 45678.9 records/sec (44.6 MB/sec),
# 5.2 ms avg latency, 245.0 ms max latency.
# 1000000 records sent, 48234.5 records/sec (47.1 MB/sec),
# 4.8 ms avg latency, 198.0 ms max latency.

# 限速测试（500 条/秒）
kafka-producer-perf-test \
  --topic perf-test \
  --num-records 10000 \
  --record-size 512 \
  --throughput 500 \
  --producer-props bootstrap.servers=BROKER

# 带压缩测试（对比不同压缩算法）
for codec in none snappy gzip lz4 zstd; do
  echo "=== 压缩算法: $codec ==="
  kafka-producer-perf-test \
    --topic perf-test \
    --num-records 100000 \
    --record-size 1024 \
    --throughput -1 \
    --producer-props \
      bootstrap.servers=BROKER \
      compression.type=$codec
done
```

---

## A.3 消费者测试命令

### 控制台消费者（kafka-console-consumer）

```bash
# 从最新 Offset 消费（实时监控）
kafka-console-consumer \
  --bootstrap-server BROKER \
  --topic trades.raw

# 从头消费（查看历史消息）
kafka-console-consumer \
  --bootstrap-server BROKER \
  --topic trades.raw \
  --from-beginning

# 显示 Key、分区、Offset 等元信息
kafka-console-consumer \
  --bootstrap-server BROKER \
  --topic trades.raw \
  --from-beginning \
  --property print.key=true \
  --property print.partition=true \
  --property print.offset=true \
  --property print.timestamp=true

# 示例输出：
# CreateTime:1710500000000  Partition:2  Offset:1024  Key:ACC-000003  Value:{"trade_id":...}

# 消费指定分区
kafka-console-consumer \
  --bootstrap-server BROKER \
  --topic trades.raw \
  --partition 0 \
  --offset 100 \
  --max-messages 50

# 消费固定条数后退出
kafka-console-consumer \
  --bootstrap-server BROKER \
  --topic trades.raw \
  --from-beginning \
  --max-messages 100

# 以消费者组身份消费（持久化 Offset）
kafka-console-consumer \
  --bootstrap-server BROKER \
  --topic trades.raw \
  --group my-test-group \
  --from-beginning
```

### 性能测试（kafka-consumer-perf-test）

```bash
# 消费性能测试
kafka-consumer-perf-test \
  --bootstrap-server BROKER \
  --topic perf-test \
  --messages 1000000 \
  --group perf-consumer-group

# 示例输出：
# start.time, end.time, data.consumed.in.MB, MB.sec, nMsg.sec,
# rebalance.time.ms, fetch.time.ms, fetch.MB.sec, fetch.nMsg.sec
# 2024-03-15 14:00:00, 2024-03-15 14:00:22, 976.6, 43.2, 45123.4,
# 350, 21650, 45.1, 46189.7
```

---

## A.4 Consumer Group 管理

### 列出消费者组

```bash
# 列出所有 Consumer Group
kafka-consumer-groups --bootstrap-server BROKER --list

# 示例输出：
# risk-detector-group
# alert-dashboard-group
# dlq-monitor-group
# __consumer-offsets-0
```

### 查看消费者组详情（Lag 监控）

```bash
# 查看指定消费者组的 Lag
kafka-consumer-groups --bootstrap-server BROKER \
  --group risk-detector-group \
  --describe

# 示例输出：
# GROUP               TOPIC         PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG  CONSUMER-ID           HOST       CLIENT-ID
# risk-detector-group trades.raw    0          12456           12460           4    rdkafka-abc123-0      /10.0.0.1  rdkafka
# risk-detector-group trades.raw    1          11234           11234           0    rdkafka-abc123-1      /10.0.0.1  rdkafka
# ...

# 查看所有消费者组的 Lag（运维巡检）
kafka-consumer-groups --bootstrap-server BROKER \
  --describe --all-groups

# 仅查看有 Lag 的消费者组（过滤正常的）
kafka-consumer-groups --bootstrap-server BROKER \
  --describe --all-groups \
  | awk 'NR==1 || $6 > 0'

# 查看消费者组成员详情（谁在消费哪个分区）
kafka-consumer-groups --bootstrap-server BROKER \
  --group risk-detector-group \
  --describe \
  --members \
  --verbose
```

### 重置 Offset（危险操作！）

```bash
# ⚠️ 重置前先用 --dry-run 预览

# 重置到最早（重新消费所有消息）
kafka-consumer-groups --bootstrap-server BROKER \
  --group risk-detector-group \
  --topic trades.raw \
  --reset-offsets \
  --to-earliest \
  --dry-run   # 先预览

# 确认无误后执行（去掉 --dry-run，加上 --execute）
kafka-consumer-groups --bootstrap-server BROKER \
  --group risk-detector-group \
  --topic trades.raw \
  --reset-offsets \
  --to-earliest \
  --execute

# 重置到最新（跳过积压消息，紧急处理用）
kafka-consumer-groups --bootstrap-server BROKER \
  --group risk-detector-group \
  --reset-offsets \
  --to-latest \
  --all-topics \
  --execute

# 重置到指定时间点（事故恢复场景）
kafka-consumer-groups --bootstrap-server BROKER \
  --group risk-detector-group \
  --topic trades.raw \
  --reset-offsets \
  --to-datetime 2024-03-15T14:00:00.000 \
  --execute

# 重置到指定 Offset
kafka-consumer-groups --bootstrap-server BROKER \
  --group risk-detector-group \
  --topic trades.raw \
  --reset-offsets \
  --to-offset 5000 \
  --execute

# 回退 N 条消息（shift-by 负数）
kafka-consumer-groups --bootstrap-server BROKER \
  --group risk-detector-group \
  --topic trades.raw:0 \
  --reset-offsets \
  --shift-by -1000 \
  --execute

# 删除消费者组（必须先停止所有 Consumer）
kafka-consumer-groups --bootstrap-server BROKER \
  --group old-group \
  --delete
```

---

## A.5 日志查看命令

### kafka-dump-log（查看日志文件内容）

```bash
# 查看日志段的消息索引（无需 Kafka 运行）
kafka-dump-log \
  --files /var/lib/kafka/data/trades.raw-0/00000000000000000000.log \
  --print-data-log

# 示例输出：
# baseOffset: 0 lastOffset: 499 count: 500 baseSequence: 0 ...
#   | offset: 0 CreateTime: 1710500000000 keysize: 11 valuesize: 342 ...

# 查看偏移量索引
kafka-dump-log \
  --files /var/lib/kafka/data/trades.raw-0/00000000000000000000.index

# 查看时间戳索引
kafka-dump-log \
  --files /var/lib/kafka/data/trades.raw-0/00000000000000000000.timeindex
```

### kafka-log-dirs（查看分区大小和 Offset）

```bash
# 查看所有分区的日志目录、大小、Offset 范围
kafka-log-dirs \
  --bootstrap-server BROKER \
  --topic-list trades.raw \
  --describe

# 示例输出（JSON 格式）：
# {"version":1,"brokers":[{"broker":1,"logDirs":[{
#   "logDir":"/var/lib/kafka/data",
#   "partitions":[{
#     "partition":"trades.raw-0",
#     "size":1073741824,
#     "offsetLag":0,
#     "isFuture":false
#   }]
# }]}]}
```

---

## A.6 Broker 和集群管理

### 查看 Broker 信息

```bash
# 查看所有 Broker 版本信息
kafka-broker-api-versions \
  --bootstrap-server BROKER

# 列出集群所有 Broker
kafka-metadata-quorum \
  --bootstrap-server BROKER \
  --command-line \
  describe --status

# 示例输出（KRaft 模式）：
# ClusterId:              MkU3OEVBNTcwNTJENDM2Qk
# LeaderId:               1
# LeaderEpoch:            5
# HighWatermark:          342
# MaxFollowerLag:         0
# MaxFollowerLagTimeMs:   0
# CurrentVoters:          [1]
# CurrentObservers:       []
```

### 副本重新分配

```bash
# 生成重新分配计划（当 Broker 负载不均时）
kafka-reassign-partitions \
  --bootstrap-server BROKER \
  --broker-list "1,2,3" \
  --topics-to-move-json-file topics.json \
  --generate

# 执行重新分配
kafka-reassign-partitions \
  --bootstrap-server BROKER \
  --reassignment-json-file reassignment.json \
  --execute

# 验证重新分配进度
kafka-reassign-partitions \
  --bootstrap-server BROKER \
  --reassignment-json-file reassignment.json \
  --verify

# 取消正在进行的重分配（紧急用）
kafka-reassign-partitions \
  --bootstrap-server BROKER \
  --reassignment-json-file reassignment.json \
  --cancel
```

### 配置管理（kafka-configs）

```bash
# 查看 Broker 动态配置
kafka-configs --bootstrap-server BROKER \
  --entity-type brokers \
  --entity-name 1 \
  --describe

# 动态修改 Broker 配置（无需重启）
kafka-configs --bootstrap-server BROKER \
  --entity-type brokers \
  --entity-name 1 \
  --alter \
  --add-config log.retention.hours=168

# 修改所有 Broker（entity-default）
kafka-configs --bootstrap-server BROKER \
  --entity-type brokers \
  --entity-default \
  --alter \
  --add-config log.retention.ms=604800000
```

---

## A.7 KRaft 模式特有命令

KRaft 模式下，Kafka 使用内置的 Raft 协议替代 ZooKeeper 管理元数据。

### 初始化集群存储

```bash
# 生成唯一的 Cluster ID（每个集群只需一次）
CLUSTER_ID=$(kafka-storage random-uuid)
echo "Cluster ID: $CLUSTER_ID"

# 在每个 Broker 节点上格式化存储
kafka-storage format \
  --config /etc/kafka/kraft/server.properties \
  --cluster-id $CLUSTER_ID

# 如果已格式化，强制重新格式化（⚠️ 数据丢失！）
kafka-storage format \
  --config /etc/kafka/kraft/server.properties \
  --cluster-id $CLUSTER_ID \
  --ignore-formatted
```

### KRaft 元数据查看

```bash
# 查看 KRaft 仲裁状态（谁是 Leader）
kafka-metadata-quorum \
  --bootstrap-server BROKER \
  describe --status

# 查看元数据日志
kafka-metadata-shell \
  --snapshot /var/lib/kafka/data/__cluster_metadata-0/00000000000000000000.checkpoint

# 内置 Shell 命令（进入后使用）
# ls /brokers          # 列出 Broker
# cat /brokers/1       # 查看 Broker 1 信息
# ls /topics           # 列出 Topic
# cat /topics/trades.raw  # 查看 Topic 元数据

# 查看 KRaft 控制器日志
kafka-dump-log \
  --files /var/lib/kafka/data/__cluster_metadata-0/00000000000000000000.log \
  --cluster-metadata-decoder
```

---

## A.8 Schema Registry 命令

Schema Registry 通过 REST API 管理。

```bash
BASE_URL="http://localhost:8081"

# 列出所有 Subject
curl -s $BASE_URL/subjects | jq .

# 查看 Subject 所有版本
curl -s $BASE_URL/subjects/trades.raw-value/versions | jq .

# 获取最新版本的 Schema
curl -s $BASE_URL/subjects/trades.raw-value/versions/latest | jq .

# 示例输出：
# {
#   "subject": "trades.raw-value",
#   "version": 1,
#   "id": 1,
#   "schema": "{\"type\":\"record\",\"name\":\"Trade\",...}"
# }

# 手动注册 Schema
curl -X POST $BASE_URL/subjects/trades.raw-value/versions \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d "{\"schema\": $(cat config/schemas/trade.avsc | jq -c tostring)}"

# 检测 Schema 兼容性（提交前验证）
curl -X POST $BASE_URL/compatibility/subjects/trades.raw-value/versions/latest \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d "{\"schema\": $(cat new_trade.avsc | jq -c tostring)}"

# 修改兼容性策略
curl -X PUT $BASE_URL/config/trades.raw-value \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d '{"compatibility": "BACKWARD"}'

# 删除 Schema（谨慎！）
curl -X DELETE $BASE_URL/subjects/old-topic-value
```

---

## A.9 常用 Docker Compose 命令（开发环境）

```bash
# 启动所有服务（后台运行）
docker-compose up -d

# 查看服务状态
docker-compose ps

# 查看服务日志
docker-compose logs kafka -f

# 进入 Kafka 容器执行命令
docker-compose exec kafka bash

# 在容器内直接运行命令
docker-compose exec kafka \
  kafka-topics --bootstrap-server localhost:9092 --list

# 停止并删除所有容器（保留数据卷）
docker-compose down

# 完全清理（删除数据卷）
docker-compose down -v

# 查看容器资源使用
docker stats riskguard-kafka
```

---

## A.10 常见问题排查命令

```bash
# 问题1：Topic 存在但无法消费
# → 检查分区是否有 Leader
kafka-topics --bootstrap-server BROKER \
  --describe --topic trades.raw \
  | grep "Leader: -1"   # -1 表示无 Leader

# 问题2：Consumer Lag 持续增长
# → 检查消费者状态
kafka-consumer-groups --bootstrap-server BROKER \
  --group risk-detector-group \
  --describe \
  | awk '{if($6>100) print "高 Lag: "$0}'

# 问题3：Producer 发送失败
# → 检查 Broker 连接
kafka-broker-api-versions --bootstrap-server BROKER

# 问题4：查看某时刻之后的消息
# → 先找 Offset，再消费
kafka-run-class kafka.tools.GetOffsetShell \
  --bootstrap-server BROKER \
  --topic trades.raw \
  --time 1710500000000   # Unix 毫秒时间戳

# 问题5：磁盘空间告急
# → 检查各 Topic 磁盘占用
kafka-log-dirs --bootstrap-server BROKER \
  --describe \
  | python3 -c "
import sys, json
data = json.load(sys.stdin)
sizes = {}
for broker in data['brokers']:
    for logDir in broker['logDirs']:
        for p in logDir['partitions']:
            topic = p['partition'].rsplit('-',1)[0]
            sizes[topic] = sizes.get(topic, 0) + p['size']
for t, s in sorted(sizes.items(), key=lambda x: -x[1]):
    print(f'{s/1024/1024:.1f} MB\t{t}')
"
```

---

*附录 B 提供 20 道 Kafka 面试题精选，帮助你在技术面试中展示深度。*
