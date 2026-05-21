# 第 8 章：监控与可观测性

## 本章你将学到

- 监控三要素：Metrics、Logs、Traces 的定位与区别
- Kafka 最关键的指标分类及其健康阈值
- 完整搭建 Prometheus + Grafana 监控栈
- 在 Python Producer/Consumer 中埋入自定义指标
- 配置 Consumer Lag 告警规则（AlertManager）
- 结构化日志最佳实践（JSON 格式 + 关键字段）
- 动手：导入 Grafana Dashboard 实战观察 Consumer Lag

---

## 8.1 监控三要素：Metrics、Logs、Traces

可观测性（Observability）不是单一工具能解决的，它由三个互补的维度构成：

```
┌──────────────────────────────────────────────────────┐
│                   可观测性三支柱                        │
│                                                      │
│  Metrics（指标）   Logs（日志）    Traces（链路追踪）     │
│  ─────────────   ──────────    ──────────────────    │
│  数字，随时间     文本，离散     请求在系统中的         │
│  变化的聚合值     事件记录       完整调用链             │
│                                                      │
│  "消费者 Lag      "2024-01-15    "这笔交易经过了         │
│   当前是 5000"    10:30:01 消费  API→Kafka→处理服务      │
│                   者重启了"      共花了 23ms"           │
└──────────────────────────────────────────────────────┘
```

### 三者的分工

| 维度 | 回答的问题 | 工具 | Kafka 中的例子 |
|------|-----------|------|---------------|
| **Metrics** | 系统现在怎样？ | Prometheus + Grafana | Consumer Lag = 50,000 |
| **Logs** | 发生了什么事？ | ELK Stack / Loki | Broker 日志、应用日志 |
| **Traces** | 为什么慢/出错？ | Jaeger / Zipkin | 一条消息的端到端延迟分布 |

**Kafka 监控的优先级**：对于大多数团队，Metrics（Prometheus + Grafana）能解决 80% 的问题，是第一要务。本章重点讲解 Metrics，兼顾 Logs。

---

## 8.2 关键 Kafka 指标分类

### 8.2.1 Broker 健康指标

Broker（Kafka 服务节点）的健康是一切的基础。

```
核心指标一览：

┌────────────────────────────────┬──────────┬──────────────────────────────┐
│ 指标名称（JMX）                 │ 健康值   │ 说明                         │
├────────────────────────────────┼──────────┼──────────────────────────────┤
│ UnderReplicatedPartitions      │ = 0      │ 未满足副本数的分区数量         │
│                                │          │ > 0 = 有副本落后，数据风险！   │
├────────────────────────────────┼──────────┼──────────────────────────────┤
│ ActiveControllerCount          │ = 1      │ 活跃 Controller 数量           │
│                                │          │ 集群中应恰好有且仅有 1 个       │
├────────────────────────────────┼──────────┼──────────────────────────────┤
│ OfflinePartitionsCount         │ = 0      │ 无 Leader 的分区数量           │
│                                │          │ > 0 = 该分区的消息无法读写！   │
├────────────────────────────────┼──────────┼──────────────────────────────┤
│ RequestHandlerAvgIdlePercent   │ > 30%    │ 请求处理线程的空闲比例         │
│                                │          │ < 30% = Broker 过载           │
├────────────────────────────────┼──────────┼──────────────────────────────┤
│ NetworkProcessorAvgIdlePercent │ > 30%    │ 网络处理线程的空闲比例         │
│                                │          │ < 30% = 网络层成为瓶颈        │
└────────────────────────────────┴──────────┴──────────────────────────────┘
```

**UnderReplicatedPartitions 解读**：

```
正常状态：
  Partition P0: Leader=Broker1, Follower=Broker2, Follower=Broker3
  → 3 个副本都同步 → UnderReplicatedPartitions = 0

异常状态：
  Partition P0: Leader=Broker1, Follower=Broker2, Broker3 宕机
  → 只有 2 个副本同步，少于 replication.factor=3
  → UnderReplicatedPartitions = 1

  含义：如果此时 Broker1 也宕机，P0 的数据可能丢失！
  行动：立即检查 Broker3 的状态
```

### 8.2.2 Producer 性能指标

```
关键 Producer 指标（通过 JMX 或 kafka-client 内置 metrics 获取）：

record-send-rate          每秒发送的消息条数（正常取决于业务量）
record-error-rate         每秒发送失败的消息数（应接近 0！）
request-latency-avg       Producer 请求平均延迟（目标 < 100ms）
batch-size-avg            批次平均大小（太小 = 网络浪费，太大 = 延迟增加）
record-queue-time-avg     消息在 Producer 缓冲队列中的等待时间
compression-rate-avg      压缩率（越高压缩效果越好，减少网络传输）
```

**batch-size-avg 调优逻辑**：

```
batch-size-avg 太小（比如 < 1KB）：
  → Producer 频繁发送小包，网络开销大
  → 调大 batch.size 和 linger.ms
  
batch-size-avg 太大（比如 > 900KB，接近 batch.size 上限）：
  → Producer 总是等满一批再发，延迟增加
  → 检查生产速率是否过高，考虑增加 Producer 实例
```

### 8.2.3 Consumer 健康指标

Consumer 指标中，**Consumer Lag（消费延迟）是最重要的！**

```
Consumer Lag = 最新偏移量（Log End Offset）- 已提交偏移量（Committed Offset）

直觉理解：
  Topic 中已有 10,000 条消息
  Consumer 已处理到第 9,500 条
  Consumer Lag = 10,000 - 9,500 = 500
  
  Lag = 0   → Consumer 实时跟上
  Lag 增长  → Consumer 跟不上生产速度（积压！）
  Lag 突然归零 → Consumer 可能重启了或出问题了（异常！）
```

| 指标 | 含义 | 告警条件 |
|------|------|---------|
| `consumer-lag` | 消费延迟（条数） | 持续 > 阈值 5 分钟 |
| `records-consumed-rate` | 每秒消费的消息数 | 突然降为 0 |
| `fetch-rate` | 每秒 Fetch 请求数 | 异常低 |
| `commit-rate` | 每秒 Offset 提交次数 | 异常低 |

### 8.2.4 Topic 健康指标

```
BytesInPerSec    每秒流入的字节数（监控流量趋势）
BytesOutPerSec   每秒流出的字节数（监控消费带宽）
MessagesInPerSec 每秒流入的消息数（监控生产速率）

BytesRejectedPerSec > 0 → 消息被拒绝（可能超过 message.max.bytes 限制）
```

---

## 8.3 Prometheus + Grafana 监控栈搭建

### 8.3.1 整体架构

```
                      ┌─────────────────┐
                      │   Grafana       │
                      │   (可视化)       │ :3000
                      └────────┬────────┘
                               │ 查询
                      ┌────────▼────────┐
                      │   Prometheus    │
                      │   (时序数据库)   │ :9090
                      └────────┬────────┘
                               │ 抓取（scrape）
              ┌────────────────┼────────────────┐
              │                │                │
    ┌─────────▼───────┐  ┌────▼────┐  ┌────────▼──────┐
    │  JMX Exporter   │  │  Node   │  │  Python App   │
    │  (Kafka Broker) │  │ Exporter│  │  (业务指标)    │
    │  :7071          │  │  :9100  │  │  :8000        │
    └─────────────────┘  └─────────┘  └───────────────┘
              │
    ┌─────────▼───────┐
    │  Kafka Broker   │
    │  (JMX: 9999)    │
    └─────────────────┘
```

### 8.3.2 Docker Compose 完整配置

```yaml
# docker-compose-monitoring.yml
version: '3.8'

services:
  # ——— Kafka 集群 ———
  zookeeper:
    image: confluentinc/cp-zookeeper:7.5.0
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
    ports:
      - "2181:2181"

  kafka:
    image: confluentinc/cp-kafka:7.5.0
    depends_on:
      - zookeeper
    ports:
      - "9092:9092"
      - "9999:9999"     # JMX 端口，供 JMX Exporter 连接
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      # 开启 JMX（必须！Prometheus 通过 JMX Exporter 采集指标）
      KAFKA_JMX_PORT: 9999
      KAFKA_JMX_HOSTNAME: kafka
      KAFKA_OPTS: "-javaagent:/opt/kafka/jmx_prometheus_javaagent.jar=7071:/opt/kafka/kafka-2_0_0.yml"
    volumes:
      - ./jmx_exporter:/opt/kafka  # 挂载 JMX Exporter jar 和配置

  # ——— JMX Exporter（独立模式，通过 HTTP 连接到 Kafka JMX）———
  # 注意：也可以用 javaagent 模式（在 Kafka JVM 内部启动），上面的配置就是 javaagent 模式
  # 这里提供独立模式作为备选
  jmx-exporter:
    image: bitnami/jmx-exporter:0.20.0
    ports:
      - "7071:7071"
    environment:
      SERVICE_PORT: 7071
    volumes:
      - ./jmx_exporter/kafka-2_0_0.yml:/opt/bitnami/jmx-exporter/conf/jmx_prometheus_httpserver.yml

  # ——— Prometheus ———
  prometheus:
    image: prom/prometheus:v2.48.0
    ports:
      - "9090:9090"
    volumes:
      - ./prometheus/prometheus.yml:/etc/prometheus/prometheus.yml
      - ./prometheus/rules:/etc/prometheus/rules
      - prometheus_data:/prometheus
    command:
      - '--config.file=/etc/prometheus/prometheus.yml'
      - '--storage.tsdb.retention.time=15d'  # 数据保留 15 天
      - '--web.enable-lifecycle'              # 允许 HTTP reload

  # ——— Grafana ———
  grafana:
    image: grafana/grafana:10.2.0
    ports:
      - "3000:3000"
    environment:
      GF_SECURITY_ADMIN_PASSWORD: admin123   # 修改默认密码！
      GF_USERS_ALLOW_SIGN_UP: 'false'
    volumes:
      - grafana_data:/var/lib/grafana
      - ./grafana/dashboards:/var/lib/grafana/dashboards
      - ./grafana/provisioning:/etc/grafana/provisioning
    depends_on:
      - prometheus

  # ——— AlertManager（处理告警通知）———
  alertmanager:
    image: prom/alertmanager:v0.26.0
    ports:
      - "9093:9093"
    volumes:
      - ./alertmanager/alertmanager.yml:/etc/alertmanager/alertmanager.yml

volumes:
  prometheus_data:
  grafana_data:
```

### 8.3.3 JMX Exporter 配置

JMX Exporter（Java Management Extensions Exporter）是 Prometheus 官方提供的工具，将 Java 应用的 JMX 指标转换为 Prometheus 格式。

```yaml
# jmx_exporter/kafka-2_0_0.yml - JMX Exporter 配置文件
# 告诉 JMX Exporter 要采集哪些 Kafka JMX 指标

lowercaseOutputName: true      # 指标名称转小写
lowercaseOutputLabelNames: true

rules:
  # ——— Broker 健康指标 ———
  - pattern: kafka.server<type=ReplicaManager, name=UnderReplicatedPartitions><>Value
    name: kafka_server_replicamanager_underreplicatedpartitions
    help: "未满足副本数的分区数量（应为 0）"

  - pattern: kafka.controller<type=KafkaController, name=ActiveControllerCount><>Value
    name: kafka_controller_kafkacontroller_activecontrollercount
    help: "活跃 Controller 数量（应为 1）"

  - pattern: kafka.controller<type=KafkaController, name=OfflinePartitionsCount><>Value
    name: kafka_controller_kafkacontroller_offlinepartitionscount
    help: "离线分区数量（应为 0）"

  # ——— Broker 性能指标 ———
  - pattern: kafka.server<type=BrokerTopicMetrics, name=MessagesInPerSec><>OneMinuteRate
    name: kafka_server_brokertopicmetrics_messagesinpersec
    help: "每秒流入消息数"

  - pattern: kafka.server<type=BrokerTopicMetrics, name=BytesInPerSec><>OneMinuteRate
    name: kafka_server_brokertopicmetrics_bytesinpersec
    help: "每秒流入字节数"

  - pattern: kafka.server<type=BrokerTopicMetrics, name=BytesOutPerSec><>OneMinuteRate
    name: kafka_server_brokertopicmetrics_bytesoutpersec
    help: "每秒流出字节数"

  # ——— 请求延迟 ———
  - pattern: kafka.network<type=RequestMetrics, name=TotalTimeMs, request=Produce><>99thPercentile
    name: kafka_network_requestmetrics_produce_totaltimems_p99
    help: "Producer 请求 P99 延迟（毫秒）"

  - pattern: kafka.network<type=RequestMetrics, name=TotalTimeMs, request=FetchConsumer><>99thPercentile
    name: kafka_network_requestmetrics_fetchconsumer_totaltimems_p99
    help: "Consumer Fetch 请求 P99 延迟（毫秒）"

  # ——— 通用规则（采集所有未明确列出的 Kafka JMX 指标）———
  - pattern: kafka.(\w+)<type=(.+), name=(.+)><>(\w+)
    name: kafka_$1_$2_$3_$4
    labels:
      service: kafka
```

### 8.3.4 Prometheus 配置

```yaml
# prometheus/prometheus.yml
global:
  scrape_interval: 15s      # 每 15 秒抓取一次指标
  evaluation_interval: 15s  # 每 15 秒评估一次告警规则

# 告警规则文件
rule_files:
  - "rules/kafka_alerts.yml"
  - "rules/consumer_lag_alerts.yml"

# 告警发送到 AlertManager
alerting:
  alertmanagers:
    - static_configs:
        - targets: ['alertmanager:9093']

# 抓取配置
scrape_configs:
  # ——— Kafka Broker（通过 JMX Exporter）———
  - job_name: 'kafka'
    static_configs:
      - targets: ['kafka:7071']  # JMX Exporter 监听地址
    scrape_interval: 30s         # Kafka 指标可以稍微少频繁
    metrics_path: /metrics

  # ——— Kafka Consumer Lag（通过 kafka-lag-exporter）———
  - job_name: 'kafka-consumer-lag'
    static_configs:
      - targets: ['kafka-lag-exporter:8000']
    scrape_interval: 30s

  # ——— 服务器节点指标（CPU、内存、磁盘）———
  - job_name: 'node'
    static_configs:
      - targets: ['node-exporter:9100']

  # ——— Python 应用自定义指标 ———
  - job_name: 'trade-processor'
    static_configs:
      - targets: ['trade-processor:8000']  # Python 应用暴露 metrics 的端口
    scrape_interval: 15s
```

---

## 8.4 Python 应用内埋点

用 `prometheus_client` 库在 Python Producer/Consumer 中嵌入自定义指标。

### 8.4.1 安装

```bash
pip install prometheus-client
```

### 8.4.2 Producer 自定义指标

```python
# instrumented_producer.py - 带 Prometheus 埋点的 Producer
import time
import json
import uuid
import logging
from kafka import KafkaProducer
from prometheus_client import (
    Counter,       # 计数器（只增不减）
    Histogram,     # 直方图（统计延迟分布）
    Gauge,         # 仪表盘（可增可减）
    start_http_server,  # 启动 metrics HTTP 服务
)

logger = logging.getLogger(__name__)

# ============================================================
# 定义 Prometheus 指标
#
# 命名规范：{应用名}_{指标类别}_{指标名}_{单位}
# labels 用于多维度切分（symbol、trade_type 等）
# ============================================================

# 计数器：发送的消息总数（按 symbol 和 trade_type 分类）
messages_sent_total = Counter(
    'trade_producer_messages_sent_total',
    '发送到 Kafka 的消息总数',
    labelnames=['symbol', 'trade_type', 'status'],  # status: success/error
)

# 直方图：消息发送延迟分布
send_latency_seconds = Histogram(
    'trade_producer_send_latency_seconds',
    '消息发送到 Kafka 的延迟（秒）',
    labelnames=['symbol'],
    # 定义桶边界：从 1ms 到 10s
    buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 5.0, 10.0],
)

# 仪表盘：Producer 缓冲队列中等待发送的消息数
buffer_queue_size = Gauge(
    'trade_producer_buffer_queue_size',
    'Producer 内存缓冲区中等待发送的消息数',
)

# 计数器：重试次数
retry_total = Counter(
    'trade_producer_retry_total',
    '消息发送重试总次数',
    labelnames=['symbol'],
)

class InstrumentedProducer:
    """带 Prometheus 监控的 Kafka Producer 包装类"""
    
    def __init__(self, bootstrap_servers: str, metrics_port: int = 8000):
        # 启动 Prometheus metrics HTTP server
        start_http_server(metrics_port)
        logger.info(f"Prometheus metrics 已启动，端口: {metrics_port}")
        
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8'),
            # 启用 Producer 重试
            retries=3,
            retry_backoff_ms=100,
        )
    
    def send_trade(self, trade: dict):
        """
        发送交易消息，自动记录指标
        
        Args:
            trade: 交易事件字典
        """
        symbol = trade.get('symbol', 'UNKNOWN')
        trade_type = trade.get('trade_type', 'UNKNOWN')
        
        start_time = time.time()
        
        try:
            # 发送消息
            future = self.producer.send(
                topic='raw-trades',
                key=trade['account_id'],
                value=trade,
            )
            
            # 等待 ack（阻塞调用，用于精确测量延迟）
            # 生产环境可以改为异步回调
            record_metadata = future.get(timeout=10)
            
            elapsed = time.time() - start_time
            
            # ——— 记录成功指标 ———
            messages_sent_total.labels(
                symbol=symbol,
                trade_type=trade_type,
                status='success',
            ).inc()
            
            send_latency_seconds.labels(symbol=symbol).observe(elapsed)
            
            logger.debug(
                f"消息已发送: topic={record_metadata.topic}, "
                f"partition={record_metadata.partition}, "
                f"offset={record_metadata.offset}, "
                f"latency={elapsed*1000:.2f}ms"
            )
            
        except Exception as e:
            elapsed = time.time() - start_time
            
            # ——— 记录失败指标 ———
            messages_sent_total.labels(
                symbol=symbol,
                trade_type=trade_type,
                status='error',
            ).inc()
            
            logger.error(f"消息发送失败: {e}, trade_id={trade.get('trade_id')}")
            raise
    
    def flush(self):
        self.producer.flush()
    
    def close(self):
        self.producer.close()


# 使用示例
if __name__ == '__main__':
    import random
    
    logging.basicConfig(level=logging.INFO)
    
    producer = InstrumentedProducer('localhost:9092', metrics_port=8000)
    
    print("Producer 已启动，Metrics 可在 http://localhost:8000/metrics 查看")
    
    symbols = ['AAPL', 'TSLA', 'BTC-USD']
    
    for i in range(1000):
        trade = {
            'trade_id': str(uuid.uuid4()),
            'account_id': f'ACC-{random.randint(1, 100):04d}',
            'symbol': random.choice(symbols),
            'quantity': round(random.uniform(1, 100), 2),
            'price': round(random.uniform(10, 500), 2),
            'trade_type': random.choice(['BUY', 'SELL']),
            'timestamp': time.time() * 1000,
        }
        producer.send_trade(trade)
        time.sleep(0.1)  # 每 100ms 一笔交易
    
    producer.close()
```

### 8.4.3 Consumer 自定义指标

```python
# instrumented_consumer.py - 带 Prometheus 埋点的 Consumer
import time
import json
import logging
from kafka import KafkaConsumer
from kafka.structs import TopicPartition
from prometheus_client import (
    Counter,
    Histogram,
    Gauge,
    start_http_server,
)

logger = logging.getLogger(__name__)

# ——— Consumer 指标定义 ———

messages_consumed_total = Counter(
    'trade_consumer_messages_consumed_total',
    '消费处理的消息总数',
    labelnames=['symbol', 'status'],  # status: success/error/skipped
)

processing_duration_seconds = Histogram(
    'trade_consumer_processing_duration_seconds',
    '每条消息的处理时长（秒）',
    labelnames=['symbol'],
    buckets=[0.0001, 0.001, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0],
)

# ⚠️ 最关键的 Consumer 指标：Consumer Lag
consumer_lag_gauge = Gauge(
    'trade_consumer_lag_messages',
    '当前 Consumer Lag（未处理的消息数）',
    labelnames=['topic', 'partition'],
)

batch_size_gauge = Gauge(
    'trade_consumer_batch_size',
    '每次 poll() 获取到的消息数量',
)

class InstrumentedConsumer:
    """带 Prometheus 监控的 Kafka Consumer"""
    
    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        group_id: str,
        metrics_port: int = 8001,
    ):
        start_http_server(metrics_port)
        logger.info(f"Consumer Metrics 已启动，端口: {metrics_port}")
        
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            auto_offset_reset='latest',
            enable_auto_commit=False,  # 手动提交，精确控制
            value_deserializer=lambda v: json.loads(v.decode('utf-8')),
            max_poll_records=500,      # 每次最多拉取 500 条
        )
        self.topic = topic
    
    def _update_lag_metrics(self):
        """
        更新 Consumer Lag 指标
        
        计算方式：
          lag = log_end_offset（topic 最新 offset）
              - committed_offset（consumer 已提交的 offset）
        """
        # 获取当前分配的分区
        partitions = self.consumer.assignment()
        if not partitions:
            return
        
        # 获取每个分区的最新 offset（Log End Offset）
        end_offsets = self.consumer.end_offsets(list(partitions))
        
        # 获取当前 committed offset
        for tp in partitions:
            committed = self.consumer.committed(tp)
            if committed is None:
                continue
            
            end_offset = end_offsets.get(tp, 0)
            lag = max(0, end_offset - committed)
            
            consumer_lag_gauge.labels(
                topic=tp.topic,
                partition=tp.partition,
            ).set(lag)
    
    def process_message(self, message) -> bool:
        """
        处理单条消息（业务逻辑）
        
        Returns:
            True: 处理成功
            False: 处理失败（需要错误处理）
        """
        trade = message.value
        symbol = trade.get('symbol', 'UNKNOWN')
        
        start_time = time.time()
        
        try:
            # ——— 实际业务逻辑 ———
            # 这里可以是：写数据库、调用 API、触发告警等
            amount = trade.get('quantity', 0) * trade.get('price', 0)
            logger.debug(f"处理交易: {trade.get('trade_id')}, 金额: {amount:.2f}")
            # ——— 业务逻辑结束 ———
            
            elapsed = time.time() - start_time
            
            messages_consumed_total.labels(symbol=symbol, status='success').inc()
            processing_duration_seconds.labels(symbol=symbol).observe(elapsed)
            
            return True
            
        except Exception as e:
            elapsed = time.time() - start_time
            messages_consumed_total.labels(symbol=symbol, status='error').inc()
            logger.error(f"消息处理失败: {e}")
            return False
    
    def run(self):
        """主消费循环"""
        logger.info(f"开始消费 topic: {self.topic}")
        
        lag_update_interval = 30  # 每 30 秒更新一次 lag 指标
        last_lag_update = 0
        
        try:
            while True:
                # 拉取消息（最多等待 1000ms）
                records = self.consumer.poll(timeout_ms=1000)
                
                if not records:
                    continue
                
                # 统计批次大小
                total_records = sum(len(msgs) for msgs in records.values())
                batch_size_gauge.set(total_records)
                
                # 处理消息
                for tp, messages in records.items():
                    for message in messages:
                        success = self.process_message(message)
                        
                        if not success:
                            # 失败处理策略：跳过（生产中应发送到死信队列）
                            logger.warning(f"跳过失败消息: offset={message.offset}")
                
                # 手动提交 offset（批量提交，性能更好）
                self.consumer.commit()
                
                # 定期更新 lag 指标
                now = time.time()
                if now - last_lag_update > lag_update_interval:
                    self._update_lag_metrics()
                    last_lag_update = now
                    
        except KeyboardInterrupt:
            logger.info("Consumer 停止")
        finally:
            self.consumer.close()


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    consumer = InstrumentedConsumer(
        bootstrap_servers='localhost:9092',
        topic='raw-trades',
        group_id='trade-monitor',
        metrics_port=8001,
    )
    consumer.run()
```

---

## 8.5 Consumer Lag 告警规则

### 8.5.1 Prometheus 告警规则

```yaml
# prometheus/rules/consumer_lag_alerts.yml
groups:
  - name: kafka_consumer_lag
    rules:
      # ——— 规则 1：Consumer Lag 过高告警 ———
      - alert: KafkaConsumerLagHigh
        expr: |
          # 任意分区的 Consumer Lag 超过 10,000 条
          kafka_consumer_group_lag > 10000
        for: 5m    # 持续 5 分钟才触发（避免偶发性 lag 告警）
        labels:
          severity: warning
        annotations:
          summary: "Consumer Lag 过高: {{ $labels.consumer_group }}"
          description: |
            Consumer Group {{ $labels.consumer_group }} 
            在 Topic {{ $labels.topic }}（分区 {{ $labels.partition }}）
            的 Lag 已达 {{ $value }} 条，持续超过 5 分钟。
            
            可能原因：
            1. Consumer 处理速度跟不上生产速度
            2. Consumer 节点宕机或重启
            3. 消费逻辑出现阻塞
            
            建议操作：
            1. 检查 Consumer 日志
            2. 增加 Consumer 实例数
            3. 检查 Consumer 处理逻辑是否有性能问题

      # ——— 规则 2：Consumer Lag 严重告警 ———
      - alert: KafkaConsumerLagCritical
        expr: |
          kafka_consumer_group_lag > 100000
        for: 2m    # 更高阈值，但更快触发
        labels:
          severity: critical
        annotations:
          summary: "⚠️ Consumer Lag 严重: {{ $labels.consumer_group }}"
          description: |
            Consumer Group {{ $labels.consumer_group }} 
            Lag 已超过 100,000 条！数据积压严重，请立即处理！

      # ——— 规则 3：Consumer 停止消费 ———
      - alert: KafkaConsumerStopped
        expr: |
          # Lag 持续存在，但 Consumer 的消费速率为 0
          kafka_consumer_group_lag > 0
          AND
          rate(kafka_consumer_group_offset[5m]) == 0
        for: 3m
        labels:
          severity: critical
        annotations:
          summary: "Consumer 已停止消费: {{ $labels.consumer_group }}"
          description: |
            Consumer Group {{ $labels.consumer_group }} 
            有未处理的消息（Lag={{ $value }}），但过去 5 分钟内没有消费任何消息。
            请检查 Consumer 进程是否存活。

  - name: kafka_broker_health
    rules:
      # ——— 规则 4：有副本未完成同步 ———
      - alert: KafkaUnderReplicatedPartitions
        expr: kafka_server_replicamanager_underreplicatedpartitions > 0
        for: 1m
        labels:
          severity: warning
        annotations:
          summary: "Kafka 副本同步落后: Broker {{ $labels.instance }}"
          description: |
            Broker {{ $labels.instance }} 有 {{ $value }} 个分区的副本未完成同步。
            如果此状态持续，可能面临数据丢失风险。

      # ——— 规则 5：无 Controller ———
      - alert: KafkaNoActiveController
        expr: sum(kafka_controller_kafkacontroller_activecontrollercount) != 1
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "Kafka 集群无活跃 Controller！"
          description: |
            集群中活跃 Controller 数量为 {{ $value }}（应为 1）。
            这会导致无法创建新分区、无法进行 Leader 选举。
            请立即检查 ZooKeeper 和 Broker 状态。
```

### 8.5.2 AlertManager 配置

```yaml
# alertmanager/alertmanager.yml
global:
  # SMTP 邮件配置（按需填写）
  smtp_smarthost: 'smtp.gmail.com:587'
  smtp_from: 'kafka-alerts@yourcompany.com'
  smtp_auth_username: 'kafka-alerts@yourcompany.com'
  smtp_auth_password: 'your-app-password'

route:
  group_by: ['alertname', 'consumer_group']
  group_wait: 30s       # 分组等待时间（聚合同类告警）
  group_interval: 5m    # 同组告警最小发送间隔
  repeat_interval: 4h   # 未解决告警的重复提醒间隔
  receiver: 'default'
  
  routes:
    # Critical 告警立即通知
    - match:
        severity: critical
      receiver: 'pagerduty-critical'
      continue: true  # 同时也发送到 default
    
    # Consumer Lag 告警发到 Slack #kafka-alerts 频道
    - match:
        alertname: KafkaConsumerLagHigh
      receiver: 'slack-kafka-alerts'

receivers:
  - name: 'default'
    email_configs:
      - to: 'data-team@yourcompany.com'
        subject: '[Kafka Alert] {{ .GroupLabels.alertname }}'
        body: |
          {{ range .Alerts }}
          告警: {{ .Annotations.summary }}
          详情: {{ .Annotations.description }}
          触发时间: {{ .StartsAt.Format "2006-01-02 15:04:05" }}
          {{ end }}

  - name: 'slack-kafka-alerts'
    slack_configs:
      - api_url: 'https://hooks.slack.com/services/YOUR/SLACK/WEBHOOK'
        channel: '#kafka-alerts'
        title: '{{ .GroupLabels.alertname }}'
        text: |
          {{ range .Alerts }}
          *{{ .Annotations.summary }}*
          {{ .Annotations.description }}
          {{ end }}

  - name: 'pagerduty-critical'
    pagerduty_configs:
      - routing_key: 'YOUR_PAGERDUTY_KEY'
        description: '{{ .GroupLabels.alertname }}: {{ .CommonAnnotations.summary }}'
```

---

## 8.6 日志最佳实践

### 8.6.1 为什么使用结构化日志

```
非结构化日志（难以查询）：
  2024-01-15 10:30:01 ERROR Failed to process trade T001 for account ACC-001, amount 5000, error: ConnectionTimeout

结构化日志（JSON 格式，易于搜索和分析）：
  {
    "timestamp": "2024-01-15T10:30:01.234Z",
    "level": "ERROR",
    "service": "trade-processor",
    "event": "trade_processing_failed",
    "trade_id": "T001",
    "account_id": "ACC-001",
    "amount": 5000.0,
    "error_type": "ConnectionTimeout",
    "error_message": "Connection timed out after 5000ms",
    "retry_count": 2,
    "processing_time_ms": 5023
  }

结构化日志的优势：
  - 可以用 jq 快速过滤：cat app.log | jq 'select(.level=="ERROR")'
  - ELK Stack 自动解析，支持字段级搜索
  - 可以统计特定 error_type 的发生频率
  - 可以关联 trade_id 追踪消息全链路
```

### 8.6.2 Python 结构化日志实现

```python
# structured_logging.py - 结构化日志配置
import logging
import json
import sys
import time
import traceback
from datetime import datetime, timezone
from typing import Any, Dict, Optional

class JsonFormatter(logging.Formatter):
    """
    将 Python logging 格式化为 JSON 格式的结构化日志
    
    输出格式（每行一个 JSON 对象，方便 ELK/Loki 解析）：
    {"timestamp": "...", "level": "INFO", "service": "...", ...}
    """
    
    def __init__(self, service_name: str = 'kafka-app', version: str = '1.0.0'):
        super().__init__()
        self.service_name = service_name
        self.version = version
    
    def format(self, record: logging.LogRecord) -> str:
        # 基础字段（每条日志都有）
        log_entry: Dict[str, Any] = {
            'timestamp': datetime.fromtimestamp(
                record.created, tz=timezone.utc
            ).isoformat(),
            'level': record.levelname,
            'service': self.service_name,
            'version': self.version,
            'logger': record.name,
            'message': record.getMessage(),
            'file': f"{record.pathname}:{record.lineno}",
        }
        
        # 如果有异常，加入堆栈信息
        if record.exc_info:
            log_entry['exception'] = {
                'type': record.exc_info[0].__name__,
                'message': str(record.exc_info[1]),
                'stacktrace': traceback.format_exception(*record.exc_info),
            }
        
        # 如果 record 中有额外字段（通过 extra= 传入），加入日志
        # 例如：logger.info("trade processed", extra={"trade_id": "T001"})
        for key, value in record.__dict__.items():
            if key not in (
                'name', 'msg', 'args', 'levelname', 'levelno',
                'pathname', 'filename', 'module', 'exc_info', 'exc_text',
                'stack_info', 'lineno', 'funcName', 'created', 'msecs',
                'relativeCreated', 'thread', 'threadName', 'processName',
                'process', 'message',
            ):
                log_entry[key] = value
        
        return json.dumps(log_entry, ensure_ascii=False, default=str)


def setup_logging(
    service_name: str,
    level: str = 'INFO',
    output: str = 'stdout',
) -> logging.Logger:
    """
    配置结构化日志
    
    Args:
        service_name: 服务名称（出现在每条日志中）
        level: 日志级别（DEBUG/INFO/WARNING/ERROR）
        output: 输出目标（stdout 或文件路径）
    
    Returns:
        配置好的 Logger 实例
    """
    logger = logging.getLogger(service_name)
    logger.setLevel(getattr(logging, level.upper()))
    
    # 清除已有的 handler（避免重复）
    logger.handlers.clear()
    
    # 创建 handler
    if output == 'stdout':
        handler = logging.StreamHandler(sys.stdout)
    else:
        handler = logging.FileHandler(output)
    
    handler.setFormatter(JsonFormatter(service_name=service_name))
    logger.addHandler(handler)
    
    return logger


# ——— 使用示例 ———

logger = setup_logging('trade-processor', level='INFO')

# 基础日志（extra 参数用于添加业务字段）
logger.info(
    "交易处理完成",
    extra={
        'trade_id': 'T001',
        'account_id': 'ACC-001',
        'symbol': 'AAPL',
        'amount': 5000.0,
        'processing_time_ms': 12.5,
        'kafka_partition': 3,
        'kafka_offset': 12345,
    }
)

# 错误日志（自动捕获堆栈信息）
try:
    raise ConnectionError("Kafka broker 连接超时")
except Exception as e:
    logger.error(
        "消息发送失败",
        exc_info=True,  # 自动添加堆栈信息
        extra={
            'trade_id': 'T002',
            'retry_count': 2,
            'error_type': type(e).__name__,
        }
    )

# 输出示例（格式化显示，实际是一行）：
# {
#   "timestamp": "2024-01-15T10:30:01.234+00:00",
#   "level": "ERROR",
#   "service": "trade-processor",
#   "message": "消息发送失败",
#   "trade_id": "T002",
#   "retry_count": 2,
#   "error_type": "ConnectionError",
#   "exception": {
#     "type": "ConnectionError",
#     "message": "Kafka broker 连接超时",
#     "stacktrace": [...]
#   }
# }
```

### 8.6.3 关键日志字段规范

```python
# log_fields.py - 定义标准日志字段（团队内部规范）

# ——— 所有服务必填字段 ———
REQUIRED_FIELDS = {
    'timestamp',    # ISO 8601 格式
    'level',        # DEBUG/INFO/WARNING/ERROR/CRITICAL
    'service',      # 服务名称
    'message',      # 日志消息
}

# ——— Kafka 相关字段（Kafka 操作时填写）———
KAFKA_FIELDS = {
    'kafka_topic',      # Topic 名称
    'kafka_partition',  # 分区号
    'kafka_offset',     # 消息 Offset
    'consumer_group',   # Consumer Group ID
}

# ——— 业务字段（交易系统）———
BUSINESS_FIELDS = {
    'trade_id',         # 交易 ID（最重要，用于追踪）
    'account_id',       # 账户 ID
    'symbol',           # 交易标的
    'amount',           # 交易金额
    'trade_type',       # BUY/SELL
}

# ——— 性能字段（分析性能问题）———
PERFORMANCE_FIELDS = {
    'processing_time_ms',   # 处理时长（毫秒）
    'retry_count',          # 重试次数
    'batch_size',           # 批次大小
}

# ——— 不应该出现在日志中的字段（PII 数据，避免数据泄露）———
FORBIDDEN_FIELDS = {
    'password',         # 密码
    'credit_card',      # 信用卡号
    'ssn',              # 身份证/社会安全号
    'ip_address',       # 用户 IP（GDPR 要求）
}
```

---

## 8.7 动手练习：导入 Grafana Dashboard

### 目标

搭建完整的 Kafka 监控环境，导入预置 Dashboard，观察 Consumer Lag 的变化。

### 步骤一：启动监控栈

```bash
# 1. 下载 JMX Exporter
mkdir -p ./jmx_exporter
wget https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/0.20.0/jmx_prometheus_javaagent-0.20.0.jar \
  -O ./jmx_exporter/jmx_prometheus_javaagent.jar

# 下载 Kafka JMX 配置文件
wget https://raw.githubusercontent.com/prometheus/jmx_exporter/main/example_configs/kafka-2_0_0.yml \
  -O ./jmx_exporter/kafka-2_0_0.yml

# 2. 创建必要目录
mkdir -p ./prometheus/rules
mkdir -p ./alertmanager
mkdir -p ./grafana/dashboards
mkdir -p ./grafana/provisioning/datasources
mkdir -p ./grafana/provisioning/dashboards

# 3. 创建 Grafana Datasource 自动配置
cat > ./grafana/provisioning/datasources/prometheus.yml << 'EOF'
apiVersion: 1
datasources:
  - name: Prometheus
    type: prometheus
    url: http://prometheus:9090
    isDefault: true
    access: proxy
EOF

# 4. 启动监控栈
docker-compose -f docker-compose-monitoring.yml up -d

# 5. 等待服务启动（约 30 秒）
sleep 30

# 验证服务状态
curl http://localhost:9090/-/healthy    # Prometheus 健康检查
curl http://localhost:3000/api/health   # Grafana 健康检查
```

### 步骤二：导入 Grafana Dashboard

```bash
# 在 Grafana Web UI（http://localhost:3000）中：
# 1. 登录（admin / admin123）
# 2. 左侧菜单 → Dashboards → Import
# 3. 输入以下 Dashboard ID（Grafana 官方 Dashboard 库）：

# Kafka Overview Dashboard（社区推荐）
# Dashboard ID: 7589 （Kafka Exporter Overview）

# 或者通过 API 导入：
curl -X POST http://localhost:3000/api/dashboards/import \
  -H "Content-Type: application/json" \
  -H "Authorization: Basic YWRtaW46YWRtaW4xMjM=" \
  -d '{
    "dashboard": {
      "__inputs": [{"name": "DS_PROMETHEUS", "value": "Prometheus"}],
      "id": null,
      "title": "Kafka Consumer Lag"
    },
    "overwrite": true,
    "inputs": [{"name": "DS_PROMETHEUS", "type": "datasource", "value": "Prometheus"}]
  }'
```

### 步骤三：模拟 Consumer Lag 变化

```python
# simulate_lag.py - 模拟 Consumer Lag 变化，观察 Grafana 反应
import time
import json
import uuid
import random
from kafka import KafkaProducer, KafkaConsumer
import threading

# 快速生产（产生 Lag）
def fast_producer(count=10000):
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    )
    print(f"开始生产 {count} 条消息...")
    for i in range(count):
        producer.send('raw-trades', value={'id': i, 'data': 'test'})
        if i % 1000 == 0:
            print(f"已生产 {i} 条")
    producer.flush()
    producer.close()
    print("生产完成！Consumer Lag 应该在 Grafana 中看到上升")

# 慢速消费（模拟处理瓶颈）
def slow_consumer():
    consumer = KafkaConsumer(
        'raw-trades',
        bootstrap_servers='localhost:9092',
        group_id='lag-demo-group',
        auto_offset_reset='earliest',
    )
    print("开始慢速消费（每条消息处理 10ms）...")
    for i, msg in enumerate(consumer):
        time.sleep(0.01)  # 模拟每条消息 10ms 处理时间
        if i % 100 == 0:
            print(f"已消费 {i} 条")

# 先快速生产，再慢速消费
# 在 Grafana 中观察 Consumer Lag 的变化曲线
t_producer = threading.Thread(target=fast_producer)
t_consumer = threading.Thread(target=slow_consumer, daemon=True)

t_consumer.start()
time.sleep(2)  # 先让 Consumer 启动

t_producer.start()  # 然后开始快速生产
t_producer.join()

print("\n观察 Grafana Dashboard（http://localhost:3000）：")
print("  - Consumer Lag 应该从 0 快速增长到约 10,000")
print("  - 然后随着慢速 Consumer 追赶，逐渐下降")
print("  - 这就是真实生产环境中 Lag 波动的模拟")

input("按 Enter 键停止...")
```

### 步骤四：验证告警规则

```bash
# 查看 Prometheus 告警状态
curl -s http://localhost:9090/api/v1/alerts | python3 -m json.tool | grep -A5 "name"

# 手动测试告警规则（修改阈值为 0，触发告警）
# 编辑 prometheus/rules/consumer_lag_alerts.yml
# 将 > 10000 改为 > 0，然后 reload
curl -X POST http://localhost:9090/-/reload

# 查看 AlertManager 接收到的告警
curl http://localhost:9093/api/v1/alerts | python3 -m json.tool
```

### 练习扩展

1. **基础**：配置邮件告警，当 Consumer Lag > 1000 时收到邮件
2. **进阶**：创建自定义 Grafana Panel，显示每个 symbol 的消费速率折线图
3. **挑战**：集成 Grafana Alerting，当 Consumer Lag 告警触发时，自动在 Slack 发通知

---

## 本章小结

| 主题 | 核心内容 |
|------|---------|
| 监控三要素 | Metrics（趋势）+ Logs（事件）+ Traces（链路） |
| 最重要指标 | Consumer Lag（消费积压）、UnderReplicatedPartitions |
| 监控栈 | Prometheus（采集）+ Grafana（展示）+ AlertManager（告警） |
| JMX Exporter | 将 Kafka JMX 指标转为 Prometheus 格式 |
| Python 埋点 | prometheus_client 库，Counter/Histogram/Gauge |
| 结构化日志 | JSON 格式，包含 trade_id 等关键字段 |
| 告警规则 | Lag > 10000 且持续 5 分钟触发告警 |

下一章，我们将深入 Kafka 的**安全与认证**——如何保护你的 Kafka 集群不被未授权访问？
