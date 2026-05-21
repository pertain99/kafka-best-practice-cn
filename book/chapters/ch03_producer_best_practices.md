# 第 3 章：Producer 最佳实践

---

## 本章你将学到

- Producer 内部工作原理：RecordAccumulator、Sender 线程、批量发送机制
- 生产环境最关键的 5 个 Producer 配置参数及其背后的权衡
- 完整的 Python Producer 实现：基础发送、Key 路由、回调处理、错误重试
- 幂等 Producer vs 事务 Producer：如何选择
- 消息 Key 的重要性：Partition 路由与消息顺序保证
- 五种压缩算法的对比实验
- Producer 监控指标解读
- 常见 Producer 错误及解决方案
- 动手实验：发送 1000 条交易记录，观察批量效果

---

## 3.1 Producer 工作原理

在写代码之前，先理解 Producer 内部是如何工作的。这对后续理解各个配置参数的含义至关重要。

### 消息从 send() 到 Broker 的完整路径

```
用户代码调用 producer.produce(topic, key, value)
        │
        ▼
┌─────────────────────────────────────────────────────────────┐
│                       Producer 进程                          │
│                                                             │
│  ① 序列化（Serializer）                                      │
│     key/value → 字节数组                                     │
│           │                                                 │
│           ▼                                                 │
│  ② 分区路由（Partitioner）                                    │
│     决定写入哪个 Partition                                    │
│           │                                                 │
│           ▼                                                 │
│  ③ RecordAccumulator（消息缓冲区）                            │
│     按 Partition 分组存放，等待批量发送                        │
│     ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  │
│     │ Partition 0  │  │ Partition 1  │  │ Partition 2  │  │
│     │ [msg1][msg2] │  │ [msg3]       │  │ [msg4][msg5] │  │
│     │ 当前 batch   │  │ 当前 batch   │  │ 当前 batch   │  │
│     └──────────────┘  └──────────────┘  └──────────────┘  │
│           │                                                 │
│           ▼  触发条件：batch.size 满 OR linger.ms 超时       │
│  ④ Sender 线程（后台网络线程）                                 │
│     将就绪的 Batch 发送到对应 Partition 的 Leader Broker      │
│           │                                                 │
│           ▼                                                 │
│  ⑤ 网络发送 + 等待 ACK                                        │
│     根据 acks 配置决定等待几个副本确认                          │
└─────────────────────────────────────────────────────────────┘
        │
        ▼ (成功)
┌─────────────────┐
│   Kafka Broker   │
│  写入磁盘/内存   │
└─────────────────┘
```

### 关键设计：双线程模型

Producer 内部有两个线程：

**主线程**（你的业务代码线程）：
- 调用 `produce()`，将消息放入 RecordAccumulator
- 消息按 Partition 分组，存入对应的 Deque（双端队列）
- 如果 Accumulator 满了，`produce()` 会阻塞（背压机制）

**Sender 线程**（后台 I/O 线程，confluent-kafka 中由 librdkafka 的 I/O 线程承担）：
- 检查哪些 Batch 已就绪（满足 `batch.size` 或超过 `linger.ms`）
- 将就绪的 Batch 通过 TCP 连接发送给 Broker
- 处理 Broker 的 ACK 响应，触发回调函数

**为什么这个设计很重要？**
- 主线程只做内存操作（极快），不被网络 I/O 阻塞
- 批量发送大幅减少网络请求次数和 Broker 处理压力
- 压缩在 Sender 线程发送前完成，对主线程透明

---

## 3.2 最关键的 5 个 Producer 配置

这 5 个配置参数决定了你的 Producer 在**可靠性**、**吞吐量**和**延迟**三个维度的表现。

### 配置 1：`acks`（确认级别）

`acks` 控制 Broker 何时告诉 Producer "消息已收到"。

| 值 | 含义 | 可靠性 | 延迟 | 适用场景 |
|----|------|--------|------|----------|
| `0` | 不等待任何确认（fire-and-forget） | ❌ 最低 | 最低 | 指标收集，允许丢失 |
| `1` | 只等待 Leader 写入确认 | ⚠️ 中 | 低 | 日志聚合，偶尔丢失可接受 |
| `all`（或`-1`） | 等待所有 ISR 副本写入确认 | ✅ 最高 | 较高 | **金融交易、核心业务** |

**ISR（In-Sync Replicas，同步副本集）**：当前与 Leader 保持同步的 Follower 集合。

```
acks=all 的写入流程：

Producer → Leader ──► Follower 1（同步）
               └──► Follower 2（同步）
                        │
              所有 ISR 都写入后，Leader 才回复 ACK
```

**实际案例**：使用 `acks=1` 时，如果 Leader 写入成功但在 Follower 同步前崩溃，新 Leader 上没有这条消息，数据永久丢失。金融场景中这是不可接受的。

```python
# 生产环境推荐配置
producer_conf = {
    'bootstrap.servers': 'localhost:9092',
    'acks': 'all',   # 最强可靠性，等待所有 ISR 副本确认
}
```

### 配置 2：`enable.idempotence`（幂等性）

幂等 Producer（Idempotent Producer）解决**网络重传导致的消息重复**问题。

**问题场景**：

```
Producer 发送消息 → Broker 写入成功 → 网络超时，ACK 丢失
                                        │
                      Producer 重试发送  ←─────────┘
                                │
                        Broker 再次写入 → 消息重复！
```

**幂等 Producer 的解决方案**：

每条消息有唯一的 `(ProducerID, SequenceNumber)` 标识。Broker 维护每个 ProducerID 最近的 SequenceNumber，如果收到重复序号的消息，直接丢弃，不重复写入。

```python
producer_conf = {
    'bootstrap.servers': 'localhost:9092',
    'acks': 'all',                    # 幂等必须配合 acks=all
    'enable.idempotence': True,       # 开启幂等性
    # 注意：开启幂等后，以下参数会自动调整为安全值：
    # max.in.flight.requests.per.connection ≤ 5
    # retries > 0
}
```

> ⚠️ **幂等性的作用范围**：仅在**单个 Producer 实例的单个会话**内保证。Producer 重启后会获得新的 ProducerID，幂等性重新计算。跨会话的精确一次（Exactly-Once）需要事务 Producer（见 3.4 节）。

### 配置 3：`retries` + `retry.backoff.ms`（重试机制）

网络抖动、Broker 短暂不可用时，Producer 应该自动重试而不是立即报错。

```python
producer_conf = {
    'bootstrap.servers': 'localhost:9092',
    'acks': 'all',
    'enable.idempotence': True,

    # 重试次数：Integer.MAX_VALUE（最大值）意味着无限重试
    # 开启幂等后默认已设为 INT_MAX，无需手动配置
    # 但显式设置更清晰：
    'retries': 2147483647,           # Integer.MAX_VALUE，几乎等于无限重试

    # 重试间隔：每次重试等待多久
    'retry.backoff.ms': 100,         # 初始重试间隔 100ms

    # 消息发送超时（delivery.timeout.ms 内如果无法成功，最终报错）
    # 默认 2 分钟，根据业务 SLA 调整
    'delivery.timeout.ms': 120000,   # 2 分钟内持续重试
}
```

**重试与幂等的配合**：

开启 `enable.idempotence=True` 后，Kafka 保证重试不会导致消息重复（通过 SequenceNumber 去重）。所以你可以放心地设置高重试次数，不必担心数据重复。

### 配置 4：`compression.type`（压缩类型）

压缩在 Producer 端进行，Broker 透明存储，Consumer 自动解压。

| 压缩算法 | 压缩率 | CPU 开销 | 适用场景 |
|----------|--------|----------|----------|
| `none` | 无 | 无 | 消息量小，不关心带宽 |
| `gzip` | 极高（~70%） | 高 | 批量历史数据，冷存储 |
| `snappy` | 高（~50%）| 低 | **推荐**：生产环境通用 |
| `lz4` | 中（~40%）| 极低 | 追求极低延迟 |
| `zstd` | 最高（~75%）| 中 | Kafka 2.1+，新项目首选 |

**为什么推荐 snappy？**

snappy 由 Google 开发，专注于解压速度而非最高压缩率。对于 Kafka 消费场景（写一次，可能读多次），解压速度比压缩率更重要。

```python
producer_conf = {
    'bootstrap.servers': 'localhost:9092',
    'acks': 'all',
    'enable.idempotence': True,
    'compression.type': 'snappy',    # 推荐：平衡压缩率（~50%）和速度
}
```

> 📌 **压缩只有在批量发送时效果显著**：单条消息压缩几乎没有收益。压缩需要配合 `batch.size` 和 `linger.ms` 使用。

### 配置 5：`batch.size` + `linger.ms`（批量优化）

这是提升 Producer 吞吐量最有效的两个参数。

```
batch.size 和 linger.ms 的协作机制：

时间轴：
t=0ms   消息 A 进入 Batch（当前大小: 1 KB）
t=2ms   消息 B 进入 Batch（当前大小: 3 KB）
t=4ms   消息 C 进入 Batch（当前大小: 6 KB）
t=5ms   ← linger.ms=5 超时，Batch 发送（即使没满）
───────────────────────────────────────────────────────
t=5ms   消息 D 进入新 Batch（当前大小: 2 KB）
t=6ms   消息 E 进入 Batch（当前大小: 14 KB）
t=7ms   消息 F 进入 Batch（当前大小: 16 KB）
        ← batch.size=16384 (16KB) 已满，立即发送（不等 linger.ms）
```

```python
producer_conf = {
    'bootstrap.servers': 'localhost:9092',
    'acks': 'all',
    'enable.idempotence': True,
    'compression.type': 'snappy',

    # Batch 最大大小（字节）
    # 默认 16384 (16 KB)，高吞吐场景可调大到 65536 (64 KB) 或 131072 (128 KB)
    'batch.size': 65536,             # 64 KB

    # 发送前等待时间（凑批）
    # 默认 0ms（立即发送），调大可以提升批量效果，但增加延迟
    # 生产环境：5~20ms 是常见值
    'linger.ms': 10,                 # 等待最多 10ms 凑批

    # 消息在 Producer 缓冲区的最大总内存
    'buffer.memory': 33554432,       # 32 MB（默认值）

    # 当缓冲区满时，produce() 调用的最大阻塞时间
    'max.block.ms': 60000,           # 阻塞最多 60 秒
}
```

**延迟 vs 吞吐量的权衡**：

```
linger.ms=0 （默认）：
  延迟最低（每条消息立即发送），吞吐量低（无批量效果）
  适合：实时交易确认、低延迟要求场景

linger.ms=5~20：
  延迟略有增加（5~20ms），吞吐量大幅提升（批量压缩效果明显）
  适合：日志收集、事件追踪、大多数业务场景

linger.ms=100+：
  延迟明显增加，吞吐量极高
  适合：离线数据导入、批量迁移
```

---

## 3.3 完整 Python Producer 实现

现在将上面所有配置整合成一个生产可用的 Producer 类。

### 3.3.1 基础 Producer

```python
# src/producer/base_producer.py
"""
基础 Producer 封装
提供：可靠发送、错误处理、优雅关闭
"""

from confluent_kafka import Producer, KafkaError, KafkaException
from loguru import logger
import json
import time
from typing import Optional, Callable, Any


class BaseKafkaProducer:
    """
    生产环境 Kafka Producer 基类
    封装了最佳实践配置和错误处理逻辑
    """

    def __init__(self, config: dict):
        """
        初始化 Producer

        Args:
            config: confluent-kafka Producer 配置字典
        """
        # 合并默认最佳实践配置和用户配置
        default_conf = {
            'acks': 'all',                    # 最强可靠性
            'enable.idempotence': True,        # 幂等性，防重复
            'retries': 2147483647,             # 无限重试
            'retry.backoff.ms': 100,           # 重试间隔 100ms
            'compression.type': 'snappy',      # 推荐压缩算法
            'batch.size': 65536,               # 64 KB Batch
            'linger.ms': 10,                   # 等待 10ms 凑批
            'buffer.memory': 33554432,         # 32 MB 缓冲区
            'delivery.timeout.ms': 120000,     # 2 分钟最终超时
            # 错误回调：网络错误等异步错误会通过这里通知
            'error_cb': self._on_error,
        }
        # 用户配置覆盖默认配置（允许调用方微调）
        final_conf = {**default_conf, **config}

        self.producer = Producer(final_conf)
        self._send_count = 0      # 已发送消息计数
        self._error_count = 0     # 错误计数
        logger.info(f"Producer 初始化完成，Bootstrap Servers: {final_conf['bootstrap.servers']}")

    def _on_error(self, err):
        """
        全局错误回调
        处理 Producer 级别的异步错误（如连接断开）
        注意：单条消息的发送结果应在 delivery callback 中处理
        """
        self._error_count += 1
        if err.fatal():
            # 致命错误：Producer 无法继续工作，必须重建
            logger.critical(f"Producer 致命错误（Fatal），需要重建 Producer: {err}")
            raise KafkaException(err)
        else:
            # 非致命错误：记录日志，Producer 会自动重试
            logger.warning(f"Producer 非致命错误（将自动重试）: {err}")

    def produce(
        self,
        topic: str,
        value: Any,
        key: Optional[str] = None,
        headers: Optional[dict] = None,
        on_delivery: Optional[Callable] = None,
    ) -> None:
        """
        发送消息（异步，非阻塞）

        Args:
            topic:       目标 Topic 名称
            value:       消息体（dict 会自动序列化为 JSON）
            key:         消息 Key（决定 Partition 路由）
            headers:     消息元数据（追踪 ID、来源等）
            on_delivery: 发送完成回调（成功或失败都会触发）
        """
        # 序列化：dict → JSON 字节串
        if isinstance(value, dict):
            value_bytes = json.dumps(value, ensure_ascii=False).encode('utf-8')
        elif isinstance(value, str):
            value_bytes = value.encode('utf-8')
        else:
            value_bytes = value  # 假设已经是字节

        # Key 序列化
        key_bytes = key.encode('utf-8') if key else None

        # Headers 转换为列表格式（confluent-kafka 要求）
        headers_list = list(headers.items()) if headers else None

        # 使用默认回调（如果没有指定）
        callback = on_delivery or self._default_delivery_callback

        try:
            self.producer.produce(
                topic=topic,
                key=key_bytes,
                value=value_bytes,
                headers=headers_list,
                callback=callback,
            )
            self._send_count += 1

            # 定期调用 poll()，触发回调并处理内部事件
            # 注意：poll(0) 是非阻塞的，只处理当前就绪的事件
            self.producer.poll(0)

        except BufferError:
            # Producer 缓冲区已满（buffer.memory 耗尽）
            # 解决方案：等待 Sender 线程排空缓冲区
            logger.warning("Producer 缓冲区已满，等待 1 秒后重试...")
            self.producer.flush(timeout=1)   # 等待最多 1 秒
            # 重试发送
            self.producer.produce(
                topic=topic,
                key=key_bytes,
                value=value_bytes,
                callback=callback,
            )

    def _default_delivery_callback(self, err, msg):
        """
        默认的消息发送完成回调

        Args:
            err: 发送错误（None 表示成功）
            msg: 发送的消息对象（包含 topic、partition、offset）
        """
        if err is not None:
            self._error_count += 1
            logger.error(
                f"消息发送失败！"
                f"Topic: {msg.topic()}, "
                f"错误: {err}"
            )
        else:
            logger.debug(
                f"消息发送成功 → "
                f"Topic: {msg.topic()}, "
                f"Partition: {msg.partition()}, "   # 实际写入的 Partition
                f"Offset: {msg.offset()}"            # 消息在 Partition 中的位置
            )

    def flush(self, timeout: float = 30.0) -> int:
        """
        等待所有待发送消息完成发送

        Args:
            timeout: 最大等待时间（秒）

        Returns:
            仍在队列中未发送的消息数（0 表示全部发送完成）
        """
        remaining = self.producer.flush(timeout=timeout)
        if remaining > 0:
            logger.warning(f"flush() 超时，仍有 {remaining} 条消息未发送")
        return remaining

    def close(self):
        """
        优雅关闭 Producer
        确保所有缓冲中的消息都被发送后再退出
        """
        logger.info(f"正在关闭 Producer，等待 {self.producer.len()} 条消息发送完成...")
        remaining = self.flush(timeout=30)
        if remaining == 0:
            logger.info(f"Producer 已关闭。总发送: {self._send_count}，错误: {self._error_count}")
        else:
            logger.warning(f"Producer 关闭时仍有 {remaining} 条消息未确认发送！")

    def __enter__(self):
        """支持 with 语句"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """离开 with 块时自动关闭"""
        self.close()

    @property
    def stats(self) -> dict:
        """返回 Producer 统计信息"""
        return {
            'sent': self._send_count,
            'errors': self._error_count,
            'in_queue': self.producer.len(),
        }
```

### 3.3.2 交易事件 Producer（RiskGuard 核心）

```python
# src/producer/transaction_producer.py
"""
RiskGuard 交易事件 Producer
发送交易记录到 Kafka，用于实时风控分析
"""

import uuid
import random
import time
from datetime import datetime, timezone
from typing import Optional
from loguru import logger

from .base_producer import BaseKafkaProducer


# 交易 Topic 名称（从配置中读取）
TOPIC_TXN_RAW = "riskguard.txn.raw"


class TransactionProducer(BaseKafkaProducer):
    """
    交易事件生产者
    演示：Key 路由、业务回调、批量发送
    """

    def __init__(self, bootstrap_servers: str = "localhost:9092"):
        super().__init__({
            'bootstrap.servers': bootstrap_servers,
            # 交易场景：高可靠性是首要需求
            'acks': 'all',
            'enable.idempotence': True,
            'compression.type': 'snappy',
            'batch.size': 65536,    # 64 KB
            'linger.ms': 5,         # 5ms 凑批，平衡延迟和吞吐
        })

    def send_transaction(
        self,
        user_id: str,
        amount: float,
        currency: str = "CNY",
        merchant_id: Optional[str] = None,
        transaction_id: Optional[str] = None,
    ) -> str:
        """
        发送单条交易事件

        Key 设计：使用 user_id 作为 Key
        → 同一用户的所有交易进入同一 Partition
        → 保证同一用户的交易按时间顺序处理（对风控模型很重要！）

        Returns:
            transaction_id（可用于追踪消息）
        """
        txn_id = transaction_id or f"txn_{uuid.uuid4().hex[:16]}"

        # 构建交易事件消息体
        payload = {
            "transaction_id": txn_id,
            "user_id": user_id,
            "amount": amount,
            "currency": currency,
            "merchant_id": merchant_id or f"merchant_{random.randint(1000, 9999)}",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "event_type": "TRANSACTION_INITIATED",
        }

        # 构建追踪 Headers
        headers = {
            "source": "riskguard-producer",
            "version": "1.0",
            "trace_id": uuid.uuid4().hex,    # 分布式追踪 ID
        }

        # 使用业务级别的回调（比默认回调记录更多业务信息）
        def on_delivery(err, msg):
            if err is not None:
                logger.error(
                    f"交易事件发送失败！"
                    f"TxnID: {txn_id}, UserID: {user_id}, "
                    f"Error: {err}"
                )
            else:
                logger.info(
                    f"交易事件已写入 Kafka ✓ "
                    f"TxnID: {txn_id}, "
                    f"Partition: {msg.partition()}, "
                    f"Offset: {msg.offset()}"
                )

        self.produce(
            topic=TOPIC_TXN_RAW,
            value=payload,
            key=user_id,           # 关键！同一用户路由到同一 Partition
            headers=headers,
            on_delivery=on_delivery,
        )

        return txn_id

    def send_batch(
        self,
        transactions: list,
        progress_interval: int = 100,
    ) -> dict:
        """
        批量发送交易事件

        Args:
            transactions: 交易记录列表，每条为 dict
            progress_interval: 每发送多少条打印一次进度

        Returns:
            统计信息 dict
        """
        start_time = time.time()
        sent = 0

        for i, txn in enumerate(transactions, 1):
            self.send_transaction(**txn)
            sent += 1

            # 定期打印进度
            if i % progress_interval == 0:
                elapsed = time.time() - start_time
                rate = i / elapsed
                logger.info(f"进度: {i}/{len(transactions)}，速率: {rate:.0f} msg/s")

            # 关键：定期 poll() 让 librdkafka 处理回调和内部事件
            # 不 poll() 的话，回调函数永远不会被触发
            if i % 1000 == 0:
                self.producer.poll(0)

        # 等待所有消息发送完成（包括网络传输和 ACK）
        logger.info("等待所有消息发送完成（flush）...")
        self.flush(timeout=30)

        elapsed = time.time() - start_time
        return {
            'total': sent,
            'elapsed_seconds': elapsed,
            'rate_per_second': sent / elapsed,
            **self.stats,
        }
```

### 3.3.3 使用示例：发送 1000 条交易记录

```python
# scripts/demo_producer.py
"""
演示：发送 1000 条交易记录，观察批量效果
"""

import random
import time
from loguru import logger

# 将项目根目录加入 Python 路径
import sys
sys.path.insert(0, '/root/riskguard')   # 根据实际路径修改

from src.producer.transaction_producer import TransactionProducer


def generate_transactions(count: int) -> list:
    """生成模拟交易数据"""
    users = [f"user_{i:04d}" for i in range(1, 51)]   # 50 个用户

    transactions = []
    for _ in range(count):
        user_id = random.choice(users)

        # 模拟不同风险级别的交易
        risk_level = random.random()
        if risk_level > 0.99:
            # 1%：高额可疑交易（>= 50000）
            amount = round(random.uniform(50000, 200000), 2)
        elif risk_level > 0.90:
            # 9%：中等交易（1000~50000）
            amount = round(random.uniform(1000, 50000), 2)
        else:
            # 90%：普通小额交易（10~1000）
            amount = round(random.uniform(10, 1000), 2)

        transactions.append({
            'user_id': user_id,
            'amount': amount,
            'currency': random.choice(['CNY', 'USD', 'EUR']),
        })

    return transactions


def main():
    logger.info("=" * 60)
    logger.info("RiskGuard Producer 演示：发送 1000 条交易记录")
    logger.info("=" * 60)

    # 生成测试数据
    txn_count = 1000
    transactions = generate_transactions(txn_count)
    logger.info(f"已生成 {txn_count} 条模拟交易数据")

    # 使用 with 语句确保 Producer 优雅关闭
    with TransactionProducer(bootstrap_servers="localhost:9092") as producer:

        logger.info("开始发送消息...")
        result = producer.send_batch(
            transactions=transactions,
            progress_interval=200,    # 每 200 条打印一次进度
        )

    # 打印结果
    logger.info("=" * 60)
    logger.info("发送完成！统计信息：")
    logger.info(f"  总发送数量:     {result['total']}")
    logger.info(f"  总耗时:         {result['elapsed_seconds']:.2f} 秒")
    logger.info(f"  吞吐量:         {result['rate_per_second']:.0f} 消息/秒")
    logger.info(f"  发送错误数:     {result['errors']}")
    logger.info("=" * 60)
    logger.info("打开 http://localhost:8080 查看 Kafka UI 中的消息！")


if __name__ == '__main__':
    main()
```

---

## 3.4 幂等 Producer vs 事务 Producer

### 幂等 Producer（Idempotent Producer）

**适用场景**：单个 Producer 写入单个 Topic，需要防止网络重传导致重复。

**保证**：在同一 Producer 实例的生命周期内，即使发生重试，消息也只会被写入 Kafka 一次（Exactly-Once 写入）。

```python
# 幂等 Producer 配置（本章使用）
conf = {
    'bootstrap.servers': 'localhost:9092',
    'enable.idempotence': True,   # 开启幂等
    'acks': 'all',                # 幂等要求 acks=all
}
```

**限制**：
- 只保证单 Producer → 单 Topic 的写入幂等
- Producer 重启后幂等性重置
- 不能跨多个 Topic 保证原子性

### 事务 Producer（Transactional Producer）

**适用场景**：需要跨多个 Topic/Partition 的原子写入，或实现"读取-处理-写入"的精确一次（Exactly-Once）语义。

```
事务使用场景：

读取 Topic A 的消息 → 处理 → 写入 Topic B + 提交 Offset

要求：Topic B 的写入 和 Offset 提交 必须同时成功或同时失败
（典型的 Kafka Streams 使用场景）
```

```python
# 事务 Producer 配置（第 6 章 Streams 章节详细讲解）
conf = {
    'bootstrap.servers': 'localhost:9092',
    'transactional.id': 'riskguard-producer-1',  # 唯一的事务 ID
    'enable.idempotence': True,                   # 事务隐含幂等
    'acks': 'all',
}

producer = Producer(conf)
producer.init_transactions()    # 初始化事务（向 Kafka 注册 transactional.id）

try:
    producer.begin_transaction()   # 开始事务

    producer.produce('topic-a', value=b'message-1')
    producer.produce('topic-b', value=b'message-2')

    # 在事务中提交 Consumer Offset（实现 Exactly-Once）
    # producer.send_offsets_to_transaction(offsets, consumer_group_metadata)

    producer.commit_transaction()  # 原子提交
    logger.info("事务提交成功")

except KafkaException as e:
    producer.abort_transaction()   # 事务回滚
    logger.error(f"事务失败，已回滚: {e}")
```

### 如何选择？

| 需求 | 选择 |
|------|------|
| 单 Producer 写入，防止网络重传重复 | **幂等 Producer**（本章） |
| 跨多 Topic 原子写入 | **事务 Producer** |
| 流处理：读取-处理-写入精确一次 | **事务 Producer**（第 6 章） |
| 批量导入，允许偶尔重复，优先速度 | **普通 Producer**（无幂等/事务） |

---

## 3.5 消息 Key 的重要性

Key 是 Kafka Producer 中最常被忽视、又极其重要的概念。

### Key 决定 Partition 路由

```python
# Kafka 的默认分区算法（Murmur2 哈希）
partition = murmur2_hash(key) % num_partitions

# 举例：Topic 有 4 个 Partition
# hash("user_001") % 4 = 2 → 始终写入 Partition 2
# hash("user_002") % 4 = 0 → 始终写入 Partition 0
# hash("user_003") % 4 = 3 → 始终写入 Partition 3
```

**相同 Key 的消息保证进入同一 Partition → 保证这些消息的处理顺序**。

### 为什么风控场景必须使用 Key？

```
场景：用户 user_001 在 1 分钟内发起 3 笔交易

不使用 Key（轮询分配）：
  交易 1 → Partition 0 → Consumer A 处理
  交易 2 → Partition 1 → Consumer B 处理
  交易 3 → Partition 2 → Consumer C 处理

  Consumer A、B、C 各自独立处理，无法感知"同一用户在短时间内连续交易"的风险模式！

使用 user_id 作为 Key：
  交易 1 → Partition 2 → Consumer B 处理
  交易 2 → Partition 2 → Consumer B 处理（相同用户，相同 Partition）
  交易 3 → Partition 2 → Consumer B 处理（同一 Consumer 按顺序处理）

  Consumer B 可以看到用户的完整交易序列，轻松检测异常模式！
```

### Key 的设计原则

**原则 1：选择能代表业务关联性的字段**

```python
# 好的 Key 选择：
# 风控系统 → user_id（同一用户的交易需要顺序处理）
# 订单系统 → order_id（同一订单的状态变更需要顺序）
# IoT 系统 → device_id（同一设备的数据需要时序处理）

# 不好的 Key 选择：
# 随机 UUID → 无法利用局部性，等于没有 Key
# 时间戳    → 所有消息集中写入少数 Partition（热点问题）
```

**原则 2：警惕 Key 分布不均（热点问题）**

```
如果 Key 分布极度不均，某些 Partition 会成为热点：

用户 ID  | 交易次数 | Partition
user_vip |  90,000  |    0      ← 热点！
user_001 |     100  |    1
user_002 |      80  |    2
user_003 |      70  |    3

解决方案：
1. 在 Key 后添加随机后缀（牺牲顺序保证）：user_vip#rand(0,3)
2. 将 Partition 数调大，分散哈希结果
3. 对超级用户单独开辟 Topic
```

**原则 3：没有顺序需求时，不要设置 Key**

没有 Key 时，Kafka 2.4+ 使用 Sticky Partitioner：一段时间内将消息黏性分配到同一 Partition，等到 Batch 发满或超时后才切换到下一个 Partition。相比轮询，这样能形成更大的 Batch，提升压缩效果。

---

## 3.6 压缩对比实验

动手实验：比较不同压缩算法的实际效果。

```python
# scripts/compression_benchmark.py
"""
压缩算法对比实验
比较：None, gzip, snappy, lz4, zstd
"""

import time
import json
import random
import uuid
from confluent_kafka import Producer
from loguru import logger


def generate_message(size_kb: int = 1) -> bytes:
    """
    生成指定大小的模拟 JSON 消息
    JSON 消息有大量重复字段，压缩效果好
    """
    base = {
        "transaction_id": str(uuid.uuid4()),
        "user_id": f"user_{random.randint(1, 10000):05d}",
        "amount": round(random.uniform(10, 10000), 2),
        "currency": "CNY",
        "merchant_name": random.choice([
            "北京超市", "上海便利店", "广州餐厅", "深圳电商", "杭州奶茶"
        ]),
        "status": random.choice(["PENDING", "SUCCESS", "FAILED"]),
        "metadata": {
            "ip": f"{random.randint(1,255)}.{random.randint(1,255)}.{random.randint(1,255)}.1",
            "device": random.choice(["iOS", "Android", "Web"]),
            "location": random.choice(["北京", "上海", "广州", "深圳", "杭州"]),
        }
    }
    msg = json.dumps(base, ensure_ascii=False)
    # 填充到目标大小
    while len(msg.encode('utf-8')) < size_kb * 1024:
        msg = msg[:-1] + ',"padding":"' + "x" * 100 + '"}'
    return msg.encode('utf-8')


def benchmark_compression(compression: str, message_count: int = 500) -> dict:
    """
    测试指定压缩算法的发送性能

    Args:
        compression: 压缩算法名称
        message_count: 发送消息数量

    Returns:
        性能统计
    """
    conf = {
        'bootstrap.servers': 'localhost:9092',
        'acks': '1',                          # 基准测试用 acks=1 降低等待
        'compression.type': compression,       # 压缩算法
        'batch.size': 65536,
        'linger.ms': 10,
        # 开启 Producer 统计（每 5 秒上报）
        'statistics.interval.ms': 5000,
        'stats_cb': lambda stats: None,        # 忽略统计回调输出
    }

    producer = Producer(conf)

    # 预生成消息（1 KB）
    messages = [generate_message(size_kb=1) for _ in range(message_count)]

    start_time = time.time()
    sent_bytes = 0

    for msg in messages:
        producer.produce(
            topic='hello-kafka',
            value=msg,
            key=f"test-{random.randint(1,10)}".encode(),
        )
        sent_bytes += len(msg)

    producer.flush(timeout=30)
    elapsed = time.time() - start_time

    return {
        'compression': compression,
        'messages': message_count,
        'original_size_kb': sent_bytes / 1024,
        'elapsed_seconds': round(elapsed, 3),
        'throughput_msg_per_sec': round(message_count / elapsed),
        'throughput_mb_per_sec': round((sent_bytes / 1024 / 1024) / elapsed, 2),
    }


if __name__ == '__main__':
    logger.info("开始压缩算法对比实验（每种算法发送 500 条 1KB 消息）...")
    logger.info("=" * 70)

    results = []
    for algo in ['none', 'gzip', 'snappy', 'lz4', 'zstd']:
        logger.info(f"测试压缩算法: {algo}...")
        result = benchmark_compression(algo, message_count=500)
        results.append(result)
        logger.info(
            f"  {algo:8s}: "
            f"{result['throughput_msg_per_sec']:5d} msg/s, "
            f"{result['throughput_mb_per_sec']:.2f} MB/s, "
            f"耗时 {result['elapsed_seconds']:.3f}s"
        )

    logger.info("=" * 70)
    logger.info("实验完成！结论（典型 JSON 消息）：")
    logger.info("  - zstd 压缩率最高，适合新项目")
    logger.info("  - snappy 速度最快，适合延迟敏感场景")
    logger.info("  - gzip 压缩率高但 CPU 开销大，适合冷存储")
    logger.info("  - lz4 速度与 snappy 相近，压缩率略低")
```

典型实验结果（参考，实际值因硬件而异）：

| 压缩算法 | 吞吐量 | CPU 开销 | 压缩率（JSON） | 推荐度 |
|----------|--------|----------|----------------|--------|
| none | 最高 | 无 | 0% | 仅在带宽极充裕时使用 |
| lz4 | 高 | 极低 | ~35% | ⭐⭐⭐⭐ 低延迟场景 |
| snappy | 高 | 低 | ~45% | ⭐⭐⭐⭐⭐ **日常推荐** |
| zstd | 中 | 中 | ~65% | ⭐⭐⭐⭐⭐ 新项目推荐 |
| gzip | 低 | 高 | ~70% | ⭐⭐ 批处理/归档 |

---

## 3.7 Producer 监控指标

了解这些指标可以帮助你及早发现 Producer 性能问题。

### 关键 JMX 指标

| 指标名 | 含义 | 健康值 | 告警阈值 |
|--------|------|--------|----------|
| `record-send-rate` | 每秒发送的消息数 | 业务正常范围 | 突然降为 0 |
| `record-error-rate` | 每秒发送失败的消息数 | 接近 0 | > 0.1 |
| `batch-size-avg` | 平均 Batch 大小（字节） | 接近 batch.size 上限 | 远小于 batch.size |
| `request-latency-avg` | Producer 请求平均延迟（ms） | < 50ms | > 200ms |
| `outgoing-byte-rate` | 每秒发送字节数 | 业务正常范围 | 异常下降 |
| `buffer-available-bytes` | 缓冲区剩余可用内存 | > 10% buffer.memory | < 1MB |
| `record-queue-time-avg` | 消息在 RecordAccumulator 中等待的平均时间 | < linger.ms | > 5×linger.ms |

### Python 中暴露 Producer 指标

```python
# 使用 confluent-kafka 的内置统计功能
from confluent_kafka import Producer
import json
from prometheus_client import Gauge, start_http_server

# Prometheus 指标定义
send_rate_gauge = Gauge('kafka_producer_send_rate', 'Records sent per second')
error_rate_gauge = Gauge('kafka_producer_error_rate', 'Record errors per second')
batch_size_gauge = Gauge('kafka_producer_batch_size_avg', 'Average batch size bytes')

def stats_callback(stats_json_str):
    """
    confluent-kafka 定期调用此回调，传入 JSON 格式的统计数据
    statistics.interval.ms 控制回调频率
    """
    stats = json.loads(stats_json_str)

    # 提取关心的指标（stats 结构较复杂，按需提取）
    for topic_name, topic_stats in stats.get('topics', {}).items():
        for partition_id, partition_stats in topic_stats.get('partitions', {}).items():
            if partition_id == '-1':
                continue   # 跳过汇总行

            # txmsgs: 该 Partition 累计发送的消息数
            # 通过与上次比较计算速率（或直接用 Prometheus Counter）
            pass

    # 发送速率（整个 Producer 级别）
    txmsgs_per_sec = stats.get('txmsgs_d', 0)    # 最近一个统计周期发送数
    send_rate_gauge.set(txmsgs_per_sec)

    # 错误率
    err_per_sec = stats.get('txerrs_d', 0)
    error_rate_gauge.set(err_per_sec)


producer = Producer({
    'bootstrap.servers': 'localhost:9092',
    'acks': 'all',
    'enable.idempotence': True,
    'statistics.interval.ms': 5000,   # 每 5 秒触发一次统计回调
    'stats_cb': stats_callback,
})

# 启动 Prometheus metrics HTTP server（供 Prometheus 抓取）
start_http_server(8000)   # 访问 http://localhost:8000/metrics
```

---

## 3.8 常见 Producer 错误及解决方案

### 错误 1：`MSG_SIZE_TOO_LARGE`

**症状**：

```
KafkaError{code=MSG_SIZE_TOO_LARGE, val=10, str="Broker: Message size too large"}
```

**原因**：消息大小超过 Broker 配置的 `message.max.bytes`（默认 1 MB）。

**解决方案**：

```python
# 方案 1：在 Producer 端拆分大消息
MAX_MSG_SIZE = 800 * 1024  # 800 KB（留 200 KB 余量）

def split_large_message(payload: dict) -> list:
    """将大消息拆分为多条小消息"""
    encoded = json.dumps(payload).encode('utf-8')
    if len(encoded) <= MAX_MSG_SIZE:
        return [payload]

    # 拆分逻辑（根据业务场景定制）
    # 例如：将 payload 中的列表字段拆成多批
    ...

# 方案 2：调整 Broker 配置（需要同时修改多处）
# server.properties:
#   message.max.bytes=10485760        # 10 MB
#   replica.fetch.max.bytes=10485760  # 同步也需要调大

# 方案 3：对超大消息使用外部存储（如 S3），Kafka 只传指针
payload = {
    "data_location": "s3://bucket/large-data-xxx.json",
    "data_size_bytes": 50000000,
    "metadata": {...}
}
```

### 错误 2：`UNKNOWN_TOPIC_OR_PART`

**症状**：

```
KafkaError{code=UNKNOWN_TOPIC_OR_PART, val=3}
```

**原因**：Topic 不存在，且 `auto.create.topics.enable=false`（生产环境通常设为 false）。

**解决方案**：

```python
# 在发送前确保 Topic 存在（使用 AdminClient 创建）
from confluent_kafka.admin import AdminClient, NewTopic

def ensure_topic_exists(bootstrap_servers: str, topic: str, partitions: int = 4):
    """如果 Topic 不存在则创建"""
    admin = AdminClient({'bootstrap.servers': bootstrap_servers})

    # 检查 Topic 是否已存在
    existing_topics = admin.list_topics(timeout=10).topics
    if topic not in existing_topics:
        new_topic = NewTopic(
            topic=topic,
            num_partitions=partitions,
            replication_factor=1,
        )
        result = admin.create_topics([new_topic])
        for t, f in result.items():
            f.result()   # 等待创建完成，如有错误会抛出异常
        logger.info(f"Topic '{topic}' 创建成功（{partitions} 个 Partition）")
    else:
        logger.debug(f"Topic '{topic}' 已存在，跳过创建")
```

### 错误 3：Producer 吞吐量低

**症状**：`record-send-rate` 指标远低于预期，`batch-size-avg` 接近 0。

**排查清单**：

```python
# 检查 1：linger.ms 是否为 0（无批量效果）
# 解决：设置 linger.ms=5 或 10

# 检查 2：每次 produce() 后是否调用了 flush()（破坏批量）
# 错误写法：
for msg in messages:
    producer.produce(topic, value=msg)
    producer.flush()   # ← 每条消息都强制刷新，批量完全失效！

# 正确写法：
for msg in messages:
    producer.produce(topic, value=msg)
    producer.poll(0)   # 非阻塞 poll，处理回调
producer.flush()       # ← 所有消息发送完再 flush

# 检查 3：compression.type 是否为 none
# 解决：使用 snappy 或 zstd

# 检查 4：网络是否成为瓶颈
# 解决：检查 request-latency-avg，如果高则检查网络和 Broker 负载
```

### 错误 4：`OFFSET_OUT_OF_RANGE`（Consumer 相关，但 Producer 配置可预防）

**预防措施**：合理设置 `log.retention.ms`，确保 Consumer 有足够时间消费数据。

### 错误 5：Producer 内存泄漏

**症状**：长期运行后 Python 进程内存持续增长。

**常见原因和解决方案**：

```python
# 问题：没有调用 poll()，导致 librdkafka 内部队列堆积
# 解决：每批次 produce 后调用 poll(0)

# 问题：Producer 对象没有关闭（GC 无法回收 C 层对象）
# 解决：显式调用 producer.flush() 和 del producer，或使用 with 语句

# 推荐：始终使用 with 语句管理 Producer 生命周期
with BaseKafkaProducer({'bootstrap.servers': 'localhost:9092'}) as producer:
    producer.produce(...)
# 离开 with 块时自动调用 close()，确保资源释放
```

---

## 小结

本章覆盖了 Producer 从原理到最佳实践的完整知识体系：

1. **双线程模型**：主线程负责内存缓冲，Sender 线程负责网络 I/O，是高吞吐的基础
2. **5 大关键配置**：
   - `acks=all` + `enable.idempotence=true`：最强可靠性保证
   - `retries=MAX` + `retry.backoff.ms`：自动处理短暂故障
   - `compression.type=snappy`：平衡压缩率和 CPU 开销
   - `batch.size=64KB` + `linger.ms=10`：批量优化，大幅提升吞吐
3. **消息 Key**：`user_id` 等业务 Key 保证同一实体的消息顺序，是风控等场景的基础
4. **幂等 vs 事务**：日常场景用幂等 Producer，跨 Topic 原子操作用事务 Producer
5. **监控指标**：`record-error-rate`、`batch-size-avg`、`request-latency-avg` 是最重要的三个

---

## 动手练习

**练习 3.1：运行 Producer 演示脚本**

1. 确保 Docker 环境已启动（`docker compose ps`）
2. 安装 Python 依赖（`pip install confluent-kafka loguru`）
3. 运行演示脚本：

```bash
cd ~/riskguard
python scripts/demo_producer.py
```

4. 打开 Kafka UI（http://localhost:8080），查看：
   - `riskguard.txn.raw` Topic 中的消息数量
   - 各 Partition 的消息分布是否均匀
   - 选择几条消息，查看 Key 值和 Headers

**练习 3.2：观察批量效果**

分别用以下配置发送 1000 条消息，对比耗时：

```python
# 配置 A：无批量（模拟低吞吐场景）
conf_a = {
    'bootstrap.servers': 'localhost:9092',
    'acks': '1',
    'linger.ms': 0,        # 立即发送
    'batch.size': 1,       # 极小 Batch（等效于逐条发送）
    'compression.type': 'none',
}

# 配置 B：批量优化（推荐配置）
conf_b = {
    'bootstrap.servers': 'localhost:9092',
    'acks': 'all',
    'enable.idempotence': True,
    'linger.ms': 10,
    'batch.size': 65536,
    'compression.type': 'snappy',
}
```

记录两组配置的发送耗时和吞吐量，感受批量优化的效果。

**练习 3.3：运行压缩对比实验**

```bash
python scripts/compression_benchmark.py
```

记录你的机器上各压缩算法的吞吐量数据，并思考：在你的应用场景（高吞吐日志 vs 低延迟交易）中，应该选择哪种压缩算法？

---

*下一章，我们将深入 Consumer 的最佳实践，学习如何设计高可用、高吞吐的消费者，以及 Offset 管理的正确姿势——这些都是 RiskGuard 风控引擎的核心。*
