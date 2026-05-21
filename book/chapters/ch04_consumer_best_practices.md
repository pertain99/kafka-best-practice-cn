# 第 4 章：Consumer 最佳实践

## 本章你将学到

- Consumer Group（消费者组）的工作原理与分区分配策略
- Rebalance（再平衡）的触发条件及其对系统的影响
- Offset（偏移量）管理的最佳实践：手动提交的正确姿势
- At-least-once 与 Exactly-once 消费语义的区别与取舍
- 完整的 Python Consumer 代码示例，含手动 Offset 提交
- 背压（Backpressure）处理：Consumer 跟不上 Producer 时怎么办
- Consumer 性能调优的核心参数
- Dead Letter Queue（死信队列）模式处理毒丸消息
- Consumer 关键监控指标

---

## 4.1 Consumer Group 工作原理

### 基本概念

Kafka 的 Consumer Group（消费者组）是 Kafka 实现横向扩展消费能力的核心机制。同一个 Group 内的多个 Consumer 实例共同消费一个或多个 Topic 的消息，每条消息只会被 Group 内的**一个** Consumer 处理。

```
Topic: prod.trading.trades.v1
分区: P0  P1  P2  P3  P4  P5

Consumer Group A (3 个实例):
  Consumer-1 → P0, P1
  Consumer-2 → P2, P3
  Consumer-3 → P4, P5
```

**关键规则：**
- 每个分区在同一时刻只能被同一 Group 内的**一个** Consumer 消费
- Consumer 数量 > 分区数：多出来的 Consumer 空闲（浪费资源）
- Consumer 数量 < 分区数：部分 Consumer 消费多个分区（负载不均衡）
- Consumer 数量 = 分区数：理想状态，每个 Consumer 负责一个分区

### 分区分配策略

Kafka 提供三种内置的分区分配策略（Partition Assignment Strategy），通过 `partition.assignment.strategy` 参数配置。

#### RangeAssignor（范围分配，默认）

按照字典序排列 Topic 的分区，然后平均分配给 Consumer。

```
Topic A: 3 个分区 (A0, A1, A2)
Topic B: 3 个分区 (B0, B1, B2)
Consumer: C1, C2

分配结果:
  C1 → A0, A1, B0, B1
  C2 → A2, B2

问题：C1 负载是 C2 的两倍！多个 Topic 时不均衡。
```

**适用场景：** 单 Topic 消费，或对分区连续性有要求的场景。

#### RoundRobinAssignor（轮询分配）

将所有 Topic 的所有分区放在一起，轮流分配给每个 Consumer。

```
Topic A: A0, A1, A2
Topic B: B0, B1, B2
Consumer: C1, C2

合并后轮询: A0→C1, A1→C2, A2→C1, B0→C2, B1→C1, B2→C2

分配结果:
  C1 → A0, A2, B1
  C2 → A1, B0, B2

优点：分配更均衡
```

**适用场景：** 多 Topic 消费，要求负载均衡的场景。

#### StickyAssignor（粘性分配，推荐生产使用）

在 Rebalance 时尽量保留原有的分配关系，减少分区迁移。

```
初始分配:
  C1 → P0, P1, P2
  C2 → P3, P4, P5

C2 宕机后 Rebalance:
  RangeAssignor:   C1 → P0, P1, P2, P3, P4, P5（全部重新分配）
  StickyAssignor:  C1 → P0, P1, P2, P3, P4, P5（C1 保留 P0/P1/P2，接管 P3/P4/P5）

C3 加入后再次 Rebalance:
  StickyAssignor:  C1 → P0, P1, P2 (保留)
                   C3 → P3, P4, P5 (接管原 C2 的)
  移动最少！
```

**为什么推荐 StickyAssignor？**
- Rebalance 期间消费暂停，分区移动越少，暂停时间越短
- 减少 Consumer 重建状态的开销（如本地缓存、数据库连接）

```python
from confluent_kafka import Consumer

consumer_config = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'risk-alert-consumer-group',
    # 使用粘性分配策略
    'partition.assignment.strategy': 'cooperative-sticky',
    'auto.offset.reset': 'earliest',
}
```

> **注意：** `cooperative-sticky` 是 Kafka 2.4+ 引入的增量式 Rebalance 协议，比老版 `sticky` 更好——它允许 Consumer 在 Rebalance 期间继续消费未被迁移的分区。

---

### Rebalance（再平衡）：触发条件与影响

Rebalance 是指 Consumer Group 重新分配分区的过程。**Rebalance 期间，所有 Consumer 停止消费**（Stop-the-World），这是 Kafka 消费端最大的性能杀手之一。

#### 触发条件

| 触发事件 | 说明 |
|---------|------|
| Consumer 加入 Group | 新实例启动 |
| Consumer 离开 Group | 实例正常关闭（调用 `close()`） |
| Consumer 崩溃/超时 | 心跳超时或 `poll()` 间隔过长 |
| Topic 分区数变更 | 增加分区 |
| Group 订阅的 Topic 变更 | 订阅新 Topic 或取消订阅 |

#### 最危险的场景：Consumer 处理时间过长导致的假死

```
Consumer poll() → 处理消息（耗时 60s）→ 下次 poll()

如果 max.poll.interval.ms = 300000（5分钟）→ 没问题
如果 max.poll.interval.ms = 30000（30秒）→ Broker 认为该 Consumer 已死 → Rebalance！
```

**最佳实践：控制 poll 间隔不超过 `max.poll.interval.ms`**

```python
# 关键配置：调整 Rebalance 相关参数
consumer_config = {
    # 心跳间隔，越小越快发现 Consumer 崩溃（默认 3000ms）
    'heartbeat.interval.ms': 3000,
    
    # Session 超时：超过此时间没收到心跳，触发 Rebalance
    # 必须在 [heartbeat.interval.ms * 3, group.max.session.timeout.ms] 范围内
    'session.timeout.ms': 30000,
    
    # 两次 poll() 之间的最大时间，超时触发 Rebalance
    # 必须大于单次消息处理的最大时间！
    'max.poll.interval.ms': 300000,  # 5 分钟
    
    # 每次 poll 拉取的最大消息数，减小可缩短单次处理时间
    'max.poll.records': 100,
}
```

---

## 4.2 Offset 管理最佳实践

Offset（偏移量）是 Kafka 消费进度的标记。每个分区的每条消息都有一个单调递增的 Offset 编号。Consumer 通过提交 Offset 来记录"我消费到哪里了"。

### 自动提交 vs 手动提交

**永远使用手动提交。** 这是本章最重要的结论。

#### 自动提交的致命问题

```python
# ❌ 危险：自动提交
consumer = Consumer({
    'enable.auto.commit': True,       # 自动提交
    'auto.commit.interval.ms': 5000,  # 每 5 秒自动提交
})

# 消息拉取成功后，Kafka 在后台每 5 秒提交一次 Offset
# 问题：如果消息拉到内存，Offset 自动提交了，但处理失败了怎么办？
# 结果：消息丢失！（Offset 已提交，但业务逻辑没有成功执行）

messages = consumer.poll(timeout=1.0)  # 假设拉到 Offset=100
# 4 秒后，自动提交把 Offset 推进到 101
# 但此时消息处理抛出异常...
# Offset 已经提交，消息永远消失了
```

#### 手动提交：掌控消费进度

```python
# ✅ 正确：手动提交，只有业务逻辑成功后才提交 Offset
consumer = Consumer({
    'enable.auto.commit': False,  # 关闭自动提交
})

messages = consumer.poll(timeout=1.0)
if messages:
    # 1. 处理消息
    process_message(messages)
    # 2. 处理成功后，才提交 Offset
    consumer.commit()  # 此时 Offset 才被推进
```

### commitSync vs commitAsync

#### commitSync（同步提交）

```python
# commitSync：阻塞等待 Broker 确认 Offset 已提交
# 优点：可靠，确认提交成功
# 缺点：阻塞，影响吞吐量

try:
    consumer.commit(asynchronous=False)  # 同步提交
    logger.info("Offset 提交成功")
except KafkaException as e:
    logger.error(f"Offset 提交失败: {e}")
    # 需要决策：重试？还是跳过？
```

**何时使用 commitSync？**
- 程序即将关闭时（Shutdown Hook）
- 处理完一批消息的最后一步
- 对数据一致性要求极高的场景（不允许任何重复消费）

#### commitAsync（异步提交）

```python
# commitAsync：不等 Broker 确认，继续处理下一批
# 优点：不阻塞，吞吐量高
# 缺点：提交失败时不重试（避免乱序提交问题）

def on_commit(err, offsets):
    """异步提交的回调函数"""
    if err:
        logger.error(f"异步 Offset 提交失败: {err}")
        # 注意：不要在这里重试！可能导致提交乱序
        # 正确做法：记录告警，等下一次 poll 后重新提交
    else:
        logger.debug(f"异步 Offset 提交成功: {offsets}")

consumer.commit(asynchronous=True, on_commit=on_commit)  # 异步提交
```

**为什么 commitAsync 不重试？**

```
假设有两次异步提交：
  提交1：Offset=100（消息 1-100）
  提交2：Offset=200（消息 101-200）

如果提交1失败，重试时序可能变成：
  提交2 成功（Offset=200）
  提交1 重试成功（Offset=100）← 把 Offset 回退了！

这会导致消息 101-200 被重复消费。
```

**最佳实践：正常消费用 `commitAsync`，关闭时用 `commitSync`**

```python
try:
    while running:
        messages = consumer.poll(timeout=1.0)
        if messages:
            process_batch(messages)
            consumer.commit(asynchronous=True)  # 正常流程：异步提交
finally:
    # 关闭前：同步提交，确保最后一批 Offset 被持久化
    consumer.commit(asynchronous=False)
    consumer.close()
```

---

### 消费语义：At-least-once vs Exactly-once

#### At-least-once（至少一次，最常用）

```
保证：每条消息至少被处理一次，可能被处理多次
实现：先处理，后提交 Offset
风险：Consumer 处理成功但提交 Offset 前崩溃 → 重启后重复消费

适用：业务逻辑是幂等的（多次处理结果相同）
例：INSERT OR UPDATE，不是 INSERT
```

#### At-most-once（至多一次）

```
保证：每条消息最多被处理一次，可能被跳过
实现：先提交 Offset，后处理
风险：提交 Offset 后处理失败 → 消息丢失

适用：允许丢失数据的场景（日志收集、非关键指标）
强烈不推荐用于金融交易！
```

#### Exactly-once（精确一次）

```
保证：每条消息恰好被处理一次
实现：Kafka Transactions + 幂等消费者，或外部事务协调
代价：性能较低，实现复杂

适用：金融交易、账户余额更新等关键业务
```

**实现幂等消费（At-least-once 的工程实践）：**

```python
import hashlib

def process_trade_idempotent(message, db_conn):
    """
    幂等消费：使用 trade_id 作为幂等键
    即使消息被重复投递，数据库状态也不会重复
    """
    trade_data = json.loads(message.value())
    trade_id = trade_data['trade_id']
    
    # 方案1：INSERT IGNORE（MySQL）或 INSERT OR IGNORE（SQLite）
    db_conn.execute("""
        INSERT OR IGNORE INTO trades (trade_id, account_id, asset_pair, side, quantity, price)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (trade_id, trade_data['account_id'], trade_data['asset_pair'],
          trade_data['side'], trade_data['quantity'], trade_data['price']))
    
    # 方案2：UPSERT（INSERT ... ON CONFLICT DO UPDATE）
    db_conn.execute("""
        INSERT INTO trades (trade_id, status, processed_at)
        VALUES (?, 'PROCESSED', NOW())
        ON CONFLICT (trade_id) DO NOTHING
    """, (trade_id,))
    
    db_conn.commit()
```

---

## 4.3 完整 Python Consumer 代码

下面是一个生产级 Python Consumer 示例，包含：
- 手动 Offset 提交（正常流程异步，关闭时同步）
- 优雅关闭（Graceful Shutdown）
- 异常处理与重试逻辑
- 死信队列（Dead Letter Queue）集成

```python
#!/usr/bin/env python3
"""
生产级 Kafka Consumer 示例
适用于 RiskGuard 项目的交易事件消费
"""

import json
import signal
import logging
import time
from typing import Optional, Callable
from confluent_kafka import Consumer, Producer, KafkaError, KafkaException, Message

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s - %(message)s'
)
logger = logging.getLogger(__name__)


class RiskGuardConsumer:
    """
    RiskGuard 交易事件消费者
    
    特性：
    - 手动 Offset 提交（At-least-once 语义）
    - 优雅关闭（SIGTERM/SIGINT 处理）
    - 自动重试（可配置次数）
    - 死信队列（处理毒丸消息）
    - Rebalance 回调（分区分配/撤销时的钩子）
    """
    
    def __init__(
        self,
        bootstrap_servers: str,
        group_id: str,
        topics: list[str],
        dlq_topic: str = "dead-letter-queue",
        max_retries: int = 3,
        retry_backoff_ms: int = 1000,
    ):
        self.topics = topics
        self.dlq_topic = dlq_topic
        self.max_retries = max_retries          # 最大重试次数
        self.retry_backoff_ms = retry_backoff_ms # 重试间隔（毫秒）
        self._running = False                    # 运行状态标志（用于优雅关闭）
        
        # Consumer 配置
        consumer_config = {
            'bootstrap.servers': bootstrap_servers,
            'group.id': group_id,
            
            # !! 关键：关闭自动提交，使用手动提交
            'enable.auto.commit': False,
            
            # 分区分配策略：cooperative-sticky 减少 Rebalance 影响
            'partition.assignment.strategy': 'cooperative-sticky',
            
            # 从最早的消息开始消费（新 Group 首次启动时）
            'auto.offset.reset': 'earliest',
            
            # 每次 poll 最多拉取 100 条（控制单次处理量，避免超过 max.poll.interval.ms）
            'max.poll.records': 100,
            
            # 两次 poll 之间最大间隔（处理时间 < 此值，否则触发 Rebalance）
            'max.poll.interval.ms': 300000,  # 5 分钟
            
            # 心跳间隔（需 < session.timeout.ms / 3）
            'heartbeat.interval.ms': 3000,
            
            # Session 超时（超过此时间没有心跳，Broker 认为 Consumer 已死）
            'session.timeout.ms': 30000,
        }
        
        # DLQ Producer 配置
        producer_config = {
            'bootstrap.servers': bootstrap_servers,
            'acks': 'all',               # 等待所有副本确认（DLQ 消息不能丢）
            'retries': 3,                # 发送失败重试次数
            'retry.backoff.ms': 500,
        }
        
        self.consumer = Consumer(consumer_config)
        self.dlq_producer = Producer(producer_config)
        
        # 注册信号处理器（支持 Docker/Kubernetes 的优雅关闭）
        signal.signal(signal.SIGTERM, self._handle_shutdown)
        signal.signal(signal.SIGINT, self._handle_shutdown)
    
    def _handle_shutdown(self, signum, frame):
        """处理关闭信号（SIGTERM/SIGINT）"""
        logger.info(f"收到关闭信号 {signum}，开始优雅关闭...")
        self._running = False
    
    def _on_assign(self, consumer, partitions):
        """分区分配回调：Consumer 被分配到新分区时调用"""
        logger.info(f"分区已分配: {[str(p) for p in partitions]}")
        # 可以在这里初始化分区级别的状态（如本地缓存）
    
    def _on_revoke(self, consumer, partitions):
        """分区撤销回调：Rebalance 前，Consumer 失去分区时调用"""
        logger.info(f"分区被撤销: {[str(p) for p in partitions]}")
        # !! 重要：在这里同步提交 Offset，防止重复消费
        # （分区被转移给其他 Consumer 前，先保存进度）
        try:
            consumer.commit(asynchronous=False)
            logger.info("Rebalance 前 Offset 已同步提交")
        except KafkaException as e:
            logger.error(f"Rebalance 前 Offset 提交失败: {e}")
    
    def _send_to_dlq(self, original_message: Message, error: Exception):
        """
        将处理失败的消息发送到死信队列（Dead Letter Queue）
        
        DLQ 消息包含原始消息内容 + 错误信息，便于后续分析和人工处理
        """
        try:
            # 构造 DLQ 消息（保留原始消息，附加错误元数据）
            dlq_payload = {
                'original_topic': original_message.topic(),
                'original_partition': original_message.partition(),
                'original_offset': original_message.offset(),
                'original_key': original_message.key().decode('utf-8') if original_message.key() else None,
                'original_value': original_message.value().decode('utf-8') if original_message.value() else None,
                'error_type': type(error).__name__,
                'error_message': str(error),
                'failed_at': time.time(),
            }
            
            self.dlq_producer.produce(
                topic=self.dlq_topic,
                key=original_message.key(),                   # 保留原始 Key（用于分区路由）
                value=json.dumps(dlq_payload).encode('utf-8'),
                callback=lambda err, msg: logger.error(f"DLQ 发送失败: {err}") if err else None,
            )
            self.dlq_producer.flush(timeout=10)  # 确保 DLQ 消息被发出
            logger.warning(
                f"消息已发送到 DLQ: topic={original_message.topic()}, "
                f"partition={original_message.partition()}, "
                f"offset={original_message.offset()}"
            )
        except Exception as dlq_err:
            # DLQ 发送失败是严重问题（数据丢失风险），需要告警
            logger.error(f"发送 DLQ 失败（严重）: {dlq_err}", exc_info=True)
    
    def _process_with_retry(self, message: Message, handler: Callable):
        """
        带重试的消息处理
        
        重试策略：指数退避（Exponential Backoff）
        如果所有重试都失败，发送到死信队列
        """
        last_exception = None
        
        for attempt in range(self.max_retries + 1):  # 0 到 max_retries
            try:
                handler(message)
                return True  # 处理成功
            except Exception as e:
                last_exception = e
                if attempt < self.max_retries:
                    # 指数退避：1s → 2s → 4s
                    wait_ms = self.retry_backoff_ms * (2 ** attempt)
                    logger.warning(
                        f"消息处理失败（第 {attempt + 1}/{self.max_retries} 次重试），"
                        f"{wait_ms}ms 后重试: {e}"
                    )
                    time.sleep(wait_ms / 1000.0)
                else:
                    logger.error(f"消息处理彻底失败（已重试 {self.max_retries} 次）: {e}", exc_info=True)
        
        # 所有重试失败，发送到 DLQ
        self._send_to_dlq(message, last_exception)
        return False
    
    def run(self, message_handler: Callable[[Message], None]):
        """
        启动消费循环
        
        Args:
            message_handler: 消息处理函数，接受 confluent_kafka.Message 对象
        """
        try:
            # 订阅 Topic（注册 Rebalance 回调）
            self.consumer.subscribe(
                self.topics,
                on_assign=self._on_assign,
                on_revoke=self._on_revoke,
            )
            
            self._running = True
            logger.info(f"Consumer 启动，订阅 Topics: {self.topics}")
            
            while self._running:
                # poll()：拉取消息，超时 1 秒后返回（即使没有新消息）
                # 注意：poll() 同时负责发送心跳，必须定期调用！
                message = self.consumer.poll(timeout=1.0)
                
                if message is None:
                    # 没有新消息（超时），继续等待
                    continue
                
                if message.error():
                    # 处理 Kafka 错误
                    if message.error().code() == KafkaError._PARTITION_EOF:
                        # 到达分区末尾（正常情况），不是真正的错误
                        logger.debug(
                            f"到达分区末尾: {message.topic()}[{message.partition()}] "
                            f"offset={message.offset()}"
                        )
                        continue
                    else:
                        # 真正的错误：记录并继续（或根据业务决定是否停止）
                        logger.error(f"Consumer 错误: {message.error()}")
                        continue
                
                # 处理消息（含重试和 DLQ）
                success = self._process_with_retry(message, message_handler)
                
                # 无论成功还是失败（发送到 DLQ），都提交 Offset
                # 这样可以继续消费后续消息，不被"毒丸消息"卡住
                self.consumer.commit(asynchronous=True)
        
        except KafkaException as e:
            logger.error(f"Kafka 致命错误: {e}", exc_info=True)
            raise
        
        finally:
            # 优雅关闭
            logger.info("Consumer 正在关闭...")
            try:
                # 关闭前同步提交最后一批 Offset（至关重要！）
                self.consumer.commit(asynchronous=False)
                logger.info("最终 Offset 已同步提交")
            except KafkaException as e:
                logger.error(f"最终 Offset 提交失败: {e}")
            finally:
                self.consumer.close()   # 关闭 Consumer（触发 Rebalance，将分区转移给其他实例）
                self.dlq_producer.flush()
                logger.info("Consumer 已关闭")


# ===================== 业务逻辑示例 =====================

def handle_trade_event(message: Message):
    """
    处理交易事件的业务逻辑
    
    这里演示如何解析消息并进行风险检查
    """
    # 解析消息
    trade_data = json.loads(message.value().decode('utf-8'))
    trade_id = trade_data.get('trade_id', 'unknown')
    
    logger.info(
        f"处理交易: trade_id={trade_id}, "
        f"asset={trade_data.get('asset_pair')}, "
        f"side={trade_data.get('side')}, "
        f"quantity={trade_data.get('quantity')}"
    )
    
    # 模拟业务逻辑：风险检查
    quantity = float(trade_data.get('quantity', 0))
    if quantity > 1_000_000:
        # 模拟大单触发风险告警
        logger.warning(f"大单告警: trade_id={trade_id}, quantity={quantity}")
        # 发送风险告警（此处为示例）
        # risk_alert_producer.produce(...)
    
    # 模拟偶发性异常（测试重试逻辑）
    import random
    if random.random() < 0.01:  # 1% 概率失败
        raise ValueError(f"随机处理失败（用于测试重试）: trade_id={trade_id}")
    
    logger.debug(f"交易处理完成: trade_id={trade_id}")


# ===================== 主入口 =====================

if __name__ == '__main__':
    consumer = RiskGuardConsumer(
        bootstrap_servers='localhost:9092',
        group_id='riskguard-consumer-group',
        topics=['prod.trading.trades.v1'],
        dlq_topic='prod.trading.trades.v1.dlq',
        max_retries=3,
        retry_backoff_ms=1000,
    )
    
    # 启动消费循环（阻塞，直到收到 SIGTERM/SIGINT）
    consumer.run(handle_trade_event)
```

---

## 4.4 背压处理（Backpressure）

### 什么是背压？

背压（Backpressure）是指 Consumer 的处理速度低于 Producer 的生产速度，导致消息在 Kafka 中积压（Lag 增长）的现象。

```
Producer 速度: 10,000 条/秒
Consumer 速度: 3,000 条/秒
积压速度:      7,000 条/秒

1 小时后积压: 7,000 × 3,600 = 25,200,000 条消息！
```

### 背压的诊断

```bash
# 查看 Consumer Lag（消费延迟）
kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --group riskguard-consumer-group

# 输出示例：
# TOPIC                      PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
# prod.trading.trades.v1     0          1000000         1050000         50000
# prod.trading.trades.v1     1          980000          1060000         80000
# prod.trading.trades.v1     2          990000          1040000         50000
# 总 Lag: 180,000 条！需要立即处理
```

### 背压处理策略

#### 策略1：水平扩展 Consumer 实例

```python
# 最简单的方案：增加 Consumer 实例数
# 限制：Consumer 数量不能超过分区数！

# 如果分区数 = 6，最多 6 个 Consumer 并行消费
# 需要先增加分区数（注意：增加分区会改变 Key 路由！）
```

```bash
# 增加分区数（只能增加，不能减少！）
kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --alter \
  --topic prod.trading.trades.v1 \
  --partitions 12  # 从 6 增加到 12
```

#### 策略2：批量处理（最有效）

```python
# ❌ 低效：逐条处理（每条消息都有单独的 DB 写入）
while running:
    message = consumer.poll(timeout=1.0)
    if message:
        save_to_db(message)  # 每条消息触发一次 DB 写入
        consumer.commit(asynchronous=True)

# ✅ 高效：批量处理（积累一批，统一写入）
BATCH_SIZE = 500
BATCH_TIMEOUT_SEC = 5.0  # 最多等待 5 秒（即使批次未满）

batch = []
batch_start_time = time.time()

while running:
    message = consumer.poll(timeout=0.1)  # 短超时，快速积累
    
    if message and not message.error():
        batch.append(message)
    
    # 满足条件时处理批次：批次满了 OR 超时了
    should_flush = (
        len(batch) >= BATCH_SIZE or 
        time.time() - batch_start_time > BATCH_TIMEOUT_SEC
    )
    
    if should_flush and batch:
        # 批量写入数据库（一次 SQL 写入 500 条）
        save_batch_to_db(batch)
        consumer.commit(asynchronous=True)
        logger.info(f"批量处理 {len(batch)} 条消息")
        batch = []
        batch_start_time = time.time()
```

#### 策略3：多线程消费（线程池模式）

```python
from concurrent.futures import ThreadPoolExecutor
import queue
import threading

class MultiThreadedConsumer:
    """
    多线程 Consumer：主线程负责拉取，工作线程负责处理
    
    注意：多线程模式下 Offset 管理更复杂！
    需要跟踪每个分区的最新完成 Offset，避免乱序提交
    """
    
    def __init__(self, bootstrap_servers, group_id, topics, num_workers=4):
        self.num_workers = num_workers
        self.work_queue = queue.Queue(maxsize=1000)  # 有界队列（防止内存溢出）
        self.completed_offsets = {}  # 记录每个分区已完成的 Offset
        self.offset_lock = threading.Lock()
        
        self.consumer = Consumer({
            'bootstrap.servers': bootstrap_servers,
            'group.id': group_id,
            'enable.auto.commit': False,
            'max.poll.records': 500,  # 多线程时可以拉取更多
        })
        self.consumer.subscribe(topics)
    
    def _worker(self, handler):
        """工作线程：从队列取消息并处理"""
        while True:
            item = self.work_queue.get()
            if item is None:  # 停止信号
                self.work_queue.task_done()
                break
            
            message, seq = item
            try:
                handler(message)
            except Exception as e:
                logger.error(f"Worker 处理失败: {e}")
            finally:
                # 记录完成的 Offset
                with self.offset_lock:
                    key = (message.topic(), message.partition())
                    self.completed_offsets[key] = max(
                        self.completed_offsets.get(key, -1),
                        message.offset()
                    )
                self.work_queue.task_done()
    
    def run(self, handler):
        """启动多线程消费"""
        # 启动工作线程池
        with ThreadPoolExecutor(max_workers=self.num_workers) as executor:
            futures = [executor.submit(self._worker, handler) 
                      for _ in range(self.num_workers)]
            
            try:
                while self._running:
                    message = self.consumer.poll(timeout=1.0)
                    if message and not message.error():
                        # 投入工作队列（阻塞式：队列满时等待，实现背压控制）
                        self.work_queue.put((message, time.time()), block=True)
            finally:
                # 发送停止信号给所有 Worker
                for _ in range(self.num_workers):
                    self.work_queue.put(None)
                self.work_queue.join()  # 等待所有消息处理完成
                # 最终 Offset 提交
                self.consumer.commit(asynchronous=False)
                self.consumer.close()
```

> ⚠️ **多线程消费的注意事项：**
> - Kafka Consumer **不是线程安全的**，只有主线程能调用 `poll()` 和 `commit()`
> - 多线程时 Offset 管理更复杂，需要确保按分区顺序提交
> - 对于严格顺序要求的场景，不适合多线程（会破坏分区内的消费顺序）

---

## 4.5 Consumer 性能调优

### 核心调优参数

| 参数 | 默认值 | 推荐值 | 作用 |
|------|--------|--------|------|
| `max.poll.records` | 500 | 100-500 | 每次 poll 拉取的最大条数 |
| `fetch.min.bytes` | 1 | 1024 | Broker 返回数据的最小字节数 |
| `fetch.max.wait.ms` | 500 | 500 | Broker 等待数据的最大时间 |
| `fetch.max.bytes` | 52428800 (50MB) | 50MB | 单次 fetch 的最大字节数 |
| `max.partition.fetch.bytes` | 1048576 (1MB) | 1MB | 单分区单次 fetch 的最大字节数 |
| `receive.buffer.bytes` | 65536 | 1048576 (1MB) | 网络接收缓冲区大小 |

### `max.poll.records` 调优

```python
# 场景1：消息处理很快（< 1ms/条）
# → 增大 max.poll.records，减少 poll 调用次数，提高吞吐量
consumer_config['max.poll.records'] = 1000

# 场景2：消息处理较慢（> 100ms/条）
# → 减小 max.poll.records，避免 max.poll.interval.ms 超时
# 假设处理时间 200ms/条，max.poll.interval.ms = 300000ms
# 安全批次大小 = 300000 / 200 = 1500 条
# 但留 20% 余量：1500 * 0.8 = 1200 条
consumer_config['max.poll.records'] = 1200
```

### `fetch.min.bytes` 和 `fetch.max.wait.ms` 调优

```python
# 低延迟模式（金融交易，要求毫秒级延迟）
consumer_config.update({
    'fetch.min.bytes': 1,        # 有数据就立即返回
    'fetch.max.wait.ms': 100,    # 最多等 100ms
})

# 高吞吐模式（日志分析，延迟不敏感）
consumer_config.update({
    'fetch.min.bytes': 65536,    # 等积累 64KB 再返回（减少网络请求次数）
    'fetch.max.wait.ms': 1000,   # 最多等 1 秒
})
```

---

## 4.6 Dead Letter Queue（死信队列）模式

### 什么是毒丸消息（Poison Pill）？

毒丸消息是指那些永远无法被成功处理的消息，例如：
- 格式错误的 JSON（`{"trade_id": 123` — 缺少闭合括号）
- 类型不匹配（数量字段传入了字符串 `"abc"`）
- 违反业务规则（负数金额）
- 依赖的下游服务不可用

如果不处理毒丸消息，Consumer 会陷入无限重试循环，**完全卡住，后续消息无法消费**。

### DLQ 架构设计

```
正常消息流:
  Producer → Topic（prod.trading.trades.v1）
              ↓
           Consumer（尝试处理，重试 N 次）
              ↓ 处理成功
           下游系统（数据库、告警系统）

毒丸消息流:
  Producer → Topic（prod.trading.trades.v1）
              ↓
           Consumer（尝试处理，重试 N 次后放弃）
              ↓ 处理失败
           DLQ Topic（prod.trading.trades.v1.dlq）
              ↓
           DLQ Consumer（人工分析 + 修复后重新投递）
```

### DLQ Topic 命名规范

```
原 Topic:  prod.trading.trades.v1
DLQ Topic: prod.trading.trades.v1.dlq

或使用专用的错误 Topic 前缀：
错误 Topic: error.prod.trading.trades.v1
```

### DLQ 消息格式（最佳实践）

```python
# DLQ 消息应包含足够的调试信息
dlq_message = {
    # === 原始消息信息 ===
    'original_topic': 'prod.trading.trades.v1',
    'original_partition': 3,
    'original_offset': 1234567,
    'original_timestamp': 1700000000000,
    'original_key': 'trade-uuid-123',
    'original_value': '{"trade_id": "uuid-123", "quantity": "INVALID"}',
    
    # === 错误信息 ===
    'error_type': 'ValueError',
    'error_message': "could not convert string to float: 'INVALID'",
    'error_traceback': '...',  # 完整堆栈信息
    'retry_count': 3,
    
    # === 处理元数据 ===
    'consumer_group': 'riskguard-consumer-group',
    'consumer_host': 'consumer-pod-xyz',
    'failed_at': '2024-01-15T10:30:00Z',
    'schema_version': 'v1',
}
```

### DLQ 重放（Replay）脚本

```python
#!/usr/bin/env python3
"""
DLQ 重放工具：将死信队列中的消息修复后重新投递到原 Topic
"""

import json
from confluent_kafka import Consumer, Producer

def replay_dlq(
    bootstrap_servers: str,
    dlq_topic: str,
    fix_function,  # 修复消息的函数
    dry_run: bool = True  # 试运行模式（不实际投递）
):
    consumer = Consumer({
        'bootstrap.servers': bootstrap_servers,
        'group.id': 'dlq-replay-group',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False,
    })
    producer = Producer({'bootstrap.servers': bootstrap_servers})
    
    consumer.subscribe([dlq_topic])
    replayed = 0
    skipped = 0
    
    try:
        while True:
            message = consumer.poll(timeout=5.0)
            if message is None:
                break  # 没有更多消息，退出
            
            if message.error():
                continue
            
            dlq_data = json.loads(message.value())
            original_value = dlq_data['original_value']
            original_topic = dlq_data['original_topic']
            
            try:
                # 调用修复函数（由操作人员提供）
                fixed_value = fix_function(original_value)
                
                if not dry_run:
                    # 重新投递到原 Topic
                    producer.produce(
                        topic=original_topic,
                        key=dlq_data.get('original_key', '').encode(),
                        value=json.dumps(fixed_value).encode(),
                    )
                    producer.flush()
                
                replayed += 1
                print(f"[{'DRY-RUN' if dry_run else 'REPLAYED'}] "
                      f"offset={dlq_data['original_offset']}")
            
            except Exception as e:
                skipped += 1
                print(f"[SKIPPED] offset={dlq_data['original_offset']}: {e}")
            
            consumer.commit(asynchronous=False)
    
    finally:
        consumer.close()
        print(f"\n重放完成: {replayed} 条成功, {skipped} 条跳过")


# 使用示例
def fix_invalid_quantity(original_value: str) -> dict:
    """修复数量字段类型错误"""
    data = json.loads(original_value)
    data['quantity'] = float(data['quantity'])  # 强制转换为浮点数
    return data

replay_dlq(
    bootstrap_servers='localhost:9092',
    dlq_topic='prod.trading.trades.v1.dlq',
    fix_function=fix_invalid_quantity,
    dry_run=True  # 先用 dry_run=True 验证，再改为 False 实际投递
)
```

---

## 4.7 Consumer 监控指标

### 关键指标

| 指标名 | 含义 | 告警阈值 |
|--------|------|---------|
| `consumer-lag` | 分区级消费延迟（条数） | > 10,000（视业务） |
| `records-consumed-rate` | 消费速率（条/秒） | 持续下降 |
| `fetch-rate` | poll 请求速率 | 持续下降 |
| `fetch-latency-avg` | 平均拉取延迟 | > 500ms |
| `commit-rate` | Offset 提交速率 | 与消费速率不匹配 |
| `rebalance-rate` | Rebalance 频率 | > 1次/小时 |
| `join-time-avg` | Rebalance 平均耗时 | > 10s |

### Prometheus + Grafana 监控集成

```python
from prometheus_client import Gauge, Counter, start_http_server
import threading

# 定义监控指标
consumer_lag_gauge = Gauge(
    'kafka_consumer_lag',
    'Kafka Consumer Lag by partition',
    ['topic', 'partition', 'group_id']
)

messages_consumed_counter = Counter(
    'kafka_messages_consumed_total',
    'Total messages consumed',
    ['topic', 'group_id', 'status']  # status: success/failed/dlq
)

def update_lag_metrics(consumer, group_id):
    """定期更新 Lag 指标（在独立线程中运行）"""
    while True:
        try:
            # 获取分区分配信息
            partitions = consumer.assignment()
            for partition in partitions:
                # 当前消费 Offset
                low, high = consumer.get_watermark_offsets(partition, timeout=5)
                committed = consumer.committed([partition], timeout=5)[0]
                
                if committed and committed.offset >= 0:
                    lag = high - committed.offset
                    consumer_lag_gauge.labels(
                        topic=partition.topic,
                        partition=str(partition.partition),
                        group_id=group_id
                    ).set(lag)
        except Exception as e:
            logger.error(f"更新 Lag 指标失败: {e}")
        
        time.sleep(30)  # 每 30 秒更新一次

# 启动 Prometheus metrics 服务器（端口 8000）
start_http_server(8000)

# 启动指标更新线程
metrics_thread = threading.Thread(target=update_lag_metrics, args=(consumer, 'riskguard-consumer-group'))
metrics_thread.daemon = True
metrics_thread.start()
```

### Kafka 自带的 Consumer Lag 监控命令

```bash
# 查看某 Group 的所有 Topic Lag
kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --group riskguard-consumer-group

# 持续监控（每 5 秒刷新）
watch -n 5 kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --group riskguard-consumer-group

# 查看所有 Consumer Group 的概览
kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --list
```

---

## 动手练习

### 练习目标

实现一个能处理消费失败并写入死信队列的 Consumer，要求：

1. **基础功能：** 订阅 `dev.trading.trades.v1` Topic，手动提交 Offset
2. **重试逻辑：** 失败消息最多重试 3 次，使用指数退避
3. **DLQ 集成：** 重试失败后写入 `dev.trading.trades.v1.dlq`
4. **监控集成：** 在控制台打印每分钟的消费速率和当前 Lag
5. **优雅关闭：** 支持 SIGTERM 信号，关闭前同步提交 Offset

### 步骤指引

```bash
# 1. 准备环境
pip install confluent-kafka

# 2. 启动本地 Kafka（Docker）
docker run -d --name kafka \
  -p 9092:9092 \
  -e KAFKA_NODE_ID=1 \
  -e KAFKA_PROCESS_ROLES=broker,controller \
  -e KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093 \
  -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 \
  -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 \
  apache/kafka:3.7.0

# 3. 创建 Topic
docker exec kafka kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create \
  --topic dev.trading.trades.v1 \
  --partitions 6 \
  --replication-factor 1

docker exec kafka kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create \
  --topic dev.trading.trades.v1.dlq \
  --partitions 6 \
  --replication-factor 1

# 4. 使用本章的 RiskGuardConsumer 代码，修改 handle_trade_event 函数
#    让 10% 的消息触发处理失败（测试重试和 DLQ）

# 5. 启动消费者并观察输出
python consumer.py

# 6. 用另一个终端发送测试消息（含格式错误的消息）
python producer_test.py

# 7. 验证 DLQ 中的消息
docker exec kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic dev.trading.trades.v1.dlq \
  --from-beginning
```

### 加分挑战

- **Exactly-once 语义：** 将消费结果写入 PostgreSQL，使用 Kafka Transactions 实现 Exactly-once
- **背压测试：** 模拟 Consumer 处理慢（`time.sleep(0.5)`），观察 Lag 增长，然后增加 Consumer 实例数解决背压
- **Rebalance 可视化：** 在 `_on_assign` 和 `_on_revoke` 回调中记录时间戳，计算 Rebalance 耗时

---

## 本章小结

| 最佳实践 | 推荐方案 |
|---------|---------|
| 分区分配策略 | `cooperative-sticky`（减少 Rebalance 影响） |
| Offset 提交 | 手动提交（`enable.auto.commit=False`） |
| 正常提交方式 | `commitAsync`（不阻塞） |
| 关闭前提交 | `commitSync`（确保最后一批不丢） |
| 消费语义 | At-least-once + 幂等消费者 |
| 毒丸消息 | Dead Letter Queue（重试 N 次后转入 DLQ） |
| 背压处理 | 批量处理 + 水平扩展 Consumer 实例 |
| 监控核心指标 | `consumer-lag`（最重要） |
