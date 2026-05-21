# 第 1 章：Kafka 是什么？

---

## 本章你将学到

- 传统架构为何在高并发、高吞吐场景下力不从心
- 消息队列（Message Queue）和流处理平台（Streaming Platform）的本质区别
- Kafka 的核心架构：Broker、Topic、Partition、Offset、Producer、Consumer
- ZooKeeper 模式 vs KRaft 模式的演进历史
- Kafka 的三大经典使用场景
- Kafka 与 RabbitMQ、Pulsar、Kinesis、Redis Streams 的横向对比
- 本书项目预览：实时交易风控系统 RiskGuard

---

## 1.1 为什么需要消息队列？

### 传统架构的三大痛点

想象你正在开发一个电商系统。用户下单后，系统需要同时：

1. 扣减库存
2. 生成订单记录
3. 发送短信通知
4. 更新用户积分
5. 通知仓库备货
6. 推送给推荐系统做行为分析

在最朴素的同步架构中，你的代码大概长这样：

```python
def place_order(user_id, product_id, quantity):
    # 所有调用都是同步的、串行的
    inventory_service.deduct(product_id, quantity)   # 调用库存服务
    order_service.create(user_id, product_id)         # 创建订单
    sms_service.send(user_id, "您的订单已确认")        # 发短信
    points_service.add(user_id, 100)                  # 加积分
    warehouse_service.notify(product_id, quantity)    # 通知仓库
    recommendation_service.track(user_id, product_id) # 行为追踪
    return "订单成功"
```

这段代码看上去很直观，但隐藏着三个定时炸弹：

---

**痛点一：服务耦合（Tight Coupling）**

上面的 `place_order` 函数直接依赖 6 个下游服务。任何一个服务宕机或接口变更，都会影响下单流程。如果推荐系统今天要升级 API，你必须同时修改下单服务——这就是**紧耦合**的代价。

```
用户请求
    │
    ▼
下单服务 ──► 库存服务    (挂了？下单失败！)
    │    ──► 订单服务    (超时？用户等待！)
    │    ──► 短信服务    (宕机？整个链路阻塞！)
    │    ──► 积分服务
    │    ──► 仓库服务
    └────► 推荐服务
```

**痛点二：请求积压（Backlog）**

双十一促销期间，每秒可能有 10 万笔订单涌入。短信服务最多每秒处理 1000 条，库存服务每秒处理 5000 次。当下游处理速度远低于上游产生速度时，请求会在内存中无限堆积，最终导致 OOM（Out of Memory，内存溢出）崩溃。

```
生产速率: 100,000 req/s
              │
              ▼
短信服务处理速率: 1,000 req/s   ← 积压 99,000/s，最终崩溃
```

**痛点三：峰值冲击（Traffic Spike）**

流量不是均匀的。早上 8 点抢购、晚上 8 点大促，瞬间流量可以是日常的 100 倍。如果所有服务都要承受这个峰值，你要么提前准备 100 倍的服务器（成本极高），要么在峰值时崩溃（损失更大）。

```
流量曲线（请求/秒）
10000 |          ***
 8000 |        **   **
 6000 |       *       *
 4000 |      *         *
 2000 |*****             *****
    0 +──────────────────────── 时间
      0  6  8  10  12  18  20  24
         ↑              ↑
       早高峰          晚高峰
```

### 消息队列如何解决这三个问题？

引入消息队列（Message Queue，MQ）后，架构变成这样：

```
用户请求
    │
    ▼
下单服务 ──► [消息队列] ──► 库存服务（异步消费）
                    │    ──► 订单服务（异步消费）
                    │    ──► 短信服务（按自身速率消费）
                    │    ──► 积分服务（按自身速率消费）
                    │    ──► 仓库服务（按自身速率消费）
                    └────► 推荐服务（按自身速率消费）
```

**解耦**：下单服务只需把消息写入队列，不关心谁来消费、何时消费。  
**削峰**：消息队列充当缓冲区，下游按自己的速率消费，不会被峰值冲垮。  
**异步**：用户下单后立即返回"成功"，后续处理异步完成，响应时间大幅降低。

---

## 1.2 消息队列 vs 流处理平台

等等——你可能见过 RabbitMQ、ActiveMQ 这些传统消息队列，Kafka 和它们有什么不同？

### 传统消息队列（以 RabbitMQ 为例）

传统 MQ 的设计理念是"**投递即删除**"：

```
Producer → [Queue] → Consumer
                ↓
           消息被消费后删除
```

核心特征：
- 消息消费后立即从队列删除
- 支持复杂的路由规则（Exchange、Binding）
- 天然支持点对点（P2P）和发布订阅（Pub/Sub）
- 通常消息量在万级/秒，适合任务队列、RPC 等场景

### 流处理平台（Kafka）

Kafka 的设计理念是"**日志即一切**"（Log is Everything）：

```
Producer → [Topic/Partition] → Consumer Group A
                    │       → Consumer Group B
                    │       → Consumer Group C
                    │
               消息持久化保留（默认 7 天）
               消费者可以重放（Replay）任意历史消息
```

核心特征：
- 消息**持久化**存储在磁盘，消费后不删除
- 消费者通过 **Offset（偏移量）** 记录自己读到哪里
- 支持**消费者重放**：从任意历史位置重新消费
- 吞吐量可达百万级/秒
- 天然支持**流处理**（Kafka Streams、ksqlDB）

### 核心差异对比

| 维度 | RabbitMQ（传统 MQ） | Kafka（流平台） |
|------|---------------------|-----------------|
| 消息存储 | 消费后删除 | 持久化保留（可配置） |
| 消费模式 | 推送（Push） | 拉取（Pull） |
| 吞吐量 | 万级/秒 | 百万级/秒 |
| 消费重放 | ❌ 不支持 | ✅ 支持 |
| 消息顺序 | 队列级有序 | Partition 级有序 |
| 适用场景 | 任务队列、RPC | 事件流、日志、实时分析 |
| 路由复杂度 | 高（Exchange 规则） | 低（基于 Topic） |

**一句话总结**：RabbitMQ 擅长"任务分发"，Kafka 擅长"事件记录与回放"。选 RabbitMQ 当你需要复杂路由和即时任务；选 Kafka 当你需要高吞吐、持久化日志和流处理。

---

## 1.3 Kafka 核心架构

下面我们深入 Kafka 的内部结构。建议先看整体图，再逐一理解各个组件。

### 整体架构图（ASCII）

```
┌─────────────────────────────────────────────────────────────────┐
│                        Kafka Cluster                            │
│                                                                 │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐         │
│  │   Broker 1   │  │   Broker 2   │  │   Broker 3   │         │
│  │              │  │              │  │              │         │
│  │ Topic: orders│  │ Topic: orders│  │ Topic: orders│         │
│  │  Partition 0 │  │  Partition 1 │  │  Partition 2 │         │
│  │  [Leader]    │  │  [Leader]    │  │  [Leader]    │         │
│  │  Partition 1 │  │  Partition 0 │  │  Partition 0 │         │
│  │  [Follower]  │  │  [Follower]  │  │  [Follower]  │         │
│  └──────────────┘  └──────────────┘  └──────────────┘         │
│                                                                 │
│  ┌─────────────────────────────────────────────┐               │
│  │              KRaft Controller Quorum         │               │
│  │        (Broker 1 + Broker 2 + Broker 3)      │               │
│  └─────────────────────────────────────────────┘               │
└─────────────────────────────────────────────────────────────────┘
          ▲                               │
          │ 写入消息                       │ 拉取消息
          │                               ▼
┌─────────────────┐             ┌─────────────────────┐
│    Producer     │             │   Consumer Group A  │
│   (生产者)       │             │                     │
│                 │             │  Consumer 1 ── P0   │
│  key="user_123" │             │  Consumer 2 ── P1   │
│  value={...}    │             │  Consumer 3 ── P2   │
└─────────────────┘             └─────────────────────┘
                                          │
                                ┌─────────────────────┐
                                │   Consumer Group B  │
                                │  (独立消费，互不影响) │
                                │  Consumer 1 ── P0,1 │
                                │  Consumer 2 ── P2   │
                                └─────────────────────┘
```

### 1.3.1 Broker（代理服务器）

**Broker** 是 Kafka 集群中的一个节点（一台服务器进程）。每个 Broker 负责：

- 存储分配给它的 Partition 数据
- 处理来自 Producer 的写入请求
- 响应 Consumer 的读取请求
- 参与 Leader 选举（KRaft 模式下）

一个 Kafka 集群通常由 3~10 个 Broker 组成，每个 Broker 有唯一的 `broker.id`。

**关键概念：Leader 和 Follower**

每个 Partition 有且仅有一个 **Leader**，其他副本是 **Follower**。

- Producer 和 Consumer **只与 Leader 交互**
- Follower 从 Leader 同步数据，作为热备份
- 如果 Leader 宕机，Follower 会选举出新的 Leader

```
Partition 0 的副本分布（Replication Factor = 3）：
  Broker 1: Partition 0 [Leader]   ← Producer/Consumer 访问这里
  Broker 2: Partition 0 [Follower] ← 从 Leader 同步
  Broker 3: Partition 0 [Follower] ← 从 Leader 同步
```

### 1.3.2 Topic（主题）

**Topic** 是消息的逻辑分类，类似数据库中的"表名"。

- Producer 向指定 Topic 写入消息
- Consumer 订阅指定 Topic 读取消息
- 一个 Topic 由一个或多个 Partition 组成

命名建议：使用 `业务域.实体类型.事件` 的格式，例如：
- `ecommerce.orders.created`
- `payment.transactions.completed`
- `riskguard.alerts.fraud`

### 1.3.3 Partition（分区）

**Partition** 是 Kafka 实现高吞吐的核心机制，也是并行度的基本单位。

```
Topic: orders（3 个 Partition）

Partition 0: [msg_0] [msg_3] [msg_6] [msg_9] ...
Partition 1: [msg_1] [msg_4] [msg_7] [msg_10] ...
Partition 2: [msg_2] [msg_5] [msg_8] [msg_11] ...
                                               ↑
                                           Offset 递增
```

**为什么要分区？**

1. **并行写入**：多个 Producer 可以同时向不同 Partition 写入
2. **并行消费**：Consumer Group 中的多个 Consumer 可以同时消费不同 Partition
3. **水平扩展**：增加 Partition 数量即可提升吞吐量

**Partition 数量怎么定？** 经验法则：Partition 数 = max(Producer 并发数, Consumer 并发数)。一般生产环境单 Topic 建议 12~48 个 Partition。

**注意**：Kafka 只保证**同一 Partition 内**消息有序，跨 Partition 不保证顺序。

### 1.3.4 Offset（偏移量）

**Offset** 是消息在 Partition 内的唯一编号，从 0 开始单调递增。

```
Partition 0:
  Offset 0: {"order_id": "001", "amount": 100}
  Offset 1: {"order_id": "002", "amount": 250}
  Offset 2: {"order_id": "003", "amount": 75}
  Offset 3: {"order_id": "004", "amount": 300}
              ↑
         Consumer 当前读到这里（committed offset = 3）
```

Consumer 通过记录 Offset 来追踪"我读到哪里了"。这使得 Kafka 支持：

- **重放（Replay）**：将 Offset 重置到历史位置，重新消费
- **精确一次（Exactly-Once）**：配合事务机制，确保每条消息恰好处理一次
- **故障恢复**：Consumer 崩溃后，从上次提交的 Offset 继续消费

Offset 存储在 Kafka 内部 Topic `__consumer_offsets` 中（现代版本），而不是 ZooKeeper。

### 1.3.5 Producer（生产者）

**Producer** 负责将消息写入 Kafka。写入时可以指定：

- **Topic**：必须，消息的目标主题
- **Key**（可选）：用于决定消息写入哪个 Partition。相同 Key 的消息保证进入同一 Partition，从而保证顺序
- **Value**：消息的实际内容，字节数组格式
- **Headers**（可选）：元数据键值对，如追踪 ID、来源标识

**Partition 路由规则**：
```
如果指定了 Key：  partition = hash(key) % num_partitions
如果没有指定 Key：使用 Sticky Partitioner（批量黏性分配，Kafka 2.4+）
如果显式指定 Partition：直接写入该 Partition
```

### 1.3.6 Consumer 和 Consumer Group（消费者与消费者组）

**Consumer** 从 Kafka 拉取（Pull）消息并处理。

**Consumer Group** 是多个 Consumer 的逻辑分组，同一 Consumer Group 内的消费者**分工合作**消费同一 Topic：

```
Topic: orders（4 个 Partition）

Consumer Group "order-processor"（2 个 Consumer）：
  Consumer 1 消费 Partition 0 + Partition 1
  Consumer 2 消费 Partition 2 + Partition 3

Consumer Group "audit-service"（4 个 Consumer）：
  Consumer 1 消费 Partition 0
  Consumer 2 消费 Partition 1
  Consumer 3 消费 Partition 2
  Consumer 4 消费 Partition 3
```

**关键规则**：
- 同一 Consumer Group 内，一个 Partition 只能被**一个** Consumer 消费（防止重复处理）
- 不同 Consumer Group **完全独立**，互不影响，都能消费全量数据
- Consumer 数量超过 Partition 数量时，多余的 Consumer 会空闲

### 1.3.7 ZooKeeper（旧）vs KRaft（新）

**Kafka 3.x 之前：依赖 ZooKeeper**

ZooKeeper（分布式协调服务）负责：
- 存储 Broker 元数据
- Leader 选举
- Topic 配置管理
- Consumer Group Offset 存储（早期版本）

```
旧架构：
Producer/Consumer → Kafka Cluster → ZooKeeper（协调）
                                      ↑
                                  独立进程，需单独维护
```

**痛点**：
- 运维复杂：需要单独部署和维护 ZooKeeper 集群（通常 3~5 个节点）
- 性能瓶颈：元数据操作经过 ZooKeeper，影响扩展能力
- 单点风险：ZooKeeper 故障会影响整个 Kafka 集群

---

**Kafka 3.x：KRaft 模式（Kafka Raft Metadata）**

KRaft 是 Kafka 内置的分布式共识协议，基于 Raft 算法，彻底移除对 ZooKeeper 的依赖：

```
新架构（KRaft）：
Producer/Consumer → Kafka Cluster
                      │
                   内置 Controller Quorum（Raft 协议）
                   元数据存储在 __cluster_metadata Topic
```

**KRaft 优势**：
- 架构简化：无需 ZooKeeper，一套系统搞定一切
- 更快 Controller 故障切换：从秒级降低到毫秒级
- 支持更大规模：单集群支持百万级 Partition（旧模式约 20 万）
- **Kafka 3.3+ 起 KRaft 正式生产可用，3.7+ 默认使用 KRaft**

> 📌 **本书全程使用 KRaft 模式**，不依赖 ZooKeeper。

---

## 1.4 Kafka 的三大使用场景

### 场景一：日志聚合（Log Aggregation）

**问题**：微服务架构下，几十上百个服务分散在不同机器上打日志，运维人员需要 SSH 到各台机器查日志，排查问题极为困难。

**解决方案**：

```
服务 A 的日志 ──►
服务 B 的日志 ──► [Kafka Topic: app-logs] ──► Elasticsearch
服务 C 的日志 ──►                          ──► S3（冷存储）
Nginx 日志    ──►                          ──► 告警系统
```

所有服务将日志写入 Kafka，下游消费者统一消费：
- **Elasticsearch**：实时索引，用 Kibana 搜索
- **S3/HDFS**：长期归档，低成本存储
- **告警系统**：实时扫描错误关键词，触发告警

**Kafka 在此的价值**：
- 解耦日志生产方（各服务）和消费方（ES、S3、告警）
- 日志写入 Kafka 后即返回，不阻塞业务逻辑
- 如果 ES 临时宕机，日志安全地保留在 Kafka 中，恢复后继续消费

### 场景二：事件驱动架构（Event-Driven Architecture，EDA）

**传统的请求-响应架构**：

```
服务 A ──HTTP──► 服务 B ──HTTP──► 服务 C
                              （同步，强依赖）
```

**事件驱动架构**：

```
服务 A 发布事件: "用户已注册" → [Kafka]
                                     │
                    ┌────────────────┤
                    │                │
                    ▼                ▼
            邮件服务（发欢迎邮件）  积分服务（送初始积分）
                    │
                    ▼
            推荐服务（初始化画像）
```

服务 A 只需发布事件，完全不知道也不关心谁会处理这个事件。新增处理逻辑时，只需新增一个消费者，**不修改任何现有代码**。

**典型 EDA 场景**：
- 用户注册后触发多个后续流程
- 订单状态变更通知多个相关系统
- 支付成功后协调库存、发货、通知

### 场景三：实时流处理（Real-time Stream Processing）

Kafka 不只是消息队列，它还是实时流处理的核心基础设施：

```
原始事件流
用户行为日志 ──►
交易记录     ──► [Kafka] ──► Kafka Streams / Flink ──► 实时结果
传感器数据   ──►                                         │
                                               ┌────────┴────────┐
                                               │                 │
                                           实时仪表盘        风控告警
                                           (Grafana)        (低延迟)
```

**典型流处理场景**：
- 实时计算用户行为分析（PV/UV、漏斗转化）
- 金融交易实时风控（发现异常交易模式）
- IoT 传感器数据实时监控（设备故障预警）
- 实时推荐系统（用户刚浏览的商品实时推送相关推荐）

---

## 1.5 Kafka vs 竞品对比

选择消息中间件时，面对众多选项难以抉择。下面是主流方案的全面对比：

| 维度 | **Kafka** | **RabbitMQ** | **Apache Pulsar** | **AWS Kinesis** | **Redis Streams** |
|------|-----------|--------------|-------------------|-----------------|-------------------|
| **定位** | 分布式事件流平台 | 消息代理 | 分布式消息+流 | 托管流服务 | 轻量内存流 |
| **吞吐量** | 极高（百万/s） | 中（万~十万/s） | 高（百万/s） | 中（取决于分片） | 中（万~十万/s） |
| **延迟** | 低（毫秒级） | 极低（微秒级） | 低（毫秒级） | 低（毫秒级） | 极低（微秒级） |
| **消息持久化** | ✅ 磁盘持久化 | ✅（可配置） | ✅ 分层存储 | ✅ 24h~365天 | ⚠️ 内存为主 |
| **消费重放** | ✅ 支持 | ❌ 不支持 | ✅ 支持 | ✅ 支持 | ✅ 有限支持 |
| **消息顺序** | Partition 级 | 队列级 | Partition 级 | Shard 级 | Stream 级 |
| **路由灵活性** | 低（Topic 路由） | 极高（Exchange） | 中 | 低 | 低 |
| **流处理** | ✅ Kafka Streams | ❌ 需外部系统 | ✅ Pulsar Functions | ⚠️ 有限 | ❌ 需外部系统 |
| **运维复杂度** | 中（KRaft 后降低） | 低 | 高（BookKeeper） | 极低（托管） | 极低 |
| **开源协议** | Apache 2.0 | MPL 2.0 | Apache 2.0 | 专有 | BSD |
| **社区生态** | 极强 | 强 | 中 | AWS 生态 | Redis 生态 |
| **适用场景** | 大数据/流处理 | 任务队列/RPC | 多租户/跨地域 | AWS 原生应用 | 缓存+简单队列 |

### 选型建议

- **选 Kafka**：需要高吞吐、消息持久化、流处理能力，团队有一定运维能力
- **选 RabbitMQ**：复杂路由逻辑，任务队列场景，团队熟悉 AMQP 协议
- **选 Pulsar**：多租户隔离需求，需要内置分层存储（冷热数据自动分离）
- **选 Kinesis**：团队全在 AWS 生态，不想运维基础设施
- **选 Redis Streams**：消息量小，已有 Redis 基础设施，对延迟极其敏感

---

## 1.6 Kafka 关键数字

作为工程师，了解这些数字有助于做出合理的架构决策：

### 吞吐量

| 场景 | 吞吐量参考 |
|------|------------|
| 单个 Producer（未优化） | ~50 MB/s |
| 单个 Producer（批量 + 压缩） | ~200~500 MB/s |
| 单个 Broker 磁盘写入 | ~1 GB/s（依赖硬盘性能） |
| 单 Consumer（顺序读） | ~500 MB/s |
| LinkedIn 生产集群峰值 | ~7 TB/day（写入） |

### 延迟

| 指标 | 参考值 |
|------|--------|
| 端到端延迟（`linger.ms=0`） | < 5ms |
| 端到端延迟（`linger.ms=5`） | 5~10ms |
| Producer 网络往返 | < 1ms（同机房） |
| Consumer 拉取延迟 | < 1ms（有消息时） |

### 关键默认值

| 配置项 | 默认值 | 含义 |
|--------|--------|------|
| `message.max.bytes` | 1 MB | 单条消息最大大小 |
| `log.retention.hours` | 168（7天） | 消息保留时间 |
| `log.segment.bytes` | 1 GB | 单个日志段文件大小 |
| `default.replication.factor` | 1 | 默认副本数（生产应设为 3） |
| `num.partitions` | 1 | 新建 Topic 默认 Partition 数 |
| `log.retention.bytes` | -1（无限） | 基于大小的保留策略 |

> ⚠️ **生产环境注意**：`replication.factor=1` 意味着没有冗余，Broker 宕机即丢数据！生产必须设为 3。

---

## 1.7 本书项目预览：RiskGuard 实时交易风控系统

为了让每章内容有实际载体，本书将围绕一个真实的工程项目贯穿始终：

**项目名称**：RiskGuard（风险卫士）

**业务场景**：某金融科技公司需要对用户的支付交易进行实时风险评估，在用户体验到交易结果之前（< 100ms），完成欺诈检测并决定是否放行。

### 系统架构预览

```
┌─────────────────────────────────────────────────────────────────┐
│                    RiskGuard 系统架构                            │
│                                                                 │
│  ┌─────────┐     ┌──────────────────────┐     ┌─────────────┐  │
│  │ 交易    │────►│  Kafka Topic:         │────►│  风控引擎   │  │
│  │ 发起端  │     │  riskguard.txn.raw   │     │  (Consumer) │  │
│  └─────────┘     └──────────────────────┘     └──────┬──────┘  │
│                                                       │         │
│                                               ┌───────┴───────┐ │
│                                               │               │ │
│                                      ┌────────▼───┐  ┌────────▼──┐│
│                                      │ 正常交易   │  │ 高风险   ││
│                                      │ Topic      │  │ 告警     ││
│                                      │ (放行)     │  │ Topic    ││
│                                      └────────────┘  └──────────┘│
│                                                           │      │
│                                                    ┌──────▼─────┐│
│                                                    │ 实时仪表盘 ││
│                                                    │ (Grafana)  ││
│                                                    └────────────┘│
└─────────────────────────────────────────────────────────────────┘
```

### 你将在本书中构建的能力

| 章节 | 核心能力 |
|------|----------|
| 第 1~2 章 | 理解 Kafka 架构，搭建本地开发环境 |
| 第 3 章 | 高可靠 Producer：幂等发送、批量优化 |
| 第 4 章 | 高性能 Consumer：并行消费、Offset 管理 |
| 第 5 章 | Schema Registry：消息格式版本化管理 |
| 第 6 章 | Kafka Streams：实时流处理和 CEP |
| 第 7 章 | 监控与告警：Prometheus + Grafana |
| 第 8 章 | 生产运维：容量规划、故障演练 |

---

## 小结

本章我们从第一原理出发，理解了为什么需要 Kafka：

1. **传统同步架构**的耦合、积压、峰值三大痛点催生了消息队列
2. **Kafka 与传统 MQ 的本质区别**在于持久化日志和消费重放能力
3. **Kafka 核心组件**：Broker 存储数据，Topic/Partition 组织数据，Producer 写入，Consumer Group 并行消费，Offset 追踪进度
4. **KRaft 模式**是 Kafka 3.x 的重大演进，彻底移除 ZooKeeper 依赖
5. **三大场景**：日志聚合、事件驱动架构、实时流处理
6. **选型判断**：高吞吐 + 持久化 + 流处理 → Kafka 是首选

---

## 动手练习

**练习 1.1：画出你当前项目的消息流**

思考你当前工作或学习中的一个系统，尝试画出：
- 哪些地方存在"服务耦合"问题？
- 如果引入 Kafka，消息从哪里来，到哪里去？
- 需要几个 Topic？大概需要多少 Partition？

**练习 1.2：对比实验**

访问以下资源，了解 Kafka 与 RabbitMQ 的真实性能对比：
- Confluent 官方性能报告：https://www.confluent.io/blog/kafka-fastest-messaging-system/
- RabbitMQ 官方文档：https://www.rabbitmq.com/blog/

根据你了解的场景，写一段 100 字以内的分析：你的场景更适合 Kafka 还是 RabbitMQ？为什么？

**练习 1.3：理解 Offset**

假设一个 Topic 有 3 个 Partition，Consumer Group 有 2 个 Consumer。
用纸和笔画出：
1. 每个 Consumer 消费哪些 Partition？
2. 如果 Consumer 1 崩溃，Consumer 2 接管后，Offset 应该从哪里继续？
3. 如果想重放最近 1 小时的数据，需要怎么操作？

（提示：下一章环境搭建后，你将用真实命令验证你的答案。）

---

*下一章，我们将动手搭建本书所需的完整 Kafka 开发环境，使用 Docker Compose 一键启动 Kafka + Schema Registry + Kafka UI + Prometheus + Grafana。*
