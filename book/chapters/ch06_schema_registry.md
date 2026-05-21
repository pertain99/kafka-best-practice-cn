# 第 6 章：Schema Registry 与数据契约

## 本章你将学到

- 为什么 JSON 不够用：无类型、无契约的痛点
- Schema Registry 的架构原理（Confluent Schema Registry）
- Avro、Protobuf、JSON Schema 的对比与选择
- Avro Schema 设计最佳实践（以 RiskGuard 交易消息为例）
- Schema 兼容性模式：BACKWARD、FORWARD、FULL、NONE
- 完整 Python 代码：Avro Producer + Consumer with Schema Registry
- Schema 演化实战：安全地新增字段、删除字段、修改类型

---

## 6.1 为什么需要 Schema Registry？

### JSON 的痛点

在小团队早期，Kafka 消息用 JSON 是完全合理的。但随着系统规模增长，JSON 的缺陷会逐渐暴露：

#### 问题1：无类型约束

```json
// Producer A 发送（认为 quantity 是数字）:
{"trade_id": "uuid-123", "quantity": 100.5, "price": 50000.0}

// Producer B 发送（代码 bug，quantity 变成了字符串）:
{"trade_id": "uuid-456", "quantity": "100.5", "price": 50000.0}
                                    ↑
                            字符串！不是数字！
```

Consumer 处理时：

```python
quantity = message['quantity']
risk_value = quantity * price  # 如果 quantity 是字符串，这里会崩溃
# TypeError: can't multiply sequence by non-int of type 'float'
```

没有 Schema 约束时，这种 bug 可能在生产运行几天后才被发现。

#### 问题2：无契约，无法检测破坏性变更

```json
// 版本1（稳定运行 3 个月）：
{"trade_id": "...", "account_id": "...", "asset_pair": "BTC/USD"}

// 开发者 A 某天"优化"了 Producer（不知道有 Consumer 依赖 account_id）：
{"trade_id": "...", "user_id": "...", "pair": "BTC/USD"}
                     ↑ 字段改名了！    ↑ 字段改名了！
```

Consumer 读取 `account_id` 得到 `None`，下游数据库写入空字段，合规系统报警——但已经处理了几千条错误数据。

#### 问题3：冗余数据，低效传输

JSON 的每条消息都包含完整的字段名：

```
{"trade_id": "uuid-123", "account_id": "acc-456", "asset_pair": "BTC/USD", 
 "side": "BUY", "quantity": 0.5, "price": 50000.0, "timestamp": 1700000000000}

字段名本身占用: 75 字节
实际数据占用:  65 字节
开销比: > 50%！
```

使用 Avro 二进制编码后，字段名不随消息传输，相同数据只需 ~20 字节。

### Schema Registry 解决什么？

Schema Registry（Schema 注册中心）是一个独立服务，集中存储和管理所有 Topic 的消息格式（Schema）。

```
传统 JSON 流：
  Producer → (随便写什么格式) → Kafka → Consumer (祈祷格式对了)

Schema Registry 流：
  Producer → 注册/验证 Schema → Kafka（消息头含 Schema ID）→ Consumer
                  ↑                                                ↓
            Schema Registry ←←←←←←←←←←←←← 按 Schema ID 获取格式定义
```

**核心收益：**
1. **类型安全**：Producer 写入前验证，不符合 Schema 则拒绝
2. **兼容性检查**：新版 Schema 上线前自动检查是否破坏现有 Consumer
3. **高效编码**：Avro/Protobuf 二进制格式，体积减少 70-90%
4. **自文档化**：Schema 即文档，新开发者能立刻理解消息结构

---

## 6.2 Schema Registry 架构

### 核心组件

```
┌─────────────────────────────────────────────────────────────────┐
│                        Kafka Cluster                            │
│   Topic: prod.trading.trades.v1                                 │
│   Message: [Magic Byte=0][Schema ID=3][Avro Binary Payload]    │
│             ↑ 5 字节头部，用于反序列化                            │
└─────────────────────────────────────────────────────────────────┘
         ↑ 写入                              ↓ 读取
┌─────────────────┐                 ┌─────────────────┐
│    Producer     │                 │    Consumer     │
│                 │                 │                 │
│  1. 序列化数据   │                 │  1. 读取消息     │
│  2. 获取/注册   │                 │  2. 提取 Schema │
│     Schema ID   │                 │     ID（头部）  │
│  3. 写入 Kafka  │                 │  3. 向 Registry │
└────────┬────────┘                 │     获取 Schema │
         │                          │  4. 反序列化    │
         ↓                          └────────┬────────┘
┌─────────────────────────────────────────────────────────────────┐
│                    Schema Registry Service                       │
│  REST API: http://schema-registry:8081                          │
│                                                                  │
│  存储：_schemas（内部 Kafka Topic）                               │
│  主题: subjects (每个 Topic 的 Schema 版本历史)                   │
│                                                                  │
│  GET /subjects                        (列出所有 Subject)         │
│  GET /subjects/{subject}/versions     (列出版本)                 │
│  POST /subjects/{subject}/versions    (注册新 Schema)            │
│  GET /schemas/ids/{id}                (按 ID 获取 Schema)        │
│  POST /compatibility/{subject}        (检查兼容性)               │
└─────────────────────────────────────────────────────────────────┘
```

### Subject 命名规则

Schema Registry 中的每个 Subject 对应一个 Topic + 值/键的组合：

```
Topic: prod.trading.trades.v1

Value Schema Subject: prod.trading.trades.v1-value   ← 最常用
Key Schema Subject:   prod.trading.trades.v1-key     （Key 也可以有 Schema）
```

### 消息格式：Magic Byte + Schema ID

Confluent Schema Registry 使用如下格式包装 Avro 消息：

```
字节: [0x00][Schema ID (4字节)][Avro 二进制数据...]
       ↑ Magic byte，标识这是 Schema Registry 格式
              ↑ 4字节大端整数，对应 Schema Registry 中的 Schema 版本 ID
```

```python
# 手动解析（了解原理，实际使用 confluent-kafka 自动处理）
import struct

def decode_schema_id(raw_bytes: bytes) -> int:
    """从 Kafka 消息字节中提取 Schema ID"""
    magic_byte = raw_bytes[0]
    assert magic_byte == 0x00, f"非 Schema Registry 格式消息，magic byte={magic_byte}"
    schema_id = struct.unpack('>I', raw_bytes[1:5])[0]  # 4字节大端整数
    avro_payload = raw_bytes[5:]
    return schema_id, avro_payload
```

---

## 6.3 Avro vs Protobuf vs JSON Schema 对比

| 维度 | Avro | Protobuf | JSON Schema |
|------|------|----------|-------------|
| **编码格式** | 二进制 | 二进制 | JSON（文本） |
| **消息大小** | ⭐⭐⭐ 最小 | ⭐⭐⭐ 最小 | ⭐ 最大 |
| **序列化速度** | ⭐⭐⭐ 快 | ⭐⭐⭐ 最快 | ⭐⭐ 中等 |
| **Schema 演化** | ⭐⭐⭐ 最好 | ⭐⭐ 好 | ⭐⭐ 好 |
| **可读性** | ⭐ 二进制不可读 | ⭐ 二进制不可读 | ⭐⭐⭐ 人可读 |
| **语言支持** | ⭐⭐⭐ 广泛 | ⭐⭐⭐ 广泛 | ⭐⭐⭐ 广泛 |
| **Schema 文件** | JSON 格式 | .proto 文件 | JSON 格式 |
| **向后兼容** | ⭐⭐⭐ 内置 | ⭐⭐ 手动管理 | ⭐⭐ 手动管理 |
| **Kafka 生态集成** | ⭐⭐⭐ 原生支持 | ⭐⭐⭐ 良好 | ⭐⭐ 一般 |
| **适用场景** | 数据工程、流处理 | 微服务 RPC | 前后端 API |

### 选择建议

```
Kafka 数据工程场景 → 推荐 Avro
  - Confluent Schema Registry 最初为 Avro 设计
  - 与 Kafka Connect、ksqlDB、Schema Registry 集成最成熟
  - 读时 Schema 设计适合流处理场景

微服务通信场景 → Protobuf
  - gRPC 生态
  - 代码生成（类型安全）
  - 跨语言 RPC 调用

需要人工可读的场景 → JSON Schema
  - 调试方便
  - 前端也需要消费
  - 消息量不大，不在乎编码效率
```

---

## 6.4 Avro Schema 设计最佳实践

### RiskGuard Trade 消息的 Avro Schema

```json
{
  "type": "record",
  "name": "Trade",
  "namespace": "com.bestcointrade.trading",
  "doc": "交易事件消息，记录一笔交易的完整信息",
  "fields": [
    {
      "name": "trade_id",
      "type": "string",
      "doc": "全局唯一的交易 ID（UUID v4）"
    },
    {
      "name": "account_id",
      "type": "string",
      "doc": "账户 ID，用于追踪账户级别的风险暴露"
    },
    {
      "name": "asset_pair",
      "type": "string",
      "doc": "交易对，格式：BASE/QUOTE，例如 BTC/USD"
    },
    {
      "name": "side",
      "type": {
        "type": "enum",
        "name": "TradeSide",
        "symbols": ["BUY", "SELL"],
        "doc": "交易方向"
      },
      "doc": "买单（BUY）或卖单（SELL）"
    },
    {
      "name": "quantity",
      "type": "double",
      "doc": "交易数量（基础货币）"
    },
    {
      "name": "price",
      "type": "double",
      "doc": "成交价格（计价货币）"
    },
    {
      "name": "timestamp",
      "type": {
        "type": "long",
        "logicalType": "timestamp-millis"
      },
      "doc": "交易时间戳（Unix 毫秒）"
    },
    {
      "name": "exchange",
      "type": ["null", "string"],
      "default": null,
      "doc": "可选：来源交易所名称（Binance、Coinbase 等）"
    },
    {
      "name": "metadata",
      "type": {
        "type": "map",
        "values": "string"
      },
      "default": {},
      "doc": "可选的扩展元数据（Key-Value 对）"
    }
  ]
}
```

### Avro 类型系统最佳实践

#### 可空字段：必须使用 Union 类型

```json
// ❌ 错误：直接用 string，不能表示 null
{"name": "exchange", "type": "string"}

// ✅ 正确：Union 类型（null 放第一个，default 为 null）
{"name": "exchange", "type": ["null", "string"], "default": null}

// ✅ 也正确：有默认值的可空字段
{"name": "fee_rate", "type": ["null", "double"], "default": null}
```

> **为什么 null 要放第一个？** Avro 规定 Union 的 `default` 值必须匹配第一个类型。如果你希望默认是 null，就把 null 放第一个。

#### 货币金额：使用 Decimal 逻辑类型（推荐）

```json
// ❌ 避免：double 有精度问题（0.1 + 0.2 ≠ 0.3 的经典 Bug）
{"name": "price", "type": "double"}

// ✅ 推荐：Decimal 逻辑类型（精确表示货币金额）
{
  "name": "price",
  "type": {
    "type": "bytes",
    "logicalType": "decimal",
    "precision": 18,  // 最多 18 位有效数字
    "scale": 8        // 小数点后 8 位（加密货币常用精度）
  },
  "doc": "成交价格（精确到 8 位小数）"
}
```

```python
from decimal import Decimal

# Python 中使用 Decimal 类型（需要配合 fastavro 或 apache-avro 库）
trade = {
    'price': Decimal('50000.00000000'),  # 精确表示
    # 不是 float 50000.0（可能有精度误差）
}
```

#### 枚举类型：添加默认值（Schema 演化必须）

```json
// ❌ 危险：枚举无默认值
{
  "name": "side",
  "type": {
    "type": "enum",
    "name": "TradeSide",
    "symbols": ["BUY", "SELL"]
  }
}

// ✅ 安全：枚举有 default（Schema 演化时，旧 Consumer 读到未知值时使用默认值）
{
  "name": "side",
  "type": {
    "type": "enum",
    "name": "TradeSide",
    "symbols": ["BUY", "SELL", "UNKNOWN"],  // 加一个 UNKNOWN 作为兜底
    "default": "UNKNOWN"
  }
}
```

#### Namespace：防止类型名冲突

```json
// 如果多个 Schema 都有叫 "Trade" 的 Record，需要 namespace 区分
{
  "type": "record",
  "name": "Trade",
  "namespace": "com.bestcointrade.trading",  // 完整名称：com.bestcointrade.trading.Trade
  ...
}
```

---

## 6.5 Schema 兼容性模式

Schema Registry 提供四种兼容性检查模式，在注册新 Schema 时自动验证。

### 四种兼容性模式详解

#### BACKWARD（向后兼容，推荐默认值）

```
定义：新 Schema（V2）可以读取 V1 的消息
场景：升级 Consumer 后，旧 Producer 还在发 V1 消息，Consumer 要能读懂

V1: {trade_id, account_id, quantity, price}
V2: {trade_id, account_id, quantity, price, exchange}  ← 新增可选字段（有 default）

V2 Consumer 读 V1 消息：exchange 字段不存在 → 使用 default null ✅
V2 Consumer 读 V2 消息：正常 ✅
```

**BACKWARD 允许的变更：**
- ✅ 新增字段（必须有 default 值）
- ✅ 删除有 default 值的字段
- ❌ 新增无 default 的必填字段
- ❌ 修改字段类型
- ❌ 删除无 default 的字段

#### FORWARD（向前兼容）

```
定义：旧 Schema（V1）可以读取 V2 的消息
场景：先升级 Producer（发 V2 消息），旧 Consumer 还在跑，要能读懂

V1: {trade_id, account_id, quantity, price}
V2: {trade_id, account_id, quantity, price, exchange}

V1 Consumer 读 V2 消息：exchange 字段是多余的，V1 Schema 忽略它 ✅
```

#### FULL（双向兼容，最严格）

```
定义：新旧 Schema 互相兼容（既 BACKWARD 又 FORWARD）
场景：保守升级，确保新旧版本混跑时完全没问题
代价：Schema 变更的自由度最小
```

#### NONE（不检查）

```
定义：不做任何兼容性检查
场景：开发初期，Schema 频繁变动；或已知需要破坏性变更（配合 Topic 版本升级）
风险：极高！生产环境不推荐
```

### 配置兼容性模式

```bash
# 查看当前全局默认兼容性模式
curl http://schema-registry:8081/config

# 修改全局默认模式为 BACKWARD
curl -X PUT \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data '{"compatibility": "BACKWARD"}' \
  http://schema-registry:8081/config

# 为特定 Subject 设置不同的模式
curl -X PUT \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data '{"compatibility": "FULL"}' \
  http://schema-registry:8081/config/prod.trading.trades.v1-value
```

### 在 Python 中检查兼容性

```python
import requests
import json

def check_schema_compatibility(
    registry_url: str,
    subject: str,
    new_schema: dict
) -> dict:
    """
    在注册前检查新 Schema 与最新版本的兼容性
    
    Args:
        registry_url: Schema Registry URL
        subject: Subject 名称（如 prod.trading.trades.v1-value）
        new_schema: 新的 Avro Schema（dict）
    
    Returns:
        {'is_compatible': True/False}
    """
    url = f"{registry_url}/compatibility/subjects/{subject}/versions/latest"
    
    payload = {"schema": json.dumps(new_schema)}
    
    response = requests.post(
        url,
        headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
        data=json.dumps(payload),
    )
    
    if response.status_code == 404:
        # Subject 不存在（第一次注册），视为兼容
        return {"is_compatible": True, "reason": "No existing schema"}
    
    response.raise_for_status()
    return response.json()


# 使用示例
new_schema = {
    "type": "record",
    "name": "Trade",
    "namespace": "com.bestcointrade.trading",
    "fields": [
        {"name": "trade_id", "type": "string"},
        {"name": "account_id", "type": "string"},
        {"name": "asset_pair", "type": "string"},
        {"name": "side", "type": {"type": "enum", "name": "TradeSide", "symbols": ["BUY", "SELL"]}},
        {"name": "quantity", "type": "double"},
        {"name": "price", "type": "double"},
        {"name": "timestamp", "type": {"type": "long", "logicalType": "timestamp-millis"}},
        # 新增字段（有 default，BACKWARD 兼容）
        {"name": "exchange", "type": ["null", "string"], "default": null},
    ]
}

result = check_schema_compatibility(
    registry_url="http://schema-registry:8081",
    subject="prod.trading.trades.v1-value",
    new_schema=new_schema
)

if result['is_compatible']:
    print("✅ Schema 兼容，可以安全注册")
else:
    print(f"❌ Schema 不兼容！原因: {result}")
    # 停止发布流程
```

---

## 6.6 完整 Python 代码：Avro Producer + Consumer

### 安装依赖

```bash
pip install confluent-kafka fastavro requests

# 如果使用 Confluent 官方 Avro 支持（推荐）：
pip install confluent-kafka[avro]
# 注：confluent-kafka[avro] 依赖 fastavro 做序列化
```

### 启动本地 Schema Registry（Docker Compose）

```yaml
# docker-compose.yaml
version: '3'
services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.5.0
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181

  kafka:
    image: confluentinc/cp-kafka:7.5.0
    depends_on: [zookeeper]
    ports:
      - "9092:9092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1

  schema-registry:
    image: confluentinc/cp-schema-registry:7.5.0
    depends_on: [kafka]
    ports:
      - "8081:8081"
    environment:
      SCHEMA_REGISTRY_HOST_NAME: schema-registry
      SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS: kafka:9092
```

```bash
docker-compose up -d
# 等待约 30 秒，直到 Schema Registry 就绪
curl http://localhost:8081/subjects  # 应返回 []
```

### 完整 Avro Producer 代码

```python
#!/usr/bin/env python3
"""
Avro Producer with Schema Registry
RiskGuard 项目：使用 Schema Registry 序列化交易事件
"""

import time
import uuid
import json
import random
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField

# ==================== Schema 定义 ====================

TRADE_SCHEMA_STR = json.dumps({
    "type": "record",
    "name": "Trade",
    "namespace": "com.bestcointrade.trading",
    "doc": "交易事件 Schema v1",
    "fields": [
        {
            "name": "trade_id",
            "type": "string",
            "doc": "全局唯一的交易 ID（UUID v4）"
        },
        {
            "name": "account_id",
            "type": "string",
            "doc": "账户 ID"
        },
        {
            "name": "asset_pair",
            "type": "string",
            "doc": "交易对，如 BTC/USD"
        },
        {
            "name": "side",
            "type": {
                "type": "enum",
                "name": "TradeSide",
                "symbols": ["BUY", "SELL"]
            },
            "doc": "交易方向"
        },
        {
            "name": "quantity",
            "type": "double",
            "doc": "交易数量"
        },
        {
            "name": "price",
            "type": "double",
            "doc": "成交价格"
        },
        {
            "name": "timestamp",
            "type": {
                "type": "long",
                "logicalType": "timestamp-millis"
            },
            "doc": "交易时间戳（Unix 毫秒）"
        },
        {
            "name": "exchange",
            "type": ["null", "string"],  # 可空字段
            "default": None,             # Python 中用 None 表示 Avro null
            "doc": "来源交易所（可选）"
        }
    ]
})


def trade_to_dict(trade: dict, ctx: SerializationContext) -> dict:
    """
    将交易对象转换为 Avro 序列化格式
    
    confluent-kafka Avro Serializer 需要一个转换函数
    通常直接返回字典即可
    """
    return trade


class TradeAvroProducer:
    """使用 Schema Registry 的 Avro Producer"""
    
    def __init__(
        self,
        bootstrap_servers: str,
        schema_registry_url: str,
        topic: str,
    ):
        self.topic = topic
        
        # 1. 初始化 Schema Registry 客户端
        schema_registry_conf = {'url': schema_registry_url}
        schema_registry_client = SchemaRegistryClient(schema_registry_conf)
        
        # 2. 创建 Avro Serializer
        # 第一次运行时，会自动向 Schema Registry 注册 Schema
        # 后续使用已注册的 Schema ID
        avro_serializer = AvroSerializer(
            schema_registry_client=schema_registry_client,
            schema_str=TRADE_SCHEMA_STR,
            to_dict=trade_to_dict,
        )
        
        # 3. 初始化 Producer
        producer_conf = {
            'bootstrap.servers': bootstrap_servers,
            'acks': 'all',
            'retries': 3,
        }
        
        from confluent_kafka.serialization import StringSerializer
        
        self.producer = Producer(producer_conf)
        self.avro_serializer = avro_serializer
        self.key_serializer = StringSerializer('utf_8')
    
    def produce_trade(self, trade: dict):
        """
        发送一条交易事件
        
        消息格式: [Magic Byte][Schema ID][Avro Binary]
        """
        try:
            # 序列化 value（Avro 二进制）
            serialized_value = self.avro_serializer(
                trade,
                SerializationContext(self.topic, MessageField.VALUE)
            )
            
            # 序列化 key（UTF-8 字符串）
            serialized_key = self.key_serializer(
                trade['trade_id'],
                SerializationContext(self.topic, MessageField.KEY)
            )
            
            # 发送消息
            self.producer.produce(
                topic=self.topic,
                key=serialized_key,
                value=serialized_value,
                on_delivery=self._on_delivery,
            )
            
            # 触发回调（不阻塞）
            self.producer.poll(0)
        
        except Exception as e:
            print(f"序列化/发送失败: {e}")
            raise
    
    def _on_delivery(self, err, msg):
        """发送回调"""
        if err:
            print(f"❌ 发送失败: {err}")
        else:
            print(
                f"✅ 发送成功: topic={msg.topic()}, "
                f"partition={msg.partition()}, "
                f"offset={msg.offset()}"
            )
    
    def flush(self):
        """等待所有待发消息发送完成"""
        self.producer.flush()


# ==================== 测试数据生成 ====================

def generate_random_trade() -> dict:
    """生成随机测试交易数据"""
    asset_pairs = ["BTC/USD", "ETH/USD", "SOL/USD", "BNB/USD"]
    exchanges = ["Binance", "Coinbase", "Kraken", None]  # None 表示 Avro null
    
    return {
        "trade_id": str(uuid.uuid4()),
        "account_id": f"acc-{random.randint(1000, 9999)}",
        "asset_pair": random.choice(asset_pairs),
        "side": random.choice(["BUY", "SELL"]),
        "quantity": round(random.uniform(0.001, 10.0), 8),
        "price": round(random.uniform(1000.0, 100000.0), 2),
        "timestamp": int(time.time() * 1000),  # 毫秒时间戳
        "exchange": random.choice(exchanges),
    }


# ==================== 主入口 ====================

if __name__ == '__main__':
    producer = TradeAvroProducer(
        bootstrap_servers='localhost:9092',
        schema_registry_url='http://localhost:8081',
        topic='dev.trading.trades.v1',
    )
    
    print("发送 10 条测试交易事件...")
    for i in range(10):
        trade = generate_random_trade()
        producer.produce_trade(trade)
        print(f"  [{i+1}/10] trade_id={trade['trade_id']}, pair={trade['asset_pair']}")
    
    producer.flush()
    print("✅ 所有消息发送完成")
```

### 完整 Avro Consumer 代码

```python
#!/usr/bin/env python3
"""
Avro Consumer with Schema Registry
自动从 Schema Registry 获取 Schema，反序列化 Avro 消息
"""

import json
import signal
from confluent_kafka import Consumer, KafkaError
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField, StringDeserializer


def dict_to_trade(data: dict, ctx: SerializationContext) -> dict:
    """
    Avro 反序列化后的 dict → 业务对象
    
    这里直接返回 dict；也可以返回自定义类的实例
    """
    return data


class TradeAvroConsumer:
    """使用 Schema Registry 的 Avro Consumer"""
    
    def __init__(
        self,
        bootstrap_servers: str,
        schema_registry_url: str,
        group_id: str,
        topics: list,
    ):
        self._running = False
        
        # 1. 初始化 Schema Registry 客户端
        schema_registry_client = SchemaRegistryClient({'url': schema_registry_url})
        
        # 2. 创建 Avro Deserializer
        # 注意：Consumer 端不需要提供 Schema 字符串
        # Schema Registry 会根据消息头部的 Schema ID 自动查找对应 Schema
        avro_deserializer = AvroDeserializer(
            schema_registry_client=schema_registry_client,
            from_dict=dict_to_trade,
            # schema_str=None  ← 不需要！自动从 Registry 获取
        )
        
        # 3. 初始化 Consumer
        consumer_conf = {
            'bootstrap.servers': bootstrap_servers,
            'group.id': group_id,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,  # 手动提交
        }
        
        self.consumer = Consumer(consumer_conf)
        self.topics = topics
        self.avro_deserializer = avro_deserializer
        self.key_deserializer = StringDeserializer('utf_8')
        
        # 注册优雅关闭信号
        signal.signal(signal.SIGTERM, self._handle_shutdown)
        signal.signal(signal.SIGINT, self._handle_shutdown)
    
    def _handle_shutdown(self, signum, frame):
        """处理关闭信号"""
        print(f"\n收到关闭信号，正在关闭...")
        self._running = False
    
    def run(self, message_handler=None):
        """
        启动消费循环
        
        Args:
            message_handler: 可选的消息处理回调，接受 (key, trade_dict) 参数
        """
        self.consumer.subscribe(self.topics)
        self._running = True
        
        print(f"Consumer 启动，订阅 Topics: {self.topics}")
        
        try:
            while self._running:
                # 拉取消息（1 秒超时）
                msg = self.consumer.poll(1.0)
                
                if msg is None:
                    continue  # 没有新消息
                
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue  # 到达分区末尾，正常
                    print(f"Consumer 错误: {msg.error()}")
                    continue
                
                try:
                    # 反序列化 key
                    key = self.key_deserializer(
                        msg.key(),
                        SerializationContext(msg.topic(), MessageField.KEY)
                    )
                    
                    # 反序列化 value（自动从 Schema Registry 获取 Schema）
                    trade = self.avro_deserializer(
                        msg.value(),
                        SerializationContext(msg.topic(), MessageField.VALUE)
                    )
                    
                    if message_handler:
                        message_handler(key, trade)
                    else:
                        # 默认处理：打印消息内容
                        self._default_handler(key, trade, msg)
                    
                    # 手动提交 Offset
                    self.consumer.commit(asynchronous=True)
                
                except Exception as e:
                    print(f"消息处理失败: {e}, topic={msg.topic()}, offset={msg.offset()}")
                    # 继续处理下一条（实际生产中这里应该发 DLQ）
        
        finally:
            # 优雅关闭：最终同步提交
            try:
                self.consumer.commit(asynchronous=False)
            except Exception as e:
                print(f"最终 Offset 提交失败: {e}")
            self.consumer.close()
            print("Consumer 已关闭")
    
    def _default_handler(self, key: str, trade: dict, msg):
        """默认消息处理：打印交易信息"""
        print(
            f"📨 收到交易:"
            f" key={key}"
            f" trade_id={trade.get('trade_id', '?')}"
            f" pair={trade.get('asset_pair', '?')}"
            f" side={trade.get('side', '?')}"
            f" quantity={trade.get('quantity', 0):.8f}"
            f" price={trade.get('price', 0):.2f}"
            f" | topic={msg.topic()}[{msg.partition()}]@{msg.offset()}"
        )


# ==================== 自定义业务逻辑 ====================

def risk_check_handler(key: str, trade: dict):
    """
    风险检查处理函数：检测大单交易
    """
    trade_value = trade['quantity'] * trade['price']
    
    if trade_value > 1_000_000:  # 超过 100 万美元的大单
        print(f"🚨 大单告警! trade_id={trade['trade_id']}, "
              f"value=${trade_value:,.2f}")
    
    if trade['side'] == 'SELL' and trade['quantity'] > 100:
        print(f"⚠️ 大量卖出告警! trade_id={trade['trade_id']}, "
              f"quantity={trade['quantity']}")


# ==================== 主入口 ====================

if __name__ == '__main__':
    consumer = TradeAvroConsumer(
        bootstrap_servers='localhost:9092',
        schema_registry_url='http://localhost:8081',
        group_id='riskguard-avro-consumer-group',
        topics=['dev.trading.trades.v1'],
    )
    
    # 使用自定义风险检查处理函数
    consumer.run(message_handler=risk_check_handler)
```

---

## 6.7 Schema 演化实战

Schema 演化（Schema Evolution）是指在不中断系统运行的情况下，安全地修改消息格式。

### 场景1：新增可选字段（最安全）✅

```json
// V1 Schema
{
  "fields": [
    {"name": "trade_id",   "type": "string"},
    {"name": "account_id", "type": "string"},
    {"name": "quantity",   "type": "double"},
    {"name": "price",      "type": "double"}
  ]
}

// V2 Schema：新增 exchange 字段
{
  "fields": [
    {"name": "trade_id",   "type": "string"},
    {"name": "account_id", "type": "string"},
    {"name": "quantity",   "type": "double"},
    {"name": "price",      "type": "double"},
    // ✅ 必须：有 default 值，BACKWARD 兼容
    {"name": "exchange", "type": ["null", "string"], "default": null}
  ]
}
```

```python
# 操作步骤：
# 1. 检查兼容性
result = check_schema_compatibility(
    "http://localhost:8081",
    "dev.trading.trades.v1-value",
    v2_schema
)
assert result['is_compatible'], "Schema 不兼容，停止升级！"

# 2. 先升级 Consumer（V2 Consumer 能读 V1 消息，因为 exchange 有 default）
# 3. 再升级 Producer（开始发 V2 消息，V1 Consumer 已无实例）
```

### 场景2：删除字段（需要谨慎）⚠️

```json
// V1 Schema（包含 internal_note 字段）
{
  "fields": [
    {"name": "trade_id",     "type": "string"},
    {"name": "internal_note","type": ["null", "string"], "default": null}
  ]
}

// V2 Schema：删除 internal_note（只允许删除有 default 的字段！）
{
  "fields": [
    {"name": "trade_id", "type": "string"}
    // internal_note 字段已删除
  ]
}
```

**BACKWARD 兼容性规则：**
- ✅ 可以删除有 `default` 的字段（V2 Consumer 读 V1 消息时，忽略多余字段）
- ❌ 不能删除没有 `default` 的必填字段

### 场景3：修改字段类型（高风险）❌

```json
// V1: quantity 是 double
{"name": "quantity", "type": "double"}

// V2: 想改成 decimal（精度更高）
{"name": "quantity", "type": {"type": "bytes", "logicalType": "decimal", "precision": 18, "scale": 8}}
```

**修改类型通常不向后兼容！** 正确做法：

```json
// 推荐：新增一个字段，而不是修改现有字段
{"name": "quantity",          "type": "double", "doc": "废弃，迁移中"},
{"name": "quantity_decimal",  "type": {"type": "bytes", "logicalType": "decimal", "precision": 18, "scale": 8}, "default": ...}
```

或者升级 Topic 版本：`prod.trading.trades.v1` → `prod.trading.trades.v2`

### 场景4：枚举添加新值 ⚠️

```json
// V1 枚举
{"type": "enum", "name": "TradeSide", "symbols": ["BUY", "SELL"]}

// V2 枚举：新增 UNKNOWN
{"type": "enum", "name": "TradeSide", "symbols": ["BUY", "SELL", "UNKNOWN"], "default": "UNKNOWN"}
```

**关键：**
- 新增枚举值时，必须添加 `default`（处理旧 Consumer 读到新值的情况）
- 枚举值**不能删除**（删除后，旧消息中的该枚举值无法反序列化）
- 枚举值**不能修改名称**

### Schema 演化速查表

| 操作 | BACKWARD | FORWARD | FULL | 备注 |
|------|---------|---------|------|------|
| 新增字段（有 default） | ✅ | ✅ | ✅ | 最安全的变更 |
| 新增字段（无 default） | ❌ | ✅ | ❌ | Consumer 先升级时可用 |
| 删除字段（有 default） | ✅ | ❌ | ❌ | 需要所有 Consumer 升级后 |
| 删除字段（无 default） | ❌ | ❌ | ❌ | 禁止！必须用 Topic 版本号 |
| 修改字段类型 | ❌ | ❌ | ❌ | 新建 Topic 版本 |
| 枚举新增值（有 default） | ✅ | ✅ | ✅ | 需要 default |
| 枚举删除值 | ❌ | ❌ | ❌ | 禁止！ |
| 修改枚举值名称 | ❌ | ❌ | ❌ | 禁止！ |

---

## 动手练习

### 练习目标

为 RiskGuard 项目的 trade 消息注册 Schema，并验证 Schema 演化。

### 步骤1：启动 Schema Registry

```bash
# 使用上面的 docker-compose.yaml 启动
docker-compose up -d

# 验证
curl http://localhost:8081/subjects
# 应返回：[]
```

### 步骤2：手动注册 Trade Schema（了解 REST API）

```bash
# 注册 Trade Schema
curl -X POST \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data '{
    "schema": "{\"type\":\"record\",\"name\":\"Trade\",\"namespace\":\"com.bestcointrade.trading\",\"fields\":[{\"name\":\"trade_id\",\"type\":\"string\"},{\"name\":\"account_id\",\"type\":\"string\"},{\"name\":\"asset_pair\",\"type\":\"string\"},{\"name\":\"side\",\"type\":{\"type\":\"enum\",\"name\":\"TradeSide\",\"symbols\":[\"BUY\",\"SELL\"]}},{\"name\":\"quantity\",\"type\":\"double\"},{\"name\":\"price\",\"type\":\"double\"},{\"name\":\"timestamp\",\"type\":{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}}]}"
  }' \
  http://localhost:8081/subjects/dev.trading.trades.v1-value/versions

# 返回：{"id": 1}（Schema ID）

# 查看已注册的 Schema
curl http://localhost:8081/subjects/dev.trading.trades.v1-value/versions/latest
```

### 步骤3：运行 Avro Producer 和 Consumer

```bash
# 终端1：启动 Consumer
python avro_consumer.py

# 终端2：运行 Producer 发送 10 条消息
python avro_producer.py

# 观察 Consumer 输出：应看到反序列化后的交易数据
```

### 步骤4：Schema 演化测试

```bash
# 1. 尝试注册不兼容的 Schema（删除 account_id 字段）
# 预期：Schema Registry 返回 409 Conflict

# 2. 注册向后兼容的 Schema（新增 exchange 字段，有 default）
# 预期：注册成功，返回新的 Schema ID

# 3. 用新 Schema 运行 Producer，旧 Consumer 是否仍能工作？
# 预期：旧 Consumer 忽略 exchange 字段，正常消费
```

### 步骤5：设置兼容性模式

```bash
# 将全局兼容性模式设置为 FULL
curl -X PUT \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data '{"compatibility": "FULL"}' \
  http://localhost:8081/config

# 尝试注册一个 FULL 模式下不允许的变更（如新增无 default 的字段）
# 预期：返回兼容性错误
```

### 加分挑战

- **性能对比：** 发送 100,000 条消息，分别用 JSON 和 Avro，对比消息大小（Topic 存储大小）和吞吐量
- **Schema 版本管理 CI/CD：** 编写一个 GitHub Actions 脚本，在 PR 合并前自动检查 Schema 兼容性（调用 `/compatibility` REST API）
- **Protobuf 对比：** 为同样的 Trade 消息写一个 `.proto` 文件，使用 `confluent-kafka` 的 Protobuf Serializer，比较与 Avro 的开发体验差异

---

## 本章小结

| 决策 | 推荐方案 |
|------|---------|
| 序列化格式 | Avro（Kafka 数据工程首选） |
| 兼容性模式 | BACKWARD（生产默认）|
| 新增字段 | 必须有 `default` 值（可空字段用 `["null", "type"]`） |
| 删除字段 | 只删有 `default` 的字段；无 `default` 的用 Topic 版本号 |
| 修改类型 | 禁止！用 Topic 版本号（v1 → v2） |
| 货币金额 | Avro `decimal` 逻辑类型，避免 `double` 精度问题 |
| Schema 注册流程 | 上线前通过 CI/CD 自动检查兼容性 |
| 监控告警 | 监控 Schema 注册失败、序列化错误率 |

---

## 三章综合回顾

| 章节 | 核心主题 | 最重要的一个结论 |
|------|---------|---------------|
| 第 4 章 Consumer | 消费可靠性 | 手动提交 Offset + 死信队列 = 数据不丢不卡 |
| 第 5 章 Topic 设计 | 容量规划 | 分区数由吞吐量和 Consumer 数共同决定，只增不减 |
| 第 6 章 Schema Registry | 数据契约 | 新增字段必须有 default，Schema 演化靠 BACKWARD 兼容 |
