# 第 2 章：安装与环境搭建

---

## 本章你将学到

- 本地开发环境的推荐配置和最低硬件要求
- 使用 Docker Compose 一键启动完整 Kafka 开发栈
- KRaft 模式的工作原理及核心配置参数
- 验证 Kafka 安装：创建 Topic、发送和消费第一条消息
- Python Kafka 开发环境配置
- 本书项目目录结构设计
- 常见安装问题的排查思路

---

## 2.1 环境要求

### 硬件要求

| 资源 | 最低配置 | 推荐配置 | 说明 |
|------|----------|----------|------|
| CPU | 2 核 | 4 核+ | Kafka 本身 CPU 消耗不高，瓶颈通常在 I/O |
| 内存 | 4 GB | 8 GB+ | Kafka JVM 堆内存默认 1 GB，加上 OS Page Cache 需要额外内存 |
| 磁盘 | 10 GB 可用 | 20 GB+ | 本书示例产生的数据量不大，主要是 Docker 镜像占用 |
| 网络 | 100 Mbps | 1 Gbps | 本地开发不是瓶颈 |

### 软件要求

**必须安装：**
- **Docker Desktop**（Windows/macOS）或 **Docker Engine**（Linux）
  - 最低版本：Docker 20.10+
  - 验证：`docker --version`
- **Docker Compose**
  - 最低版本：Compose v2.0+（`docker compose` 命令，注意没有连字符）
  - 验证：`docker compose version`

**可选但推荐：**
- **Python 3.9+**：运行本书代码示例
- **curl / httpie**：调用 Schema Registry REST API
- **jq**：格式化 JSON 输出，方便调试

### 安装 Docker

**macOS（推荐 Docker Desktop）：**
```bash
# 下载 Docker Desktop for Mac
# https://www.docker.com/products/docker-desktop/
# 安装后在系统偏好设置中调整内存分配（建议 4 GB+）
```

**Linux（Ubuntu/Debian）：**
```bash
# 安装 Docker Engine
curl -fsSL https://get.docker.com | sh

# 将当前用户加入 docker 组（避免每次 sudo）
sudo usermod -aG docker $USER
newgrp docker

# 安装 Docker Compose Plugin
sudo apt-get install docker-compose-plugin

# 验证
docker --version        # Docker version 24.x.x
docker compose version  # Docker Compose version v2.x.x
```

**Windows（WSL2 + Docker Desktop）：**

Windows 用户强烈建议使用 WSL2（Windows Subsystem for Linux 2）+ Docker Desktop 组合，在 WSL2 Ubuntu 终端中运行所有命令，体验与 Linux 一致。

---

## 2.2 完整 Docker Compose 配置

下面是本书使用的完整 `docker-compose.yml`，包含 5 个服务：Kafka（KRaft 模式）、Schema Registry、Kafka UI、Prometheus 和 Grafana。

创建项目目录并新建文件：

```bash
mkdir -p ~/riskguard && cd ~/riskguard
```

创建 `docker-compose.yml`：

```yaml
# docker-compose.yml
# RiskGuard 本地开发环境
# Kafka 3.x KRaft 模式（无 ZooKeeper）

version: '3.8'

# 所有服务共享同一网络，可通过服务名互相访问
networks:
  kafka-net:
    driver: bridge

volumes:
  kafka_data:        # Kafka 日志数据持久化
  prometheus_data:   # Prometheus 监控数据持久化
  grafana_data:      # Grafana 仪表盘配置持久化

services:

  # ──────────────────────────────────────────
  # Kafka 3.x（KRaft 模式，无 ZooKeeper）
  # 使用 Bitnami Kafka 镜像，配置简洁
  # ──────────────────────────────────────────
  kafka:
    image: bitnami/kafka:3.7
    container_name: kafka
    networks:
      - kafka-net
    ports:
      - "9092:9092"    # 外部客户端访问端口（宿主机访问 Kafka）
      - "9093:9093"    # KRaft Controller 内部通信端口
      - "9094:9094"    # 容器内部服务间访问端口
    volumes:
      - kafka_data:/bitnami/kafka
    environment:
      # ── KRaft 核心配置 ──
      KAFKA_CFG_NODE_ID: 1                          # 节点唯一 ID
      KAFKA_CFG_PROCESS_ROLES: controller,broker    # 同时担任 Controller 和 Broker 角色
      KAFKA_CFG_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093  # Controller 投票集群

      # ── 监听器配置 ──
      # PLAINTEXT: 容器内部服务访问（如 Schema Registry）
      # EXTERNAL: 宿主机 Python 代码访问
      # CONTROLLER: KRaft 内部选举通信
      KAFKA_CFG_LISTENERS: PLAINTEXT://:9094,EXTERNAL://:9092,CONTROLLER://:9093
      KAFKA_CFG_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9094,EXTERNAL://localhost:9092
      KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,EXTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT
      KAFKA_CFG_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_CFG_INTER_BROKER_LISTENER_NAME: PLAINTEXT

      # ── 主题默认配置 ──
      KAFKA_CFG_NUM_PARTITIONS: 3                   # 新 Topic 默认 3 个 Partition
      KAFKA_CFG_DEFAULT_REPLICATION_FACTOR: 1       # 单节点环境，副本数为 1
      KAFKA_CFG_OFFSETS_TOPIC_REPLICATION_FACTOR: 1 # __consumer_offsets 副本数
      KAFKA_CFG_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_CFG_TRANSACTION_STATE_LOG_MIN_ISR: 1

      # ── 日志保留策略 ──
      KAFKA_CFG_LOG_RETENTION_HOURS: 168            # 消息保留 7 天
      KAFKA_CFG_LOG_SEGMENT_BYTES: 1073741824       # 单日志段 1 GB
      KAFKA_CFG_LOG_RETENTION_CHECK_INTERVAL_MS: 300000  # 每 5 分钟检查一次

      # ── 性能相关 ──
      KAFKA_HEAP_OPTS: "-Xmx1g -Xms512m"           # JVM 堆内存：最小 512MB，最大 1GB

      # ── 允许自动创建 Topic（开发环境开启，生产环境应关闭）──
      KAFKA_CFG_AUTO_CREATE_TOPICS_ENABLE: true

    healthcheck:
      # 检查 Kafka 是否已就绪（能响应 metadata 请求）
      test: ["CMD-SHELL", "kafka-topics.sh --bootstrap-server localhost:9092 --list"]
      interval: 30s
      timeout: 10s
      retries: 5
      start_period: 30s

  # ──────────────────────────────────────────
  # Confluent Schema Registry
  # 管理 Avro/JSON/Protobuf Schema 版本
  # ──────────────────────────────────────────
  schema-registry:
    image: confluentinc/cp-schema-registry:7.6.0
    container_name: schema-registry
    networks:
      - kafka-net
    ports:
      - "8081:8081"    # Schema Registry REST API 端口
    depends_on:
      kafka:
        condition: service_healthy   # 等待 Kafka 健康后再启动
    environment:
      # Schema Registry 监听地址
      SCHEMA_REGISTRY_HOST_NAME: schema-registry
      SCHEMA_REGISTRY_LISTENERS: http://0.0.0.0:8081

      # 连接到 Kafka（使用容器内部网络）
      SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS: kafka:9094

      # Schema 存储在 Kafka 内部 Topic 中
      SCHEMA_REGISTRY_KAFKASTORE_TOPIC: _schemas
      SCHEMA_REGISTRY_KAFKASTORE_TOPIC_REPLICATION_FACTOR: 1

      # 兼容性策略：BACKWARD（新版 Schema 可读取旧数据）
      SCHEMA_REGISTRY_SCHEMA_COMPATIBILITY_LEVEL: BACKWARD

    healthcheck:
      test: ["CMD-SHELL", "curl -f http://localhost:8081/subjects || exit 1"]
      interval: 30s
      timeout: 10s
      retries: 5
      start_period: 20s

  # ──────────────────────────────────────────
  # Kafka UI（Provectus）
  # Web 界面：查看 Topic、Consumer Group、消息
  # 访问：http://localhost:8080
  # ──────────────────────────────────────────
  kafka-ui:
    image: provectuslabs/kafka-ui:latest
    container_name: kafka-ui
    networks:
      - kafka-net
    ports:
      - "8080:8080"    # Kafka UI Web 界面
    depends_on:
      kafka:
        condition: service_healthy
      schema-registry:
        condition: service_healthy
    environment:
      # Kafka 连接配置
      KAFKA_CLUSTERS_0_NAME: local-kafka             # 集群显示名称
      KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: kafka:9094  # 使用容器内部地址

      # Schema Registry 集成（可在 UI 中查看 Schema）
      KAFKA_CLUSTERS_0_SCHEMAREGISTRY: http://schema-registry:8081

      # UI 功能开关
      KAFKA_CLUSTERS_0_READONLY: false               # 允许在 UI 中创建 Topic、发消息
      DYNAMIC_CONFIG_ENABLED: true                   # 允许运行时修改配置

  # ──────────────────────────────────────────
  # Prometheus（监控数据采集）
  # 采集 Kafka JMX 指标
  # ──────────────────────────────────────────
  prometheus:
    image: prom/prometheus:v2.51.0
    container_name: prometheus
    networks:
      - kafka-net
    ports:
      - "9090:9090"    # Prometheus Web UI
    volumes:
      - prometheus_data:/prometheus
      - ./config/prometheus.yml:/etc/prometheus/prometheus.yml:ro  # 挂载配置文件
    command:
      - '--config.file=/etc/prometheus/prometheus.yml'
      - '--storage.tsdb.path=/prometheus'
      - '--storage.tsdb.retention.time=15d'   # 保留 15 天指标数据
      - '--web.enable-lifecycle'               # 允许通过 API 热重载配置

  # ──────────────────────────────────────────
  # Grafana（监控可视化）
  # 访问：http://localhost:3000
  # 默认账号：admin / admin
  # ──────────────────────────────────────────
  grafana:
    image: grafana/grafana:10.4.0
    container_name: grafana
    networks:
      - kafka-net
    ports:
      - "3000:3000"    # Grafana Web UI
    volumes:
      - grafana_data:/var/lib/grafana
      - ./config/grafana/provisioning:/etc/grafana/provisioning:ro  # 预配置数据源和仪表盘
    environment:
      GF_SECURITY_ADMIN_PASSWORD: admin     # 初始密码（生产环境必须修改）
      GF_USERS_ALLOW_SIGN_UP: false         # 禁止公开注册
      GF_SERVER_HTTP_PORT: 3000
    depends_on:
      - prometheus
```

### 创建 Prometheus 配置文件

```bash
mkdir -p ~/riskguard/config
```

创建 `~/riskguard/config/prometheus.yml`：

```yaml
# config/prometheus.yml
# Prometheus 数据采集配置

global:
  scrape_interval: 15s      # 每 15 秒采集一次指标
  evaluation_interval: 15s  # 每 15 秒评估一次告警规则

scrape_configs:
  # 采集 Prometheus 自身的指标
  - job_name: 'prometheus'
    static_configs:
      - targets: ['localhost:9090']

  # 采集 Kafka 指标（通过 JMX Exporter，第 7 章详细配置）
  # 目前先注释掉，第 7 章监控章节再开启
  # - job_name: 'kafka'
  #   static_configs:
  #     - targets: ['kafka-jmx-exporter:9999']
```

---

## 2.3 KRaft 模式详解

### 为什么要抛弃 ZooKeeper？

ZooKeeper 诞生于 2008 年，当时 Kafka 还不存在，它被设计为通用分布式协调服务。Kafka 借用它来存储元数据和进行 Leader 选举，这个"借用"关系带来了以下问题：

**问题 1：运维负担加倍**

部署 Kafka 集群，同时还需要维护一套 ZooKeeper 集群（通常 3 或 5 个节点）。两套系统意味着两套监控、两套备份、两套升级流程。

**问题 2：元数据瓶颈**

所有 Kafka 元数据（Broker 信息、Topic 配置、Partition Leader）都存在 ZooKeeper 中。ZooKeeper 的写入性能有限（约 10,000 ops/s），当 Partition 数量达到几十万时，Controller 重启需要数分钟才能从 ZooKeeper 加载全部元数据。

**问题 3：Controller 切换慢**

Kafka 的 Controller（负责 Leader 选举的特殊 Broker）依赖 ZooKeeper Session 超时来检测失败，通常需要 30~120 秒才能完成 Controller 切换。

### KRaft 的解决方案

KRaft（Kafka Raft）将元数据管理内化到 Kafka 本身：

```
KRaft 架构示意：

┌─────────────────────────────────────────┐
│           Kafka KRaft 集群               │
│                                         │
│  ┌─────────────────────────────────┐    │
│  │     Controller Quorum (Raft)    │    │
│  │                                 │    │
│  │  Node 1 [Active Controller]     │    │
│  │  Node 2 [Controller Voter]      │    │
│  │  Node 3 [Controller Voter]      │    │
│  │                                 │    │
│  │  元数据存储在 __cluster_metadata  │    │
│  │  Topic（Raft 日志）              │    │
│  └─────────────────────────────────┘    │
│                                         │
│  ┌───────────┐  ┌───────────┐           │
│  │ Broker 1  │  │ Broker 2  │           │
│  │ (也可同时 │  │ (纯 Broker │           │
│  │ 是 Voter) │  │ 角色)     │           │
│  └───────────┘  └───────────┘           │
└─────────────────────────────────────────┘
```

**KRaft 的三种节点角色**：

| 角色 | 说明 | 典型部署 |
|------|------|----------|
| `broker` | 纯数据节点，处理 Producer/Consumer 请求 | 大规模集群的数据节点 |
| `controller` | 纯控制节点，负责元数据管理和 Leader 选举 | 专用控制节点 |
| `broker,controller` | 同时担任两种角色 | **本书使用**，适合小型集群/开发环境 |

**KRaft 关键改进**：

| 指标 | ZooKeeper 模式 | KRaft 模式 |
|------|----------------|------------|
| Controller 切换时间 | 30~120 秒 | < 100 毫秒 |
| 最大 Partition 数 | ~200,000 | ~2,000,000 |
| 元数据加载时间 | 数分钟（大集群） | 秒级 |
| 组件数量 | Kafka + ZooKeeper | 仅 Kafka |

### KRaft 核心配置解读

回顾 `docker-compose.yml` 中的 KRaft 配置：

```yaml
# 节点 ID，集群中唯一
KAFKA_CFG_NODE_ID: 1

# 此节点同时承担 Controller 和 Broker 角色
KAFKA_CFG_PROCESS_ROLES: controller,broker

# Controller Quorum 成员列表
# 格式：{nodeId}@{host}:{port}
# 单节点开发环境只有自己一个投票者
KAFKA_CFG_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093
```

**多节点 KRaft 集群（生产参考）**：

```yaml
# 3 节点集群的 Controller Quorum 配置（每个节点都需要设置）
KAFKA_CFG_CONTROLLER_QUORUM_VOTERS: 1@kafka-1:9093,2@kafka-2:9093,3@kafka-3:9093
```

> 📌 **开发环境单节点**：本书使用单节点 KRaft，`PROCESS_ROLES=controller,broker`，节点既是控制器又是数据节点。生产环境建议控制节点和数据节点分离。

---

## 2.4 启动环境

### 启动所有服务

```bash
cd ~/riskguard

# 后台启动所有服务（-d 表示 detached 模式）
docker compose up -d

# 查看启动日志（可选，Ctrl+C 退出日志跟踪）
docker compose logs -f
```

预期输出（约 30~60 秒后所有服务变为 healthy）：

```
[+] Running 6/6
 ✔ Network riskguard_kafka-net     Created
 ✔ Container kafka                 Healthy
 ✔ Container schema-registry       Healthy
 ✔ Container kafka-ui              Started
 ✔ Container prometheus            Started
 ✔ Container grafana               Started
```

### 验证所有服务正常运行

```bash
# 查看所有容器状态
docker compose ps

# 预期输出：
# NAME              STATUS          PORTS
# kafka             Up (healthy)    0.0.0.0:9092->9092/tcp
# schema-registry   Up (healthy)    0.0.0.0:8081->8081/tcp
# kafka-ui          Up              0.0.0.0:8080->8080/tcp
# prometheus        Up              0.0.0.0:9090->9090/tcp
# grafana           Up              0.0.0.0:3000->3000/tcp
```

---

## 2.5 验证 Kafka 安装

### 步骤 1：进入 Kafka 容器

```bash
# 进入 Kafka 容器的 Bash Shell
docker exec -it kafka bash
```

### 步骤 2：查看 Broker 状态

```bash
# 列出所有 Broker（应该看到 Broker ID: 1）
kafka-broker-api-versions.sh \
  --bootstrap-server localhost:9092 \
  2>/dev/null | head -5

# 预期输出（显示支持的 API 版本）：
# localhost:9092 (id: 1 rack: null) -> (
#   Produce(0): 0 to 11 [usable: 11],
#   Fetch(1): 0 to 16 [usable: 16],
#   ...
# )
```

```bash
# 查看集群元数据（KRaft 模式）
kafka-metadata-quorum.sh \
  --bootstrap-server localhost:9092 \
  describe --status

# 预期输出：
# ClusterId:              xxxxxxxxxxxxxxxxxxxx
# LeaderId:               1
# LeaderEpoch:            1
# HighWatermark:          XXX
# MaxFollowerLag:         0
# MaxFollowerLagTimeMs:   0
# CurrentVoters:          [1]
# CurrentObservers:       []
```

### 步骤 3：创建第一个 Topic

```bash
# 创建名为 hello-kafka 的 Topic
# --partitions 3 : 3 个分区
# --replication-factor 1 : 1 个副本（单节点只能为 1）
kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create \
  --topic hello-kafka \
  --partitions 3 \
  --replication-factor 1

# 预期输出：
# Created topic hello-kafka.
```

```bash
# 列出所有 Topic
kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --list

# 预期输出：
# __consumer_offsets    ← Kafka 内部 Topic，存储 Consumer Offset
# hello-kafka           ← 我们刚创建的
```

```bash
# 查看 Topic 详情（Partition 分布、Leader、副本）
kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --topic hello-kafka

# 预期输出：
# Topic: hello-kafka   TopicId: XXX   PartitionCount: 3   ReplicationFactor: 1
#   Topic: hello-kafka   Partition: 0   Leader: 1   Replicas: 1   Isr: 1
#   Topic: hello-kafka   Partition: 1   Leader: 1   Replicas: 1   Isr: 1
#   Topic: hello-kafka   Partition: 2   Leader: 1   Replicas: 1   Isr: 1
```

### 步骤 4：发送第一条消息

打开第一个终端，进入 Kafka 容器，启动 Producer（控制台生产者）：

```bash
# 启动控制台 Producer
kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic hello-kafka \
  --property "key.separator=:" \
  --property "parse.key=true"

# 输入以下消息（格式：key:value）：
user_001:{"event": "login", "timestamp": "2024-01-15T08:00:00Z"}
user_002:{"event": "purchase", "item": "Kafka Guide Book", "price": 49.99}
user_001:{"event": "logout", "timestamp": "2024-01-15T08:30:00Z"}

# 按 Ctrl+D 退出 Producer
```

### 步骤 5：消费消息

打开第二个终端，进入同一个 Kafka 容器：

```bash
docker exec -it kafka bash

# 从头开始消费所有消息（--from-beginning）
kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic hello-kafka \
  --from-beginning \
  --property "print.key=true" \
  --property "key.separator= -> "

# 预期输出（顺序可能因 Partition 分布不同）：
# user_001 -> {"event": "login", "timestamp": "2024-01-15T08:00:00Z"}
# user_001 -> {"event": "logout", "timestamp": "2024-01-15T08:30:00Z"}
# user_002 -> {"event": "purchase", "item": "Kafka Guide Book", "price": 49.99}
```

> 📌 **注意**：相同 Key（`user_001`）的两条消息会进入同一个 Partition，所以它们在输出中保持顺序。不同 Key 的消息可能属于不同 Partition，消费顺序不保证。

### 步骤 6：用 Consumer Group 消费

```bash
# 用指定 Consumer Group 消费
kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic hello-kafka \
  --group my-test-group \
  --from-beginning

# 查看 Consumer Group 的消费情况
kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --group my-test-group

# 预期输出（LAG=0 表示消费已追上最新消息）：
# GROUP          TOPIC       PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
# my-test-group  hello-kafka 0          1               1               0
# my-test-group  hello-kafka 1          1               1               0
# my-test-group  hello-kafka 2          1               1               0
```

退出容器：

```bash
exit
```

---

## 2.6 Python 开发环境配置

### 安装 Python 依赖

本书使用以下 Python 库：

```bash
# 创建虚拟环境（推荐）
python3 -m venv ~/riskguard-venv
source ~/riskguard-venv/bin/activate  # Linux/macOS
# Windows: ~/riskguard-venv\Scripts\activate

# 安装核心依赖
pip install \
  confluent-kafka==2.4.0 \      # Kafka 客户端（C 绑定，性能最佳）
  fastavro==1.9.4 \              # Avro 序列化（比 avro-python3 更快）
  requests==2.31.0 \             # 调用 Schema Registry REST API
  prometheus-client==0.20.0 \   # 暴露 Prometheus 指标
  python-dotenv==1.0.1 \        # 从 .env 文件加载配置
  loguru==0.7.2                  # 更好用的日志库

# 创建 requirements.txt 方便团队共享环境
pip freeze > requirements.txt
```

### 验证 Python 环境

```python
# verify_env.py
# 验证所有依赖安装正确

from confluent_kafka import Producer, Consumer  # Kafka 客户端
from confluent_kafka.schema_registry import SchemaRegistryClient  # Schema Registry
import requests
import prometheus_client
from loguru import logger

def check_kafka_connection():
    """验证 Kafka 连接"""
    conf = {'bootstrap.servers': 'localhost:9092'}
    producer = Producer(conf)
    
    # 获取集群元数据
    metadata = producer.list_topics(timeout=5)
    broker_count = len(metadata.brokers)
    topic_count = len(metadata.topics)
    
    logger.info(f"✅ Kafka 连接成功！Broker 数量: {broker_count}, Topic 数量: {topic_count}")
    return True

def check_schema_registry():
    """验证 Schema Registry 连接"""
    resp = requests.get("http://localhost:8081/subjects", timeout=5)
    resp.raise_for_status()
    
    logger.info(f"✅ Schema Registry 连接成功！已注册 Schema: {resp.json()}")
    return True

def check_kafka_ui():
    """验证 Kafka UI"""
    resp = requests.get("http://localhost:8080", timeout=5)
    
    if resp.status_code == 200:
        logger.info("✅ Kafka UI 正常！访问 http://localhost:8080 查看")
    return True

if __name__ == "__main__":
    logger.info("开始验证 RiskGuard 开发环境...")
    
    try:
        check_kafka_connection()
        check_schema_registry()
        check_kafka_ui()
        logger.success("🎉 所有组件验证通过！开发环境就绪。")
    except Exception as e:
        logger.error(f"❌ 验证失败：{e}")
        logger.info("请检查 Docker 容器是否全部启动：docker compose ps")
```

运行验证脚本：

```bash
python verify_env.py

# 预期输出：
# 2024-01-15 08:00:00 | INFO | ✅ Kafka 连接成功！Broker 数量: 1, Topic 数量: 3
# 2024-01-15 08:00:00 | INFO | ✅ Schema Registry 连接成功！已注册 Schema: []
# 2024-01-15 08:00:00 | INFO | ✅ Kafka UI 正常！访问 http://localhost:8080 查看
# 2024-01-15 08:00:00 | SUCCESS | 🎉 所有组件验证通过！开发环境就绪。
```

---

## 2.7 项目目录结构

本书遵循清晰的项目结构，每章新增对应目录：

```
~/riskguard/
├── docker-compose.yml          # 基础设施配置
├── requirements.txt             # Python 依赖
├── .env                         # 环境变量（不提交到 Git）
├── .env.example                 # 环境变量模板（提交到 Git）
│
├── config/                      # 配置文件目录
│   ├── prometheus.yml           # Prometheus 采集配置
│   └── grafana/
│       └── provisioning/        # Grafana 预配置（数据源、仪表盘）
│
├── schemas/                     # Avro Schema 定义（第 5 章）
│   ├── transaction.avsc         # 交易事件 Schema
│   └── risk_alert.avsc          # 风险告警 Schema
│
├── src/                         # 核心源码
│   ├── __init__.py
│   ├── config.py                # 配置管理（从 .env 加载）
│   ├── producer/                # 第 3 章：Producer
│   │   ├── __init__.py
│   │   ├── transaction_producer.py   # 交易事件生产者
│   │   └── base_producer.py          # 封装公共 Producer 逻辑
│   ├── consumer/                # 第 4 章：Consumer
│   │   ├── __init__.py
│   │   └── risk_consumer.py          # 风控消费者
│   └── streams/                 # 第 6 章：流处理
│       └── fraud_detector.py         # 欺诈检测流处理逻辑
│
├── scripts/                     # 运维脚本
│   ├── create_topics.sh         # 初始化所有 Topic
│   ├── reset_offsets.sh         # 重置 Consumer Offset
│   └── load_test.py             # 压测脚本
│
└── tests/                       # 测试
    ├── test_producer.py
    └── test_consumer.py
```

创建目录结构：

```bash
cd ~/riskguard
mkdir -p config/grafana/provisioning schemas src/{producer,consumer,streams} scripts tests

# 创建 Python 包初始化文件
touch src/__init__.py src/producer/__init__.py src/consumer/__init__.py

# 创建 .env 文件（存放敏感配置）
cat > .env << 'EOF'
# Kafka 连接配置
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_SECURITY_PROTOCOL=PLAINTEXT

# Schema Registry
SCHEMA_REGISTRY_URL=http://localhost:8081

# 应用配置
APP_ENV=development
LOG_LEVEL=INFO
EOF

# 创建 .env.example（供团队参考）
cp .env .env.example

echo "目录结构创建完成！"
```

---

## 2.8 常见安装问题排查

### 问题 1：端口冲突

**症状**：

```
Error response from daemon: driver failed programming external connectivity:
Bind for 0.0.0.0:9092 failed: port is already allocated
```

**原因**：宿主机 9092、8080、9090 等端口已被其他程序占用。

**解决方案**：

```bash
# 查看哪个进程占用了 9092 端口
lsof -i :9092      # macOS/Linux
netstat -ano | findstr 9092  # Windows

# 方案 1：停止占用端口的进程
kill -9 <PID>

# 方案 2：修改 docker-compose.yml 映射到其他端口
# 例如将 9092:9092 改为 19092:9092
# 注意：同时修改 Python 代码中的连接地址
```

### 问题 2：内存不足

**症状**：

```
kafka exited with code 137
```

或 Kafka 容器频繁重启。

**原因**：Docker 分配的内存不足，JVM 被系统 OOM Killer 杀死。

**解决方案**：

```bash
# 方案 1：减少 Kafka JVM 堆内存
# 在 docker-compose.yml 中修改：
KAFKA_HEAP_OPTS: "-Xmx512m -Xms256m"  # 降低堆内存

# 方案 2：增加 Docker 内存限制
# macOS/Windows：Docker Desktop → Settings → Resources → Memory，调高到 4 GB+
# Linux：Docker Engine 默认使用宿主机内存，检查宿主机可用内存：
free -h
```

### 问题 3：Kafka 启动超时，health check 失败

**症状**：

```
kafka is unhealthy
```

**排查步骤**：

```bash
# 查看 Kafka 容器日志
docker compose logs kafka | tail -50

# 常见原因 1：KRaft 元数据目录损坏（删除 Volume 重新初始化）
docker compose down -v  # 警告：-v 会删除所有数据 Volume
docker compose up -d

# 常见原因 2：时钟偏差（容器时间与宿主机不同步）
docker exec -it kafka date  # 查看容器时间
date                         # 查看宿主机时间
# 如果相差超过 2 秒，重启 Docker 服务
```

### 问题 4：Schema Registry 无法连接到 Kafka

**症状**：

```
io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryException:
Failed to connect to Kafka broker
```

**原因**：Schema Registry 在 Kafka 完全就绪之前就尝试连接了。

**解决方案**：

```bash
# 方案 1：等待几秒后重启 Schema Registry
docker compose restart schema-registry

# 方案 2：在 docker-compose.yml 中已配置 depends_on + healthcheck
# 确保 kafka 的 healthcheck 配置正确，让 schema-registry 等待 kafka healthy
```

### 问题 5：Python confluent-kafka 安装失败

**症状**：

```
ERROR: Failed building wheel for confluent-kafka
```

**原因**：confluent-kafka 是 C 扩展库，需要编译环境。

**解决方案**：

```bash
# Linux（Ubuntu/Debian）
sudo apt-get install -y python3-dev librdkafka-dev

# macOS（Homebrew）
brew install librdkafka
export C_INCLUDE_PATH=/opt/homebrew/include
export LIBRARY_PATH=/opt/homebrew/lib

# 重新安装
pip install confluent-kafka

# 如果仍然失败，使用预编译 wheel（但需要匹配 Python 版本）
pip install --prefer-binary confluent-kafka
```

### 问题 6：容器网络问题（宿主机无法访问 Kafka）

**症状**：

```python
# Python 代码报错
KafkaException: Failed to resolve 'kafka:9092'
```

**原因**：使用了容器内部地址（`kafka:9094`）而不是宿主机地址。

**解决方案**：

```python
# 错误配置（容器内部地址，只在容器间有效）
KAFKA_BOOTSTRAP_SERVERS=kafka:9094

# 正确配置（宿主机 Python 代码应使用 localhost 或宿主机 IP）
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
```

---

## 小结

本章我们完成了：

1. **环境准备**：Docker 安装和基础工具配置
2. **完整 Docker Compose 配置**：5 个服务（Kafka、Schema Registry、Kafka UI、Prometheus、Grafana）
3. **KRaft 模式原理**：了解为何 Kafka 3.x 抛弃 ZooKeeper，以及 Controller Quorum 的工作方式
4. **安装验证**：创建 Topic、发送和消费消息、检查 Consumer Group Offset
5. **Python 环境**：安装 confluent-kafka 等依赖，验证连接
6. **项目结构**：建立清晰的代码目录组织
7. **排查方法**：6 类常见问题的解决方案

---

## 动手练习

**练习 2.1：启动环境，用 Kafka UI 查看 Broker 信息**

1. 运行 `docker compose up -d`，等待所有服务 healthy
2. 打开浏览器访问 http://localhost:8080
3. 在 Kafka UI 中找到以下信息并截图/记录：
   - Broker 的版本号和 ID
   - `hello-kafka` Topic 的 Partition 分布
   - Consumer Group `my-test-group` 的 LAG 值

**练习 2.2：创建 RiskGuard 初始 Topic**

在 Kafka 容器中执行以下命令，为后续章节预创建 Topic：

```bash
docker exec -it kafka bash

# 创建交易原始事件 Topic（4 个 Partition，对应 4 个 Consumer 并发）
kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create \
  --topic riskguard.txn.raw \
  --partitions 4 \
  --replication-factor 1 \
  --config retention.ms=604800000   # 7 天

# 创建风险告警 Topic
kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create \
  --topic riskguard.alerts.fraud \
  --partitions 2 \
  --replication-factor 1

# 验证
kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --list
```

**练习 2.3：Kafka UI 发送测试消息**

通过 Kafka UI（http://localhost:8080）发送一条消息到 `riskguard.txn.raw`，内容为：

```json
{
  "transaction_id": "txn_test_001",
  "user_id": "user_001",
  "amount": 9999.99,
  "currency": "CNY",
  "timestamp": "2024-01-15T08:00:00Z"
}
```

然后在 Kafka UI 的 "Messages" 页面验证消息已经写入，观察它被分配到哪个 Partition。

---

*下一章，我们将深入 Producer 的最佳实践，学习如何写出生产级别的高可靠、高吞吐 Python Producer，这也是 RiskGuard 系统的起点。*
