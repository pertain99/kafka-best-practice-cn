# Kafka 最佳实践实战
## 书籍大纲

**副标题**：从零安装到生产级流处理系统

---

## 目标读者
- 有 Python/Java 基础，想学 Kafka 的工程师
- 已会 Kafka 基础，想提升到生产级最佳实践的工程师
- 备战数据工程/后端面试的候选人

## 技术栈
- Kafka 3.x (KRaft 模式，无 Zookeeper)
- Python (kafka-python / confluent-kafka)
- Docker Compose (本地开发)
- End-to-End 项目：实时交易风控系统

---

## 章节列表

| 章节 | 标题 | 核心内容 |
|------|------|---------|
| Ch01 | Kafka 是什么 | 消息队列 vs 流平台，架构，核心概念 |
| Ch02 | 安装与环境搭建 | Docker Compose，KRaft 模式，验证安装 |
| Ch03 | Producer 最佳实践 | 幂等性，acks，压缩，批量，重试策略 |
| Ch04 | Consumer 最佳实践 | 消费者组，Offset 管理，Rebalance，背压 |
| Ch05 | Topic 设计与分区策略 | 分区数计算，Key 设计，保留策略，压缩 |
| Ch06 | Schema Registry | Avro/Protobuf，Schema 演化，兼容性 |
| Ch07 | Kafka Streams | 流处理，窗口，聚合，Join |
| Ch08 | 监控与可观测性 | JMX，Prometheus，Grafana，关键指标 |
| Ch09 | 安全与认证 | TLS，SASL，ACL，数据加密 |
| Ch10 | 生产级运维 | 扩容，日志清理，性能调优，故障排查 |
| Ch11 | End-to-End 项目 | 实时交易风控系统（完整可运行） |
| 附录A | 常用命令速查 | kafka-topics，kafka-console-* 等 |
| 附录B | 面试题精选 | 20道高频 Kafka 面试题+答案 |

---

## End-to-End 项目设计

**项目**：实时交易风控系统（RiskGuard）

**数据流**：
```
交易生成器 → Topic: raw-trades
    → 风控检测 Consumer（大额/频率/异常）
    → Topic: risk-alerts
    → 告警聚合 Consumer
    → 控制台实时展示
```

**特性**：
- 完整 Docker Compose 一键启动
- Python 实现（Producer + Consumer + Streams）
- 幂等 Producer，手动 Offset 管理
- Schema Registry + Avro 序列化
- Prometheus 指标暴露
- 完整单元测试
