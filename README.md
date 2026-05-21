# Kafka 最佳实践实战

> 从零安装到生产级流处理系统

[![Python 3.11+](https://img.shields.io/badge/Python-3.11+-blue.svg)](https://python.org)
[![Kafka 3.x](https://img.shields.io/badge/Kafka-3.x_KRaft-231F20.svg)](https://kafka.apache.org)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

## 内容

| 章节 | 标题 |
|------|------|
| Ch01 | Kafka 是什么 |
| Ch02 | 安装与环境搭建 |
| Ch03 | Producer 最佳实践 |
| Ch04 | Consumer 最佳实践 |
| Ch05 | Topic 设计与分区策略 |
| Ch06 | Schema Registry 与数据契约 |
| Ch07 | Kafka Streams 流处理 |
| Ch08 | 监控与可观测性 |
| Ch09 | 安全与认证 |
| Ch10 | 生产级运维与调优 |
| Ch11 | End-to-End 项目：实时交易风控系统 RiskGuard |
| 附录A | 常用命令速查 |
| 附录B | 面试题精选 20 道 |

## 快速开始

```bash
cd project
make start    # 启动 Kafka + Schema Registry + UI
make setup    # 创建 Topics
make produce  # 启动交易生成器
make consume  # 启动风控检测器
make dashboard # 实时告警展示
```

## 项目：RiskGuard 实时交易风控

```
trade_generator.py → trades.raw → risk_detector.py → risk.alerts → alert_dashboard.py
```
