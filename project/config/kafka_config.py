"""
kafka_config.py — RiskGuard 集中配置管理
=========================================
所有 Kafka 连接参数、Producer/Consumer 配置、Topic 名称都集中在此文件管理。
修改配置只需改此文件，无需逐一修改各组件。
"""

import os

# =============================================================================
# 基础连接配置
# =============================================================================

# Kafka Broker 地址（支持环境变量覆盖，适配 Docker/K8s 部署）
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

# Schema Registry 地址
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")

# =============================================================================
# Producer 配置（最强可靠性 + 高吞吐平衡）
# =============================================================================

PRODUCER_CONFIG = {
    # ── 连接 ──────────────────────────────────────────────────────────────────
    "bootstrap.servers": BOOTSTRAP_SERVERS,

    # ── 可靠性：acks=all 要求 ISR 中所有副本确认 ────────────────────────────
    # acks=0  → 发完就走，最高吞吐，可能丢消息（适合日志/指标等场景）
    # acks=1  → Leader 确认即可，中等可靠（适合非关键业务日志）
    # acks=all → ISR 全部确认，最强可靠（金融/风控必须）
    "acks": "all",

    # ── 幂等 Producer：防止网络重试造成重复写入 ──────────────────────────────
    # 开启后 Producer 自动分配 PID，每条消息带序列号
    # Broker 端去重，保证 exactly-once（单分区内）
    "enable.idempotence": True,

    # ── 压缩：snappy 在速度和压缩率之间取得最佳平衡 ─────────────────────────
    # none    → 不压缩（最低 CPU，最高带宽）
    # gzip    → 最高压缩率（~70%），但 CPU 消耗大
    # snappy  → 中等压缩（~50%），极低 CPU（推荐金融场景）
    # lz4     → 类似 snappy，解压速度更快
    # zstd    → 新一代，压缩率最高同时速度快（Kafka 2.1+）
    "compression.type": "snappy",

    # ── 批量：64KB 批量大小，减少网络往返次数 ────────────────────────────────
    "batch.size": 65536,

    # ── 延迟：等待最多 5ms 凑批，权衡吞吐和延迟 ─────────────────────────────
    # linger.ms=0  → 立即发送（最低延迟，但批次小）
    # linger.ms=5  → 等 5ms 凑批（推荐生产环境）
    # linger.ms=20 → 高吞吐场景（批量日志写入）
    "linger.ms": 5,

    # ── 重试：幂等模式下可以无限重试（不会造成重复） ─────────────────────────
    "retries": 2147483647,          # 接近无限
    "retry.backoff.ms": 100,        # 每次重试等待 100ms

    # ── 在途请求数：幂等模式下最多 5 个并发请求 ──────────────────────────────
    # 注意：enable.idempotence=True 时此值自动限制为 ≤5
    "max.in.flight.requests.per.connection": 5,

    # ── 请求超时 ──────────────────────────────────────────────────────────────
    "request.timeout.ms": 30000,
    "delivery.timeout.ms": 120000,  # 总交付超时（含重试）

    # ── 缓冲区：Producer 内存缓冲总大小 ─────────────────────────────────────
    "buffer.memory": 33554432,      # 32 MB
}

# =============================================================================
# Consumer 配置（手动 Offset 提交，高可靠）
# =============================================================================

CONSUMER_CONFIG = {
    # ── 连接 ──────────────────────────────────────────────────────────────────
    "bootstrap.servers": BOOTSTRAP_SERVERS,

    # ── 消费者组：同组内多个 Consumer 自动分配分区 ───────────────────────────
    "group.id": "risk-detector-group",

    # ── 重置策略：earliest = 从头消费（适合首次部署）────────────────────────
    # earliest → 从最早的消息开始（适合重跑/首次部署）
    # latest   → 从最新消息开始（适合实时监控，不关心历史）
    "auto.offset.reset": "earliest",

    # ── 禁用自动提交：必须手动 commitSync/commitAsync ────────────────────────
    # 自动提交的问题：消息拿到但处理失败，offset 已提交 → 丢消息
    # 手动提交：处理成功后才提交 → at-least-once 保证
    "enable.auto.commit": False,

    # ── 单次 poll 最多拉取条数 ────────────────────────────────────────────────
    "max.poll.records": 500,

    # ── 会话超时：Consumer 超过此时间无心跳 → 触发 Rebalance ─────────────────
    "session.timeout.ms": 30000,

    # ── 心跳间隔：应为 session.timeout.ms 的 1/3 ────────────────────────────
    "heartbeat.interval.ms": 3000,

    # ── 最大 poll 间隔：两次 poll() 之间最长处理时间 ─────────────────────────
    # 超过此时间，Consumer 被踢出组（触发 Rebalance）
    "max.poll.interval.ms": 300000,  # 5 分钟

    # ── fetch 配置 ────────────────────────────────────────────────────────────
    "fetch.min.bytes": 1,            # 立即返回（低延迟模式）
    "fetch.max.wait.ms": 500,        # 最多等 500ms 凑数据
}

# ── 告警仪表盘消费者（独立消费者组，不影响风控检测器）────────────────────────
DASHBOARD_CONSUMER_CONFIG = {
    **CONSUMER_CONFIG,
    "group.id": "alert-dashboard-group",
    "auto.offset.reset": "latest",   # 仪表盘只关心最新告警
}

# ── DLQ 消费者（监控死信队列）────────────────────────────────────────────────
DLQ_CONSUMER_CONFIG = {
    **CONSUMER_CONFIG,
    "group.id": "dlq-monitor-group",
    "auto.offset.reset": "earliest",
}

# =============================================================================
# Topic 名称配置
# =============================================================================

TOPICS = {
    # 原始交易流：所有交易进入此 Topic
    "raw_trades": "trades.raw",

    # 风险告警：风控检测器输出，包含触发的规则和详情
    "risk_alerts": "risk.alerts",

    # 死信队列：处理失败的消息进入此 Topic，供人工审查
    "dlq": "trades.dlq",
}

# =============================================================================
# Topic 详细配置（用于初始化脚本）
# =============================================================================

TOPIC_CONFIGS = {
    "trades.raw": {
        "num_partitions": 6,           # 6 个分区，支持 6 个并发消费者
        "replication_factor": 1,       # 开发环境单副本（生产建议 3）
        "config": {
            "retention.ms": str(7 * 24 * 60 * 60 * 1000),   # 7 天
            "compression.type": "snappy",
            "cleanup.policy": "delete",
            "segment.ms": str(24 * 60 * 60 * 1000),          # 1 天
            "max.message.bytes": "10485760",                  # 10 MB
        },
    },
    "risk.alerts": {
        "num_partitions": 3,
        "replication_factor": 1,
        "config": {
            "retention.ms": str(30 * 24 * 60 * 60 * 1000),  # 30 天
            "compression.type": "snappy",
            "cleanup.policy": "compact",        # Log Compaction：保留最新告警状态
            "min.cleanable.dirty.ratio": "0.5",
            "segment.ms": str(24 * 60 * 60 * 1000),
        },
    },
    "trades.dlq": {
        "num_partitions": 3,
        "replication_factor": 1,
        "config": {
            "retention.ms": str(30 * 24 * 60 * 60 * 1000),  # 30 天
            "compression.type": "snappy",
            "cleanup.policy": "delete",
        },
    },
}

# =============================================================================
# 风控规则阈值（集中管理，便于调整）
# =============================================================================

RISK_RULES = {
    # 规则1：大额交易告警阈值（CAD）
    "large_trade_threshold_cad": float(os.getenv("LARGE_TRADE_THRESHOLD", "50000")),

    # 规则2：频率告警：时间窗口（秒）
    "frequency_window_seconds": int(os.getenv("FREQ_WINDOW_SECONDS", "60")),

    # 规则2：频率告警：窗口内最大交易笔数
    "frequency_max_trades": int(os.getenv("FREQ_MAX_TRADES", "5")),

    # 规则3：价格异常：允许偏离市场价的最大百分比
    "price_deviation_pct": float(os.getenv("PRICE_DEVIATION_PCT", "5.0")),
}

# =============================================================================
# Schema Registry 配置
# =============================================================================

SCHEMA_REGISTRY_CONFIG = {
    "url": SCHEMA_REGISTRY_URL,
}

# Schema 主题名称（Schema Registry 中的 Subject 名称）
SCHEMA_SUBJECTS = {
    "trade": "trades.raw-value",
    "risk_alert": "risk.alerts-value",
}

# =============================================================================
# Prometheus 指标配置
# =============================================================================

METRICS_CONFIG = {
    "producer_port": int(os.getenv("PRODUCER_METRICS_PORT", "8000")),
    "consumer_port": int(os.getenv("CONSUMER_METRICS_PORT", "8001")),
    "dashboard_port": int(os.getenv("DASHBOARD_METRICS_PORT", "8002")),
}
