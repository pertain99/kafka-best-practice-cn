"""
setup_topics.py — RiskGuard Topic 初始化脚本
==============================================
创建所有必要的 Kafka Topic，并设置正确的：
  - 分区数
  - 副本因子
  - 保留策略
  - 压缩类型

运行方式：
  python scripts/setup_topics.py
  python scripts/setup_topics.py --bootstrap-server localhost:9092
  python scripts/setup_topics.py --dry-run   # 仅预览，不实际创建
"""

import argparse
import sys
import time
import logging
from pathlib import Path

# 将项目根目录加入 Python 路径
sys.path.insert(0, str(Path(__file__).parent.parent))

from confluent_kafka.admin import AdminClient, NewTopic, ConfigResource
from confluent_kafka.admin import ConfigSource
from confluent_kafka import KafkaException

from config.kafka_config import BOOTSTRAP_SERVERS, TOPIC_CONFIGS

# =============================================================================
# 日志配置
# =============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("setup-topics")


# =============================================================================
# Topic 初始化核心逻辑
# =============================================================================

def get_existing_topics(admin_client: AdminClient) -> set[str]:
    """获取 Kafka 集群中已存在的 Topic 列表。"""
    metadata = admin_client.list_topics(timeout=10)
    return set(metadata.topics.keys())


def create_topics(
    admin_client: AdminClient,
    topic_configs: dict,
    dry_run: bool = False,
) -> dict[str, bool]:
    """
    批量创建 Topic。

    Args:
        admin_client: Kafka AdminClient 实例
        topic_configs: Topic 配置字典（来自 kafka_config.py）
        dry_run: 若为 True，仅打印计划，不实际创建

    Returns:
        字典：{topic_name: True/False}，True 表示创建成功
    """
    existing = get_existing_topics(admin_client)
    results = {}

    # 筛选出需要创建的 Topic（跳过已存在的）
    to_create = []
    for topic_name, config in topic_configs.items():
        if topic_name in existing:
            logger.info(f"⏭  Topic 已存在，跳过: {topic_name}")
            results[topic_name] = True
            continue

        new_topic = NewTopic(
            topic=topic_name,
            num_partitions=config["num_partitions"],
            replication_factor=config["replication_factor"],
            config=config.get("config", {}),
        )
        to_create.append(new_topic)

        # 打印计划
        logger.info(
            f"📋 计划创建 Topic: {topic_name}\n"
            f"   分区数: {config['num_partitions']}\n"
            f"   副本因子: {config['replication_factor']}\n"
            f"   配置: {config.get('config', {})}"
        )

    if dry_run:
        logger.info("🔍 Dry-run 模式，跳过实际创建")
        for t in to_create:
            results[t.topic] = True
        return results

    if not to_create:
        logger.info("✅ 所有 Topic 已存在，无需创建")
        return results

    # 执行批量创建
    logger.info(f"🚀 开始创建 {len(to_create)} 个 Topic...")
    futures = admin_client.create_topics(to_create, request_timeout=30)

    for topic, future in futures.items():
        try:
            future.result()  # 阻塞直到完成
            logger.info(f"✅ Topic 创建成功: {topic}")
            results[topic] = True
        except KafkaException as e:
            logger.error(f"❌ Topic 创建失败: {topic} — {e}")
            results[topic] = False

    return results


def verify_topics(admin_client: AdminClient, expected_topics: list[str]) -> bool:
    """验证所有预期 Topic 是否已成功创建。"""
    logger.info("🔍 验证 Topic 创建结果...")

    # 等待 Broker 元数据同步
    time.sleep(2)

    existing = get_existing_topics(admin_client)
    all_ok = True

    for topic in expected_topics:
        if topic in existing:
            metadata = admin_client.list_topics(topic=topic, timeout=10)
            partition_count = len(metadata.topics[topic].partitions)
            logger.info(f"  ✅ {topic} — {partition_count} 个分区")
        else:
            logger.error(f"  ❌ {topic} — 未找到！")
            all_ok = False

    return all_ok


def print_summary(topic_configs: dict) -> None:
    """打印 Topic 配置摘要表格。"""
    print("\n" + "=" * 75)
    print(f"  {'Topic 名称':<25} {'分区数':>6}  {'保留时间':>12}  {'压缩策略':>15}")
    print("=" * 75)

    for topic_name, config in topic_configs.items():
        cfg = config.get("config", {})
        retention_ms = int(cfg.get("retention.ms", 0))
        retention_days = retention_ms / (24 * 60 * 60 * 1000)
        cleanup = cfg.get("cleanup.policy", "delete")
        compression = cfg.get("compression.type", "none")
        partitions = config["num_partitions"]

        retention_str = f"{int(retention_days)} 天" if retention_days else "默认"
        print(
            f"  {topic_name:<25} {partitions:>6}  {retention_str:>12}  "
            f"{cleanup}/{compression:>10}"
        )

    print("=" * 75 + "\n")


# =============================================================================
# 主程序
# =============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="RiskGuard — Kafka Topic 初始化脚本",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例：
  # 使用默认配置创建 Topics
  python scripts/setup_topics.py

  # 指定 Kafka 地址
  python scripts/setup_topics.py --bootstrap-server kafka:29092

  # 仅预览，不实际创建
  python scripts/setup_topics.py --dry-run

  # 删除已有 Topic 并重建（危险！仅开发环境使用）
  python scripts/setup_topics.py --recreate
        """,
    )
    parser.add_argument(
        "--bootstrap-server",
        default=BOOTSTRAP_SERVERS,
        help=f"Kafka Broker 地址（默认: {BOOTSTRAP_SERVERS}）",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="仅预览配置，不实际创建 Topic",
    )
    parser.add_argument(
        "--recreate",
        action="store_true",
        help="删除并重建所有 Topic（⚠️ 数据将丢失！仅开发环境）",
    )
    args = parser.parse_args()

    logger.info("=" * 55)
    logger.info("  RiskGuard — Kafka Topic 初始化")
    logger.info("=" * 55)
    logger.info(f"  Kafka Broker: {args.bootstrap_server}")
    logger.info(f"  Dry-run:      {args.dry_run}")
    logger.info(f"  Recreate:     {args.recreate}")
    logger.info("=" * 55)

    # 打印配置摘要
    print_summary(TOPIC_CONFIGS)

    # 初始化 AdminClient
    admin_client = AdminClient({"bootstrap.servers": args.bootstrap_server})

    # 验证连接
    try:
        admin_client.list_topics(timeout=10)
        logger.info("✅ 已成功连接到 Kafka Broker")
    except Exception as e:
        logger.error(f"❌ 无法连接到 Kafka Broker: {e}")
        logger.error(f"   请确认 Kafka 已启动：docker-compose up -d kafka")
        sys.exit(1)

    # 如果需要重建，先删除
    if args.recreate and not args.dry_run:
        logger.warning("⚠️  即将删除所有 RiskGuard Topic！（3 秒后执行）")
        for i in range(3, 0, -1):
            logger.warning(f"   {i}...")
            time.sleep(1)

        existing = get_existing_topics(admin_client)
        to_delete = [t for t in TOPIC_CONFIGS.keys() if t in existing]

        if to_delete:
            futures = admin_client.delete_topics(to_delete, request_timeout=30)
            for topic, future in futures.items():
                try:
                    future.result()
                    logger.info(f"🗑  已删除: {topic}")
                except Exception as e:
                    logger.error(f"删除失败: {topic} — {e}")
            time.sleep(3)  # 等待删除完成

    # 创建 Topics
    results = create_topics(admin_client, TOPIC_CONFIGS, dry_run=args.dry_run)

    # 验证结果
    if not args.dry_run:
        all_ok = verify_topics(admin_client, list(TOPIC_CONFIGS.keys()))

        if all_ok:
            logger.info("🎉 所有 Topic 初始化完成！")
            logger.info("   下一步：python scripts/setup_topics.py --help")
        else:
            logger.error("❌ 部分 Topic 初始化失败，请检查 Kafka 日志")
            sys.exit(1)
    else:
        logger.info("🔍 Dry-run 完成。使用 --recreate 删除现有数据并重建。")


if __name__ == "__main__":
    main()
