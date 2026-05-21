# =============================================================================
# RiskGuard — Makefile
# 一键启动/停止/管理 RiskGuard 实时交易风控系统
# =============================================================================
# 使用方式：
#   make start    → 启动所有 Docker 服务
#   make setup    → 初始化 Kafka Topics
#   make produce  → 启动交易生成器
#   make consume  → 启动风控检测器
#   make dashboard → 启动告警仪表盘
#   make test     → 运行单元测试
# =============================================================================

.DEFAULT_GOAL := help
PYTHON        := python3
DOCKER        := docker-compose

.PHONY: help start stop restart setup produce consume dashboard test logs \
        clean ps shell-kafka topics status install

# ── 颜色配置（终端输出美化）─────────────────────────────────────────────────
GREEN  := \033[0;32m
YELLOW := \033[1;33m
CYAN   := \033[0;36m
RESET  := \033[0m

# =============================================================================
# 帮助信息
# =============================================================================

help: ## 显示此帮助菜单
	@echo ""
	@echo "  🛡️  RiskGuard — 实时交易风控系统"
	@echo ""
	@echo "  $(CYAN)基础操作$(RESET)"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| sort \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "    $(GREEN)%-15s$(RESET) %s\n", $$1, $$2}'
	@echo ""
	@echo "  $(YELLOW)快速启动流程：$(RESET)"
	@echo "    1. make start      # 启动 Docker 服务"
	@echo "    2. make setup      # 初始化 Topics（等 Kafka 健康后）"
	@echo "    3. make consume    # 新终端：启动风控检测器"
	@echo "    4. make dashboard  # 新终端：启动告警仪表盘"
	@echo "    5. make produce    # 新终端：开始生成交易数据"
	@echo ""

# =============================================================================
# Docker 服务管理
# =============================================================================

start: ## 启动所有服务（Kafka, Schema Registry, Kafka UI, Prometheus, Grafana）
	@echo "$(CYAN)▶  启动 RiskGuard 所有服务...$(RESET)"
	$(DOCKER) up -d
	@echo ""
	@echo "$(GREEN)✅ 服务已启动！$(RESET)"
	@echo ""
	@echo "  🌐 Kafka UI:      http://localhost:8080"
	@echo "  📊 Grafana:       http://localhost:3000  (admin/riskguard123)"
	@echo "  🔍 Prometheus:    http://localhost:9090"
	@echo "  📋 Schema Reg:    http://localhost:8081"
	@echo ""
	@echo "  $(YELLOW)等待 Kafka 就绪后运行: make setup$(RESET)"

stop: ## 停止所有服务（保留数据卷）
	@echo "$(YELLOW)⏹  停止所有服务...$(RESET)"
	$(DOCKER) stop
	@echo "$(GREEN)✅ 所有服务已停止$(RESET)"

restart: ## 重启所有服务
	@echo "$(YELLOW)🔄 重启服务...$(RESET)"
	$(DOCKER) restart
	@echo "$(GREEN)✅ 服务已重启$(RESET)"

clean: ## 停止并删除所有容器和数据卷（⚠️ 数据将丢失！）
	@echo "$(YELLOW)⚠️  即将删除所有容器和数据卷！$(RESET)"
	@read -p "   确认继续？(y/N): " confirm && [ "$$confirm" = "y" ] || exit 1
	$(DOCKER) down -v --remove-orphans
	@echo "$(GREEN)✅ 清理完成$(RESET)"

ps: ## 查看所有容器状态
	$(DOCKER) ps

logs: ## 查看 Kafka 日志（实时跟踪）
	$(DOCKER) logs -f kafka

logs-all: ## 查看所有服务日志
	$(DOCKER) logs -f

# =============================================================================
# Kafka 初始化
# =============================================================================

setup: ## 初始化 Topics 和 Schema（在 start 之后运行）
	@echo "$(CYAN)📋 初始化 Kafka Topics...$(RESET)"
	$(PYTHON) scripts/setup_topics.py
	@echo ""
	@echo "$(GREEN)✅ Topics 初始化完成！$(RESET)"
	@echo "$(YELLOW)下一步：在三个独立终端分别运行 make consume / make dashboard / make produce$(RESET)"

setup-dry: ## 预览 Topic 配置（不实际创建）
	$(PYTHON) scripts/setup_topics.py --dry-run

setup-recreate: ## 删除并重建所有 Topics（⚠️ 数据将丢失！仅开发环境）
	$(PYTHON) scripts/setup_topics.py --recreate

topics: ## 列出所有 Topics（通过 kafka-topics.sh）
	$(DOCKER) exec riskguard-kafka \
		kafka-topics --bootstrap-server localhost:9092 --list

status: ## 查看所有 Topic 详情（分区、副本、Leader）
	$(DOCKER) exec riskguard-kafka \
		kafka-topics --bootstrap-server localhost:9092 --describe

# =============================================================================
# 应用程序
# =============================================================================

install: ## 安装 Python 依赖
	@echo "$(CYAN)📦 安装依赖...$(RESET)"
	pip install -r requirements.txt
	@echo "$(GREEN)✅ 依赖安装完成$(RESET)"

produce: ## 启动交易生成器（5条/秒，注入异常数据）
	@echo "$(CYAN)📤 启动交易生成器...$(RESET)"
	$(PYTHON) producer/trade_generator.py \
		--rate 5 \
		--inject-anomalies \
		--anomaly-rate 0.15

produce-fast: ## 高速模式：20条/秒，运行 60 秒
	$(PYTHON) producer/trade_generator.py \
		--rate 20 \
		--duration 60 \
		--inject-anomalies

produce-clean: ## 纯净模式：不注入异常，观察正常流量
	$(PYTHON) producer/trade_generator.py \
		--rate 5 \
		--no-anomalies

consume: ## 启动风控检测器（手动 Offset 提交）
	@echo "$(CYAN)🔍 启动风控检测器...$(RESET)"
	$(PYTHON) consumer/risk_detector.py

consume-debug: ## 启动风控检测器（Debug 日志级别）
	$(PYTHON) consumer/risk_detector.py --log-level DEBUG

dashboard: ## 启动实时告警仪表盘（控制台）
	@echo "$(CYAN)📊 启动告警仪表盘...$(RESET)"
	$(PYTHON) consumer/alert_dashboard.py

dashboard-log: ## 仪表盘日志模式（不清屏，适合 tmux）
	$(PYTHON) consumer/alert_dashboard.py --no-clear --no-rich

# =============================================================================
# 测试
# =============================================================================

test: ## 运行所有单元测试
	@echo "$(CYAN)🧪 运行单元测试...$(RESET)"
	pytest tests/ -v --tb=short
	@echo "$(GREEN)✅ 测试完成$(RESET)"

test-coverage: ## 运行测试并生成覆盖率报告
	pytest tests/ -v --cov=. --cov-report=html --cov-report=term-missing
	@echo "$(GREEN)✅ 覆盖率报告已生成: htmlcov/index.html$(RESET)"

test-fast: ## 快速测试（不显示详细输出）
	pytest tests/ -q

# =============================================================================
# 调试工具
# =============================================================================

shell-kafka: ## 进入 Kafka 容器 Shell
	$(DOCKER) exec -it riskguard-kafka bash

consume-raw: ## 实时查看 trades.raw 原始消息（控制台消费者）
	$(DOCKER) exec riskguard-kafka \
		kafka-console-consumer \
		--bootstrap-server localhost:9092 \
		--topic trades.raw \
		--from-beginning

consume-alerts: ## 实时查看 risk.alerts 告警消息
	$(DOCKER) exec riskguard-kafka \
		kafka-console-consumer \
		--bootstrap-server localhost:9092 \
		--topic risk.alerts \
		--from-beginning

consume-dlq: ## 查看死信队列（trades.dlq）
	$(DOCKER) exec riskguard-kafka \
		kafka-console-consumer \
		--bootstrap-server localhost:9092 \
		--topic trades.dlq \
		--from-beginning

consumer-groups: ## 查看消费者组状态
	$(DOCKER) exec riskguard-kafka \
		kafka-consumer-groups \
		--bootstrap-server localhost:9092 \
		--list

lag: ## 查看 Consumer Lag（风控检测器）
	$(DOCKER) exec riskguard-kafka \
		kafka-consumer-groups \
		--bootstrap-server localhost:9092 \
		--group risk-detector-group \
		--describe

market-prices: ## 查看当前模拟市场价格
	$(PYTHON) scripts/market_prices.py
