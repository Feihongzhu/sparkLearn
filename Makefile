COMPOSE=docker compose -f docker/docker-compose.yml

.PHONY: up down logs submit demo data clean wait plan gold gold-demo iceberg-setup iceberg-ingest iceberg-features iceberg-demo iceberg-test iceberg-interactive

up:
	$(COMPOSE) up -d

down:
	$(COMPOSE) down -v

logs:
	$(COMPOSE) logs -f

# 生成样本数据（本机 Python）
data:
	python3 scripts/generate_orders.py --output data/orders.csv --num-records 1000

# 在容器内提交作业（使用 spark-master）
submit:
	$(COMPOSE) exec spark-master /opt/spark/bin/spark-submit \
		--master spark://spark-master:7077 \
		--conf spark.ui.port=4040 \
		/workspace/src/batch/ingest_bronze.py \
		--input /workspace/data/orders.csv \
		--output /workspace/data/bronze/orders
# 导出物理计划到 data/plan.txt
plan: up wait data
	$(COMPOSE) exec spark-master /opt/spark/bin/spark-submit \
		--master spark://spark-master:7077 \
		--conf spark.ui.port=4040 \
		/workspace/src/batch/ingest_bronze.py \
		--input /workspace/data/orders.csv \
		--output /workspace/data/bronze/orders \
		--explain /workspace/data/plan.txt

# 一键演示：启动 → 生成数据 → 提交作业
demo: up wait data submit
# 运行 gold 聚合：读取 bronze 明细，输出 gold 指标
gold:
	$(COMPOSE) exec spark-master /opt/spark/bin/spark-submit \
		--master spark://spark-master:7077 \
		--conf spark.ui.port=4040 \
		/workspace/src/batch/gold_daily_metrics.py \
		--silver /workspace/data/bronze/orders \
		--gold /workspace/data/gold \
		--topn 5

# 一键 gold 演示：启动 → 生成数据 → bronze → gold
gold-demo: up wait data submit gold

# 等待 Spark Master UI 就绪
wait:
	@echo "waiting spark master UI ..." ; \
	until curl -sSf http://localhost:8080 >/dev/null ; do \
	  sleep 1 ; \
	done ; \
	echo "spark master is ready"

clean:
	rm -rf data/bronze/orders data/*.crc data/iceberg-warehouse

# ========== Iceberg 相关命令 ==========

# 下载 Iceberg Spark Runtime JAR
iceberg-setup:
	@echo "下载 Iceberg Spark Runtime JAR..."
	@mkdir -p jars
	@if [ ! -f jars/iceberg-spark-runtime-3.5_2.12-1.4.3.jar ]; then \
		curl -L -o jars/iceberg-spark-runtime-3.5_2.12-1.4.3.jar \
		https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.4.3/iceberg-spark-runtime-3.5_2.12-1.4.3.jar; \
		echo "✅ Iceberg JAR 下载完成"; \
	else \
		echo "✅ Iceberg JAR 已存在"; \
	fi

# 本地运行 Iceberg 数据摄入（使用本地 Spark）
iceberg-ingest-local: data
	@echo "运行 Iceberg 数据摄入（本地模式）..."
	PYSPARK_PYTHON=python3 python3 -m src.batch.ingest_iceberg \
		--input data/orders.csv \
		--table local.db.orders \
		--local

# 在 Docker 容器内运行 Iceberg 数据摄入
iceberg-ingest: up wait data iceberg-setup
	@echo "运行 Iceberg 数据摄入（集群模式）..."
	$(COMPOSE) exec spark-master /opt/spark/bin/spark-submit \
		--master spark://spark-master:7077 \
		--jars /workspace/jars/iceberg-spark-runtime-3.5_2.12-1.4.3.jar \
		/workspace/src/batch/ingest_iceberg.py \
		--input /workspace/data/orders.csv \
		--table local.db.orders

# 本地运行 Iceberg 高级特性演示
iceberg-features-local:
	@echo "运行 Iceberg 高级特性演示（本地模式）..."
	PYSPARK_PYTHON=python3 python3 -m src.batch.iceberg_features \
		--table local.db.orders \
		--local

# 在 Docker 容器内运行 Iceberg 高级特性演示
iceberg-features: iceberg-ingest
	@echo "运行 Iceberg 高级特性演示（集群模式）..."
	$(COMPOSE) exec spark-master /opt/spark/bin/spark-submit \
		--master spark://spark-master:7077 \
		--jars /workspace/jars/iceberg-spark-runtime-3.5_2.12-1.4.3.jar \
		/workspace/src/batch/iceberg_features.py \
		--table local.db.orders

# 一键 Iceberg 完整演示（本地）
iceberg-demo-local: data
	@echo "========== Iceberg 完整演示（本地模式） =========="
	@echo "1️⃣ 数据摄入..."
	$(MAKE) iceberg-ingest-local
	@echo "\n2️⃣ 高级特性演示..."
	$(MAKE) iceberg-features-local
	@echo "\n✅ Iceberg 演示完成！"

# 一键 Iceberg 完整演示（Docker）
iceberg-demo: up wait data iceberg-setup
	@echo "========== Iceberg 完整演示（集群模式） =========="
	$(MAKE) iceberg-ingest
	$(MAKE) iceberg-features
	@echo "\n✅ Iceberg 演示完成！"

# 运行 Iceberg 测试
iceberg-test:
	@echo "运行 Iceberg 单元测试..."
	PYSPARK_PYTHON=python3 pytest tests/test_iceberg.py -v

# 运行 Iceberg 交互式演示
iceberg-interactive:
	@echo "启动 Iceberg 交互式演示..."
	PYSPARK_PYTHON=python3 python3 scripts/iceberg_interactive.py


