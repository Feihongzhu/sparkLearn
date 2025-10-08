COMPOSE=docker compose -f docker/docker-compose.yml

.PHONY: up down logs submit demo data clean wait plan

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

# 等待 Spark Master UI 就绪
wait:
	@echo "waiting spark master UI ..." ; \
	until curl -sSf http://localhost:8080 >/dev/null ; do \
	  sleep 1 ; \
	done ; \
	echo "spark master is ready"

clean:
	rm -rf data/bronze/orders data/*.crc


