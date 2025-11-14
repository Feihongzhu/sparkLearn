## SparkLearn 学习与项目指南（本地 Docker 优先）

本仓库用于系统化学习 Spark，并产出一个可演示、可复现的数据项目（批/流一体 + 湖仓/Delta）。可以把本仓库直接推送到 GitHub 持续追踪进度与成果。

### 目标与成果
- **面试可用**：掌握 Spark SQL/Structured Streaming/性能调优/Delta Lake/Iceberg 核心能力。
- **可展示项目**：端到端数据管道（Kafka → Spark → Delta/Iceberg → 指标查询/可视化），含复现实验数据与性能数字。
- **工程化**：Docker Compose 一键拉起依赖，含数据质量与编排骨架。

### 仓库结构（规划）
```
sparkLearn/
  ├─ src/
  │   ├─ common/           # Spark 会话与 I/O 封装、幂等工具
  │   ├─ batch/            # 批处理作业（Silver/Gold）
  │   └─ streaming/        # 流作业（Kafka → Delta/指标）
  ├─ dq/                   # 数据质量规则（Great Expectations 等）
  ├─ dags/                 # Airflow DAG（可选）
  ├─ conf/                 # spark-defaults.conf、log4j2.xml
  ├─ docker/               # docker-compose.yml 与相关配置
  ├─ tests/                # pytest：DataFrame/契约测试
  ├─ notebooks/            # 探索/调优记录
  ├─ data/                 # 本地样本/生成数据（默认忽略）
  ├─ checkpoints/          # 流作业检查点（忽略）
  └─ README.md             # 本指南
```

### 学习路线（建议 8–10 周）
- 第 1 周：Spark 基础（DataFrame vs RDD、惰性/DAG、行动/转换），本地 `spark-submit` 跑通。
- 第 2 周：Spark SQL 与窗口函数、`pandas_udf`，完成复杂聚合/去重报表。
- 第 3 周：调优（分区、AQE、广播 join、缓存策略），基于 Spark UI 记录优化前后数据。
- 第 4 周：Structured Streaming（watermark、状态聚合、exactly-once），实时 GMV 指标。
- 第 5 周：Delta Lake（ACID、Schema Evolution、Time Travel、OPTIMIZE/VACUUM）。
- 第 6 周：Apache Iceberg（表格式、隐藏分区、分区演变、MERGE INTO、增量读取）。
- 第 7 周：编排与数据质量（Airflow/Prefect + Great Expectations）。
- 第 8 周（可选）：Spark ML Pipeline（离线训练 + 批预测）。
- 第 9 周（可选）：K8s/YARN/云上（EMR/Dataproc）与成本优化。
- 第 10 周：项目打磨与面试强化（指标固化、故障演练、问答脚本）。

### 本地 Docker 工作流（骨架）
后续将提供 `docker/docker-compose.yml`，统一启动以下服务：
- Spark master/worker
- Kafka + Zookeeper（或 KRaft）
- MinIO（S3 兼容对象存储）
- Airflow + Scheduler/Webserver（可选）
- Superset（可选）

占位命令（待加入 Makefile）：
```
# 启动/停止环境（示例，占位）
docker compose -f docker/docker-compose.yml up -d
docker compose -f docker/docker-compose.yml down -v
```

### 里程碑（Milestones）
- M1 基础跑通：本地读取 CSV → 写入 Parquet/Delta，产出 1 份聚合报表。
- M2 调优报告：针对一次作业完成分区/并行度/AQE/广播 join 等优化对比。
- M3 实时指标：Structured Streaming 从 Kafka 写入 Delta，近 15 分钟 GMV/TopN。
- M4 湖仓能力（Delta）：Time Travel 回放修复，展示 Schema Evolution。
- M5 湖仓能力（Iceberg）：隐藏分区、MERGE INTO、增量 ETL、表维护。
- M6 工程化：编排 + 数据质量校验 + 最小可演示看板/API。

### 进度追踪清单（勾选即可）
- 环境
  - [ ] 安装 JDK 11 / Python 3.10+
  - [ ] 安装 Docker Desktop
  - [ ] 初始化 `docker/docker-compose.yml` 并成功启动
  - [ ] Spark 本地 `spark-submit` 跑通
- 基础与 SQL
  - [ ] DataFrame 基础与常见转换
  - [ ] 窗口函数 TopN/去重
  - [ ] UDF/`pandas_udf` 使用与坑位
- 调优
  - [ ] 分区与并行度设置
  - [ ] 广播 join / 倾斜治理
  - [ ] AQE 与缓存策略
  - [ ] Spark UI 报告（前后对比）
- 流处理
  - [ ] Kafka → Structured Streaming → Delta 写入
  - [ ] watermark/状态聚合/晚到处理
  - [ ] checkpoint 幂等与重放验证
- 湖仓（Delta）
  - [ ] ACID 与 MERGE
  - [ ] Schema Evolution / Time Travel
  - [ ] OPTIMIZE / VACUUM / Z-Order（可选）
- 湖仓（Iceberg）
  - [ ] 基础概念与架构理解
  - [ ] 创建 Iceberg 表与数据写入
  - [ ] Time Travel 查询历史数据
  - [ ] Schema Evolution（添加/删除/重命名列）
  - [ ] MERGE INTO 实现 UPSERT
  - [ ] 增量读取构建 ETL 管道
  - [ ] 表维护（合并小文件、清理快照）
  - [ ] 隐藏分区与分区演变
  - [ ] Iceberg vs Delta vs Hudi 对比分析
- 编排与数据质量
  - [ ] Airflow DAG 编排
  - [ ] Great Expectations 规则与失败报警
- 可视化/服务
  - [ ] Superset 指标看板 或 FastAPI 查询接口
- 云与成本（可选）
  - [ ] 至少一种云环境部署记录与成本数字
- 面试准备
  - [ ] 项目白皮书（架构图/流程/权衡）
  - [ ] 高频问答脚本

### 性能记录模板
| 作业 | 场景 | 优化项 | 任务数 | Shuffle(MB) | 文件数/大小 | Stage 总时长 | p95 延迟 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 示例 | 优化前 | baseline |  |  |  |  |  |
| 示例 | 优化后 | AQE+广播 |  |  |  |  |  |

### 实验数据与复现
- 数据生成：`data/` 下放置样本/合成数据（默认忽略提交）。
- 复现脚本：后续将添加 `make` 目标或 `scripts/` 脚本，一键下载/生成数据并执行作业。

### 每日/每周学习日志（模板）
```
YYYY-MM-DD
- 学习主题：
- 实作内容：
- 产出（PR/Notebook/指标）：
- 问题与解决：
- 下一步计划：
```

### 分支与提交约定（建议）
- 分支：`main`（稳定）与 `feature/*`（新功能/实验）。
- 提交信息：`type(scope): summary`，例如 `feat(streaming): add 15min GMV window agg`。

### 常见问答（占位）
- DataFrame 比 RDD 的优势？Catalyst/Tungsten 做了什么？
- 如何处理数据倾斜？（盐值、预聚合、广播/倾斜分区隔离）
- Structured Streaming 如何做到 exactly-once 到 Delta？
- Delta 与 Hudi/Iceberg 的取舍？
- Iceberg 的隐藏分区有什么优势？
- 如何使用 Iceberg 实现增量 ETL？

## 🧊 Iceberg 快速开始

### 安装依赖

```bash
# 安装 Python 依赖
pip install -r requirements.txt

# 下载 Iceberg JAR（自动）
make iceberg-setup
```

### 运行 Iceberg 示例

```bash
# 方式 1：本地模式（推荐快速学习）
make iceberg-demo-local

# 方式 2：Docker 集群模式
make iceberg-demo

# 单独运行各个步骤
make iceberg-ingest-local      # 数据摄入
make iceberg-features-local    # 高级特性演示

# 运行单元测试
make iceberg-test
```

### Iceberg 学习资源

详细的 Iceberg 学习指南请参考：[ICEBERG_GUIDE.md](ICEBERG_GUIDE.md)

包含内容：
- 📚 核心概念和架构
- 🛠️ 环境配置和依赖安装
- 📖 基本使用（CRUD、Time Travel、Schema Evolution）
- 🚀 高级特性（MERGE INTO、增量读取、表维护）
- 🆚 与 Delta Lake、Hudi 的对比
- 🧪 实践练习和常见问题

—

后续我会补充：`docker/docker-compose.yml`、`Makefile`、样例 `src/` 作业与 `tests/` 骨架。欢迎直接在 Issues/PR 中以本 README 的清单驱动推进。


