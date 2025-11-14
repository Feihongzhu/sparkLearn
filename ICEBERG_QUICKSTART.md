# Iceberg 快速入门 - 10 分钟上手

本指南帮助你在 10 分钟内快速上手 Apache Iceberg。

## 🚀 第一步：环境准备

### 1. 安装依赖

```bash
# 安装 Python 依赖
pip install -r requirements.txt

# 下载 Iceberg JAR
make iceberg-setup
```

### 2. 验证环境

```bash
# 检查 Python 和 Spark 版本
python3 --version  # 需要 3.8+
python3 -c "import pyspark; print(pyspark.__version__)"  # 需要 3.5.0
```

## 📝 第二步：创建你的第一个 Iceberg 表

### 方式一：使用提供的示例（最快）

```bash
# 生成测试数据
make data

# 运行 Iceberg 摄入任务（本地模式）
make iceberg-ingest-local
```

### 方式二：手动编写代码

创建文件 `my_first_iceberg.py`：

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# 创建带 Iceberg 支持的 Spark Session
spark = (
    SparkSession.builder
    .appName("MyFirstIceberg")
    .master("local[*]")
    .config("spark.sql.extensions", 
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.local.type", "hadoop")
    .config("spark.sql.catalog.local.warehouse", "data/iceberg-warehouse")
    .getOrCreate()
)

# 创建示例数据
data = [
    ("order_001", "user_A", 100.0, "2025-11-01"),
    ("order_002", "user_B", 200.0, "2025-11-02"),
    ("order_003", "user_A", 150.0, "2025-11-03"),
]

df = spark.createDataFrame(data, ["order_id", "user_id", "amount", "order_date"])
df = df.withColumn("order_date", F.to_date("order_date"))

# 创建 Iceberg 表
spark.sql("CREATE DATABASE IF NOT EXISTS local.my_db")

df.writeTo("local.my_db.orders") \
  .using("iceberg") \
  .create()

print("✅ Iceberg 表创建成功！")

# 读取数据
result = spark.table("local.my_db.orders")
result.show()

spark.stop()
```

运行：

```bash
python3 my_first_iceberg.py
```

## 🔍 第三步：探索 Iceberg 特性

### 1. 查询元数据

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("IcebergExplore").getOrCreate()

# 查看快照历史
spark.sql("SELECT * FROM local.my_db.orders.snapshots").show()

# 查看数据文件
spark.sql("SELECT file_path, record_count FROM local.my_db.orders.files").show()
```

### 2. 追加数据

```python
# 创建新数据
new_data = [
    ("order_004", "user_C", 300.0, "2025-11-04"),
]

new_df = spark.createDataFrame(new_data, ["order_id", "user_id", "amount", "order_date"])
new_df = new_df.withColumn("order_date", F.to_date("order_date"))

# 追加到 Iceberg 表
new_df.writeTo("local.my_db.orders").using("iceberg").append()

# 验证
spark.table("local.my_db.orders").show()
```

### 3. Time Travel（时间旅行）

```python
# 获取所有快照
snapshots = spark.sql("SELECT snapshot_id FROM local.my_db.orders.snapshots")
first_snapshot = snapshots.first()[0]

# 查询第一个快照的数据
df_history = spark.read \
    .option("snapshot-id", first_snapshot) \
    .format("iceberg") \
    .load("local.my_db.orders")

print(f"第一个快照的记录数: {df_history.count()}")
df_history.show()
```

### 4. Schema Evolution（模式演变）

```python
# 添加新列
spark.sql("ALTER TABLE local.my_db.orders ADD COLUMN status STRING")

# 更新新列
spark.sql("""
    UPDATE local.my_db.orders
    SET status = 'completed'
    WHERE amount > 100
""")

# 查看结果
spark.sql("SELECT * FROM local.my_db.orders").show()
```

### 5. MERGE INTO（合并更新）

```python
# 创建更新数据
updates = [
    ("order_001", "user_A", 999.0, "2025-11-01", "updated"),
    ("order_999", "user_Z", 500.0, "2025-11-05", "new"),
]

updates_df = spark.createDataFrame(
    updates, 
    ["order_id", "user_id", "amount", "order_date", "status"]
)
updates_df = updates_df.withColumn("order_date", F.to_date("order_date"))

# 创建临时视图
updates_df.createOrReplaceTempView("updates")

# 执行 MERGE
spark.sql("""
    MERGE INTO local.my_db.orders t
    USING updates u
    ON t.order_id = u.order_id
    WHEN MATCHED THEN
        UPDATE SET 
            t.amount = u.amount,
            t.status = u.status
    WHEN NOT MATCHED THEN
        INSERT *
""")

# 查看结果
spark.sql("SELECT * FROM local.my_db.orders ORDER BY order_id").show()
```

## 🎯 第四步：运行完整演示

我们已经为你准备了完整的示例代码：

```bash
# 运行完整演示（包含所有特性）
make iceberg-demo-local

# 或者单独运行各个部分
make iceberg-ingest-local       # 数据摄入
make iceberg-features-local     # 高级特性
```

## 📊 第五步：查看结果

### 查看创建的文件

```bash
# 查看 Iceberg warehouse 目录结构
tree data/iceberg-warehouse/db/orders/

# 典型的目录结构：
# data/iceberg-warehouse/db/orders/
# ├── metadata/
# │   ├── v1.metadata.json      # 表元数据
# │   ├── snap-*.avro            # 快照清单
# │   └── *.avro                 # manifest 文件
# └── data/
#     └── *.parquet              # 实际数据文件
```

### 使用 Spark SQL 交互式查询

```bash
# 启动 Spark SQL CLI（需要先下载 Iceberg JAR）
spark-sql \
  --jars jars/iceberg-spark-runtime-3.5_2.12-1.4.3.jar \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.local.type=hadoop \
  --conf spark.sql.catalog.local.warehouse=data/iceberg-warehouse

# 在 SQL CLI 中执行查询
spark-sql> USE local.db;
spark-sql> SHOW TABLES;
spark-sql> SELECT * FROM orders LIMIT 10;
spark-sql> SELECT * FROM orders.snapshots;
```

## 🧪 第六步：运行测试

```bash
# 运行 Iceberg 单元测试
make iceberg-test

# 或直接使用 pytest
pytest tests/test_iceberg.py -v
```

## 📚 下一步学习

恭喜！你已经完成了 Iceberg 快速入门。接下来可以：

1. **深入学习**：阅读 [ICEBERG_GUIDE.md](ICEBERG_GUIDE.md) 了解更多细节
2. **实践练习**：
   - 构建一个完整的 ETL 管道
   - 实现 CDC（Change Data Capture）场景
   - 优化大规模数据表的性能
3. **探索高级特性**：
   - 分区演变
   - Z-Order 排序
   - 与 Flink/Trino 集成

## ❓ 常见问题

### Q1: 运行时报错 `ClassNotFoundException: org.apache.iceberg...`

**A**: 确保已经下载 Iceberg JAR：

```bash
make iceberg-setup
```

如果使用自己的脚本，需要在 `spark-submit` 时指定：

```bash
spark-submit --jars jars/iceberg-spark-runtime-3.5_2.12-1.4.3.jar your_script.py
```

### Q2: 如何删除 Iceberg 表？

**A**: 使用 SQL DROP TABLE：

```python
spark.sql("DROP TABLE local.my_db.orders")
```

### Q3: Iceberg 数据存储在哪里？

**A**: 默认存储在 `data/iceberg-warehouse/` 目录下。可以在配置中修改：

```python
.config("spark.sql.catalog.local.warehouse", "你的路径")
```

### Q4: 如何清理测试数据？

**A**: 删除 warehouse 目录：

```bash
rm -rf data/iceberg-warehouse
```

### Q5: 是否可以在生产环境使用？

**A**: 可以！Iceberg 已经在 Netflix、Apple、LinkedIn 等公司的生产环境中广泛使用。生产环境建议：
- 使用云端对象存储（S3、GCS、Azure Blob）
- 使用 Hive Metastore 或 AWS Glue 作为 catalog
- 定期运行表维护任务（合并小文件、清理快照）

## 🎓 学习资源

- **官方文档**: https://iceberg.apache.org/
- **本项目指南**: [ICEBERG_GUIDE.md](ICEBERG_GUIDE.md)
- **示例代码**: `src/batch/ingest_iceberg.py` 和 `src/batch/iceberg_features.py`
- **测试用例**: `tests/test_iceberg.py`

---

**开始你的 Iceberg 之旅吧！🚀**

有问题？查看 [ICEBERG_GUIDE.md](ICEBERG_GUIDE.md) 或提 Issue！

