# 🧊 Iceberg 速查表 (Cheat Sheet)

快速参考 Apache Iceberg 的常用命令和代码片段。

---

## 📦 环境设置

### Spark Session 配置

```python
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("IcebergApp")
    .master("local[*]")
    # Iceberg 扩展
    .config("spark.sql.extensions", 
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    # Catalog 配置
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.local.type", "hadoop")
    .config("spark.sql.catalog.local.warehouse", "data/iceberg-warehouse")
    .getOrCreate()
)
```

---

## 🗂️ 基本 DDL 操作

### 创建数据库

```sql
CREATE DATABASE IF NOT EXISTS local.my_db;
```

### 创建表

```sql
-- SQL 方式
CREATE TABLE local.my_db.orders (
    order_id STRING,
    user_id STRING,
    amount DOUBLE,
    order_date DATE
)
USING iceberg
PARTITIONED BY (order_date);
```

```python
# DataFrame 方式
df.writeTo("local.my_db.orders") \
  .using("iceberg") \
  .partitionedBy("order_date") \
  .create()
```

### 查看表

```sql
SHOW TABLES IN local.my_db;
DESCRIBE local.my_db.orders;
DESCRIBE EXTENDED local.my_db.orders;
```

### 删除表

```sql
DROP TABLE local.my_db.orders;
```

---

## 📝 数据操作 (DML)

### 插入数据

```sql
INSERT INTO local.my_db.orders 
VALUES ('order_001', 'user_A', 100.0, '2025-11-01');
```

```python
# DataFrame 方式
df.writeTo("local.my_db.orders").using("iceberg").append()
```

### 查询数据

```sql
SELECT * FROM local.my_db.orders WHERE order_date = '2025-11-01';
```

```python
df = spark.table("local.my_db.orders")
df.show()
```

### 更新数据

```sql
UPDATE local.my_db.orders
SET amount = 200.0
WHERE order_id = 'order_001';
```

### 删除数据

```sql
DELETE FROM local.my_db.orders
WHERE order_date < '2025-01-01';
```

---

## 🔄 MERGE INTO (UPSERT)

```sql
MERGE INTO local.my_db.orders t
USING updates u
ON t.order_id = u.order_id
WHEN MATCHED THEN
    UPDATE SET t.amount = u.amount
WHEN NOT MATCHED THEN
    INSERT *
```

```python
# Python 示例
updates_df.createOrReplaceTempView("updates")

spark.sql("""
    MERGE INTO local.my_db.orders t
    USING updates u
    ON t.order_id = u.order_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")
```

---

## 🕐 Time Travel

### 使用快照 ID

```sql
SELECT * FROM local.my_db.orders
VERSION AS OF 1234567890;
```

```python
df = spark.read \
    .option("snapshot-id", 1234567890) \
    .format("iceberg") \
    .load("local.my_db.orders")
```

### 使用时间戳

```sql
SELECT * FROM local.my_db.orders
TIMESTAMP AS OF '2025-11-01 10:00:00';
```

```python
df = spark.read \
    .option("as-of-timestamp", "1696636800000") \
    .format("iceberg") \
    .load("local.my_db.orders")
```

---

## 📋 Schema Evolution

### 添加列

```sql
ALTER TABLE local.my_db.orders 
ADD COLUMN status STRING;
```

### 删除列

```sql
ALTER TABLE local.my_db.orders 
DROP COLUMN status;
```

### 重命名列

```sql
ALTER TABLE local.my_db.orders 
RENAME COLUMN amount TO total_amount;
```

### 修改列类型

```sql
ALTER TABLE local.my_db.orders 
ALTER COLUMN amount TYPE DECIMAL(10, 2);
```

---

## 📖 增量读取

### 读取两个快照之间的数据

```python
df = spark.read \
    .format("iceberg") \
    .option("start-snapshot-id", start_id) \
    .option("end-snapshot-id", end_id) \
    .load("local.my_db.orders")
```

### 从某个快照开始读取

```python
df = spark.read \
    .format("iceberg") \
    .option("start-snapshot-id", start_id) \
    .load("local.my_db.orders")
```

---

## 🔍 元数据查询

### 查看快照历史

```sql
SELECT snapshot_id, committed_at, operation 
FROM local.my_db.orders.snapshots
ORDER BY committed_at DESC;
```

### 查看数据文件

```sql
SELECT file_path, record_count, file_size_in_bytes
FROM local.my_db.orders.files;
```

### 查看历史记录

```sql
SELECT * FROM local.my_db.orders.history;
```

### 查看分区信息

```sql
SELECT * FROM local.my_db.orders.partitions;
```

### 查看清单（Manifest）

```sql
SELECT * FROM local.my_db.orders.manifests;
```

---

## 🔧 表维护操作

### 合并小文件

```sql
CALL local.system.rewrite_data_files(
    table => 'my_db.orders'
);

-- 指定目标文件大小
CALL local.system.rewrite_data_files(
    table => 'my_db.orders',
    options => map('target-file-size-bytes', '536870912')  -- 512MB
);
```

### 清理过期快照

```sql
CALL local.system.expire_snapshots(
    table => 'my_db.orders',
    older_than => TIMESTAMP '2025-10-01 00:00:00',
    retain_last => 5
);
```

### 删除孤儿文件

```sql
CALL local.system.remove_orphan_files(
    table => 'my_db.orders',
    older_than => TIMESTAMP '2025-09-01 00:00:00'
);
```

### 重写清单文件

```sql
CALL local.system.rewrite_manifests('my_db.orders');
```

---

## 🎯 分区操作

### 创建分区表

```sql
CREATE TABLE local.my_db.orders (
    order_id STRING,
    ts TIMESTAMP,
    amount DOUBLE
)
USING iceberg
PARTITIONED BY (days(ts));  -- 按天分区
```

### 分区变换函数

```sql
-- 按年分区
PARTITIONED BY (years(ts))

-- 按月分区
PARTITIONED BY (months(ts))

-- 按天分区
PARTITIONED BY (days(ts))

-- 按小时分区
PARTITIONED BY (hours(ts))

-- 桶分区
PARTITIONED BY (bucket(10, user_id))

-- 截断分区
PARTITIONED BY (truncate(10, user_id))
```

### 修改分区策略

```sql
ALTER TABLE local.my_db.orders
REPLACE PARTITION FIELD days(ts) WITH months(ts);
```

---

## 📊 性能优化

### 设置表属性

```sql
ALTER TABLE local.my_db.orders 
SET TBLPROPERTIES (
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'snappy',
    'write.target-file-size-bytes' = '536870912'  -- 512MB
);
```

### Z-Order 排序

```sql
CALL local.system.rewrite_data_files(
    table => 'my_db.orders',
    strategy => 'sort',
    sort_order => 'user_id,order_date'
);
```

---

## 🐍 Python 常用代码片段

### 读取 Iceberg 表

```python
# 方式 1: 使用 table
df = spark.table("local.my_db.orders")

# 方式 2: 使用 read
df = spark.read.format("iceberg").load("local.my_db.orders")
```

### 写入 Iceberg 表

```python
# Append 模式
df.writeTo("local.my_db.orders").using("iceberg").append()

# Overwrite 模式
df.writeTo("local.my_db.orders").using("iceberg").overwrite()

# 传统 API
df.write.format("iceberg").mode("append").save("local.my_db.orders")
```

### 获取快照信息

```python
snapshots = spark.sql("SELECT * FROM local.my_db.orders.snapshots")
latest_snapshot = snapshots.orderBy("committed_at", ascending=False).first()
snapshot_id = latest_snapshot["snapshot_id"]
```

### 批量插入数据

```python
from pyspark.sql import functions as F

# 创建批量数据
data = [
    ("order_001", "user_A", 100.0),
    ("order_002", "user_B", 200.0),
]

df = spark.createDataFrame(data, ["order_id", "user_id", "amount"])
df = df.withColumn("order_date", F.current_date())

# 追加到 Iceberg 表
df.writeTo("local.my_db.orders").using("iceberg").append()
```

---

## 🛠️ Makefile 命令速查

```bash
# 下载 Iceberg JAR
make iceberg-setup

# 本地数据摄入
make iceberg-ingest-local

# 本地高级特性演示
make iceberg-features-local

# 本地完整演示
make iceberg-demo-local

# 交互式演示（推荐！）
make iceberg-interactive

# 运行单元测试
make iceberg-test

# 清理数据
make clean
```

---

## 📚 表属性配置

### 常用表属性

```sql
-- 写入格式
'write.format.default' = 'parquet'  -- parquet 或 orc

-- 压缩编码
'write.parquet.compression-codec' = 'snappy'  -- snappy, gzip, zstd

-- 目标文件大小
'write.target-file-size-bytes' = '536870912'  -- 512MB

-- 快照保留
'history.expire.max-snapshot-age-ms' = '604800000'  -- 7天

-- 元数据刷新间隔
'commit.retry.num-retries' = '4'
```

---

## 🔐 最佳实践

### ✅ DO (推荐做法)

```python
# ✅ 使用隐藏分区
CREATE TABLE orders (...) PARTITIONED BY (days(ts))

# ✅ 定期合并小文件
CALL system.rewrite_data_files('orders')

# ✅ 定期清理快照
CALL system.expire_snapshots('orders', retain_last => 5)

# ✅ 使用 writeTo API
df.writeTo("orders").using("iceberg").append()
```

### ❌ DON'T (避免做法)

```python
# ❌ 不要创建过多小文件
for row in data:
    single_df.writeTo("orders").append()  # 每次写一行

# ❌ 不要保留过多快照
# 定期清理，避免元数据膨胀

# ❌ 不要手动管理分区路径
df.write.partitionBy("date").parquet("path/date=2025-11-01")  # 不推荐
```

---

## 🚨 常见错误和解决方案

### 错误 1: ClassNotFoundException

```
java.lang.ClassNotFoundException: org.apache.iceberg.spark.SparkCatalog
```

**解决**: 下载并添加 Iceberg JAR

```bash
make iceberg-setup
```

### 错误 2: 表不存在

```
org.apache.iceberg.exceptions.NoSuchTableException: Table does not exist
```

**解决**: 先创建表或使用正确的表名

```sql
CREATE TABLE IF NOT EXISTS local.my_db.orders (...) USING iceberg;
```

### 错误 3: 快照不存在

```
Cannot find snapshot with id
```

**解决**: 查看可用快照

```sql
SELECT snapshot_id FROM orders.snapshots;
```

---

## 📞 更多资源

- 完整指南: `ICEBERG_GUIDE.md`
- 快速入门: `ICEBERG_QUICKSTART.md`
- 总结文档: `ICEBERG_SUMMARY.md`
- 官方文档: https://iceberg.apache.org/

---

**保存此文件以便快速查询！🚀**

