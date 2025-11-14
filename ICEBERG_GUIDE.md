# Apache Iceberg 学习指南

## 📚 什么是 Apache Iceberg？

Apache Iceberg 是一个开源的表格式（table format），专为大规模分析型数据湖设计。它是一个**数据表的元数据层**，位于存储格式（如 Parquet、ORC）之上。

### 核心概念

```
应用层 (Spark, Flink, Trino)
       ↓
表格式层 (Iceberg)  ← 元数据管理、事务、版本控制
       ↓
存储格式 (Parquet, ORC)
       ↓
存储系统 (HDFS, S3, 本地文件系统)
```

## 🎯 为什么使用 Iceberg？

### 1. **ACID 事务**
- 保证数据一致性，支持并发读写
- 避免读取到不完整的数据

### 2. **Schema Evolution（模式演变）**
- 添加、删除、重命名列
- 向后兼容，无需重写所有数据

### 3. **Time Travel（时间旅行）**
- 查询任意历史版本的数据
- 用于数据审计、回滚、对比分析

### 4. **隐藏分区（Hidden Partitioning）**
- 用户查询时不需要知道分区细节
- 自动分区裁剪，提升查询性能

### 5. **增量读取**
- 只读取变更的数据，提升 ETL 效率
- 支持流式处理场景

### 6. **性能优化**
- 数据跳过（Data Skipping）
- 小文件合并（Compaction）
- Z-Order 排序优化

## 🏗️ Iceberg 架构

### 元数据层次结构

```
Catalog (目录)
  ↓
Table (表)
  ↓
Metadata File (元数据文件) - 指向快照
  ↓
Snapshot (快照) - 表在某个时间点的状态
  ↓
Manifest List (清单列表) - 指向多个 manifest
  ↓
Manifest File (清单文件) - 包含数据文件列表和统计信息
  ↓
Data Files (数据文件) - 实际的 Parquet/ORC 文件
```

### 快照（Snapshot）机制

每次写操作都会创建一个新快照：
- **快照 ID**: 唯一标识一个版本
- **快照时间**: 创建时间
- **操作类型**: append, overwrite, delete 等
- **清单列表**: 指向数据文件的元数据

## 🛠️ 环境配置

### 1. 依赖安装

```bash
# 使用 pip 安装（Python）
pip install pyspark==3.5.0

# 下载 Iceberg Spark Runtime JAR（根据 Spark 版本选择）
# Spark 3.5: iceberg-spark-runtime-3.5_2.12:1.4.3
```

### 2. Spark 配置

在 `spark_session.py` 中配置：

```python
builder = (
    SparkSession.builder
    # Iceberg 扩展
    .config("spark.sql.extensions", 
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    # Catalog 配置
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.local.type", "hadoop")
    .config("spark.sql.catalog.local.warehouse", "data/iceberg-warehouse")
)
```

### 3. Catalog 类型选择

| Catalog 类型 | 适用场景 | 特点 |
|-------------|---------|------|
| Hadoop      | 本地/HDFS | 使用文件系统存储元数据 |
| Hive        | 已有 Hive 环境 | 兼容 Hive Metastore |
| AWS Glue    | AWS 云环境 | 托管式元数据服务 |
| REST        | 多引擎共享 | 通过 REST API 访问 |

## 📖 基本使用

### 1. 创建 Iceberg 表

**方式一：通过 SQL**

```python
spark.sql("""
    CREATE TABLE local.db.orders (
        order_id STRING,
        user_id STRING,
        amount DOUBLE,
        ts TIMESTAMP
    )
    USING iceberg
    PARTITIONED BY (days(ts))
""")
```

**方式二：通过 DataFrame**

```python
df.writeTo("local.db.orders") \
  .using("iceberg") \
  .partitionedBy("order_date") \
  .create()
```

### 2. 写入数据

```python
# Append 模式
df.writeTo("local.db.orders").using("iceberg").append()

# Overwrite 模式
df.writeTo("local.db.orders").using("iceberg").overwrite()

# 也可以使用传统 write API
df.write.format("iceberg").mode("append").save("local.db.orders")
```

### 3. 读取数据

```python
# 读取当前版本
df = spark.table("local.db.orders")

# 或使用 read API
df = spark.read.format("iceberg").load("local.db.orders")
```

### 4. Time Travel

```python
# 使用快照 ID
df = spark.read \
    .option("snapshot-id", 1234567890) \
    .format("iceberg") \
    .load("local.db.orders")

# 使用时间戳
df = spark.read \
    .option("as-of-timestamp", "1696636800000") \
    .format("iceberg") \
    .load("local.db.orders")

# 使用 SQL
spark.sql("""
    SELECT * FROM local.db.orders
    VERSION AS OF 1234567890
""")
```

### 5. Schema Evolution

```python
# 添加列
spark.sql("ALTER TABLE local.db.orders ADD COLUMN status STRING")

# 删除列
spark.sql("ALTER TABLE local.db.orders DROP COLUMN status")

# 重命名列
spark.sql("ALTER TABLE local.db.orders RENAME COLUMN amount TO total_amount")
```

### 6. 更新和删除

```python
# UPDATE
spark.sql("""
    UPDATE local.db.orders
    SET status = 'completed'
    WHERE order_id = '12345'
""")

# DELETE
spark.sql("""
    DELETE FROM local.db.orders
    WHERE ts < '2025-01-01'
""")
```

### 7. MERGE INTO (UPSERT)

```python
spark.sql("""
    MERGE INTO local.db.orders t
    USING updates u
    ON t.order_id = u.order_id
    WHEN MATCHED THEN
        UPDATE SET t.amount = u.amount, t.ts = u.ts
    WHEN NOT MATCHED THEN
        INSERT *
""")
```

### 8. 增量读取

```python
# 读取两个快照之间的增量数据
df = spark.read \
    .format("iceberg") \
    .option("start-snapshot-id", start_id) \
    .option("end-snapshot-id", end_id) \
    .load("local.db.orders")
```

## 🔍 元数据查询

Iceberg 提供了丰富的元数据表：

### 1. 快照历史

```sql
SELECT * FROM local.db.orders.snapshots
```

字段：
- `committed_at`: 快照创建时间
- `snapshot_id`: 快照 ID
- `parent_id`: 父快照 ID
- `operation`: 操作类型（append, overwrite, delete）

### 2. 数据文件

```sql
SELECT 
    file_path, 
    record_count, 
    file_size_in_bytes 
FROM local.db.orders.files
```

### 3. 历史记录

```sql
SELECT * FROM local.db.orders.history
```

### 4. 分区信息

```sql
SELECT * FROM local.db.orders.partitions
```

## 🚀 高级特性

### 1. 表维护操作

**合并小文件（Compaction）**

```python
spark.sql("""
    CALL local.system.rewrite_data_files(
        table => 'db.orders',
        options => map('target-file-size-bytes', '536870912')
    )
""")
```

**清理过期快照**

```python
spark.sql("""
    CALL local.system.expire_snapshots(
        table => 'db.orders',
        older_than => TIMESTAMP '2025-10-01 00:00:00',
        retain_last => 5
    )
""")
```

**删除孤儿文件**

```python
spark.sql("""
    CALL local.system.remove_orphan_files(
        table => 'db.orders',
        older_than => TIMESTAMP '2025-09-01 00:00:00'
    )
""")
```

### 2. 分区演变

Iceberg 支持修改分区策略而无需重写数据：

```sql
ALTER TABLE local.db.orders 
REPLACE PARTITION FIELD days(ts) 
WITH months(ts)
```

### 3. 排序优化

```python
spark.sql("""
    CALL local.system.rewrite_data_files(
        table => 'db.orders',
        strategy => 'sort',
        sort_order => 'user_id,ts'
    )
""")
```

## 📊 性能优化建议

### 1. 选择合适的分区策略
- **时间分区**: 适合时序数据，使用 `days(ts)` 或 `months(ts)`
- **范围分区**: 适合有明确范围的数据，如 `bucket(10, user_id)`

### 2. 定期合并小文件
- 小文件过多会影响查询性能
- 建议定期运行 `rewrite_data_files`

### 3. 清理过期快照
- 过多快照会占用存储空间
- 保留最近的 N 个快照即可

### 4. 使用数据跳过
- Iceberg 自动收集列级别的统计信息（min, max, null count）
- 查询时自动跳过不相关的文件

## 🆚 Iceberg vs Delta Lake vs Hudi

| 特性 | Iceberg | Delta Lake | Hudi |
|-----|---------|------------|------|
| **开源** | ✅ Apache | ✅ Linux Foundation | ✅ Apache |
| **ACID** | ✅ | ✅ | ✅ |
| **Time Travel** | ✅ | ✅ | ✅ |
| **Schema Evolution** | ✅ 更灵活 | ✅ | ✅ |
| **隐藏分区** | ✅ | ❌ | ❌ |
| **分区演变** | ✅ | ❌ | ❌ |
| **引擎支持** | Spark, Flink, Trino, Hive | 主要 Spark | Spark, Flink, Hive |
| **AWS 集成** | ✅ Glue | ✅ | ✅ |
| **成熟度** | 高 | 高 | 中 |
| **社区** | Netflix, Apple | Databricks | Uber |

### 选择建议

- **Iceberg**: 需要多引擎支持、复杂分区需求
- **Delta Lake**: 深度使用 Databricks 平台
- **Hudi**: CDC 场景、更新频繁的数据

## 🧪 实践练习

### 练习 1: 基础 CRUD 操作
1. 创建 Iceberg 表
2. 插入数据
3. 查询数据
4. 更新数据
5. 删除数据

### 练习 2: Time Travel
1. 多次写入数据
2. 查看快照历史
3. 回滚到历史版本
4. 对比不同版本的数据

### 练习 3: Schema Evolution
1. 添加新列
2. 修改列类型
3. 重命名列
4. 验证历史数据仍可访问

### 练习 4: MERGE INTO
1. 实现 CDC 场景
2. 处理更新和插入混合操作
3. 验证数据一致性

### 练习 5: 增量 ETL
1. 使用增量读取构建 ETL 管道
2. 只处理新增数据
3. 追踪已处理的快照 ID

## 📚 参考资源

- [Iceberg 官方文档](https://iceberg.apache.org/)
- [Iceberg GitHub](https://github.com/apache/iceberg)
- [Iceberg Spark 集成](https://iceberg.apache.org/docs/latest/spark-ddl/)
- [Iceberg 架构设计](https://iceberg.apache.org/docs/latest/spec/)

## 🎓 进阶学习路径

1. **Week 1-2**: 基础概念和 CRUD 操作
2. **Week 3**: Time Travel 和 Schema Evolution
3. **Week 4**: MERGE INTO 和增量读取
4. **Week 5**: 表维护和性能优化
5. **Week 6**: 与其他组件集成（Flink, Trino）
6. **Week 7**: 生产环境部署（AWS Glue, S3）
7. **Week 8**: 故障恢复和监控

## ⚠️ 常见问题

### 1. Iceberg JAR 版本不匹配
**问题**: `ClassNotFoundException` 或 `NoSuchMethodError`

**解决**: 确保 Iceberg JAR 版本与 Spark 版本匹配
- Spark 3.3 → iceberg-spark-runtime-3.3_2.12
- Spark 3.4 → iceberg-spark-runtime-3.4_2.12
- Spark 3.5 → iceberg-spark-runtime-3.5_2.12

### 2. 写入权限问题
**问题**: `PermissionDeniedException`

**解决**: 检查 warehouse 目录权限

### 3. 快照过多
**问题**: 存储空间占用大

**解决**: 定期执行 `expire_snapshots`

### 4. 小文件过多
**问题**: 查询慢

**解决**: 定期执行 `rewrite_data_files`

---

**Happy Learning! 🚀**

