"""
Apache Iceberg 高级特性演示

包含:
1. Time Travel (时间旅行)
2. Schema Evolution (模式演变)
3. MERGE INTO (合并更新)
4. 增量读取
5. 表维护操作 (OPTIMIZE, EXPIRE SNAPSHOTS)
"""
import argparse
from datetime import datetime

from pyspark.sql import functions as F

from src.common.spark_session import build_spark


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Iceberg advanced features demo")
    parser.add_argument("--table", default="local.db.orders", help="Iceberg table name")
    parser.add_argument("--local", action="store_true", help="Use local[*] Spark")
    parser.add_argument(
        "--feature",
        choices=["time_travel", "schema_evolution", "merge", "incremental", "maintenance", "all"],
        default="all",
        help="选择要演示的特性"
    )
    return parser.parse_args()


def demo_time_travel(spark, table_name: str):
    """
    演示 Time Travel 功能
    
    Iceberg 可以查询表的历史快照，用于:
    - 数据回滚
    - 历史数据分析
    - 审计和合规
    """
    print("\n" + "="*80)
    print("🕐 Time Travel 演示")
    print("="*80)
    
    # 查看所有快照
    print("\n1️⃣ 查看表的快照历史:")
    snapshots = spark.sql(f"SELECT snapshot_id, committed_at, operation FROM {table_name}.snapshots")
    snapshots.show(truncate=False)
    
    # 获取第一个快照 ID
    snapshot_ids = snapshots.select("snapshot_id").collect()
    if len(snapshot_ids) > 0:
        first_snapshot_id = snapshot_ids[0][0]
        
        print(f"\n2️⃣ 查询第一个快照的数据 (snapshot_id={first_snapshot_id}):")
        df_history = spark.read \
            .option("snapshot-id", first_snapshot_id) \
            .format("iceberg") \
            .load(table_name)
        
        print(f"历史快照记录数: {df_history.count()}")
        df_history.show(5)
        
        # 使用时间戳查询
        print("\n3️⃣ 使用时间戳查询历史数据:")
        spark.sql(f"""
            SELECT COUNT(*) as record_count, 
                   MIN(ts) as earliest_ts, 
                   MAX(ts) as latest_ts
            FROM {table_name}
            VERSION AS OF {first_snapshot_id}
        """).show()
    else:
        print("⚠️ 暂无快照数据")


def demo_schema_evolution(spark, table_name: str):
    """
    演示 Schema Evolution 功能
    
    Iceberg 支持灵活的模式演变:
    - 添加列
    - 删除列
    - 重命名列
    - 修改列类型（兼容的类型）
    """
    print("\n" + "="*80)
    print("📋 Schema Evolution 演示")
    print("="*80)
    
    print("\n1️⃣ 当前表结构:")
    spark.sql(f"DESCRIBE {table_name}").show(truncate=False)
    
    # 添加新列
    print("\n2️⃣ 添加新列 'discount':")
    spark.sql(f"ALTER TABLE {table_name} ADD COLUMN discount DOUBLE")
    
    print("\n添加后的表结构:")
    spark.sql(f"DESCRIBE {table_name}").show(truncate=False)
    
    # 更新新列的值
    print("\n3️⃣ 为新列设置默认值:")
    spark.sql(f"""
        UPDATE {table_name}
        SET discount = CASE 
            WHEN amount > 1000 THEN 0.1
            WHEN amount > 500 THEN 0.05
            ELSE 0.0
        END
        WHERE discount IS NULL
    """)
    
    print("\n查看更新后的数据:")
    spark.sql(f"SELECT order_id, amount, discount FROM {table_name} LIMIT 5").show()


def demo_merge_into(spark, table_name: str):
    """
    演示 MERGE INTO 功能
    
    Iceberg 的 MERGE INTO 支持:
    - UPSERT 操作（插入或更新）
    - 基于条件的更新和删除
    - 复杂的 CDC (Change Data Capture) 场景
    """
    print("\n" + "="*80)
    print("🔄 MERGE INTO 演示")
    print("="*80)
    
    # 创建一些更新数据
    print("\n1️⃣ 创建更新数据:")
    updates_data = [
        ("order_001", "user_001", "item_A", "Electronics", 999.99, 2, "2025-10-08 10:00:00"),
        ("order_999", "user_999", "item_Z", "Books", 29.99, 1, "2025-10-08 11:00:00"),
    ]
    
    from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
    
    updates_schema = StructType([
        StructField("order_id", StringType(), False),
        StructField("user_id", StringType(), False),
        StructField("item_id", StringType(), False),
        StructField("category", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("ts", TimestampType(), True),
    ])
    
    updates_df = spark.createDataFrame(updates_data, updates_schema) \
        .withColumn("ts", F.to_timestamp("ts")) \
        .withColumn("amount", F.col("price") * F.col("quantity")) \
        .withColumn("ingest_date", F.to_date(F.col("ts"))) \
        .withColumn("year", F.year(F.col("ts"))) \
        .withColumn("month", F.month(F.col("ts")))
    
    updates_df.show()
    
    # 创建临时视图
    updates_df.createOrReplaceTempView("updates")
    
    print("\n2️⃣ 执行 MERGE INTO:")
    print("规则: 如果 order_id 存在则更新，否则插入")
    
    spark.sql(f"""
        MERGE INTO {table_name} t
        USING updates u
        ON t.order_id = u.order_id
        WHEN MATCHED THEN
            UPDATE SET 
                t.price = u.price,
                t.quantity = u.quantity,
                t.amount = u.amount,
                t.ts = u.ts
        WHEN NOT MATCHED THEN
            INSERT *
    """)
    
    print("\n✅ MERGE 完成！查看结果:")
    spark.sql(f"SELECT * FROM {table_name} WHERE order_id IN ('order_001', 'order_999')").show()


def demo_incremental_read(spark, table_name: str):
    """
    演示增量读取功能
    
    Iceberg 支持高效的增量读取:
    - 只读取自上次读取以来的新数据
    - 用于增量 ETL 管道
    """
    print("\n" + "="*80)
    print("📖 增量读取演示")
    print("="*80)
    
    # 获取快照信息
    snapshots = spark.sql(f"SELECT snapshot_id, committed_at FROM {table_name}.snapshots ORDER BY committed_at")
    snapshot_ids = snapshots.select("snapshot_id").collect()
    
    if len(snapshot_ids) >= 2:
        start_snapshot = snapshot_ids[0][0]
        end_snapshot = snapshot_ids[-1][0]
        
        print(f"\n1️⃣ 读取快照 {start_snapshot} 到 {end_snapshot} 之间的增量数据:")
        
        incremental_df = spark.read \
            .format("iceberg") \
            .option("start-snapshot-id", start_snapshot) \
            .option("end-snapshot-id", end_snapshot) \
            .load(table_name)
        
        print(f"增量数据记录数: {incremental_df.count()}")
        incremental_df.show(10)
    else:
        print("⚠️ 快照数量不足，需要至少 2 个快照才能演示增量读取")


def demo_maintenance(spark, table_name: str):
    """
    演示表维护操作
    
    Iceberg 的维护操作:
    - OPTIMIZE: 合并小文件，提高查询性能
    - EXPIRE SNAPSHOTS: 删除过期快照，释放存储空间
    - REWRITE DATA FILES: 重写数据文件以优化布局
    """
    print("\n" + "="*80)
    print("🔧 表维护操作演示")
    print("="*80)
    
    # 查看当前文件状态
    print("\n1️⃣ 当前数据文件状态:")
    files_df = spark.sql(f"""
        SELECT 
            COUNT(*) as file_count,
            SUM(record_count) as total_records,
            AVG(file_size_in_bytes) as avg_file_size,
            SUM(file_size_in_bytes) as total_size
        FROM {table_name}.files
    """)
    files_df.show()
    
    # 执行 OPTIMIZE（合并小文件）
    print("\n2️⃣ 执行 OPTIMIZE 合并小文件:")
    spark.sql(f"CALL local.system.rewrite_data_files(table => '{table_name}')")
    
    print("\n优化后的文件状态:")
    files_df = spark.sql(f"""
        SELECT 
            COUNT(*) as file_count,
            SUM(record_count) as total_records,
            AVG(file_size_in_bytes) as avg_file_size,
            SUM(file_size_in_bytes) as total_size
        FROM {table_name}.files
    """)
    files_df.show()
    
    # 查看快照数量
    print("\n3️⃣ 当前快照数量:")
    snapshot_count = spark.sql(f"SELECT COUNT(*) as count FROM {table_name}.snapshots")
    snapshot_count.show()
    
    print("\n💡 提示: 可以使用 EXPIRE SNAPSHOTS 删除过期快照:")
    print(f"   CALL local.system.expire_snapshots(table => '{table_name}', older_than => TIMESTAMP '2025-10-01 00:00:00')")


def main():
    args = parse_args()
    spark = build_spark("IcebergFeatures", local=bool(args.local), enable_iceberg=True)
    
    table_name = args.table
    feature = args.feature
    
    # 检查表是否存在
    tables = spark.sql("SHOW TABLES").collect()
    table_exists = any(table_name.split(".")[-1] in str(row) for row in tables)
    
    if not table_exists:
        print(f"⚠️ 表 {table_name} 不存在！请先运行 ingest_iceberg.py 创建表。")
        spark.stop()
        return
    
    # 根据选择演示不同特性
    if feature == "all" or feature == "time_travel":
        demo_time_travel(spark, table_name)
    
    if feature == "all" or feature == "schema_evolution":
        demo_schema_evolution(spark, table_name)
    
    if feature == "all" or feature == "merge":
        demo_merge_into(spark, table_name)
    
    if feature == "all" or feature == "incremental":
        demo_incremental_read(spark, table_name)
    
    if feature == "all" or feature == "maintenance":
        demo_maintenance(spark, table_name)
    
    print("\n" + "="*80)
    print("✅ Iceberg 特性演示完成！")
    print("="*80)
    
    spark.stop()


if __name__ == "__main__":
    main()

