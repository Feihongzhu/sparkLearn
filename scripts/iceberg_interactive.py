#!/usr/bin/env python3
"""
Iceberg 交互式演示脚本

在 Python REPL 中快速体验 Iceberg 功能
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def create_spark_session():
    """创建带 Iceberg 支持的 Spark Session"""
    print("🚀 正在创建 Spark Session...")
    spark = (
        SparkSession.builder
        .appName("IcebergInteractive")
        .master("local[*]")
        .config("spark.sql.extensions", 
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.local.type", "hadoop")
        .config("spark.sql.catalog.local.warehouse", "data/iceberg-warehouse")
        .config("spark.sql.defaultCatalog", "local")
        .getOrCreate()
    )
    print("✅ Spark Session 创建成功！")
    return spark


def setup_demo_table(spark):
    """创建示例表和数据"""
    print("\n📊 创建示例数据...")
    
    # 创建数据库
    spark.sql("CREATE DATABASE IF NOT EXISTS local.demo")
    
    # 创建示例数据
    data = [
        ("order_001", "user_A", "laptop", 999.99, 1, "2025-11-01 10:00:00"),
        ("order_002", "user_B", "mouse", 29.99, 2, "2025-11-01 11:00:00"),
        ("order_003", "user_A", "keyboard", 79.99, 1, "2025-11-02 09:00:00"),
        ("order_004", "user_C", "monitor", 299.99, 2, "2025-11-02 14:00:00"),
        ("order_005", "user_B", "laptop", 1299.99, 1, "2025-11-03 10:30:00"),
    ]
    
    df = spark.createDataFrame(
        data, 
        ["order_id", "user_id", "product", "price", "quantity", "order_time"]
    )
    
    df = df.withColumn("order_time", F.to_timestamp("order_time")) \
           .withColumn("amount", F.col("price") * F.col("quantity")) \
           .withColumn("order_date", F.to_date("order_time"))
    
    # 删除旧表（如果存在）
    spark.sql("DROP TABLE IF EXISTS local.demo.orders")
    
    # 创建 Iceberg 表
    df.writeTo("local.demo.orders") \
      .using("iceberg") \
      .partitionedBy("order_date") \
      .create()
    
    print("✅ 示例表创建成功！")
    print("\n📋 表内容：")
    spark.table("local.demo.orders").show()
    
    return spark


def print_menu():
    """打印菜单"""
    print("\n" + "="*60)
    print("🧊 Iceberg 交互式演示")
    print("="*60)
    print("1. 查看表数据")
    print("2. 查看快照历史")
    print("3. 查看数据文件")
    print("4. 追加数据")
    print("5. Time Travel（查询历史版本）")
    print("6. Schema Evolution（添加列）")
    print("7. UPDATE 数据")
    print("8. DELETE 数据")
    print("9. MERGE INTO 演示")
    print("10. 增量读取")
    print("11. 表维护（合并小文件）")
    print("0. 退出")
    print("="*60)


def demo_query(spark):
    """查看表数据"""
    print("\n📊 当前表数据：")
    spark.table("local.demo.orders").show()
    print(f"总记录数: {spark.table('local.demo.orders').count()}")


def demo_snapshots(spark):
    """查看快照历史"""
    print("\n📸 快照历史：")
    spark.sql("SELECT snapshot_id, committed_at, operation FROM local.demo.orders.snapshots").show(truncate=False)


def demo_files(spark):
    """查看数据文件"""
    print("\n📁 数据文件：")
    spark.sql("""
        SELECT 
            file_path, 
            record_count, 
            ROUND(file_size_in_bytes / 1024, 2) as size_kb
        FROM local.demo.orders.files
    """).show(truncate=False)


def demo_append(spark):
    """追加数据"""
    print("\n➕ 追加新数据...")
    new_data = [
        ("order_006", "user_D", "headphone", 199.99, 1, "2025-11-04 15:00:00"),
    ]
    
    new_df = spark.createDataFrame(
        new_data,
        ["order_id", "user_id", "product", "price", "quantity", "order_time"]
    )
    
    new_df = new_df.withColumn("order_time", F.to_timestamp("order_time")) \
                   .withColumn("amount", F.col("price") * F.col("quantity")) \
                   .withColumn("order_date", F.to_date("order_time"))
    
    new_df.writeTo("local.demo.orders").using("iceberg").append()
    
    print("✅ 数据追加成功！")
    demo_query(spark)


def demo_time_travel(spark):
    """Time Travel 演示"""
    print("\n🕐 Time Travel 演示：")
    
    snapshots = spark.sql("SELECT snapshot_id FROM local.demo.orders.snapshots ORDER BY committed_at")
    snapshot_list = snapshots.collect()
    
    if len(snapshot_list) > 0:
        first_snapshot = snapshot_list[0][0]
        print(f"\n查询第一个快照 (ID: {first_snapshot}):")
        
        df_history = spark.read \
            .option("snapshot-id", first_snapshot) \
            .format("iceberg") \
            .load("local.demo.orders")
        
        df_history.show()
        print(f"历史快照记录数: {df_history.count()}")
    else:
        print("⚠️ 暂无快照历史")


def demo_schema_evolution(spark):
    """Schema Evolution 演示"""
    print("\n📝 Schema Evolution 演示：")
    print("添加新列 'discount'...")
    
    try:
        spark.sql("ALTER TABLE local.demo.orders ADD COLUMN discount DOUBLE")
        print("✅ 列添加成功！")
        
        spark.sql("""
            UPDATE local.demo.orders
            SET discount = CASE 
                WHEN amount > 1000 THEN 0.1
                WHEN amount > 500 THEN 0.05
                ELSE 0.0
            END
            WHERE discount IS NULL
        """)
        
        print("\n更新后的数据：")
        spark.sql("SELECT order_id, amount, discount FROM local.demo.orders LIMIT 5").show()
    except Exception as e:
        if "already exists" in str(e):
            print("⚠️ 列 'discount' 已存在")
        else:
            print(f"❌ 错误: {e}")


def demo_update(spark):
    """UPDATE 演示"""
    print("\n✏️ UPDATE 演示：")
    print("将 user_A 的所有订单金额增加 10%...")
    
    spark.sql("""
        UPDATE local.demo.orders
        SET amount = amount * 1.1
        WHERE user_id = 'user_A'
    """)
    
    print("✅ 更新完成！")
    print("\nuser_A 的订单：")
    spark.sql("SELECT * FROM local.demo.orders WHERE user_id = 'user_A'").show()


def demo_delete(spark):
    """DELETE 演示"""
    print("\n🗑️ DELETE 演示：")
    print("删除金额小于 50 的订单...")
    
    before_count = spark.table("local.demo.orders").count()
    
    spark.sql("DELETE FROM local.demo.orders WHERE amount < 50")
    
    after_count = spark.table("local.demo.orders").count()
    
    print(f"✅ 删除完成！删除了 {before_count - after_count} 条记录")
    demo_query(spark)


def demo_merge(spark):
    """MERGE INTO 演示"""
    print("\n🔄 MERGE INTO 演示：")
    
    # 创建更新数据
    updates_data = [
        ("order_001", "user_A", "laptop", 899.99, 1, "2025-11-01 10:00:00"),  # 更新价格
        ("order_999", "user_E", "tablet", 499.99, 1, "2025-11-05 09:00:00"),  # 新订单
    ]
    
    updates_df = spark.createDataFrame(
        updates_data,
        ["order_id", "user_id", "product", "price", "quantity", "order_time"]
    )
    
    updates_df = updates_df.withColumn("order_time", F.to_timestamp("order_time")) \
                           .withColumn("amount", F.col("price") * F.col("quantity")) \
                           .withColumn("order_date", F.to_date("order_time"))
    
    updates_df.createOrReplaceTempView("updates")
    
    print("更新数据：")
    updates_df.show()
    
    spark.sql("""
        MERGE INTO local.demo.orders t
        USING updates u
        ON t.order_id = u.order_id
        WHEN MATCHED THEN
            UPDATE SET 
                t.price = u.price,
                t.amount = u.amount
        WHEN NOT MATCHED THEN
            INSERT *
    """)
    
    print("✅ MERGE 完成！")
    demo_query(spark)


def demo_incremental(spark):
    """增量读取演示"""
    print("\n📖 增量读取演示：")
    
    snapshots = spark.sql("SELECT snapshot_id FROM local.demo.orders.snapshots ORDER BY committed_at")
    snapshot_list = snapshots.collect()
    
    if len(snapshot_list) >= 2:
        start_snapshot = snapshot_list[0][0]
        end_snapshot = snapshot_list[-1][0]
        
        print(f"读取从快照 {start_snapshot} 到 {end_snapshot} 的增量数据：")
        
        incremental_df = spark.read \
            .format("iceberg") \
            .option("start-snapshot-id", start_snapshot) \
            .option("end-snapshot-id", end_snapshot) \
            .load("local.demo.orders")
        
        print(f"增量记录数: {incremental_df.count()}")
        incremental_df.show()
    else:
        print("⚠️ 快照数量不足（至少需要 2 个）")


def demo_maintenance(spark):
    """表维护演示"""
    print("\n🔧 表维护演示：")
    
    print("1️⃣ 当前文件状态：")
    demo_files(spark)
    
    print("\n2️⃣ 执行文件合并...")
    try:
        spark.sql("CALL local.system.rewrite_data_files(table => 'demo.orders')")
        print("✅ 文件合并完成！")
        
        print("\n优化后的文件状态：")
        demo_files(spark)
    except Exception as e:
        print(f"⚠️ 维护操作失败: {e}")
        print("提示: 小规模数据可能不需要文件合并")


def main():
    """主函数"""
    spark = create_spark_session()
    setup_demo_table(spark)
    
    actions = {
        "1": demo_query,
        "2": demo_snapshots,
        "3": demo_files,
        "4": demo_append,
        "5": demo_time_travel,
        "6": demo_schema_evolution,
        "7": demo_update,
        "8": demo_delete,
        "9": demo_merge,
        "10": demo_incremental,
        "11": demo_maintenance,
    }
    
    while True:
        print_menu()
        choice = input("\n请选择操作 (0-11): ").strip()
        
        if choice == "0":
            print("\n👋 感谢使用 Iceberg 交互式演示！")
            break
        
        if choice in actions:
            try:
                actions[choice](spark)
            except Exception as e:
                print(f"\n❌ 操作失败: {e}")
            
            input("\n按 Enter 继续...")
        else:
            print("❌ 无效的选择，请重试")
    
    spark.stop()


if __name__ == "__main__":
    main()

