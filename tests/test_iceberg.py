"""
Iceberg 功能测试
"""
import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType

from src.common.spark_session import build_spark


@pytest.fixture(scope="module")
def spark():
    """创建带 Iceberg 支持的 Spark Session"""
    spark = build_spark("IcebergTest", local=True, enable_iceberg=True)
    yield spark
    spark.stop()


@pytest.fixture
def sample_data(spark):
    """创建测试数据"""
    schema = StructType([
        StructField("order_id", StringType(), False),
        StructField("user_id", StringType(), False),
        StructField("item_id", StringType(), False),
        StructField("category", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("ts", TimestampType(), True),
    ])
    
    data = [
        ("order_001", "user_001", "item_A", "Electronics", 499.99, 1, "2025-10-06 10:00:00"),
        ("order_002", "user_002", "item_B", "Books", 29.99, 2, "2025-10-06 11:00:00"),
        ("order_003", "user_001", "item_C", "Electronics", 899.99, 1, "2025-10-07 09:00:00"),
    ]
    
    df = spark.createDataFrame(data, schema)
    df = df.withColumn("ts", F.to_timestamp("ts")) \
           .withColumn("amount", F.col("price") * F.col("quantity")) \
           .withColumn("ingest_date", F.to_date(F.col("ts")))
    
    return df


def test_create_iceberg_table(spark, sample_data):
    """测试创建 Iceberg 表"""
    table_name = "local.test_db.orders"
    
    # 创建数据库
    spark.sql("CREATE DATABASE IF NOT EXISTS local.test_db")
    
    # 删除表（如果存在）
    spark.sql(f"DROP TABLE IF EXISTS {table_name}")
    
    # 写入数据到 Iceberg 表
    sample_data.writeTo(table_name).using("iceberg").create()
    
    # 验证表存在
    tables = spark.sql("SHOW TABLES IN local.test_db").collect()
    assert any("orders" in str(row) for row in tables)
    
    # 验证数据
    df_read = spark.table(table_name)
    assert df_read.count() == 3


def test_iceberg_append(spark, sample_data):
    """测试 Iceberg 追加数据"""
    table_name = "local.test_db.orders"
    
    initial_count = spark.table(table_name).count()
    
    # 追加新数据
    new_data = sample_data.limit(1)
    new_data.writeTo(table_name).using("iceberg").append()
    
    # 验证数据量增加
    final_count = spark.table(table_name).count()
    assert final_count == initial_count + 1


def test_iceberg_time_travel(spark):
    """测试 Time Travel 功能"""
    table_name = "local.test_db.orders"
    
    # 获取快照历史
    snapshots = spark.sql(f"SELECT snapshot_id FROM {table_name}.snapshots").collect()
    assert len(snapshots) > 0
    
    # 使用第一个快照读取数据
    first_snapshot_id = snapshots[0][0]
    df_history = spark.read \
        .option("snapshot-id", first_snapshot_id) \
        .format("iceberg") \
        .load(table_name)
    
    assert df_history.count() >= 0


def test_iceberg_schema_evolution(spark):
    """测试 Schema Evolution"""
    table_name = "local.test_db.orders"
    
    # 添加新列
    spark.sql(f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS status STRING")
    
    # 验证新列存在
    schema = spark.table(table_name).schema
    field_names = [field.name for field in schema.fields]
    assert "status" in field_names


def test_iceberg_update(spark):
    """测试更新操作"""
    table_name = "local.test_db.orders"
    
    # 更新数据
    spark.sql(f"""
        UPDATE {table_name}
        SET category = 'Updated'
        WHERE order_id = 'order_001'
    """)
    
    # 验证更新
    updated = spark.sql(f"""
        SELECT category 
        FROM {table_name} 
        WHERE order_id = 'order_001'
    """).collect()
    
    assert len(updated) > 0
    assert updated[0][0] == 'Updated'


def test_iceberg_delete(spark):
    """测试删除操作"""
    table_name = "local.test_db.orders"
    
    initial_count = spark.table(table_name).count()
    
    # 删除数据
    spark.sql(f"""
        DELETE FROM {table_name}
        WHERE order_id = 'order_002'
    """)
    
    # 验证删除
    final_count = spark.table(table_name).count()
    assert final_count < initial_count


def test_iceberg_merge(spark, sample_data):
    """测试 MERGE INTO 操作"""
    table_name = "local.test_db.orders"
    
    # 创建更新数据
    updates = sample_data.limit(2).withColumn("price", F.lit(999.99))
    updates.createOrReplaceTempView("updates")
    
    # 执行 MERGE
    spark.sql(f"""
        MERGE INTO {table_name} t
        USING updates u
        ON t.order_id = u.order_id
        WHEN MATCHED THEN
            UPDATE SET t.price = u.price
        WHEN NOT MATCHED THEN
            INSERT *
    """)
    
    # 验证更新
    updated = spark.sql(f"""
        SELECT price 
        FROM {table_name} 
        WHERE order_id IN ('order_001', 'order_002')
    """).collect()
    
    assert len(updated) > 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

