"""
Iceberg 数据摄入示例：CSV -> Iceberg 表
演示 Iceberg 的基本使用、ACID 事务、Schema Evolution、Time Travel 等特性
"""
import argparse
from pathlib import Path

from pyspark.sql import functions as F
from pyspark.sql import types as T

from src.common.spark_session import build_spark


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="CSV -> Iceberg table ingest job")
    parser.add_argument("--input", required=True, help="Input CSV file path")
    parser.add_argument("--table", default="local.db.orders", help="Iceberg table name (catalog.database.table)")
    parser.add_argument("--local", action="store_true", help="Use local[*] Spark for debugging")
    parser.add_argument("--mode", default="append", choices=["append", "overwrite"], help="Write mode")
    return parser.parse_args()


def read_csv_with_schema(spark, input_path: str):
    """读取 CSV 文件并定义 Schema"""
    schema = T.StructType([
        T.StructField("order_id", T.StringType(), False),
        T.StructField("user_id", T.StringType(), False),
        T.StructField("item_id", T.StringType(), False),
        T.StructField("category", T.StringType(), True),
        T.StructField("price", T.DoubleType(), True),
        T.StructField("quantity", T.IntegerType(), True),
        T.StructField("ts", T.TimestampType(), True),
    ])
    return (
        spark.read
        .option("header", True)
        .schema(schema)
        .csv(input_path)
    )


def transform(df):
    """数据转换：添加计算字段和分区字段"""
    return (
        df.withColumn("amount", F.col("price") * F.col("quantity"))
          .withColumn("ingest_date", F.to_date(F.col("ts")))
          .withColumn("year", F.year(F.col("ts")))
          .withColumn("month", F.month(F.col("ts")))
    )


def write_to_iceberg(spark, df, table_name: str, mode: str = "append"):
    """
    写入 Iceberg 表
    
    Iceberg 的特点：
    1. 自动 ACID 事务
    2. 隐藏分区（用户无需手动指定分区路径）
    3. Schema Evolution 支持
    """
    # 确保数据库存在
    db_name = ".".join(table_name.split(".")[:-1])
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")
    
    # 写入 Iceberg 表
    (
        df.write
        .format("iceberg")
        .mode(mode)
        # Iceberg 使用隐藏分区，在表创建时定义
        .save(table_name)
    )
    
    print(f"✅ 成功写入 Iceberg 表: {table_name}")


def create_partitioned_table(spark, table_name: str):
    """
    创建分区 Iceberg 表
    
    Iceberg 的隐藏分区特性：
    - 用户查询时不需要知道分区字段
    - 自动进行分区裁剪优化
    """
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            order_id STRING,
            user_id STRING,
            item_id STRING,
            category STRING,
            price DOUBLE,
            quantity INT,
            ts TIMESTAMP,
            amount DOUBLE,
            ingest_date DATE,
            year INT,
            month INT
        )
        USING iceberg
        PARTITIONED BY (days(ts))
        TBLPROPERTIES (
            'write.format.default' = 'parquet',
            'write.parquet.compression-codec' = 'snappy'
        )
    """)
    print(f"✅ 创建 Iceberg 表: {table_name}")


def main():
    args = parse_args()
    spark = build_spark("IngestIceberg", local=bool(args.local), enable_iceberg=True)
    
    input_path = args.input
    table_name = args.table
    mode = args.mode
    
    # 创建 Iceberg 表（如果不存在）
    create_partitioned_table(spark, table_name)
    
    # 读取和转换数据
    df = read_csv_with_schema(spark, input_path)
    df_out = transform(df)
    
    # 写入 Iceberg 表
    write_to_iceberg(spark, df_out, table_name, mode)
    
    # 显示表的元数据
    print("\n📊 表的快照历史:")
    spark.sql(f"SELECT * FROM {table_name}.snapshots").show(truncate=False)
    
    print("\n📁 表的数据文件:")
    spark.sql(f"SELECT file_path, record_count, file_size_in_bytes FROM {table_name}.files").show(truncate=False)
    
    print("\n📈 表的统计信息:")
    spark.sql(f"DESCRIBE EXTENDED {table_name}").show(truncate=False)
    
    spark.stop()


if __name__ == "__main__":
    main()

