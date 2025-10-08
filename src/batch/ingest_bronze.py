import argparse
from pathlib import Path

from pyspark.sql import functions as F
from pyspark.sql import types as T

from src.common.spark_session import build_spark


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="CSV -> Parquet (Bronze) ingest job")
    parser.add_argument("--input", required=True, help="Input CSV file path")
    parser.add_argument("--output", required=True, help="Output directory for Parquet/Delta")
    parser.add_argument("--format", default="parquet", choices=["parquet", "delta"], help="Output format")
    parser.add_argument("--local", action="store_true", help="Use local[*] Spark for debugging")
    parser.add_argument("--explain", help="Write physical plan to a file path (e.g., data/plan.txt)")
    return parser.parse_args()


def read_csv_with_schema(input_path: str):
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
    return (
        df.withColumn("amount", F.col("price") * F.col("quantity"))
          .withColumn("ingest_date", F.to_date(F.col("ts")))
    )


def write_output(df, output_path: str, fmt: str):
    mode = "overwrite"
    if fmt == "delta":
        (
            df.write
            .format("delta")
            .mode(mode)
            .partitionBy("ingest_date")
            .save(output_path)
        )
    else:
        (
            df.write
            .mode(mode)
            .partitionBy("ingest_date")
            .parquet(output_path)
        )


def main():
    args = parse_args()
    spark_local = bool(args.local)
    global spark
    spark = build_spark("IngestBronze", local=spark_local)

    input_path = args.input
    output_path = args.output
    fmt = args.format
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)

    df = read_csv_with_schema(input_path)
    df_out = transform(df)
    # 导出物理计划（DataFrame 写出前）
    if args.explain:
        plan = df_out._jdf.queryExecution().executedPlan().toString()
        p = Path(args.explain)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(plan)
    write_output(df_out, output_path, fmt)

    spark.stop()


if __name__ == "__main__":
    main()


