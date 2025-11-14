import argparse
from pathlib import Path

from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.common.spark_session import build_spark


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Aggregate daily metrics (GMV, orders, topN)")
    p.add_argument("--silver", required=True, help="Input detail table path (bronze/silver)")
    p.add_argument("--gold", required=True, help="Output gold dir path")
    p.add_argument("--topn", type=int, default=5, help="TopN products by amount")
    p.add_argument("--local", action="store_true")
    return p.parse_args()


def build_daily(df):
    # 约定 df 含有: ingest_date, amount, order_id, item_id
    daily = (
        df.groupBy("ingest_date")
          .agg(
              F.sum("amount").alias("gmv"),
              F.countDistinct("order_id").alias("order_cnt"),
          )
    )
    return daily


def build_topn(df, n: int):
    per_day_item = (
        df.groupBy("ingest_date", "item_id")
          .agg(F.sum("amount").alias("amount_sum"))
    )
    ranked = (
        per_day_item
        .withColumn(
            "rn",
            F.row_number().over(
                Window.partitionBy("ingest_date").orderBy(F.desc("amount_sum"))
            ),
        )
        .filter(F.col("rn") <= n)
    )
    return ranked.select("ingest_date", "item_id", "amount_sum", "rn")


def main():
    args = parse_args()
    global spark
    spark = build_spark("GoldDaily", local=bool(args.local))

    df = spark.read.parquet(args.silver)
    daily = build_daily(df)
    topn = build_topn(df, args.topn)

    out_daily = str(Path(args.gold) / "daily")
    out_topn = str(Path(args.gold) / "topn")

    daily.write.mode("overwrite").parquet(out_daily)
    topn.write.mode("overwrite").parquet(out_topn)

    spark.stop()


if __name__ == "__main__":
    main()


