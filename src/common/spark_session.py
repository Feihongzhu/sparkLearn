from pyspark.sql import SparkSession


def build_spark(app_name: str = "SparkLearn", local: bool = False) -> SparkSession:
    """
    构建 SparkSession。默认连接到 cluster（在 docker-compose 中是 master URL）。
    local=True 时用于本机调试（非容器）。
    """
    builder = (
        SparkSession
        .builder
        .appName(app_name)
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    )

    if local:
        builder = builder.master("local[*]")

    spark = builder.getOrCreate()
    return spark


