from pyspark.sql import SparkSession


def build_spark(app_name: str = "SparkLearn", local: bool = False, enable_iceberg: bool = False) -> SparkSession:
    """
    构建 SparkSession。默认连接到 cluster（在 docker-compose 中是 master URL）。
    local=True 时用于本机调试（非容器）。
    enable_iceberg=True 时启用 Iceberg 支持。
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

    # 启用 Iceberg 配置
    if enable_iceberg:
        builder = (
            builder
            # Iceberg Spark 运行时扩展
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            # Iceberg catalog 配置（使用 Hadoop catalog，数据存储在本地文件系统）
            .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.local.type", "hadoop")
            .config("spark.sql.catalog.local.warehouse", "data/iceberg-warehouse")
            # 设置默认 catalog
            .config("spark.sql.defaultCatalog", "local")
        )

    spark = builder.getOrCreate()
    return spark


