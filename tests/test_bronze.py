import os
from pathlib import Path


def test_bronze_output_exists():
    base = Path("data/bronze/orders")
    # 至少存在一个分区目录或 parquet 文件
    exists = base.exists() and any(base.rglob("*.parquet"))
    assert exists, "Bronze 输出目录不存在或没有生成 parquet 文件"


