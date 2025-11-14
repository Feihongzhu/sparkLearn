from pathlib import Path


def test_gold_outputs_exist():
    base = Path("data/gold")
    daily = base / "daily"
    topn = base / "topn"
    assert daily.exists() and any(daily.rglob("*.parquet")), "gold/daily 缺失或无 parquet"
    assert topn.exists() and any(topn.rglob("*.parquet")), "gold/topn 缺失或无 parquet"



