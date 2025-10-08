import argparse
import csv
import random
from datetime import datetime, timedelta
from pathlib import Path


def parse_args():
    parser = argparse.ArgumentParser(description="Generate synthetic orders CSV")
    parser.add_argument("--output", required=True, help="Output CSV file path")
    parser.add_argument("--num-records", type=int, default=1000, help="Number of records")
    return parser.parse_args()


def main():
    args = parse_args()
    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    categories = ["electronics", "clothing", "home", "beauty", "sports"]
    start = datetime.utcnow() - timedelta(days=2)

    with out_path.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["order_id", "user_id", "item_id", "category", "price", "quantity", "ts"])

        for i in range(args.num_records):
            order_id = f"o_{i:06d}"
            user_id = f"u_{random.randint(1, 500):05d}"
            item_id = f"it_{random.randint(1, 200):05d}"
            category = random.choice(categories)
            price = round(random.uniform(5.0, 500.0), 2)
            quantity = random.randint(1, 5)
            ts = start + timedelta(minutes=random.randint(0, 60 * 36))
            writer.writerow([order_id, user_id, item_id, category, price, quantity, ts.isoformat(sep=" ", timespec="seconds")])


if __name__ == "__main__":
    main()


