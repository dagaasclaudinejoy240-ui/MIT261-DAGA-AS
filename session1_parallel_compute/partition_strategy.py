"""
Evaluates partition-key candidates and records Customer_ID as the Session 1 key.

Chosen key:
    Customer_ID

Reason:
    It is the PK of the Customer entity, survives the join as the Sales FK,
    directly supports independent per-customer revenue aggregation, and has
    moderate observed skew.
"""

import json
import sys
import pandas as pd
import config as cfg
from load_and_join import build_working_dataset

def describe_key(df, key):
    counts = df.groupby(key, dropna=False).size().sort_values()
    minimum = int(counts.min())
    maximum = int(counts.max())
    return {
        "key": key,
        "distinct": int(len(counts)),
        "min": minimum,
        "median": float(counts.median()),
        "max": maximum,
        "mean": float(counts.mean()),
        "skew_ratio_max_min": float(maximum / minimum) if minimum else None,
        "largest_key": str(counts.idxmax()),
    }

def main():
    working, _ = build_working_dataset(verbose=False, write=False)
    cfg.banner("SESSION 1 - PARTITION STRATEGY")

    candidates = ["Customer_ID", "Product_ID", "State"]
    report = {}

    for key in candidates:
        r = describe_key(working, key)
        report[key] = r
        print(
            f"{key}: distinct={r['distinct']:,} | "
            f"min={r['min']:,} | median={r['median']:.1f} | "
            f"max={r['max']:,} | skew={r['skew_ratio_max_min']:.2f}:1 | "
            f"largest={r['largest_key']}"
        )

    final = {
        "chosen_partition_key": cfg.PARTITION_KEY,
        "entity_owner": "Customer entity (customers.csv)",
        "relationship": "Customer 1 -> many Sales",
        "workload": (
            "Compute transaction count, total revenue, mean revenue, "
            "total quantity, and average rating per Customer_ID."
        ),
        "candidates": report,
    }

    cfg.OUT_PARTITION_STRATEGY.write_text(
        json.dumps(final, indent=2), encoding="utf-8"
    )
    print(f"\nChosen key: {cfg.PARTITION_KEY}")
    print(f"Wrote {cfg.OUT_PARTITION_STRATEGY}")
    return 0

if __name__ == "__main__":
    sys.exit(main())
