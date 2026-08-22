"""
Measures physical Spark partition sizes after repartitioning by Customer_ID.
"""

import csv
import sys
import config as cfg
from parallel_compute import build_joined

def main():
    cfg.require_files()
    cfg.banner("SESSION 1 - PARTITION BALANCE AND SKEW")

    spark = cfg.build_spark()
    try:
        joined, _ = build_joined(spark, verbose=False)
        partitioned = joined.repartition(
            cfg.CHOSEN_PARTITIONS, cfg.PARTITION_KEY
        )

        sizes = (
            partitioned.rdd
            .mapPartitionsWithIndex(
                lambda index, rows: [(index, sum(1 for _ in rows))]
            )
            .collect()
        )
        sizes = sorted(sizes)

        total = sum(count for _, count in sizes)
        predicted = total / cfg.CHOSEN_PARTITIONS

        output = []
        for index, count in sizes:
            ratio = count / predicted if predicted else 0
            output.append({
                "partition": index,
                "predicted_even_count": round(predicted, 2),
                "actual_count": count,
                "ratio_to_even": round(ratio, 4),
            })
            print(
                f"partition_{index}: predicted={predicted:,.1f} "
                f"| actual={count:,} | ratio={ratio:.4f}"
            )

        with open(cfg.OUT_PARTITIONS, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=output[0].keys())
            writer.writeheader()
            writer.writerows(output)

        print(f"\nWrote {cfg.OUT_PARTITIONS}")
        return 0
    finally:
        spark.stop()

if __name__ == "__main__":
    sys.exit(main())
