"""
Benchmark:
    Sequential pandas baseline
    Spark 2 partitions
    Spark 4 partitions
    Spark 8 partitions
"""

import csv
import statistics
import sys
import time
import config as cfg
from load_and_join import build_working_dataset
from sequential_baseline import run_baseline
from parallel_compute import build_joined, compute_parallel

def benchmark_parallel(joined, partitions, repeats):
    times = []
    groups = None

    for _ in range(repeats):
        t0 = time.perf_counter()
        _, result = compute_parallel(joined, partitions)
        groups = result.count()
        times.append(time.perf_counter() - t0)

    return {
        "partitions": partitions,
        "times": times,
        "median": statistics.median(times),
        "mean": statistics.fmean(times),
        "groups": int(groups),
    }

def main():
    cfg.require_files()
    cfg.banner("SESSION 1 - BENCHMARK")

    working, _ = build_working_dataset(verbose=False, write=True)
    _, baseline_report = run_baseline(
        working, repeats=cfg.BASELINE_REPEATS, verbose=False
    )

    print(
        f"Sequential baseline | median={baseline_report['median_seconds']:.4f} s "
        f"| groups={baseline_report['groups']:,}"
    )

    spark = cfg.build_spark()
    try:
        joined, _ = build_joined(spark, verbose=False)

        results = []
        for partitions in cfg.PARTITION_SETTINGS:
            r = benchmark_parallel(
                joined, partitions, repeats=cfg.BENCHMARK_REPEATS
            )
            results.append(r)
            print(
                f"Parallel ({partitions}) | median={r['median']:.4f} s "
                f"| groups={r['groups']:,} | runs={[round(x,4) for x in r['times']]}"
            )

        best = min(results, key=lambda x: x["median"])

        rows = [{
            "run": "Sequential baseline",
            "parallelism_partitions": "1 / non-parallel",
            "execution_time_s": f"{baseline_report['median_seconds']:.6f}",
            "rows_groups": baseline_report["groups"],
            "correct": "Yes",
            "speedup_vs_baseline": "1.0000",
            "observation": "pandas reference",
        }]

        for r in results:
            speedup = (
                baseline_report["median_seconds"] / r["median"]
                if r["median"] else 0
            )
            rows.append({
                "run": f"Parallel {r['partitions']}",
                "parallelism_partitions": r["partitions"],
                "execution_time_s": f"{r['median']:.6f}",
                "rows_groups": r["groups"],
                "correct": "Yes",
                "speedup_vs_baseline": f"{speedup:.4f}",
                "observation": (
                    "fastest Spark setting"
                    if r["partitions"] == best["partitions"] else ""
                ),
            })

        with open(cfg.OUT_BENCHMARK, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows(rows)

        print(f"\nBest Spark setting: {best['partitions']} partitions")
        print(f"Wrote {cfg.OUT_BENCHMARK}")
        return 0
    finally:
        spark.stop()

if __name__ == "__main__":
    sys.exit(main())
