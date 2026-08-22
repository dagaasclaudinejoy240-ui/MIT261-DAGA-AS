"""
Sequential pandas baseline.

The aggregation exactly matches the Spark implementation.
"""

import statistics
import sys
import time
import pandas as pd
import config as cfg
from load_and_join import build_working_dataset

def compute(working):
    result = (
        working.groupby(cfg.PARTITION_KEY, dropna=False)
        .agg(
            txn_count=("Order_ID", "count"),
            revenue_total=("Total_Amount", "sum"),
            revenue_mean=("Total_Amount", "mean"),
            quantity_total=("Quantity", "sum"),
            rating_mean=("Rating", "mean"),
        )
        .reset_index()
    )
    return result

def run_baseline(working=None, repeats=cfg.BASELINE_REPEATS, verbose=True):
    if repeats < 1:
        raise ValueError("repeats must be at least 1")

    if working is None:
        working, _ = build_working_dataset(verbose=False, write=False)

    times = []
    result = None

    for _ in range(repeats):
        t0 = time.perf_counter()
        result = compute(working)
        times.append(time.perf_counter() - t0)

    assert result is not None

    report = {
        "runs_seconds": times,
        "median_seconds": statistics.median(times),
        "mean_seconds": statistics.fmean(times),
        "groups": int(len(result)),
        "repeats": repeats,
    }

    if verbose:
        cfg.banner("SESSION 1 - SEQUENTIAL BASELINE")
        for i, t in enumerate(times, 1):
            print(f"Run {i}: {t:.6f} s")
        print(f"Median: {report['median_seconds']:.6f} s")
        print(f"Groups: {report['groups']:,}")

    return result, report

def main():
    working, _ = build_working_dataset(verbose=False, write=True)
    result, _ = run_baseline(working)
    result.sort_values(cfg.PARTITION_KEY).to_csv(cfg.OUT_BASELINE, index=False)
    print(f"\nWrote {cfg.OUT_BASELINE}")
    return 0

if __name__ == "__main__":
    sys.exit(main())
