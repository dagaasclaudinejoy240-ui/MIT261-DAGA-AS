"""
PySpark local-mode implementation.

Spark reads the three CSV files directly, broadcasts the two entity tables,
repartitions by Customer_ID, aggregates, and validates against pandas.
"""

import json
import sys
import time
import pandas as pd
import config as cfg
from sequential_baseline import compute as baseline_compute
from load_and_join import build_working_dataset

def build_joined(spark, verbose=True):
    from pyspark.sql import functions as F

    sales = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(str(cfg.SALES_FILE))
    )
    customers = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(str(cfg.CUSTOMERS_FILE))
    )
    products = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(str(cfg.PRODUCTS_FILE))
    )

    sales = (
        sales
        .withColumn("Order_Date", F.to_date("Order_Date"))
        .withColumn("Delivery_Date", F.to_date("Delivery_Date"))
    )

    before = sales.count()

    # Select entity fields with aliases where Sales already contains a snapshot.
    customer_dim = customers.select(
        "Customer_ID",
        F.col("Customer_Name"),
        F.col("Gender"),
        F.col("Age").alias("Customer_Master_Age"),
        F.col("Age_Group").alias("Customer_Master_Age_Group"),
        F.col("Date_of_Birth"),
        F.col("Email"),
        F.col("Phone"),
        F.col("City").alias("Customer_Master_City"),
        F.col("State").alias("Customer_Master_State"),
        F.col("Pincode"),
        F.col("Registration_Date"),
        F.col("Customer_Tier"),
        F.col("Total_Orders").alias("Customer_Master_Total_Orders"),
        F.col("Total_Spent").alias("Customer_Master_Total_Spent"),
    )

    product_dim = products.select(
        "Product_ID",
        "Product_Name",
        "Category",
        "Brand",
        "Original_Price",
        "Discount_Percent",
        "Discount_Amount",
        "Selling_Price",
        "Stock_Quantity",
        "Weight_kg",
        "Avg_Rating",
        "Total_Reviews",
    )

    t0 = time.perf_counter()

    joined = (
        sales
        .join(F.broadcast(customer_dim), on="Customer_ID", how="inner")
        .join(F.broadcast(product_dim), on="Product_ID", how="inner")
        .cache()
    )

    after = joined.count()
    elapsed = time.perf_counter() - t0

    if before != after:
        raise AssertionError(f"Spark join changed row count: {before} -> {after}")

    plan = joined._jdf.queryExecution().executedPlan().toString()
    broadcast_occurrences = plan.count("BroadcastHashJoin")
    sort_merge_occurrences = plan.count("SortMergeJoin")

    if verbose:
        cfg.banner("SPARK JOIN")
        print(f"Rows before join  : {before:,}")
        print(f"Rows after join   : {after:,}")
        print(f"Difference        : {after - before:,}")
        print(f"Join/cache time   : {elapsed:.4f} s")
        print(f"BroadcastHashJoin text occurrences: {broadcast_occurrences}")
        print(f"SortMergeJoin text occurrences    : {sort_merge_occurrences}")

    return joined, {
        "rows_before": before,
        "rows_after": after,
        "join_seconds": elapsed,
        "broadcast_hash_join_occurrences": broadcast_occurrences,
        "sort_merge_join_occurrences": sort_merge_occurrences,
    }

def compute_parallel(joined, partitions):
    from pyspark.sql import functions as F

    partitioned = joined.repartition(partitions, cfg.PARTITION_KEY)

    result = (
        partitioned
        .groupBy(cfg.PARTITION_KEY)
        .agg(
            F.count(F.lit(1)).alias("txn_count"),
            F.sum("Total_Amount").alias("revenue_total"),
            F.avg("Total_Amount").alias("revenue_mean"),
            F.sum("Quantity").alias("quantity_total"),
            F.avg("Rating").alias("rating_mean"),
        )
    )

    return partitioned, result

def validate(result, baseline):
    parallel = result.orderBy(cfg.PARTITION_KEY).toPandas()
    baseline = baseline.sort_values(cfg.PARTITION_KEY).reset_index(drop=True)

    comparison = parallel.merge(
        baseline,
        on=cfg.PARTITION_KEY,
        suffixes=("_par", "_base"),
        validate="one_to_one",
    )

    count_diff = float(
        (comparison["txn_count_par"] - comparison["txn_count_base"]).abs().max()
    )
    total_diff = float(
        (comparison["revenue_total_par"] - comparison["revenue_total_base"]).abs().max()
    )
    mean_diff = float(
        (comparison["revenue_mean_par"] - comparison["revenue_mean_base"]).abs().max()
    )
    qty_diff = float(
        (comparison["quantity_total_par"] - comparison["quantity_total_base"]).abs().max()
    )

    # Rating can be NaN for customers with no ratings in their sales rows.
    rating_delta = (
        comparison["rating_mean_par"].fillna(0)
        - comparison["rating_mean_base"].fillna(0)
    ).abs()
    rating_diff = float(rating_delta.max())

    max_baseline_total = float(comparison["revenue_total_base"].abs().max())
    total_allowed = max(cfg.TOLERANCE, max_baseline_total * 1e-12)

    assert len(parallel) == len(baseline)
    assert count_diff == 0
    assert qty_diff == 0
    assert total_diff <= total_allowed
    assert mean_diff <= cfg.TOLERANCE
    assert rating_diff <= cfg.TOLERANCE

    return {
        "parallel_groups": int(len(parallel)),
        "baseline_groups": int(len(baseline)),
        "max_txn_count_diff": count_diff,
        "max_revenue_total_diff": total_diff,
        "max_revenue_mean_diff": mean_diff,
        "max_quantity_total_diff": qty_diff,
        "max_rating_mean_diff": rating_diff,
        "allowed_revenue_total_tolerance": total_allowed,
        "result": "PASS",
    }

def main():
    cfg.require_files()
    cfg.banner("SESSION 1 - PARALLEL COMPUTE")

    working_pd, _ = build_working_dataset(verbose=False, write=True)
    baseline = baseline_compute(working_pd)

    spark = cfg.build_spark()
    try:
        joined, join_info = build_joined(spark)

        t0 = time.perf_counter()
        partitioned, result = compute_parallel(joined, cfg.CHOSEN_PARTITIONS)
        groups = result.count()
        parallel_seconds = time.perf_counter() - t0

        print(f"\nConfigured partitions : {cfg.CHOSEN_PARTITIONS}")
        print(f"Actual partitions     : {partitioned.rdd.getNumPartitions()}")
        print(f"Result groups         : {groups:,}")
        print(f"Parallel time         : {parallel_seconds:.4f} s")

        validation = validate(result, baseline)

        print("\nCORRECTNESS")
        for key, value in validation.items():
            print(f"{key}: {value}")

        final_pd = result.orderBy(cfg.PARTITION_KEY).toPandas()
        final_pd.to_parquet(cfg.OUT_FINAL, index=False)

        full_report = {
            "join": join_info,
            "configured_partitions": cfg.CHOSEN_PARTITIONS,
            "actual_partitions": partitioned.rdd.getNumPartitions(),
            "parallel_seconds": parallel_seconds,
            "validation": validation,
        }
        cfg.OUT_VALIDATION.write_text(
            json.dumps(full_report, indent=2), encoding="utf-8"
        )

        print(f"\nWrote {cfg.OUT_FINAL}")
        print(f"Wrote {cfg.OUT_VALIDATION}")
        return 0
    finally:
        spark.stop()

if __name__ == "__main__":
    sys.exit(main())
