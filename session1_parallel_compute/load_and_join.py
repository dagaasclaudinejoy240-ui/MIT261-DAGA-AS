"""
Loads and joins the three relational files.

Join path:
    sales.csv
      -> customers.csv on Customer_ID
      -> products.csv on Product_ID

Both joins are validated as many-to-one and must preserve all 250,000 sales rows.

Run:
    python load_and_join.py
"""

import sys
import time
import pandas as pd
import config as cfg

def load_tables():
    cfg.require_files()

    sales = pd.read_csv(cfg.SALES_FILE)
    customers = pd.read_csv(cfg.CUSTOMERS_FILE)
    products = pd.read_csv(cfg.PRODUCTS_FILE)

    sales["Order_Date"] = pd.to_datetime(sales["Order_Date"], errors="raise")
    sales["Delivery_Date"] = pd.to_datetime(sales["Delivery_Date"], errors="raise")
    customers["Date_of_Birth"] = pd.to_datetime(customers["Date_of_Birth"], errors="raise")
    customers["Registration_Date"] = pd.to_datetime(customers["Registration_Date"], errors="raise")

    return sales, customers, products

def build_working_dataset(verbose=True, write=True):
    t0 = time.perf_counter()
    sales, customers, products = load_tables()

    rows_before = len(sales)

    # Avoid duplicate City/State/Age columns from the customer entity by suffixing.
    joined = sales.merge(
        customers,
        on="Customer_ID",
        how="inner",
        validate="many_to_one",
        suffixes=("", "_Customer"),
    )

    rows_after_customer = len(joined)

    joined = joined.merge(
        products,
        on="Product_ID",
        how="inner",
        validate="many_to_one",
        suffixes=("", "_Product"),
    )

    rows_after_product = len(joined)

    assert rows_after_customer == rows_before, (
        f"Customer join changed row count: {rows_before} -> {rows_after_customer}"
    )
    assert rows_after_product == rows_before, (
        f"Product join changed row count: {rows_before} -> {rows_after_product}"
    )

    customer_orphans = int((~sales["Customer_ID"].isin(customers["Customer_ID"])).sum())
    product_orphans = int((~sales["Product_ID"].isin(products["Product_ID"])).sum())

    assert customer_orphans == 0
    assert product_orphans == 0

    elapsed = time.perf_counter() - t0

    report = {
        "rows_before_join": int(rows_before),
        "rows_after_customer_join": int(rows_after_customer),
        "rows_after_product_join": int(rows_after_product),
        "row_difference": int(rows_after_product - rows_before),
        "customer_fk_orphans": customer_orphans,
        "product_fk_orphans": product_orphans,
        "join_seconds": elapsed,
        "join_path": "sales -> customers on Customer_ID -> products on Product_ID",
    }

    if write:
        joined.to_parquet(cfg.OUT_JOINED, index=False)

    if verbose:
        cfg.banner("SESSION 1 - LOAD AND JOIN")
        print("Join path:")
        print(" sales.csv -> customers.csv on Customer_ID")
        print("           -> products.csv on Product_ID")
        print()
        print(f"Rows before join          : {rows_before:,}")
        print(f"After customer join       : {rows_after_customer:,}")
        print(f"After product join        : {rows_after_product:,}")
        print(f"Difference                : {rows_after_product - rows_before:,}")
        print(f"Customer FK orphan rows   : {customer_orphans}")
        print(f"Product FK orphan rows    : {product_orphans}")
        print(f"Join/reconciliation time  : {elapsed:.4f} s")
        if write:
            print(f"\nWrote {cfg.OUT_JOINED}")

    return joined, report

def main():
    build_working_dataset()
    return 0

if __name__ == "__main__":
    sys.exit(main())
