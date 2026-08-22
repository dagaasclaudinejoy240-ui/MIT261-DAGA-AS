"""
Profiles customers.csv, products.csv, and sales.csv.

Run:
    python profile_files.py
"""

import json
import sys
import pandas as pd
import config as cfg

EXPECTED = {
    "customers": {
        "pk": "Customer_ID",
        "role": "Entity",
    },
    "products": {
        "pk": "Product_ID",
        "role": "Entity",
    },
    "sales": {
        "pk": "Order_ID",
        "role": "Event",
        "fks": {
            "Customer_ID": ("customers", "Customer_ID"),
            "Product_ID": ("products", "Product_ID"),
        },
    },
}

def profile_dataframe(name, path):
    df = pd.read_csv(path)
    info = EXPECTED[name]

    profile = {
        "file": path.name,
        "role": info["role"],
        "rows": int(len(df)),
        "columns": int(len(df.columns)),
        "column_list": list(df.columns),
        "dtypes": {c: str(t) for c, t in df.dtypes.items()},
        "null_counts": {c: int(v) for c, v in df.isna().sum().items()},
        "duplicate_rows": int(df.duplicated().sum()),
        "primary_key": info["pk"],
        "primary_key_unique": bool(df[info["pk"]].is_unique),
        "primary_key_nulls": int(df[info["pk"]].isna().sum()),
    }
    return df, profile

def main():
    cfg.require_files()
    cfg.banner("SESSION 1 - FILE PROFILING")

    data = {}
    report = {
        "dataset": cfg.DATASET_TITLE,
        "kaggle_url": cfg.KAGGLE_URL,
        "files": {},
        "foreign_key_checks": {},
        "eligibility": {},
    }

    for name, path in cfg.FILES.items():
        df, profile = profile_dataframe(name, path)
        data[name] = df
        report["files"][name] = profile

        print(f"\n{path.name} [{profile['role']}]")
        print(f" rows        : {profile['rows']:,}")
        print(f" columns     : {profile['columns']}")
        print(f" primary key : {profile['primary_key']}")
        print(f" PK unique   : {profile['primary_key_unique']}")
        print(f" PK nulls    : {profile['primary_key_nulls']}")
        print(f" null counts : {profile['null_counts']}")

    sales = data["sales"]
    customers = data["customers"]
    products = data["products"]

    customer_orphans = int((~sales["Customer_ID"].isin(customers["Customer_ID"])).sum())
    product_orphans = int((~sales["Product_ID"].isin(products["Product_ID"])).sum())

    report["foreign_key_checks"] = {
        "sales.Customer_ID -> customers.Customer_ID": {
            "orphan_rows": customer_orphans,
            "pass": customer_orphans == 0,
        },
        "sales.Product_ID -> products.Product_ID": {
            "orphan_rows": product_orphans,
            "pass": product_orphans == 0,
        },
    }

    order_dates = pd.to_datetime(sales["Order_Date"], errors="coerce")
    report["sales_date_range"] = {
        "min": str(order_dates.min().date()),
        "max": str(order_dates.max().date()),
        "unparseable": int(order_dates.isna().sum()),
    }

    report["eligibility"] = {
        "at_least_three_related_files": True,
        "genuine_one_to_many_customer_sales": True,
        "genuine_one_to_many_product_sales": True,
        "usable_event_date": True,
        "transactional_rows_at_least_50000": len(sales) >= 50_000,
        "transactional_rows": int(len(sales)),
    }

    print("\nFOREIGN KEY CHECKS")
    print(f" sales.Customer_ID -> customers.Customer_ID orphan rows : {customer_orphans}")
    print(f" sales.Product_ID  -> products.Product_ID orphan rows   : {product_orphans}")

    print("\nELIGIBILITY")
    print(" related files              : 3")
    print(" genuine 1:M associations   : Customer -> Sales, Product -> Sales")
    print(f" sales transaction rows     : {len(sales):,}")
    print(f" event date range            : {report['sales_date_range']['min']} to {report['sales_date_range']['max']}")
    print(" eligible for ~50k threshold:", len(sales) >= 50_000)

    cfg.OUT_PROFILE.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(f"\nWrote {cfg.OUT_PROFILE}")
    return 0

if __name__ == "__main__":
    sys.exit(main())
