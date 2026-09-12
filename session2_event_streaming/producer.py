from __future__ import annotations
import argparse, csv, json, time, uuid
from pathlib import Path
import pandas as pd
import config as cfg
from event_log import EventLog, self_test

def _find_col(df, *names):
    lowered = {str(c).lower(): c for c in df.columns}
    for name in names:
        if name in df.columns:
            return name
        if name.lower() in lowered:
            return lowered[name.lower()]
    return None

def build_events():
    if not cfg.SALES_CSV.exists():
        raise FileNotFoundError(f"Missing {cfg.SALES_CSV}")

    sales = pd.read_csv(cfg.SALES_CSV, low_memory=False)
    customers = pd.read_csv(cfg.CUSTOMERS_CSV, low_memory=False) if cfg.CUSTOMERS_CSV.exists() else None
    products = pd.read_csv(cfg.PRODUCTS_CSV, low_memory=False) if cfg.PRODUCTS_CSV.exists() else None

    customer_id = _find_col(sales, "Customer_ID", "customer_id")
    product_id = _find_col(sales, "Product_ID", "product_id")
    order_id = _find_col(sales, "Order_ID", "order_id")
    order_date = _find_col(sales, "Order_Date", "order_date")
    amount = _find_col(sales, "Total_Amount", "total_amount", "Sales", "Amount")
    quantity = _find_col(sales, "Quantity", "quantity")
    rating = _find_col(sales, "Rating", "rating")
    payment_method = _find_col(sales, "Payment_Method", "payment_method", "Payment Method", "PaymentMode", "Payment_Mode")

    required = {"Customer_ID": customer_id, "Order_ID": order_id, "Order_Date": order_date,
                "Total_Amount": amount, "Payment_Method": payment_method}
    missing = [k for k,v in required.items() if v is None]
    if missing:
        raise ValueError(f"Required sales columns not found: {missing}. Found: {list(sales.columns)}")

    enriched = sales.copy()

    if customers is not None:
        ckey = _find_col(customers, "Customer_ID", "customer_id")
        if ckey:
            keep = [ckey]
            for preferred in ("State", "City", "Region", "Customer_Segment", "Segment"):
                c = _find_col(customers, preferred)
                if c and c not in keep:
                    keep.append(c)
            if len(keep) > 1:
                before = len(enriched)
                enriched = enriched.merge(customers[keep].drop_duplicates(ckey), how="left",
                                          left_on=customer_id, right_on=ckey, suffixes=("", "_customer"))
                assert len(enriched) == before

    if products is not None and product_id:
        pkey = _find_col(products, "Product_ID", "product_id")
        if pkey:
            keep = [pkey]
            for preferred in ("Category", "Sub_Category", "Product_Category", "Brand"):
                c = _find_col(products, preferred)
                if c and c not in keep:
                    keep.append(c)
            if len(keep) > 1:
                before = len(enriched)
                enriched = enriched.merge(products[keep].drop_duplicates(pkey), how="left",
                                          left_on=product_id, right_on=pkey, suffixes=("", "_product"))
                assert len(enriched) == before

    parsed = pd.to_datetime(enriched[order_date], errors="coerce")
    enriched = enriched.assign(__event_ts=parsed)
    enriched = enriched.sort_values(["__event_ts", order_id], na_position="last").reset_index(drop=True)

    events = []
    namespace = uuid.UUID("7782aac0-a222-4e39-b4c2-579a4e050aed")
    for _, row in enriched.iterrows():
        oid = str(row[order_id])
        cid = str(row[customer_id])
        payload = {}
        for col in enriched.columns:
            if col == "__event_ts":
                continue
            value = row[col]
            if pd.isna(value):
                payload[str(col)] = None
            elif hasattr(value, "item"):
                try: payload[str(col)] = value.item()
                except Exception: payload[str(col)] = str(value)
            else:
                payload[str(col)] = value
        payload["event_id"] = str(uuid.uuid5(namespace, oid))
        payload["event_type"] = "sale.recorded"
        payload["event_time"] = row["__event_ts"].isoformat() if pd.notna(row["__event_ts"]) else str(row[order_date])
        payload["Customer_ID"] = cid
        payload["Order_ID"] = oid
        payload["Order_Date"] = payload["event_time"]
        payload["Payment_Method"] = str(row[payment_method]) if pd.notna(row[payment_method]) else "Unknown"
        payload["mechanism"] = payload["Payment_Method"]
        if amount:
            try: payload["Total_Amount"] = float(row[amount])
            except Exception: payload["Total_Amount"] = 0.0
        if quantity:
            try: payload["Quantity"] = float(row[quantity])
            except Exception: payload["Quantity"] = 0.0
        if rating:
            try: payload["Rating"] = float(row[rating])
            except Exception: payload["Rating"] = None
        events.append((cid, payload))
    return events

def produce(reset=False):
    log = EventLog()
    if reset:
        log.reset()
    events = build_events()
    t0 = time.perf_counter()
    log.append_many(events)
    seconds = max(time.perf_counter() - t0, 1e-9)
    ends = log.end_offsets()
    counts = [ends[p] for p in range(cfg.PARTITIONS)]
    nonzero = [c for c in counts if c > 0]
    skew = max(nonzero)/min(nonzero) if nonzero else 0
    summary = {
        "events": len(events),
        "seconds": seconds,
        "events_per_second": len(events)/seconds,
        "log_size_bytes": log.size_bytes(),
        "partition_counts": counts,
        "skew_ratio": skew,
        "topic": cfg.TOPIC,
        "partition_key": cfg.PARTITION_KEY,
        "mechanism_field": cfg.MECHANISM_FIELD,
        "event_time_field": cfg.EVENT_TIME_FIELD,
    }
    (cfg.RESULTS / "producer_summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    with (cfg.RESULTS / "session2_throughput.csv").open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["component","events","seconds","events_per_second"])
        w.writeheader()
        w.writerow({"component":"producer","events":len(events),"seconds":f"{seconds:.6f}",
                    "events_per_second":f"{len(events)/seconds:.2f}"})
    print(json.dumps(summary, indent=2))
    return summary

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--reset", action="store_true")
    p.add_argument("--self-test", action="store_true")
    args = p.parse_args()
    if args.self_test:
        result = self_test()
        (cfg.RESULTS / "log_self_test.json").write_text(json.dumps(result, indent=2), encoding="utf-8")
        print(json.dumps(result, indent=2))
    else:
        produce(reset=args.reset)

if __name__ == "__main__":
    main()
