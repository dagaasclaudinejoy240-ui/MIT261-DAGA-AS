from __future__ import annotations
import json
import pandas as pd
import config as cfg

def norm_id(s):
    return s.astype(str).str.replace(r"\.0$", "", regex=True)

def reconcile():
    stream_path=cfg.RESULTS/"streamed_customer_revenue.csv"
    if not cfg.BASELINE_CSV.exists():
        raise FileNotFoundError(f"Session 1 baseline missing: {cfg.BASELINE_CSV}")
    if not stream_path.exists():
        raise FileNotFoundError(f"Stream projection missing: {stream_path}")

    b=pd.read_csv(cfg.BASELINE_CSV)
    s=pd.read_csv(stream_path)
    if "Customer_ID" not in b.columns or "Customer_ID" not in s.columns:
        raise ValueError("Customer_ID must exist in both Session 1 and Session 2 outputs.")

    b["Customer_ID"]=norm_id(b["Customer_ID"])
    s["Customer_ID"]=norm_id(s["Customer_ID"])

    bset=set(b["Customer_ID"]); sset=set(s["Customer_ID"])
    sets_identical=bset==sset
    merged=b.merge(s,on="Customer_ID",suffixes=("_batch","_stream"),how="outer",indicator=True)

    def maxdiff(col):
        a=f"{col}_batch"; c=f"{col}_stream"
        if a not in merged.columns or c not in merged.columns: return None
        x=pd.to_numeric(merged[a],errors="coerce")
        y=pd.to_numeric(merged[c],errors="coerce")
        return float((x-y).abs().fillna(0).max())

    count_diff=maxdiff("txn_count")
    revenue_total_diff=maxdiff("revenue_total")
    revenue_mean_diff=maxdiff("revenue_mean")
    quantity_diff=maxdiff("quantity_total")
    rating_diff=maxdiff("rating_mean")

    exact_count = (count_diff == 0) if count_diff is not None else False
    numeric_ok = all(
        d is None or d <= cfg.TOLERANCE
        for d in (revenue_total_diff,revenue_mean_diff,quantity_diff,rating_diff)
    )
    passed=sets_identical and exact_count and numeric_ok

    report={
        "result":"PASS" if passed else "FAIL",
        "batch_groups":len(b),
        "stream_groups":len(s),
        "group_sets_identical":sets_identical,
        "batch_records":int(pd.to_numeric(b.get("txn_count",pd.Series(dtype=float)),errors="coerce").fillna(0).sum()),
        "stream_records":int(pd.to_numeric(s.get("txn_count",pd.Series(dtype=float)),errors="coerce").fillna(0).sum()),
        "max_txn_count_diff":count_diff,
        "max_revenue_total_diff":revenue_total_diff,
        "max_revenue_mean_diff":revenue_mean_diff,
        "max_quantity_total_diff":quantity_diff,
        "max_rating_mean_diff":rating_diff,
        "tolerance":cfg.TOLERANCE,
    }
    (cfg.RESULTS/"reconciliation_report.json").write_text(json.dumps(report,indent=2),encoding="utf-8")
    print(json.dumps(report,indent=2))
    if not passed:
        raise SystemExit(1)
    return report

if __name__=="__main__":
    reconcile()
