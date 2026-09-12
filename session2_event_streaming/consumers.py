from __future__ import annotations
import argparse, csv, json, time
from collections import defaultdict
import pandas as pd
import config as cfg
from event_log import EventLog, load_offsets, save_offsets, reset_group, lag

GROUPS = ["customer-revenue-projector", "audit-writer", "high-value-alerter"]

def _num(v, default=0.0):
    try:
        if v is None or v == "": return default
        return float(v)
    except Exception:
        return default

def _event_value(v: dict, preferred: str):
    if preferred in v:
        return v[preferred]
    low = preferred.lower()
    for k,val in v.items():
        if str(k).lower() == low:
            return val
    return None

class Consumer:
    def __init__(self, group):
        self.group = group
        self.log = EventLog()
        self.offsets = load_offsets(group)
        self.processed = 0
        self.duplicates_skipped = 0
        self.seen = set()

    def handle(self, event):
        raise NotImplementedError

    def finish(self):
        pass

    def run(self):
        t0 = time.perf_counter()
        batches_since_commit = 0
        pending = dict(self.offsets)
        for p in range(cfg.PARTITIONS):
            batch = []
            for event in self.log.read_partition(p, self.offsets.get(p, 0)):
                batch.append(event)
                if len(batch) >= cfg.CONSUMER_BATCH:
                    self._process_batch(batch)
                    pending[p] = batch[-1].offset + 1
                    batches_since_commit += 1
                    if batches_since_commit >= cfg.COMMIT_EVERY:
                        save_offsets(self.group, pending)
                        self.offsets = dict(pending)
                        batches_since_commit = 0
                    batch = []
            if batch:
                self._process_batch(batch)
                pending[p] = batch[-1].offset + 1
                batches_since_commit += 1
            save_offsets(self.group, pending)
            self.offsets = dict(pending)

        self.finish()
        seconds = max(time.perf_counter()-t0, 1e-9)
        return {
            "group": self.group,
            "processed": self.processed,
            "seconds": seconds,
            "events_per_second": self.processed/seconds if seconds else 0,
            "duplicates_skipped": self.duplicates_skipped,
            "final_lag": sum(lag(self.group, self.log).values()),
        }

    def _process_batch(self, batch):
        for e in batch:
            eid = e.value.get("event_id")
            if eid in self.seen:
                self.duplicates_skipped += 1
                continue
            self.seen.add(eid)
            self.handle(e)
            self.processed += 1

class CustomerRevenueProjector(Consumer):
    def __init__(self, group="customer-revenue-projector"):
        super().__init__(group)
        self.agg = defaultdict(lambda: {"txn_count":0, "revenue_total":0.0, "quantity_total":0.0,
                                        "rating_sum":0.0, "rating_n":0})
    def handle(self, e):
        v = e.value
        cid = str(_event_value(v, "Customer_ID"))
        a = self.agg[cid]
        a["txn_count"] += 1
        a["revenue_total"] += _num(_event_value(v, "Total_Amount"))
        a["quantity_total"] += _num(_event_value(v, "Quantity"))
        r = _event_value(v, "Rating")
        if r not in (None, ""):
            a["rating_sum"] += _num(r)
            a["rating_n"] += 1
    def finish(self):
        rows = []
        for cid,a in sorted(self.agg.items()):
            rows.append({
                "Customer_ID": cid,
                "txn_count": a["txn_count"],
                "revenue_total": a["revenue_total"],
                "revenue_mean": a["revenue_total"]/a["txn_count"] if a["txn_count"] else 0,
                "quantity_total": a["quantity_total"],
                "rating_mean": a["rating_sum"]/a["rating_n"] if a["rating_n"] else "",
            })
        pd.DataFrame(rows).to_csv(cfg.RESULTS / "streamed_customer_revenue.csv", index=False)

class AuditWriter(Consumer):
    def __init__(self, group="audit-writer", output=None):
        super().__init__(group)
        self.output = output or (cfg.RESULTS / "audit_log.jsonl")
        self.handle_count = 0
    def handle(self, e):
        with self.output.open("a", encoding="utf-8") as f:
            f.write(json.dumps(e.value, ensure_ascii=False, default=str) + "\n")
        self.handle_count += 1

class HighValueAlerter(Consumer):
    def __init__(self, group="high-value-alerter"):
        super().__init__(group)
        self.rows = []
    def handle(self, e):
        amount = _num(_event_value(e.value, "Total_Amount"))
        if amount >= cfg.HIGH_VALUE_THRESHOLD:
            self.rows.append({
                "event_id": e.value.get("event_id"),
                "Order_ID": _event_value(e.value, "Order_ID"),
                "Customer_ID": _event_value(e.value, "Customer_ID"),
                "Total_Amount": amount,
                "event_time": e.value.get("event_time"),
            })
    def finish(self):
        pd.DataFrame(self.rows).to_csv(cfg.RESULTS / "high_value_alerts.csv", index=False)

def write_lag_report():
    log = EventLog()
    rows = []
    ends = log.end_offsets()
    for group in GROUPS:
        committed = load_offsets(group)
        for p in range(cfg.PARTITIONS):
            rows.append({
                "group": group,
                "partition": p,
                "end_offset": ends[p],
                "committed": committed.get(p,0),
                "lag": ends[p]-committed.get(p,0),
            })
    pd.DataFrame(rows).to_csv(cfg.RESULTS / "consumer_lag.csv", index=False)

def reset_outputs():
    for g in GROUPS:
        reset_group(g)
    for name in ("streamed_customer_revenue.csv","audit_log.jsonl","high_value_alerts.csv","consumer_lag.csv","consumer_summary.json"):
        p = cfg.RESULTS / name
        if p.exists(): p.unlink()

def run_all(reset=False):
    if reset:
        reset_outputs()
    consumers = [CustomerRevenueProjector(), AuditWriter(), HighValueAlerter()]
    summaries = []
    for c in consumers:
        print(f"Running {c.group}...")
        s = c.run()
        summaries.append(s)
        print(json.dumps(s, indent=2))
    write_lag_report()
    (cfg.RESULTS / "consumer_summary.json").write_text(json.dumps(summaries, indent=2), encoding="utf-8")
    return summaries

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--reset", action="store_true")
    args = ap.parse_args()
    run_all(args.reset)
