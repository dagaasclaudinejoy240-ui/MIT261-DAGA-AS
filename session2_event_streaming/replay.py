from __future__ import annotations
import argparse, json, time
from collections import defaultdict
import pandas as pd
import config as cfg
from event_log import EventLog

def num(v):
    try: return float(v)
    except Exception: return 0.0

def get(v, name):
    if name in v: return v[name]
    for k,x in v.items():
        if str(k).lower() == name.lower(): return x
    return None

def project_all(log):
    agg = defaultdict(lambda:[0,0.0])
    n=0
    for e in log.read_all():
        cid=str(get(e.value,"Customer_ID"))
        agg[cid][0]+=1
        agg[cid][1]+=num(get(e.value,"Total_Amount"))
        n+=1
    return agg,n

def run_demo(partial_from=None):
    log=EventLog()

    t0=time.perf_counter()
    first,n=project_all(log)
    new_consumer_seconds=time.perf_counter()-t0

    second,n2=project_all(log)
    deterministic = first == second and n == n2

    # Late-joining analytics uses the Session 1 sales payment mechanism.
    analytics=defaultdict(lambda:[0,0.0])
    available_dim=cfg.MECHANISM_FIELD
    for e in log.read_all():
        v=e.value
        x=get(v,cfg.MECHANISM_FIELD)
        if x in (None,""):
            x=get(v,"mechanism")
        x="Unknown" if x in (None,"") else str(x)
        analytics[x][0]+=1
        analytics[x][1]+=num(get(v,cfg.AMOUNT_FIELD))
    rows=[{"dimension":cfg.MECHANISM_FIELD,"value":k,"events":c,"revenue":r}
          for k,(c,r) in sorted(analytics.items(), key=lambda kv:(-kv[1][1],kv[0]))[:100]]
    pd.DataFrame(rows).to_csv(cfg.RESULTS/"late_joining_analytics.csv",index=False)

    all_events=list(log.read_all())
    if partial_from is None:
        times=[str(e.value.get("event_time","")) for e in all_events if e.value.get("event_time")]
        partial_from=sorted(times)[int(len(times)*0.8)] if times else ""
    partial=[e for e in all_events if str(e.value.get("event_time","")) >= str(partial_from)]

    report={
        "new_consumer_events":n,
        "new_consumer_seconds":new_consumer_seconds,
        "rewind_deterministic":deterministic,
        "replay_groups":len(first),
        "offline_catchup_events":n,
        "partial_from":partial_from,
        "partial_events":len(partial),
        "total_events":len(all_events),
        "analytics_dimension":available_dim or "none",
        "analytics_rows":len(rows),
    }
    (cfg.RESULTS/"replay_report.json").write_text(json.dumps(report,indent=2),encoding="utf-8")
    print(json.dumps(report,indent=2))
    return report

if __name__=="__main__":
    ap=argparse.ArgumentParser()
    ap.add_argument("--partial-from",default=None)
    args=ap.parse_args()
    run_demo(args.partial_from)
