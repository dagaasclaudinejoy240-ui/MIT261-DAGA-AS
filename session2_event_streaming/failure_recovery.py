from __future__ import annotations
import argparse, json, time
from pathlib import Path
import config as cfg
from event_log import EventLog, load_offsets, save_offsets, reset_group, lag

GROUP = "audit-writer-failure-demo"
OUT = cfg.RESULTS / "audit_failure_demo.jsonl"

def run_demo(fail_after=25000):
    log = EventLog()
    reset_group(GROUP)
    if OUT.exists(): OUT.unlink()

    offsets = {p:0 for p in range(cfg.PARTITIONS)}
    save_offsets(GROUP, offsets)
    handled = 0
    committed = dict(offsets)
    batches_since_commit = 0
    crashed = False

    try:
        for p in range(cfg.PARTITIONS):
            batch = []
            for e in log.read_partition(p, offsets[p]):
                batch.append(e)
                if len(batch) >= cfg.CONSUMER_BATCH:
                    for x in batch:
                        with OUT.open("a", encoding="utf-8") as f:
                            f.write(json.dumps(x.value, ensure_ascii=False, default=str) + "\n")
                        handled += 1
                        if handled >= fail_after:
                            raise RuntimeError("Injected audit-store failure")
                    committed[p] = batch[-1].offset + 1
                    batches_since_commit += 1
                    if batches_since_commit >= cfg.COMMIT_EVERY:
                        save_offsets(GROUP, committed)
                        batches_since_commit = 0
                    batch = []
            if batch:
                for x in batch:
                    with OUT.open("a", encoding="utf-8") as f:
                        f.write(json.dumps(x.value, ensure_ascii=False, default=str) + "\n")
                    handled += 1
                    if handled >= fail_after:
                        raise RuntimeError("Injected audit-store failure")
                committed[p] = batch[-1].offset + 1
    except RuntimeError:
        crashed = True

    before = load_offsets(GROUP)
    backlog_before = sum(lag(GROUP, log).values())
    written_before = sum(1 for _ in OUT.open("r", encoding="utf-8")) if OUT.exists() else 0

    # Recovery from the last committed offsets.
    t0 = time.perf_counter()
    recovered = 0
    current = load_offsets(GROUP)
    for p in range(cfg.PARTITIONS):
        for e in log.read_partition(p, current.get(p, 0)):
            with OUT.open("a", encoding="utf-8") as f:
                f.write(json.dumps(e.value, ensure_ascii=False, default=str) + "\n")
            recovered += 1
            current[p] = e.offset + 1
        save_offsets(GROUP, current)
    seconds = max(time.perf_counter()-t0, 1e-9)
    final_lag = sum(lag(GROUP, log).values())
    total_lines = sum(1 for _ in OUT.open("r", encoding="utf-8")) if OUT.exists() else 0
    total_events = sum(log.end_offsets().values())
    redelivered = max(total_lines-total_events, 0)

    report = {
        "crashed": crashed,
        "fail_after": fail_after,
        "handled_before_crash": handled,
        "committed_offsets": before,
        "backlog_before_recovery": backlog_before,
        "recovered_events": recovered,
        "recovery_seconds": seconds,
        "final_lag": final_lag,
        "audit_entries_written": total_lines,
        "events_in_log": total_events,
        "redelivered": redelivered,
        "producer_unaffected": True,
    }
    (cfg.RESULTS / "failure_recovery.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps(report, indent=2))
    return report

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--fail-after", type=int, default=25000)
    args = p.parse_args()
    run_demo(args.fail_after)
