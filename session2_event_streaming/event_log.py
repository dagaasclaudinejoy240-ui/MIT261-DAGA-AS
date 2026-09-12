from __future__ import annotations
import json
import shutil
import time
import zlib
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator
import config as cfg

@dataclass
class Event:
    partition: int
    offset: int
    key: str
    value: dict

def partition_for(key: str, num_partitions: int = cfg.PARTITIONS) -> int:
    return zlib.crc32(str(key).encode("utf-8")) % num_partitions

class EventLog:
    def __init__(self, topic: str = cfg.TOPIC, partitions: int = cfg.PARTITIONS):
        self.topic = topic
        self.partitions = partitions
        self.dir = cfg.LOG_ROOT / topic
        self.dir.mkdir(parents=True, exist_ok=True)
        for p in range(partitions):
            self._path(p).touch(exist_ok=True)

    def _path(self, p: int) -> Path:
        return self.dir / f"partition_{p}.jsonl"

    def reset(self):
        """
        Reset the topic without deleting its directory.

        On Windows (especially inside OneDrive), shutil.rmtree() can fail when
        Explorer, antivirus, indexing, or sync briefly holds a handle to the
        event_log directory. Truncating the partition files provides the same
        logical reset while avoiding that directory-lock problem.
        """
        self.dir.mkdir(parents=True, exist_ok=True)

        for p in range(self.partitions):
            path = self._path(p)

            # Retry briefly because OneDrive/antivirus may momentarily lock a file.
            last_error = None
            for attempt in range(8):
                try:
                    with path.open("w", encoding="utf-8"):
                        pass
                    last_error = None
                    break
                except PermissionError as exc:
                    last_error = exc
                    time.sleep(0.20 * (attempt + 1))

            if last_error is not None:
                raise PermissionError(
                    f"Could not reset event-log partition file after retries: {path}. "
                    "Close any program that has the file open and pause OneDrive sync briefly."
                ) from last_error

    def end_offset(self, p: int) -> int:
        with self._path(p).open("r", encoding="utf-8") as f:
            return sum(1 for _ in f)

    def end_offsets(self) -> dict[int, int]:
        return {p: self.end_offset(p) for p in range(self.partitions)}

    def append(self, key: str, value: dict) -> Event:
        p = partition_for(key, self.partitions)
        offset = self.end_offset(p)
        rec = {"offset": offset, "key": str(key), "value": value}
        with self._path(p).open("a", encoding="utf-8") as f:
            f.write(json.dumps(rec, ensure_ascii=False, default=str) + "\n")
        return Event(p, offset, str(key), value)

    def append_many(self, items):
        handles = {p: self._path(p).open("a", encoding="utf-8") for p in range(self.partitions)}
        offsets = self.end_offsets()
        try:
            for key, value in items:
                p = partition_for(key, self.partitions)
                rec = {"offset": offsets[p], "key": str(key), "value": value}
                handles[p].write(json.dumps(rec, ensure_ascii=False, default=str) + "\n")
                offsets[p] += 1
        finally:
            for h in handles.values():
                h.close()

    def read_partition(self, p: int, start: int = 0) -> Iterator[Event]:
        with self._path(p).open("r", encoding="utf-8") as f:
            for i, line in enumerate(f):
                if i < start:
                    continue
                rec = json.loads(line)
                yield Event(p, int(rec["offset"]), rec["key"], rec["value"])

    def read_all(self, starts: dict[int, int] | None = None):
        starts = starts or {}
        for p in range(self.partitions):
            yield from self.read_partition(p, starts.get(p, 0))

    def size_bytes(self) -> int:
        return sum(self._path(p).stat().st_size for p in range(self.partitions))

def _state_path(group: str) -> Path:
    safe = group.replace("/", "_")
    return cfg.STATE / f"{safe}.json"

def load_offsets(group: str) -> dict[int, int]:
    path = _state_path(group)
    if not path.exists():
        return {p: 0 for p in range(cfg.PARTITIONS)}
    raw = json.loads(path.read_text(encoding="utf-8"))
    return {int(k): int(v) for k, v in raw.items()}

def save_offsets(group: str, offsets: dict[int, int]):
    _state_path(group).write_text(
        json.dumps({str(k): int(v) for k, v in offsets.items()}, indent=2),
        encoding="utf-8",
    )

def reset_group(group: str):
    p = _state_path(group)
    if p.exists():
        p.unlink()

def lag(group: str, log: EventLog | None = None) -> dict[int, int]:
    log = log or EventLog()
    committed = load_offsets(group)
    ends = log.end_offsets()
    return {p: ends[p] - committed.get(p, 0) for p in range(cfg.PARTITIONS)}

def self_test():
    import tempfile
    original = cfg.LOG_ROOT
    # Use the real implementation in a temporary topic inside configured root.
    topic = "__self_test__"
    log = EventLog(topic=topic, partitions=4)
    log.reset()

    assert partition_for("CUST-42", 4) == partition_for("CUST-42", 4)
    for i in range(8):
        log.append("CUST-42", {"event_id": f"e{i}", "n": i})

    events = list(log.read_partition(partition_for("CUST-42", 4)))
    assert [e.offset for e in events] == list(range(len(events)))

    again = list(log.read_partition(partition_for("CUST-42", 4)))
    assert [e.value["event_id"] for e in events] == [e.value["event_id"] for e in again]

    g1, g2 = "__test_a__", "__test_b__"
    reset_group(g1); reset_group(g2)
    ends = log.end_offsets()
    save_offsets(g1, ends)
    assert sum(lag(g1, log).values()) == 0
    assert sum(lag(g2, log).values()) == 8

    save_offsets(g1, {p: 0 for p in range(4)})
    assert sum(lag(g1, log).values()) == 8

    reopened = EventLog(topic=topic, partitions=4)
    assert sum(reopened.end_offsets().values()) == 8

    # Best-effort cleanup; avoid failing the self-test because Windows/OneDrive
    # temporarily holds a directory handle.
    try:
        for p in range(log.partitions):
            path = log._path(p)
            if path.exists():
                path.unlink()
        log.dir.rmdir()
    except OSError:
        pass
    reset_group(g1); reset_group(g2)

    return {
        "Stable key routing": "PASS",
        "Ordering within a partition": "PASS",
        "Non-destructive read": "PASS",
        "Consumer group isolation": "PASS",
        "Replay": "PASS",
        "Durability": "PASS",
    }

if __name__ == "__main__":
    print(json.dumps(self_test(), indent=2))
