import subprocess, sys
from pathlib import Path

HERE=Path(__file__).resolve().parent
STEPS=[
    ["producer.py","--self-test"],
    ["producer.py","--reset"],
    ["consumers.py","--reset"],
    ["reconcile.py"],
    ["failure_recovery.py"],
    ["replay.py"],
]
for step in STEPS:
    print("\n"+"="*72)
    print(" ".join(step))
    print("="*72)
    rc=subprocess.call([sys.executable,*step],cwd=HERE)
    if rc:
        raise SystemExit(rc)
print("\nSESSION 2 PIPELINE COMPLETE")
