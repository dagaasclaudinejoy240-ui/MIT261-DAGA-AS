import os
from pathlib import Path

BASE = Path(__file__).resolve().parent
PROJECT_ROOT = BASE.parent
RESULTS_DIR = BASE / 'results'
RESULTS_DIR.mkdir(exist_ok=True)

# Session continuity: use the actual sibling outputs produced by Sessions 1 and 2.
SESSION1_BASELINE = PROJECT_ROOT / 'session1_parallel_compute' / 'results' / 'baseline_result.csv'
SESSION2_REFERENCE = PROJECT_ROOT / 'session2_event_streaming' / 'results' / 'streamed_customer_revenue.csv'
SESSION3_OUTPUT = RESULTS_DIR / 'session3_customer_revenue.csv'

# PostgreSQL is the primary Session 3 runtime data source.
PGHOST = os.getenv('PGHOST', 'localhost')
PGPORT = int(os.getenv('PGPORT', '5432'))
PGDATABASE = os.getenv('PGDATABASE', 'indian_ecommerce')
PGUSER = os.getenv('PGUSER', 'postgres')
PGPASSWORD = os.getenv('PGPASSWORD', '')
PGSCHEMA = os.getenv('PGSCHEMA', 'public')

HTTP_HOST = '127.0.0.1'
HTTP_PORT = 8765
GRPC_HOST = '127.0.0.1'
GRPC_PORT = 50071
TOLERANCE = 1e-6
BENCH_WARMUP = 20
BENCH_REPEATS = 120
BATCH_ROWS = 50
STREAM_ROWS = 1000
