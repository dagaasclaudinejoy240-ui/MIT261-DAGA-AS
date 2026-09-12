from __future__ import annotations
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parent
SESSION1 = REPO / "session1_parallel_compute"
DATASETS = SESSION1 / "datasets"
SESSION1_RESULTS = SESSION1 / "results"

RESULTS = HERE / "results"
LOG_ROOT = HERE / "event_log"
STATE = HERE / "state"

CUSTOMERS_CSV = DATASETS / "customers.csv"
PRODUCTS_CSV = DATASETS / "products.csv"
SALES_CSV = DATASETS / "sales.csv"
BASELINE_CSV = SESSION1_RESULTS / "baseline_result.csv"

TOPIC = "sale.recorded"
PARTITIONS = 4
PARTITION_KEY = "Customer_ID"
EVENT_TIME_FIELD = "Order_Date"
MECHANISM_FIELD = "Payment_Method"
AMOUNT_FIELD = "Total_Amount"
QUANTITY_FIELD = "Quantity"
RATING_FIELD = "Rating"

CONSUMER_BATCH = 500
COMMIT_EVERY = 5
HIGH_VALUE_THRESHOLD = 5000.0
TOLERANCE = 1e-6

RESULTS.mkdir(parents=True, exist_ok=True)
LOG_ROOT.mkdir(parents=True, exist_ok=True)
STATE.mkdir(parents=True, exist_ok=True)
