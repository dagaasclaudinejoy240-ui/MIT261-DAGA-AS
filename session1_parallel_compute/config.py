"""
Configuration for MIT 261 Session 1
Dataset: Indian E-Commerce Sales Analytics Dataset

Core relational files:
    customers.csv  - Entity
    products.csv   - Entity
    sales.csv      - Event / transaction file
"""

from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "datasets"
RESULTS_DIR = BASE_DIR / "results"
DOCS_DIR = BASE_DIR / "docs"
ARCH_DIR = BASE_DIR / "architecture"

for directory in (DATA_DIR, RESULTS_DIR, DOCS_DIR, ARCH_DIR):
    directory.mkdir(parents=True, exist_ok=True)

DATASET_TITLE = "Indian E-Commerce Sales Analytics Dataset"
KAGGLE_URL = "https://www.kaggle.com/datasets/jatinkhandelwal112/indian-e-commerce-sales-analytics-dataset"

CUSTOMERS_FILE = DATA_DIR / "customers.csv"
PRODUCTS_FILE = DATA_DIR / "products.csv"
SALES_FILE = DATA_DIR / "sales.csv"

FILES = {
    "customers": CUSTOMERS_FILE,
    "products": PRODUCTS_FILE,
    "sales": SALES_FILE,
}

# Session 1 workload
PARTITION_KEY = "Customer_ID"
METRIC_FIELD = "Total_Amount"
EVENT_TIME_FIELD = "Order_Date"

PARTITION_SETTINGS = (2, 4, 8)
BENCHMARK_REPEATS = 3
BASELINE_REPEATS = 5
CHOSEN_PARTITIONS = 4
TOLERANCE = 1e-6

SPARK_MASTER = "local[*]"
SPARK_DRIVER_MEMORY = "3g"
SPARK_SHUFFLE_PARTITIONS = "8"
SPARK_APP_NAME = "MIT261-Indian-Ecommerce-Session1"

OUT_PROFILE = RESULTS_DIR / "file_profile.json"
OUT_JOINED = RESULTS_DIR / "working_dataset.parquet"
OUT_BASELINE = RESULTS_DIR / "baseline_result.csv"
OUT_FINAL = RESULTS_DIR / "customer_revenue.parquet"
OUT_VALIDATION = RESULTS_DIR / "validation_report.json"
OUT_BENCHMARK = RESULTS_DIR / "session1_benchmark.csv"
OUT_PARTITIONS = RESULTS_DIR / "partition_sizes.csv"
OUT_PARTITION_STRATEGY = RESULTS_DIR / "partition_strategy.json"

def require_files():
    missing = [str(path) for path in FILES.values() if not path.exists()]
    if missing:
        raise FileNotFoundError(
            "Missing dataset file(s):\n- " + "\n- ".join(missing) +
            "\n\nPlace customers.csv, products.csv, and sales.csv inside the datasets folder."
        )

def build_spark():
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder
        .appName(SPARK_APP_NAME)
        .master(SPARK_MASTER)
        .config("spark.driver.memory", SPARK_DRIVER_MEMORY)
        .config("spark.sql.shuffle.partitions", SPARK_SHUFFLE_PARTITIONS)
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark

def banner(title):
    print("\n" + "=" * 72)
    print(title)
    print("=" * 72)
