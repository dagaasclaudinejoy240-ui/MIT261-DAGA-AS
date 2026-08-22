# MIT261-DAGA-AS — Session 1 Parallel Compute

## Project Title
**Indian E-Commerce Sales Analytics — Partitioned and Bounded Parallel Processing**

## Course
**MIT 261 – Parallel and Distributed Systems**  
**Session 1 – Foundations in In-Memory Cluster Compute**

This repository contains the Session 1 implementation for the MIT 261 individual capstone. The activity requires a multi-file relational Kaggle dataset, documented joins, partitioning, bounded parallelism, benchmarking, correctness validation, partition-skew analysis, and continuity into Sessions 2 to 6.

## Dataset

**Dataset:** Indian E-Commerce Sales Analytics Dataset  
**Source:** https://www.kaggle.com/datasets/jatinkhandelwal112/indian-e-commerce-sales-analytics-dataset

### Core files used
- `customers.csv` — Entity
- `products.csv` — Entity
- `sales.csv` — Event / transactional file

### Dataset volume
- `customers.csv` — 40,000 rows
- `products.csv` — 2,000 rows
- `sales.csv` — 250,000 transactional rows

The dataset satisfies the Session 1 requirement for at least three related files and approximately 50,000 or more transactional rows.

## Relational Model

```text
Customer 1 ─────< Sales
Product  1 ─────< Sales
```

### Keys

**customers.csv**
- Primary key: `Customer_ID`

**products.csv**
- Primary key: `Product_ID`

**sales.csv**
- Primary key: `Order_ID`
- Foreign key: `Customer_ID` → `customers.Customer_ID`
- Foreign key: `Product_ID` → `products.Product_ID`

Foreign-key checks showed no orphan rows for either relationship.

## Event Time and Partition Key

- **Partition key:** `Customer_ID`
- **Entity owner:** Customer
- **Event-time field:** `Order_Date`
- **Metric field:** `Total_Amount`

`Customer_ID` was selected because it survives both joins, is owned by the Customer entity, and allows customer-level aggregation to be processed independently.

Observed customer-level sales distribution:
- Distinct `Customer_ID` values: approximately 39,914
- Minimum records per customer: 1
- Median records per customer: 6
- Maximum records per customer: 19
- Approximate skew ratio: 19:1

## Computational Workload

The workload computes the following per `Customer_ID`:
- Transaction count
- Total revenue
- Mean revenue
- Total quantity purchased
- Average rating

Expected output fields:

```text
Customer_ID
txn_count
revenue_total
revenue_mean
quantity_total
rating_mean
```

## Join Path

```text
sales.csv
  → customers.csv on Customer_ID
  → products.csv on Product_ID
```

Both joins use a many-to-one relationship from the Sales event file to the corresponding entity file. The implementation validates that the join does not duplicate or lose transactional rows.

## Technologies

- Python 3.x
- pandas
- PySpark
- pyarrow
- Graphviz
- Git / GitHub
- VS Code
- Windows

## Repository Structure

```text
MIT261-DAGA-AS/
└── session1_parallel_compute/
    ├── datasets/
    │   ├── customers.csv
    │   ├── products.csv
    │   └── sales.csv
    ├── results/
    │   ├── file_profile.json
    │   ├── working_dataset.parquet
    │   ├── baseline_result.csv
    │   ├── customer_revenue.parquet
    │   ├── validation_report.json
    │   ├── session1_benchmark.csv
    │   ├── partition_sizes.csv
    │   └── partition_strategy.json
    ├── docs/
    │   ├── entity-model-session1.dot
    │   └── entity-model-session1.png
    ├── architecture/
    │   ├── architecture-session1.dot
    │   └── architecture-session1.png
    ├── config.py
    ├── profile_files.py
    ├── load_and_join.py
    ├── partition_strategy.py
    ├── sequential_baseline.py
    ├── parallel_compute.py
    ├── benchmark.py
    ├── partition_analysis.py
    ├── render_diagrams.py
    └── RUN_ORDER.txt
```

## Setup

### 1. Open the Session 1 folder

```powershell
cd "C:\Users\JAMESROY\OneDrive\Desktop\NDMU MASTERAL\MIT261-DAGA-AS\session1_parallel_compute"
```

### 2. Activate the virtual environment

```powershell
.\.venv\Scripts\Activate.ps1
```

If PowerShell blocks activation:

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy RemoteSigned
.\.venv\Scripts\Activate.ps1
```

### 3. Install required Python packages

```powershell
pip install pandas pyspark pyarrow
```

### 4. Verify Graphviz

```powershell
dot -V
```

If Graphviz is installed but not found in the VS Code terminal:

```powershell
$env:Path += ";C:\Program Files\Graphviz\bin"
dot -V
```

## Execution Order

Run the scripts in this order:

```powershell
python profile_files.py
python load_and_join.py
python partition_strategy.py
python sequential_baseline.py
python parallel_compute.py
python benchmark.py
python partition_analysis.py
python render_diagrams.py
```

### Script outputs

`profile_files.py`
- profiles every dataset file
- checks row counts, columns, data types, nulls, primary keys, and foreign keys
- writes `results/file_profile.json`

`load_and_join.py`
- loads and joins the three relational files
- validates many-to-one relationships
- reconciles row counts
- writes `results/working_dataset.parquet`

`partition_strategy.py`
- evaluates partition-key candidates
- records the selected `Customer_ID`
- writes `results/partition_strategy.json`

`sequential_baseline.py`
- runs the pandas reference workload
- writes `results/baseline_result.csv`

`parallel_compute.py`
- performs the same workload in PySpark
- explicitly repartitions by `Customer_ID`
- validates Spark results against pandas
- writes `results/customer_revenue.parquet`
- writes `results/validation_report.json`

`benchmark.py`
- benchmarks 2, 4, and 8 Spark partitions against the sequential baseline
- writes `results/session1_benchmark.csv`

`partition_analysis.py`
- measures physical Spark partition sizes
- writes `results/partition_sizes.csv`

`render_diagrams.py`
- generates the entity model and architecture diagram
- writes:
  - `docs/entity-model-session1.png`
  - `architecture/architecture-session1.png`

## Bounded Parallelism

The project benchmarks:

```text
2 partitions
4 partitions
8 partitions
```

The final configured run uses:

```text
CHOSEN_PARTITIONS = 4
```

The benchmark results should be used to justify the best-performing setting on the actual machine used for testing.

## Join Strategy

The Spark implementation broadcasts the smaller entity tables:
- `customers.csv`
- `products.csv`

The larger `sales.csv` file remains the main transactional dataset.

Broadcast joins are used to reduce unnecessary shuffle cost when the entity tables are small enough to replicate across workers.

## Correctness Validation

The implementation checks:
- group counts
- exact transaction counts
- exact quantity totals
- revenue totals within numeric tolerance
- mean revenue within numeric tolerance
- average rating within numeric tolerance

Floating-point values use a small tolerance because Spark and pandas may aggregate in different orders.

Validation output:

```text
results/validation_report.json
```

## Partition Balance and Skew

The project records:
- distinct partition-key values
- minimum records per key
- median records per key
- maximum records per key
- largest key
- physical Spark partition sizes

Possible mitigation strategies for a larger workload include:
- repartitioning
- salting hot keys
- range partitioning
- composite keys
- alternative natural partition keys

## Session 1 Outputs

```text
results/file_profile.json
results/working_dataset.parquet
results/baseline_result.csv
results/customer_revenue.parquet
results/validation_report.json
results/session1_benchmark.csv
results/partition_sizes.csv
results/partition_strategy.json
docs/entity-model-session1.png
architecture/architecture-session1.png
```

## Multi-Session Continuity

### Session 2 – Event Streaming and Replay
`sales.csv` supplies the event stream.
- Event-time field: `Order_Date`
- Replay order: chronological by `Order_Date`
- `Order_ID` can provide deterministic ordering for records on the same date

### Session 3 – High Performance Inter-Service Communication
Possible service/API boundaries:
- Customer service
- Product service
- Sales / Revenue service

### Session 4 – Distributed Analytics and Real-Time Visualization
Possible analytics:
- daily revenue
- monthly revenue
- transaction counts
- customer spending windows
- product/category trends

### Session 5 – Container Orchestration
The Session 1 processing component can be containerised with:
- Python
- Java
- PySpark
- pandas
- pyarrow
- dataset/input mount
- results/output mount

### Session 6 – Infrastructure-as-Code, CI/CD, and Monitoring
Automation can reproduce and verify:
- profiling
- join reconciliation
- parallel execution
- correctness validation
- benchmark regression
- output generation

## Reproducibility Notes

For valid benchmark comparisons:
- use the same machine
- use the same input files
- use the same join path
- use the same workload
- use the same output calculation
- repeat benchmark conditions where feasible
- record unusual memory, GC, or system-contention events

## Git / GitHub

After modifying project files:

```powershell
git add .
git commit -m "Update Session 1 project"
git push
```

To obtain the commit hash:

```powershell
git rev-parse HEAD
```

## Academic Integrity and AI-Use Disclosure

ChatGPT (OpenAI) was used to assist with:
- interpreting the Session 1 activity requirements
- troubleshooting Python, PySpark, Graphviz, Git, and Windows issues
- refining validation and documentation
- preparing README documentation based on the project's dataset and scripts

The student remains responsible for executing the code, reviewing the outputs, validating the joins and benchmarks, and verifying the final submission.

## References

- Kaggle — Indian E-Commerce Sales Analytics Dataset
- MIT 261 Session 1 Laboratory Activity — Foundations in In-Memory Cluster Compute
- Python
- pandas
- PySpark
- Graphviz
- Git / GitHub
