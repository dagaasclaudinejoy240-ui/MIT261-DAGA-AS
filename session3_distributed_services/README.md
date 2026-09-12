# EcomInsight — Session 3 (PostgreSQL + Session 1/2 continuity)

This Session 3 version uses PostgreSQL as its live service data source and reconciles against the actual output artifacts from Sessions 1 and 2.

## Runtime architecture

`PostgreSQL indian_ecommerce` → six Session 3 services → JSON / protobuf / gRPC → Session 3 customer-revenue output → reconciliation against Session 1 Spark + Session 2 event-stream results.

### PostgreSQL source
- Host: `localhost` by default
- Port: `5432` by default
- Database: `indian_ecommerce`
- Schema: `public`
- Tables: `customers`, `products`, `sales`

### Earlier-session artifacts used directly
- Session 1: `../session1_parallel_compute/results/baseline_result.csv`
- Session 2: `../session2_event_streaming/results/streamed_customer_revenue.csv`

Session 3 intentionally fails with a clear message if either earlier-session artifact is missing. It does not fabricate or replay those results.

## First-time setup

1. Create/activate the Session 3 virtual environment.
2. Install requirements:

```powershell
python -m pip install -r requirements.txt
```

3. Copy `.env.example` to `.env`:

```powershell
Copy-Item .env.example .env
```

4. Open `.env` and replace `CHANGE_ME` with your PostgreSQL password. Do not submit or commit `.env`.

## Verify the database

```powershell
python database.py
```

Expected row counts are 40,000 customers, 2,000 products, and 250,000 sales.

## Run Session 3

```powershell
python contracts.py
python test_wire.py
python services.py
python run_pipeline.py
python session3_dashboard.py
```

Or double-click `RUN_SESSION3_GUI.bat` after the environment is configured.

Benchmark timings are measured on the current machine and should be rerun after changing the data source.
