ECOMINSIGHT — SESSION 2 EVENT STREAMING
======================================

This package is adapted to the Indian E-Commerce Session 1 dataset.

It follows the Session 2 durable-log concepts from the supplied MIT 261 material:
topic, partition, offset, independent consumer groups, at-least-once recovery,
replay, and reconciliation against Session 1.

PROJECT PLACEMENT
-----------------
Place this folder beside your existing session1_parallel_compute folder:

MIT261-DAGA-AS/
├─ session1_parallel_compute/
│  ├─ datasets/
│  │  ├─ customers.csv
│  │  ├─ products.csv
│  │  └─ sales.csv
│  └─ results/
│     └─ baseline_result.csv
└─ session2_event_streaming/
   ├─ gui.py
   ├─ event_log.py
   ├─ producer.py
   ├─ consumers.py
   ├─ failure_recovery.py
   ├─ replay.py
   ├─ reconcile.py
   └─ ...

DEPENDENCIES
------------
Use the same virtual environment as Session 1:

    python -m pip install pandas

No Kafka broker is required for this version. The event log is file-backed, so
the GUI demonstrates the same streaming abstractions without needing a broker.

RUN
---
    python session2_event_streaming\gui.py

or double-click RUN_GUI.bat from inside the session2_event_streaming folder.

ADAPTATION TO YOUR DATASET
--------------------------
Topic:          sale.recorded
Partition key:  Customer_ID
Partitions:     4
Event time:     Order_Date
Measure:        Total_Amount

Main consumers:
1. customer-revenue-projector
   Rebuilds the Session 1 per-customer metrics incrementally.

2. audit-writer
   Writes an independent append-only audit log.

3. high-value-alerter
   Writes high-value sales to high_value_alerts.csv.

Reconciliation compares streamed_customer_revenue.csv with the existing
Session 1 results/baseline_result.csv.

IMPORTANT
---------
The GUI intentionally does not show old/stale result files as current results
on startup. A tab is populated only after the corresponding stage has been run
successfully during the current GUI session.


DATASET-SPECIFIC SESSION 2 MAPPING
----------------------------------
Event source       : session1_parallel_compute/datasets/sales.csv
Topic              : sale.recorded
Event identifier   : Order_ID
Partition key      : Customer_ID
Event-time field   : Order_Date
Mechanism field    : Payment_Method
Revenue measure    : Total_Amount

The producer sorts the Session 1 sales rows chronologically by Order_Date before
appending them to the durable log. Each event carries Payment_Method (also exposed
as 'mechanism') so Replay analytics reports revenue and event counts by payment
method. Customer_ID remains the partition key so the Session 2 stream stays aligned
with the Session 1 customer-level baseline used for reconciliation.
