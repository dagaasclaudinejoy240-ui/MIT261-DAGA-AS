THIS VERSION IS BASED ON THE ACTUAL UPLOADED DATASET
===================================================

Dataset contents inspected:
    customers.csv : 40,000 rows x 15 columns
    products.csv  : 2,000 rows x 12 columns
    sales.csv     : 250,000 rows x 21 columns

Exact primary/foreign keys:
    customers.Customer_ID       PK
    products.Product_ID         PK
    sales.Order_ID              PK
    sales.Customer_ID           FK -> customers.Customer_ID
    sales.Product_ID            FK -> products.Product_ID

Observed foreign-key integrity:
    sales.Customer_ID -> customers.Customer_ID : 0 orphan rows
    sales.Product_ID  -> products.Product_ID    : 0 orphan rows

Event date:
    sales.Order_Date

Date range in uploaded dataset:
    2024-06-01 to 2026-06-30

Chosen partition key:
    Customer_ID

Observed Customer_ID distribution in sales:
    distinct : 39,914
    min      : 1 sale
    median   : 6 sales
    max      : 19 sales
    max/min  : 19:1

The dataset satisfies the Session 1 volume requirement because sales.csv
contains 250,000 transactional rows.
