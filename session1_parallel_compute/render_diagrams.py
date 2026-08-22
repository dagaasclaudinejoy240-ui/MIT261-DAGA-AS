"""
Creates:
    docs/entity-model-session1.png
    architecture/architecture-session1.png
"""

import shutil
import subprocess
import sys
import config as cfg

NAVY = "#1F3864"
BLUE = "#2E74B5"
LIGHT = "#D9EAF7"
AMBER = "#C55A11"
GREY = "#767171"

ENTITY = f"""
digraph EntityModel {{
  rankdir=LR;
  bgcolor="white";
  splines=polyline;
  fontname="Helvetica";
  labelloc="t";
  fontsize=17;
  label=<<b>Indian E-Commerce Sales — Session 1 Entity Model</b><br/>
  <font point-size="11">two genuine 1:M relationships · metric = Total_Amount</font><br/>>;

  node [shape=plaintext fontname="Helvetica"];
  edge [color="{BLUE}" fontname="Helvetica" fontsize=10 penwidth=1.6];

  Customer [label=<
    <table border="0" cellborder="1" cellspacing="0" cellpadding="5">
      <tr><td bgcolor="{LIGHT}"><b>Customer</b><br/><font point-size="9">Entity · customers.csv</font></td></tr>
      <tr><td align="left"><b>Customer_ID : String «PK» «partitionKey»</b></td></tr>
      <tr><td align="left">Customer_Name : String</td></tr>
      <tr><td align="left">State : String</td></tr>
      <tr><td align="left">Customer_Tier : String</td></tr>
    </table>>];

  Sale [label=<
    <table border="0" cellborder="1" cellspacing="0" cellpadding="5">
      <tr><td bgcolor="{LIGHT}"><b>Sale</b><br/><font point-size="9">Event · sales.csv</font></td></tr>
      <tr><td align="left"><b>Order_ID : String «PK»</b></td></tr>
      <tr><td align="left">Customer_ID : String «FK»</td></tr>
      <tr><td align="left">Product_ID : String «FK»</td></tr>
      <tr><td align="left">Order_Date : Date <b>«eventTime»</b></td></tr>
      <tr><td align="left">Quantity : Integer</td></tr>
      <tr><td align="left" bgcolor="#FFF2CC"><b>Total_Amount : Double «metricField»</b></td></tr>
      <tr><td align="left">Order_Status : String</td></tr>
      <tr><td align="left">Payment_Mode : String</td></tr>
    </table>>];

  Product [label=<
    <table border="0" cellborder="1" cellspacing="0" cellpadding="5">
      <tr><td bgcolor="{LIGHT}"><b>Product</b><br/><font point-size="9">Entity · products.csv</font></td></tr>
      <tr><td align="left"><b>Product_ID : String «PK»</b></td></tr>
      <tr><td align="left">Product_Name : String</td></tr>
      <tr><td align="left">Category : String</td></tr>
      <tr><td align="left">Brand : String</td></tr>
      <tr><td align="left">Selling_Price : Double</td></tr>
    </table>>];

  Customer -> Sale [dir=both arrowtail=none arrowhead=none xlabel="1          0..*\\nCustomer_ID"];
  Product -> Sale [dir=both arrowtail=none arrowhead=none xlabel="1          0..*\\nProduct_ID"];
}}
"""

ARCHITECTURE = f"""
digraph Architecture {{
  rankdir=LR;
  bgcolor="white";
  splines=ortho;
  fontname="Helvetica";
  labelloc="t";
  fontsize=17;
  label=<<b>Indian E-Commerce Sales — Session 1 In-Memory Parallel Compute</b><br/>
  <font point-size="11">PySpark local mode · bounded parallelism = {cfg.CHOSEN_PARTITIONS}</font><br/>>;

  node [shape=box style="rounded,filled" fontname="Helvetica" fontsize=10];
  edge [color="{BLUE}"];

  sales [label="sales.csv\\nEvent · 250,000 rows" fillcolor="{LIGHT}"];
  customers [label="customers.csv\\nEntity · 40,000 rows" fillcolor="{LIGHT}"];
  products [label="products.csv\\nEntity · 2,000 rows" fillcolor="{LIGHT}"];

  profile [label="profile_files.py\\nprofiling + FK checks" fillcolor="#FFF2CC"];
  join [label="load_and_join.py\\nvalidated 1:M joins" fillcolor="#FFF2CC"];
  repart [label="repartition({cfg.CHOSEN_PARTITIONS}, 'Customer_ID')" fillcolor="#E2EFDA"];
  agg [label="parallel_compute.py\\ncount · revenue · quantity · rating" fillcolor="#E2EFDA"];
  base [label="sequential_baseline.py\\npandas reference" fillcolor="#FCE4D6"];
  valid [label="correctness validation\\ncounts exact · floats within tolerance" fillcolor="#FCE4D6"];
  output [label="customer_revenue.parquet" fillcolor="{LIGHT}" color="{NAVY}"];
  bench [label="session1_benchmark.csv" fillcolor="{LIGHT}" color="{NAVY}"];

  sales -> profile;
  customers -> profile;
  products -> profile;
  profile -> join;
  join -> repart;
  repart -> agg;
  join -> base;
  agg -> valid;
  base -> valid;
  valid -> output;
  agg -> bench;
}}
"""

def render(dot_text, output):
    dot = shutil.which("dot")
    dot_file = output.with_suffix(".dot")
    dot_file.write_text(dot_text, encoding="utf-8")

    if not dot:
        raise RuntimeError(
            f"Graphviz 'dot' was not found. DOT source created at {dot_file}"
        )

    subprocess.run(
        [dot, "-Tpng", str(dot_file), "-o", str(output)],
        check=True,
    )

def main():
    try:
        render(ENTITY, cfg.DOCS_DIR / "entity-model-session1.png")
        render(
            ARCHITECTURE,
            cfg.ARCH_DIR / "architecture-session1.png",
        )
    except Exception as exc:
        print(f"Diagram rendering failed: {exc}")
        return 1

    print(f"Wrote {cfg.DOCS_DIR / 'entity-model-session1.png'}")
    print(f"Wrote {cfg.ARCH_DIR / 'architecture-session1.png'}")
    return 0

if __name__ == "__main__":
    sys.exit(main())
