from __future__ import annotations
import csv, json, os, queue, subprocess, sys, threading
from pathlib import Path
import tkinter as tk
from tkinter import ttk, messagebox, filedialog
import config as cfg
from event_log import EventLog, load_offsets

HERE=Path(__file__).resolve().parent
RESULTS=cfg.RESULTS

STAGES=[
    ("1. Log self-test","producer.py",["--self-test"],"Verify six durable-log guarantees."),
    ("2. Produce","producer.py",["--reset"],"Publish Session 1 sales as ordered sale.recorded events."),
    ("3. Consume","consumers.py",["--reset"],"Run three independent consumer groups."),
    ("4. Reconcile","reconcile.py",[],"Compare stream projection with Session 1 baseline."),
    ("5. Failure & recovery","failure_recovery.py",[],"Inject an audit-consumer crash and recover."),
    ("6. Replay","replay.py",[],"Demonstrate late join, rewind, catch-up and partial replay."),
]
ARTIFACTS=[
    ("log_self_test.json","producer.py"),("producer_summary.json","producer.py"),
    ("session2_throughput.csv","producer.py"),("streamed_customer_revenue.csv","consumers.py"),
    ("audit_log.jsonl","consumers.py"),("high_value_alerts.csv","consumers.py"),
    ("consumer_lag.csv","consumers.py"),("reconciliation_report.json","reconcile.py"),
    ("failure_recovery.json","failure_recovery.py"),("replay_report.json","replay.py"),
    ("late_joining_analytics.csv","replay.py"),
]

def jread(name):
    try:return json.loads((RESULTS/name).read_text(encoding="utf-8"))
    except Exception:return {}
def cread(name):
    try:
        with (RESULTS/name).open("r",encoding="utf-8-sig",newline="") as f:return list(csv.DictReader(f))
    except Exception:return []
def human(n):
    try:n=float(n)
    except:return "—"
    for u in ("B","KB","MB","GB"):
        if n<1024 or u=="GB":return f"{n:.0f} {u}" if u=="B" else f"{n:.2f} {u}"
        n/=1024

class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("EcomInsight — Session 2 Event Streaming Console")
        self.geometry("1530x920"); self.minsize(1180,720)
        self.navy="#213f70"; self.dark="#17345f"; self.bg="#fbfbf9"; self.panel="#fff"
        self.green="#5b8625"; self.greenbg="#e4f0db"; self.orange="#c95b0b"; self.orangebg="#fde5d4"
        self.bluebg="#eef3fb"; self.border="#b9b6ae"; self.text="#10243f"; self.gray="#6f8098"
        self.configure(bg=self.bg)
        self.q=queue.Queue(); self.running=False
        self.status={x[0]:"not run" for x in STAGES}
        self._styles(); self._header(); self._tabs()
        self._pipeline(); self._durable(); self._consumers(); self._failure(); self._replay(); self._recon(); self._monetization(); self._console()
        self.refresh(); self.after(100,self._drain)

    def _styles(self):
        s=ttk.Style(self)
        try:s.theme_use("clam")
        except:pass
        s.configure("TNotebook",background=self.bg)
        s.configure("TNotebook.Tab",padding=(17,9),font=("Segoe UI",9),background="#f5f5f2",foreground=self.navy)
        s.map("TNotebook.Tab",background=[("selected",self.navy)],foreground=[("selected","white")])
        s.configure("Treeview",font=("Segoe UI",9),rowheight=27,background="white",fieldbackground="white")
        s.configure("Treeview.Heading",font=("Segoe UI",9,"bold"),background=self.navy,foreground="white",padding=(5,5))

    def _header(self):
        h=tk.Frame(self,bg=self.navy,height=130); h.pack(fill="x"); h.pack_propagate(False)
        tk.Label(h,text="EcomInsight — Session 2 Event Streaming Console",font=("Georgia",20,"bold"),
                 fg="white",bg=self.navy).pack(anchor="w",padx=25,pady=(22,6))
        tk.Label(h,text="MIT 261 Parallel and Distributed Systems  ·  topic sale.recorded  ·  mechanism Payment_Method  ·  event time Order_Date  ·  4 partitions keyed on Customer_ID",
                 font=("Segoe UI",9),fg="#d7e5f7",bg=self.navy).pack(anchor="w",padx=25)

    def _tabs(self):
        self.nb=ttk.Notebook(self); self.nb.pack(fill="both",expand=True)
        self.tabs={}
        for name in ["Pipeline","Durable log","Consumers & lag","Failure & recovery","Replay","Reconciliation","Monetization","Console"]:
            f=tk.Frame(self.nb,bg=self.bg); self.nb.add(f,text=name); self.tabs[name]=f

    def scrollable_tab_body(self, tab_name, padx=28, pady=17):
        """Create a vertically scrollable body for a notebook tab."""
        host = self.tabs[tab_name]

        outer = tk.Frame(host, bg=self.bg)
        outer.pack(fill="both", expand=True)

        canvas = tk.Canvas(outer, bg=self.bg, highlightthickness=0, borderwidth=0)
        vbar = ttk.Scrollbar(outer, orient="vertical", command=canvas.yview)
        canvas.configure(yscrollcommand=vbar.set)

        vbar.pack(side="right", fill="y")
        canvas.pack(side="left", fill="both", expand=True)

        content = tk.Frame(canvas, bg=self.bg)
        window_id = canvas.create_window((0, 0), window=content, anchor="nw")

        def update_scrollregion(event=None):
            canvas.configure(scrollregion=canvas.bbox("all"))

        def fit_width(event):
            canvas.itemconfigure(window_id, width=event.width)

        content.bind("<Configure>", update_scrollregion)
        canvas.bind("<Configure>", fit_width)

        def on_mousewheel(event):
            if getattr(event, "delta", 0):
                step = -1 if event.delta > 0 else 1
                canvas.yview_scroll(step * 3, "units")
            elif getattr(event, "num", None) == 4:
                canvas.yview_scroll(-3, "units")
            elif getattr(event, "num", None) == 5:
                canvas.yview_scroll(3, "units")

        def bind_wheel(event=None):
            canvas.bind_all("<MouseWheel>", on_mousewheel)
            canvas.bind_all("<Button-4>", on_mousewheel)
            canvas.bind_all("<Button-5>", on_mousewheel)

        def unbind_wheel(event=None):
            canvas.unbind_all("<MouseWheel>")
            canvas.unbind_all("<Button-4>")
            canvas.unbind_all("<Button-5>")

        outer.bind("<Enter>", bind_wheel)
        outer.bind("<Leave>", unbind_wheel)

        inner = tk.Frame(content, bg=self.bg)
        inner.pack(fill="both", expand=True, padx=padx, pady=pady)
        return inner

    def btn(self,p,text,cmd,primary=False,warm=False):
        bg=self.navy if primary else (self.orangebg if warm else "#f4f6fa")
        fg="white" if primary else (self.orange if warm else self.navy)
        return tk.Button(p,text=text,command=cmd,bg=bg,fg=fg,activebackground=self.dark if primary else "#e7ebf2",
                         activeforeground=fg,relief="flat",font=("Segoe UI",8),padx=13,pady=7,cursor="hand2",
                         highlightbackground=self.border,highlightthickness=1)

    def section_title(self,p,text):
        tk.Label(p,text=text,bg=self.bg,fg=self.navy,font=("Georgia",16,"bold")).pack(anchor="w",pady=(0,10))

    def banner(self,p,var,orange=False):
        b=tk.Frame(p,bg=self.orangebg if orange else self.greenbg)
        b.pack(fill="x",pady=(0,12))
        tk.Label(b,textvariable=var,bg=b["bg"],fg=self.orange if orange else self.green,
                 font=("Segoe UI",9,"bold"),anchor="w",justify="left",padx=14,pady=11).pack(fill="x")

    def tree(self,p,cols,heads,widths,height=8):
        f=tk.Frame(p,bg="white",highlightbackground=self.border,highlightthickness=1)
        t=ttk.Treeview(f,columns=cols,show="headings",height=height)
        ybar=ttk.Scrollbar(f,orient="vertical",command=t.yview)
        xbar=ttk.Scrollbar(f,orient="horizontal",command=t.xview)
        t.configure(yscrollcommand=ybar.set,xscrollcommand=xbar.set)

        for c,h,w in zip(cols,heads,widths):
            t.heading(c,text=h)
            t.column(c,width=w,anchor="w",stretch=True)

        f.grid_rowconfigure(0,weight=1)
        f.grid_columnconfigure(0,weight=1)
        t.grid(row=0,column=0,sticky="nsew")
        ybar.grid(row=0,column=1,sticky="ns")
        xbar.grid(row=1,column=0,sticky="ew")
        return f,t

    def metric(self,p,var,sub,green=False):
        f=tk.Frame(p,bg="#f1f5fb",highlightbackground="#d4deee",highlightthickness=1)
        tk.Label(f,textvariable=var,font=("Georgia",24,"bold"),fg=self.green if green else self.navy,bg="#f1f5fb").pack(pady=(15,3))
        tk.Label(f,text=sub,font=("Segoe UI",8),fg=self.gray,bg="#f1f5fb").pack(pady=(0,14)); return f

    def _pipeline(self):
        w = self.scrollable_tab_body("Pipeline")
        cards=tk.Frame(w,bg=self.bg); cards.pack(fill="x",pady=(0,14))
        self.m_events=tk.StringVar(value="—"); self.m_groups=tk.StringVar(value="—"); self.m_lag=tk.StringVar(value="—"); self.m_pass=tk.StringVar(value="—")
        for c in (self.metric(cards,self.m_events,"events in durable log"),self.metric(cards,self.m_groups,"consumer groups tracked"),
                  self.metric(cards,self.m_lag,"total lag across all groups",True),self.metric(cards,self.m_pass,"reconciliation vs Session 1",True)):
            c.pack(side="left",fill="x",expand=True,padx=5)
        a=tk.Frame(w,bg=self.bg); a.pack(fill="x",pady=(0,12))
        tk.Label(a,text="Run a stage",font=("Georgia",11,"bold"),bg=self.bg,fg=self.navy).pack(side="left",padx=(0,10))
        for label,script,args,_ in STAGES:
            short=label.split(". ",1)[1]
            self.btn(a,short,lambda l=label:self.run_stage(l),warm=("Failure" in short)).pack(side="left",padx=2)
        self.btn(a,"Run everything",self.run_all,primary=True).pack(side="left",padx=(12,4))
        self.btn(a,"Refresh from results/",self.refresh).pack(side="left",padx=4)
        f,self.stage_tree=self.tree(w,("stage","status","headline"),["Stage","Status","Headline result"],[280,130,900],6); f.pack(fill="x",pady=(0,12))
        tk.Label(w,text="Artifacts in results/",bg=self.bg,fg=self.navy,font=("Georgia",11,"bold")).pack(anchor="w",pady=(0,5))
        f,self.art_tree=self.tree(w,("a","writer","status","size"),["Artifact","Written by","Status","Size"],[360,300,140,130],8); f.pack(fill="both",expand=True)

    def _durable(self):
        w = self.scrollable_tab_body("Durable log")
        top=tk.Frame(w,bg=self.bg); top.pack(fill="x")
        tk.Label(top,text="The log and its partitions",bg=self.bg,fg=self.navy,font=("Georgia",16,"bold")).pack(side="left")
        self.btn(top,"Produce (--reset)",lambda:self.run_stage("2. Produce")).pack(side="right",padx=(5,0))
        self.btn(top,"Run self-test",lambda:self.run_stage("1. Log self-test")).pack(side="right",padx=5)
        self.durable_banner=tk.StringVar(value="No events produced in this GUI session."); self.banner(w,self.durable_banner)
        body=tk.Frame(w,bg=self.bg); body.pack(fill="both",expand=True)
        left=tk.Frame(body,bg=self.bg); left.pack(side="left",fill="both",expand=True,padx=(0,12))
        right=tk.Frame(body,bg=self.bg); right.pack(side="left",fill="both",expand=True)
        tk.Label(left,text="Partition distribution",bg=self.bg,fg=self.navy,font=("Georgia",11,"bold")).pack(anchor="w")
        self.part_canvas=tk.Canvas(left,height=360,bg="white",highlightthickness=0); self.part_canvas.pack(fill="x",pady=(8,8))
        f,self.part_tree=self.tree(left,("p","events","share","obs"),["Partition","Events","Share","Observation"],[130,130,130,390],5); f.pack(fill="x")
        tk.Label(right,text="Guarantees verified by the self-test",bg=self.bg,fg=self.navy,font=("Georgia",11,"bold")).pack(anchor="w")
        f,self.guarantee_tree=self.tree(right,("g","r"),["Guarantee","Result"],[370,120],6); f.pack(fill="x",pady=(8,14))
        self.skew_text=tk.StringVar(value="Produce events to see partition skew.")
        box=tk.Frame(right,bg=self.orangebg); box.pack(fill="x")
        tk.Label(box,textvariable=self.skew_text,bg=self.orangebg,fg=self.orange,font=("Segoe UI",9),justify="left",wraplength=470,padx=14,pady=14).pack(fill="x")

    def _consumers(self):
        w = self.scrollable_tab_body("Consumers & lag")
        top=tk.Frame(w,bg=self.bg); top.pack(fill="x")
        tk.Label(top,text="Three groups, one topic, independent offsets",bg=self.bg,fg=self.navy,font=("Georgia",16,"bold")).pack(side="left")
        self.btn(top,"Run all consumers",lambda:self.run_stage("3. Consume"),primary=True).pack(side="right")
        self.consumer_banner=tk.StringVar(value="No consumer run yet."); self.banner(w,self.consumer_banner)
        tk.Label(w,text="Throughput of the last run",bg=self.bg,fg=self.navy,font=("Georgia",11,"bold")).pack(anchor="w")
        self.consumer_canvas=tk.Canvas(w,height=250,bg="white",highlightthickness=0); self.consumer_canvas.pack(fill="x",pady=(8,10))
        f,self.consumer_tree=self.tree(w,("g","processed","seconds","eps","dup","lag"),["Group","Processed","Seconds","Events/sec","Duplicates skipped","Final lag"],
                                       [260,140,130,160,180,120],4); f.pack(fill="x",pady=(0,12))
        tk.Label(w,text="Committed offsets, per group per partition",bg=self.bg,fg=self.navy,font=("Georgia",11,"bold")).pack(anchor="w",pady=(0,5))
        f,self.lag_tree=self.tree(w,("g","p","end","committed","lag"),["Group","Partition","End offset","Committed","Lag"],[260,130,150,150,120],10); f.pack(fill="both",expand=True)

    def _failure(self):
        w = self.scrollable_tab_body("Failure & recovery")
        top=tk.Frame(w,bg=self.bg); top.pack(fill="x")
        tk.Label(top,text="Crash the audit consumer on purpose",bg=self.bg,fg=self.navy,font=("Georgia",16,"bold")).pack(side="left")
        self.fail_n=tk.StringVar(value="25000")
        tk.Entry(top,textvariable=self.fail_n,width=9,justify="right").pack(side="right",padx=(5,4))
        tk.Label(top,text="fail after N events:",bg=self.bg,fg=self.gray).pack(side="right")
        self.btn(top,"Inject failure then recover",self.run_failure,primary=True).pack(side="right",padx=(8,5))
        self.failure_banner=tk.StringVar(value="No failure injected yet."); self.banner(w,self.failure_banner,orange=True)
        tk.Label(w,text="Blast radius — what else failed with it",bg=self.bg,fg=self.navy,font=("Georgia",11,"bold")).pack(anchor="w")
        f,self.blast_tree=self.tree(w,("c","s","e"),["Component","Status","Evidence"],[280,170,700],5); f.pack(fill="x",pady=(8,14))
        cards=tk.Frame(w,bg=self.bg); cards.pack(fill="x")
        self.f_handled=tk.StringVar(value="—"); self.f_backlog=tk.StringVar(value="—"); self.f_entries=tk.StringVar(value="—"); self.f_redel=tk.StringVar(value="—")
        for c in (self.metric(cards,self.f_handled,"handled before crash"),self.metric(cards,self.f_backlog,"backlog left in log"),
                  self.metric(cards,self.f_entries,"audit entries written"),self.metric(cards,self.f_redel,"redelivered on restart")):
            c.pack(side="left",fill="x",expand=True,padx=5)

    def _replay(self):
        w = self.scrollable_tab_body("Replay")
        top=tk.Frame(w,bg=self.bg); top.pack(fill="x")
        tk.Label(top,text="What a durable log gives you for free",bg=self.bg,fg=self.navy,font=("Georgia",16,"bold")).pack(side="left")
        self.btn(top,"Run all four demonstrations",lambda:self.run_stage("6. Replay"),primary=True).pack(side="right")
        self.replay_banner=tk.StringVar(value="No replay demonstrations run yet."); self.banner(w,self.replay_banner)
        f,self.replay_tree=self.tree(w,("d","p","r"),["Demonstration","What it proves","Result"],[330,670,310],5); f.pack(fill="x",pady=(0,15))
        tk.Label(w,text="Revenue by Payment Method — late-joining consumer using Session 1 Payment_Method",bg=self.bg,fg=self.navy,font=("Georgia",11,"bold")).pack(anchor="w",pady=(0,5))
        f,self.analytics_tree=self.tree(w,("dim","value","events","revenue"),["Mechanism field","Payment Method","Events","Revenue"],[180,380,140,200],10); f.pack(fill="both",expand=True)

    def _recon(self):
        w = self.scrollable_tab_body("Reconciliation")
        top=tk.Frame(w,bg=self.bg); top.pack(fill="x")
        tk.Label(top,text="Does the stream agree with Session 1's batch answer?",bg=self.bg,fg=self.navy,font=("Georgia",16,"bold")).pack(side="left")
        self.btn(top,"Reconcile now",lambda:self.run_stage("4. Reconcile"),primary=True).pack(side="right")
        self.recon_banner=tk.StringVar(value="No reconciliation run yet."); self.banner(w,self.recon_banner)
        f,self.recon_tree=self.tree(w,("m","b","s","d"),["Measure","Session 1 — batch","Session 2 — stream","Difference"],[360,300,300,300],7); f.pack(fill="x",pady=(0,15))
        tk.Label(w,text="Pass conditions",bg=self.bg,fg=self.navy,font=("Georgia",11,"bold")).pack(anchor="w",pady=(0,5))
        f,self.cond_tree=self.tree(w,("c","k","held"),["Condition","Kind","Held?"],[760,180,130],5); f.pack(fill="x")

    def _monetization(self):
        w = self.scrollable_tab_body("Monetization")
        top=tk.Frame(w,bg=self.bg); top.pack(fill="x")
        tk.Label(top,text="Monetization — revenue opportunities from your sales database",bg=self.bg,fg=self.navy,font=("Georgia",16,"bold")).pack(side="left")
        self.btn(top,"Analyze sales",self.refresh_monetization,primary=True).pack(side="right")
        self.monet_banner=tk.StringVar(value="Click Analyze sales to calculate monetization opportunities from sales.csv.")
        self.banner(w,self.monet_banner)

        cards=tk.Frame(w,bg=self.bg); cards.pack(fill="x",pady=(0,14))
        self.mon_revenue=tk.StringVar(value="—"); self.mon_orders=tk.StringVar(value="—")
        self.mon_aov=tk.StringVar(value="—"); self.mon_customers=tk.StringVar(value="—")
        for c in (self.metric(cards,self.mon_revenue,"total sales revenue",True),
                  self.metric(cards,self.mon_orders,"orders analyzed"),
                  self.metric(cards,self.mon_aov,"average order value",True),
                  self.metric(cards,self.mon_customers,"customers analyzed")):
            c.pack(side="left",fill="x",expand=True,padx=5)

        tk.Label(w,text="Revenue by payment mechanism",bg=self.bg,fg=self.navy,font=("Georgia",11,"bold")).pack(anchor="w",pady=(0,5))
        f,self.mon_payment_tree=self.tree(w,("payment","orders","revenue","share","aov"),
            ["Payment Method","Orders","Revenue","Revenue Share","Avg Order Value"],[280,130,200,150,180],8)
        f.pack(fill="x",pady=(0,14))

        tk.Label(w,text="Top customer monetization opportunities",bg=self.bg,fg=self.navy,font=("Georgia",11,"bold")).pack(anchor="w",pady=(0,5))
        f,self.mon_customer_tree=self.tree(w,("customer","orders","revenue","aov","action"),
            ["Customer ID","Orders","Revenue","Avg Order Value","Suggested Action"],[210,110,170,160,520],10)
        f.pack(fill="x",pady=(0,14))

        tk.Label(w,text="Revenue actions supported by the dataset",bg=self.bg,fg=self.navy,font=("Georgia",11,"bold")).pack(anchor="w",pady=(0,5))
        f,self.mon_action_tree=self.tree(w,("strategy","evidence","action"),
            ["Strategy","Evidence from database","Monetization action"],[300,500,600],6)
        f.pack(fill="x")

    def refresh_monetization(self):
        try:
            sales_path=cfg.SESSION1/"datasets"/"sales.csv"
            with sales_path.open("r",encoding="utf-8-sig",newline="") as f:
                rows=list(csv.DictReader(f))
            if not rows: raise ValueError("sales.csv contains no rows.")

            def getv(r,*names):
                for n in names:
                    if n in r and r.get(n) not in (None,""): return r.get(n)
                return ""
            def num(v):
                try:return float(str(v).replace(",","").replace("₹","").strip())
                except:return 0.0

            payments={}; customers={}; revenue=0.0
            for r in rows:
                amount=num(getv(r,"Total_Amount","Total Amount","total_amount","Revenue","Sales"))
                cid=getv(r,"Customer_ID","Customer ID","customer_id") or "Unknown"
                pm=getv(r,"Payment_Method","Payment Method","payment_method","Payment_Mode","PaymentMode") or "Unknown"
                revenue+=amount
                p=payments.setdefault(pm,{"orders":0,"revenue":0.0}); p["orders"]+=1; p["revenue"]+=amount
                c=customers.setdefault(cid,{"orders":0,"revenue":0.0}); c["orders"]+=1; c["revenue"]+=amount

            orders=len(rows); overall_aov=revenue/orders if orders else 0
            self.mon_revenue.set(f"₹{revenue:,.2f}"); self.mon_orders.set(f"{orders:,}")
            self.mon_aov.set(f"₹{overall_aov:,.2f}"); self.mon_customers.set(f"{len(customers):,}")

            self.clear(self.mon_payment_tree)
            ranked=sorted(payments.items(),key=lambda x:x[1]["revenue"],reverse=True)
            payment_labels={
                "UPI":"UPI (Unified Payments Interface)",
                "COD":"COD (Cash on Delivery)",
            }
            for pm,s in ranked:
                share=s["revenue"]/revenue*100 if revenue else 0
                aov=s["revenue"]/s["orders"] if s["orders"] else 0
                display_pm=payment_labels.get(str(pm).strip().upper(),pm)
                self.mon_payment_tree.insert("","end",values=(display_pm,f"{s['orders']:,}",f"₹{s['revenue']:,.2f}",f"{share:.1f}%",f"₹{aov:,.2f}"))

            self.clear(self.mon_customer_tree)
            for cid,s in sorted(customers.items(),key=lambda x:x[1]["revenue"],reverse=True)[:25]:
                aov=s["revenue"]/s["orders"] if s["orders"] else 0
                action=("VIP loyalty + personalized repeat-purchase offer" if s["orders"]>=5
                        else "Cross-sell complementary products" if aov>=overall_aov
                        else "Retention offer to encourage another purchase")
                self.mon_customer_tree.insert("","end",values=(cid,f"{s['orders']:,}",f"₹{s['revenue']:,.2f}",f"₹{aov:,.2f}",action))

            self.clear(self.mon_action_tree)
            top_pm=ranked[0][0] if ranked else "—"; top_rev=ranked[0][1]["revenue"] if ranked else 0
            actions=[
                ("Payment-method optimization",f"{top_pm} is the highest-revenue payment mechanism at ₹{top_rev:,.2f}.","Prioritize checkout reliability and targeted promotions around the strongest payment mechanism."),
                ("High-value customer retention",f"{len(customers):,} Customer_ID values analyzed and ranked by revenue.","Use loyalty rewards and personalized offers for high-value repeat customers."),
                ("Increase order value",f"Current average order value is ₹{overall_aov:,.2f}.","Use bundles and cross-sell recommendations to increase revenue per order."),
                ("Real-time high-value conversion","Session 2 processes sale.recorded events and has a high-value-alerter consumer.","Use high-value events to trigger timely retention/cross-sell actions.")
            ]
            for x in actions:self.mon_action_tree.insert("","end",values=x)
            self.monet_banner.set("Analysis complete — monetization opportunities are calculated from your sales.csv data.")
        except Exception as e:
            self.monet_banner.set(f"Analysis failed: {e}")
            messagebox.showerror("Monetization",str(e))

    def _console(self):
        p=self.tabs["Console"]; w=tk.Frame(p,bg=self.bg); w.pack(fill="both",expand=True,padx=28,pady=17)
        top=tk.Frame(w,bg=self.bg); top.pack(fill="x")
        tk.Label(top,text="Verbatim output of every stage",bg=self.bg,fg=self.navy,font=("Georgia",16,"bold")).pack(side="left")
        self.btn(top,"Clear",lambda:self.console.delete("1.0","end")).pack(side="right")
        self.console=tk.Text(w,bg="#0f2038",fg="white",insertbackground="white",font=("Consolas",9),wrap="word",relief="flat",padx=14,pady=12)
        self.console.pack(fill="both",expand=True,pady=(10,0))

    def clear(self,t):
        for i in t.get_children():t.delete(i)

    def draw_bars(self,canvas,labels,values):
        canvas.delete("all")
        canvas.update_idletasks(); W=max(canvas.winfo_width(),800); H=max(canvas.winfo_height(),220)
        if not values:return
        mx=max(values) or 1; margin=55; gap=35; bw=max((W-2*margin-gap*(len(values)-1))/len(values),40)
        for i,(lab,val) in enumerate(zip(labels,values)):
            x0=margin+i*(bw+gap); x1=x0+bw; y1=H-45; y0=y1-(H-95)*(val/mx)
            canvas.create_rectangle(x0,y0,x1,y1,fill="#347bb8",outline="")
            canvas.create_text((x0+x1)/2,y0-12,text=f"{val:,.0f}",fill=self.navy,font=("Segoe UI",9))
            canvas.create_text((x0+x1)/2,H-25,text=lab,fill=self.gray,font=("Segoe UI",8))
        canvas.create_line(margin,H-45,W-margin,H-45,fill="#cbd5e3")

    def refresh(self):
        prod=jread("producer_summary.json"); selftest=jread("log_self_test.json"); consumers=jread("consumer_summary.json")
        recon=jread("reconciliation_report.json"); fail=jread("failure_recovery.json"); replay=jread("replay_report.json")
        lagrows=cread("consumer_lag.csv"); analytics=cread("late_joining_analytics.csv")
        self._refresh_pipeline(prod, consumers, recon, fail, replay)
        self._refresh_durable(prod,selftest)
        self._refresh_consumers(consumers,lagrows)
        self._refresh_failure(fail)
        self._refresh_replay(replay,analytics)
        self._refresh_recon(recon)

    def _refresh_pipeline(self, prod, consumers, recon, fail, replay):
        self.m_events.set("—"); self.m_groups.set("—"); self.m_lag.set("—"); self.m_pass.set("—")
        if self.status["2. Produce"]=="passed":self.m_events.set(f"{prod.get('events',0):,}")
        if self.status["3. Consume"]=="passed":
            self.m_groups.set(str(len(consumers)))
            self.m_lag.set(str(sum(int(x.get("final_lag",0)) for x in consumers)))
        if self.status["4. Reconcile"]=="passed":self.m_pass.set(recon.get("result","—"))
        self.clear(self.stage_tree)
        for label, script, args, desc in STAGES:
            st = self.status[label]
            headline = desc

            if st == "passed":
                if label == "2. Produce":
                    headline = (
                        f"{prod.get('events', 0):,} events · "
                        f"{prod.get('events_per_second', 0):,.0f}/s · "
                        f"skew {prod.get('skew_ratio', 0):.2f}:1"
                    )
                elif label == "3. Consume":
                    headline = (
                        f"{len(consumers)} groups · "
                        f"total lag {sum(int(x.get('final_lag', 0)) for x in consumers)}"
                    )
                elif label == "4. Reconcile":
                    headline = (
                        f"{recon.get('result', '')} · "
                        f"groups {recon.get('stream_groups', '—')} · "
                        f"Δcount {recon.get('max_txn_count_diff', '—')}"
                    )
                elif label == "5. Failure & recovery":
                    headline = (
                        f"backlog {fail.get('backlog_before_recovery', '—')} · "
                        f"redelivered {fail.get('redelivered', '—')}"
                    )
                elif label == "6. Replay":
                    headline = (
                        f"late joiner saw {replay.get('new_consumer_events', '—')} events · "
                        f"deterministic={replay.get('rewind_deterministic', '—')}"
                    )
                elif label == "1. Log self-test":
                    headline = "All six guarantees hold — each backed by an assertion"

            self.stage_tree.insert("", "end", values=(label, st, headline))

        self.clear(self.art_tree)
        writer_status={s:l for l,s,_,_ in STAGES}
        for name,writer in ARTIFACTS:
            label=writer_status.get(writer); p=RESULTS/name
            shown=bool(label and self.status[label]=="passed" and p.exists())
            self.art_tree.insert("", "end", values=(name,writer,"written" if shown else "not run",human(p.stat().st_size) if shown else "—"))

    def _refresh_durable(self,prod,selftest):
        self.clear(self.part_tree); self.clear(self.guarantee_tree)
        if self.status["2. Produce"]=="passed":
            counts=prod.get("partition_counts",[]); total=sum(counts)
            self.durable_banner.set(f"{prod.get('events',0):,} events produced in {prod.get('seconds',0):.3f}s · {prod.get('events_per_second',0):,.0f} events/second · log {human(prod.get('log_size_bytes',0))} · skew {prod.get('skew_ratio',0):.2f}:1")
            self.draw_bars(self.part_canvas,[f"partition {i}" for i in range(len(counts))],counts)
            even=total/len(counts) if counts else 0
            for i,c in enumerate(counts):
                share=(c/total*100) if total else 0
                obs=f"{(c-even)/even*100:+.1f}% vs even share" if even else ""
                self.part_tree.insert("","end",values=(f"partition {i}",f"{c:,}",f"{share:.1f}%",obs))
            self.skew_text.set(f"Customer_ID values hash onto 4 partitions; events are ordered by Order_Date and carry Payment_Method as the transaction mechanism. Current max/min skew is {prod.get('skew_ratio',0):.2f}:1. If each partition had its own consumer instance, the slowest partition would determine completion time.")
        else:
            self.durable_banner.set("No events produced in this GUI session."); self.draw_bars(self.part_canvas,[],[])
        if self.status["1. Log self-test"]=="passed":
            for k,v in selftest.items():self.guarantee_tree.insert("","end",values=(k,v))

    def _refresh_consumers(self,consumers,lagrows):
        self.clear(self.consumer_tree); self.clear(self.lag_tree)
        if self.status["3. Consume"]!="passed":
            self.consumer_banner.set("No consumer run yet."); self.draw_bars(self.consumer_canvas,[],[]); return
        vals=[]
        for s in consumers:
            self.consumer_tree.insert("","end",values=(s.get("group"),f"{s.get('processed',0):,}",f"{s.get('seconds',0):.3f}",f"{s.get('events_per_second',0):,.0f}",s.get("duplicates_skipped",0),s.get("final_lag",0)))
            vals.append(float(s.get("events_per_second",0)))
        self.draw_bars(self.consumer_canvas,[s.get("group","").replace("-","\n") for s in consumers],vals)
        total_lag=sum(int(s.get("final_lag",0)) for s in consumers)
        self.consumer_banner.set(f"{len(consumers)} groups finished · total lag {total_lag} · each group maintained independent committed offsets")
        for r in lagrows:self.lag_tree.insert("","end",values=(r.get("group"),r.get("partition"),r.get("end_offset"),r.get("committed"),r.get("lag")))

    def _refresh_failure(self,r):
        self.clear(self.blast_tree)
        if self.status["5. Failure & recovery"]!="passed":
            self.failure_banner.set("No failure injected yet."); [v.set("—") for v in (self.f_handled,self.f_backlog,self.f_entries,self.f_redel)]; return
        self.failure_banner.set(f"Crashed after handling {r.get('handled_before_crash',0):,} events · backlog {r.get('backlog_before_recovery',0):,} · recovered {r.get('recovered_events',0):,} · final lag {r.get('final_lag',0)}")
        rows=[("Producer","UNAFFECTED",f"{r.get('events_in_log',0):,} events already durable"),
              ("customer-revenue-projector","UNAFFECTED","independent consumer group"),
              ("high-value-alerter","UNAFFECTED","independent consumer group"),
              ("audit-writer demo","DOWN → RECOVERED",f"backlog {r.get('backlog_before_recovery',0):,}; final lag {r.get('final_lag',0)}")]
        for x in rows:self.blast_tree.insert("","end",values=x)
        self.f_handled.set(f"{r.get('handled_before_crash',0):,}"); self.f_backlog.set(f"{r.get('backlog_before_recovery',0):,}")
        self.f_entries.set(f"{r.get('audit_entries_written',0):,}"); self.f_redel.set(f"{r.get('redelivered',0):,}")

    def _refresh_replay(self,r,analytics):
        self.clear(self.replay_tree); self.clear(self.analytics_tree)
        if self.status["6. Replay"]!="passed":
            self.replay_banner.set("No replay demonstrations run yet."); return
        self.replay_banner.set(f"All four demonstrations ran. Late joiner consumed {r.get('new_consumer_events',0):,} events.")
        demos=[
            ("A new consumer reads all history","It did not exist at production time; it read from offset 0",f"{r.get('new_consumer_events',0):,} events"),
            ("Rewind and reprocess","Two runs from offset 0 must produce identical results",f"{r.get('replay_groups',0)} groups · identical={r.get('rewind_deterministic')}"),
            ("An offline consumer catches up","The producer is not asked to resend anything",f"{r.get('offline_catchup_events',0):,} events"),
            (f"Partial replay from {r.get('partial_from','')}","Offsets are positions; a time replay filters/scans forward",f"{r.get('partial_events',0):,} of {r.get('total_events',0):,}"),
        ]
        for d in demos:self.replay_tree.insert("","end",values=d)
        for x in analytics[:100]:self.analytics_tree.insert("","end",values=(x.get("dimension"),x.get("value"),x.get("events"),x.get("revenue")))

    def _refresh_recon(self,r):
        self.clear(self.recon_tree); self.clear(self.cond_tree)
        if self.status["4. Reconcile"]!="passed":
            self.recon_banner.set("No reconciliation run yet."); return
        self.recon_banner.set(f"{r.get('result')} — {r.get('stream_groups')} customer groups, {r.get('stream_records'):,} transactions, max mean difference {r.get('max_revenue_mean_diff')} against tolerance {r.get('tolerance')}")
        rows=[
            ("Group sets identical",f"{r.get('batch_groups')} customers",f"{r.get('stream_groups')} customers","identical" if r.get("group_sets_identical") else "different"),
            ("Total records",f"{r.get('batch_records'):,}",f"{r.get('stream_records'):,}",r.get("max_txn_count_diff")),
            ("Maximum revenue-total difference","—","—",r.get("max_revenue_total_diff")),
            ("Maximum revenue-mean difference","—","—",r.get("max_revenue_mean_diff")),
            ("Tolerance applied","—","—",r.get("tolerance")),
        ]
        for x in rows:self.recon_tree.insert("","end",values=x)
        cond=[
            ("The streamed Customer_ID set equals the Session 1 batch set","EXACT",r.get("group_sets_identical")),
            ("Maximum absolute difference in txn_count is exactly 0","EXACT",r.get("max_txn_count_diff")==0),
            ("Revenue aggregates are within tolerance","APPROXIMATE",(r.get("max_revenue_mean_diff") or 0)<=r.get("tolerance",1e-6)),
            ("Every main consumer group finishes at lag 0","EXACT",self.m_lag.get()=="0"),
        ]
        for x in cond:self.cond_tree.insert("","end",values=(x[0],x[1],"yes" if x[2] else "no"))

    def run_failure(self):
        try:n=int(self.fail_n.get())
        except:messagebox.showerror("Invalid value","Fail-after value must be an integer.");return
        self.run_stage("5. Failure & recovery",extra=["--fail-after",str(n)])

    def run_stage(self,label,extra=None):
        if self.running: messagebox.showinfo("EcomInsight","Another stage is running."); return
        info=next((x for x in STAGES if x[0]==label),None)
        if not info:return
        # dependencies
        deps={"2. Produce":["1. Log self-test"],"3. Consume":["2. Produce"],"4. Reconcile":["3. Consume"],
              "5. Failure & recovery":["2. Produce"],"6. Replay":["2. Produce"]}
        for d in deps.get(label,[]):
            if self.status[d]!="passed":
                messagebox.showinfo("EcomInsight",f"Run {d} first."); return
        _,script,args,_=info; args=list(args)+(extra or [])
        self.running=True; self.status[label]="running"; self.refresh(); self.nb.select(self.tabs["Console"])
        self.console.insert("end",f"\n{'='*72}\n{label} — {script} {' '.join(args)}\n{'='*72}\n"); self.console.see("end")
        def worker():
            rc=self.execute(script,args); self.q.put(("done",label,rc))
        threading.Thread(target=worker,daemon=True).start()

    def execute(self,script,args):
        try:
            p=subprocess.Popen([sys.executable,"-u",str(HERE/script),*args],cwd=str(HERE),stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True,bufsize=1,errors="replace")
            for line in p.stdout:self.q.put(("text",line))
            return p.wait()
        except Exception as e:self.q.put(("text",f"ERROR: {e}\n"));return 1

    def run_all(self):
        if self.running:messagebox.showinfo("EcomInsight","Another stage is running.");return
        self.running=True; self.nb.select(self.tabs["Console"])
        def worker():
            for label,script,args,_ in STAGES:
                self.status[label]="running"; self.q.put(("refresh",)); self.q.put(("text",f"\n{'='*72}\n{label}\n{'='*72}\n"))
                rc=self.execute(script,list(args)); self.status[label]="passed" if rc==0 else "failed"; self.q.put(("refresh",))
                if rc:self.q.put(("all_done",False));return
            self.q.put(("all_done",True))
        threading.Thread(target=worker,daemon=True).start()

    def _drain(self):
        try:
            while True:
                m=self.q.get_nowait()
                if m[0]=="text":self.console.insert("end",m[1]);self.console.see("end")
                elif m[0]=="refresh":self.refresh()
                elif m[0]=="done":
                    _,label,rc=m; self.status[label]="passed" if rc==0 else "failed"; self.running=False; self.refresh()
                    self.console.insert("end",f"\n>>> FINISHED: {label} (exit code {rc})\n")
                elif m[0]=="all_done":
                    self.running=False; self.refresh(); self.console.insert("end","\n>>> PIPELINE COMPLETE\n" if m[1] else "\n>>> PIPELINE STOPPED WITH ERROR\n")
        except queue.Empty:pass
        self.after(100,self._drain)

if __name__=="__main__":
    App().mainloop()
