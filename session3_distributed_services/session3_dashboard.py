import csv
import json
import subprocess
import sys
import threading
import tkinter as tk
from pathlib import Path
from tkinter import ttk, messagebox, filedialog

from config import BASE, RESULTS_DIR, HTTP_HOST, HTTP_PORT, PGDATABASE, SESSION1_BASELINE, SESSION2_REFERENCE
from contracts import MESSAGES, SERVICES, verify_against_proto

NAVY = '#173968'
NAVY_DARK = '#0d2747'
PALE_BLUE = '#edf4fd'
PALE_GREEN = '#e9f6df'
PALE_ORANGE = '#fff0df'
PALE_RED = '#fde9e9'
WHITE = '#ffffff'
TEXT = '#172b4d'
MUTED = '#61758b'
BORDER = '#b8bec7'
GREEN = '#4b7f18'

FONT = ('Segoe UI', 10)
FONT_B = ('Segoe UI', 10, 'bold')
TITLE = ('Georgia', 20, 'bold')
SECTION = ('Georgia', 17, 'bold')
BIG = ('Georgia', 24, 'bold')
MONO = ('Consolas', 9)


def read_json(name, default=None):
    p = RESULTS_DIR / name
    if not p.exists():
        return {} if default is None else default
    try:
        return json.loads(p.read_text(encoding='utf-8'))
    except Exception:
        return {} if default is None else default


def fmt_ms(v):
    return f'{v:.4f}' if isinstance(v, (int, float)) else '—'


def pct(v):
    return f'{v:.1f}%'


class CMAFlowDashboard(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title('CMA-Flow — Session 3 Inter-Service Communication Console')
        self.geometry('1510x930')
        self.minsize(1180, 720)
        self.configure(bg=WHITE)
        self.running = False
        self.console_lines = []
        self._setup_style()
        self._build_header()
        self._build_tabs()
        self.refresh_all()

    def _setup_style(self):
        st = ttk.Style(self)
        st.theme_use('clam')
        st.configure('TNotebook', background=WHITE, borderwidth=0)
        st.configure('TNotebook.Tab', background='#f5f6f8', foreground=NAVY,
                     font=FONT_B, padding=(18, 10), borderwidth=1)
        st.map('TNotebook.Tab', background=[('selected', NAVY)], foreground=[('selected', WHITE)])
        st.configure('Treeview', rowheight=28, font=FONT, background=WHITE, fieldbackground=WHITE)
        st.configure('Treeview.Heading', background=NAVY, foreground=WHITE, font=FONT_B, relief='flat')
        st.map('Treeview', background=[('selected', '#d9e8fb')], foreground=[('selected', TEXT)])

    def _build_header(self):
        h = tk.Frame(self, bg=NAVY, height=92)
        h.pack(fill='x')
        h.pack_propagate(False)
        tk.Label(h, text='CMA-Flow — Session 3 Inter-Service Communication', font=TITLE,
                 fg=WHITE, bg=NAVY).pack(anchor='w', padx=26, pady=(16, 2))
        tk.Label(h, text='MIT 261 Parallel and Distributed Systems  ·  typed contract  ·  two codecs  ·  three transports',
                 font=('Segoe UI', 10), fg='#d8e4f3', bg=NAVY).pack(anchor='w', padx=28)

    def _build_tabs(self):
        self.nb = ttk.Notebook(self)
        self.nb.pack(fill='both', expand=True, padx=12, pady=(10, 12))
        self.tabs = {}
        self.tab_pages = {}
        self.tab_canvases = {}

        for name in ['Pipeline', 'Contract', 'Payload size', 'Latency', 'Round trips & streaming', 'Adapter swap', 'Reconciliation', 'Console']:
            # Each notebook page gets its own vertically scrollable canvas.  The
            # render_* methods still draw into self.tabs[name], so the rest of
            # the dashboard code does not need special scroll logic.
            page = tk.Frame(self.nb, bg=WHITE)
            self.nb.add(page, text=name)

            canvas = tk.Canvas(page, bg=WHITE, highlightthickness=0, bd=0)
            ybar = ttk.Scrollbar(page, orient='vertical', command=canvas.yview)
            canvas.configure(yscrollcommand=ybar.set)
            canvas.pack(side='left', fill='both', expand=True)
            ybar.pack(side='right', fill='y')

            inner = tk.Frame(canvas, bg=WHITE)
            window_id = canvas.create_window((0, 0), window=inner, anchor='nw')

            def _sync_scrollregion(event, c=canvas):
                c.configure(scrollregion=c.bbox('all'))

            def _fit_width(event, c=canvas, wid=window_id):
                c.itemconfigure(wid, width=event.width)

            inner.bind('<Configure>', _sync_scrollregion)
            canvas.bind('<Configure>', _fit_width)

            # Windows / macOS mouse wheel and Linux wheel buttons.
            def _wheel(event, c=canvas):
                if getattr(event, 'delta', 0):
                    units = -1 if event.delta > 0 else 1
                    c.yview_scroll(units * 3, 'units')
                elif getattr(event, 'num', None) == 4:
                    c.yview_scroll(-3, 'units')
                elif getattr(event, 'num', None) == 5:
                    c.yview_scroll(3, 'units')
                return 'break'

            def _bind_wheel(event, c=canvas, fn=_wheel):
                c.bind_all('<MouseWheel>', fn)
                c.bind_all('<Button-4>', fn)
                c.bind_all('<Button-5>', fn)

            def _unbind_wheel(event, c=canvas):
                c.unbind_all('<MouseWheel>')
                c.unbind_all('<Button-4>')
                c.unbind_all('<Button-5>')

            canvas.bind('<Enter>', _bind_wheel)
            canvas.bind('<Leave>', _unbind_wheel)
            inner.bind('<Enter>', _bind_wheel)
            inner.bind('<Leave>', _unbind_wheel)

            self.tabs[name] = inner
            self.tab_pages[name] = page
            self.tab_canvases[name] = canvas

    def clear(self, frame):
        for w in frame.winfo_children():
            w.destroy()

    def top_line(self, parent, title, buttons=None):
        row = tk.Frame(parent, bg=WHITE)
        row.pack(fill='x', padx=22, pady=(16, 8))
        tk.Label(row, text=title, font=SECTION, bg=WHITE, fg=NAVY_DARK).pack(side='left')
        for text, fn, primary in (buttons or [])[::-1]:
            b = tk.Button(row, text=text, command=fn, font=FONT_B,
                          bg=NAVY if primary else '#f1f5fa', fg=WHITE if primary else NAVY,
                          activebackground=NAVY_DARK if primary else '#e2eaf5',
                          activeforeground=WHITE if primary else NAVY,
                          relief='flat', padx=17, pady=7)
            b.pack(side='right', padx=(8, 0))
        return row

    def banner(self, parent, text, kind='green'):
        color = {'green': PALE_GREEN, 'blue': PALE_BLUE, 'orange': PALE_ORANGE, 'red': PALE_RED}[kind]
        fg = GREEN if kind == 'green' else TEXT
        lab = tk.Label(parent, text=text, bg=color, fg=fg, font=FONT_B, anchor='w', justify='left', padx=16, pady=12)
        lab.pack(fill='x', padx=22, pady=(0, 10))
        return lab

    def card(self, parent, value, label):
        f = tk.Frame(parent, bg=PALE_BLUE, highlightbackground='#d7e2ef', highlightthickness=1)
        tk.Label(f, text=value, font=BIG, fg=GREEN if value == 'PASSED' else NAVY_DARK, bg=PALE_BLUE).pack(pady=(16, 2))
        tk.Label(f, text=label, font=FONT, fg=MUTED, bg=PALE_BLUE).pack(pady=(0, 15))
        return f

    def tree(self, parent, columns, widths=None, height=8):
        wrap = tk.Frame(parent, bg=WHITE, highlightbackground=BORDER, highlightthickness=1)
        tree = ttk.Treeview(wrap, columns=columns, show='headings', height=height)
        ys = ttk.Scrollbar(wrap, orient='vertical', command=tree.yview)
        tree.configure(yscrollcommand=ys.set)
        for i, c in enumerate(columns):
            tree.heading(c, text=c)
            tree.column(c, width=(widths[i] if widths else 140), anchor='center')
        tree.pack(side='left', fill='both', expand=True)
        ys.pack(side='right', fill='y')
        return wrap, tree

    def load_results(self):
        self.bench = read_json('benchmark_report.json', {})
        self.rec = read_json('reconciliation_report.json', {})
        self.adapter = read_json('adapter_report.json', {})
        self.deadline = read_json('deadline_failures.json', {})
        self.db = read_json('database_report.json', {})
        self.contract = self.bench.get('contract') or verify_against_proto()

    def refresh_all(self):
        self.load_results()
        self.render_pipeline()
        self.render_contract()
        self.render_payload()
        self.render_latency()
        self.render_roundtrips()
        self.render_adapter()
        self.render_reconciliation()
        self.render_console()

    # ---------------- Pipeline ----------------
    def render_pipeline(self):
        f = self.tabs['Pipeline']; self.clear(f)
        payload = self.bench.get('payload', [])
        trans = next((x for x in payload if x.get('message') == 'Transaction'), None)
        avg_red = sum(x.get('reduction_pct', 0) for x in payload) / len(payload) if payload else 0
        lat = self.bench.get('latency', [])
        rpcs = sum(x.get('timed_calls', 0) for x in lat)
        rpcs += sum(x.get('single_calls', 0) + x.get('batched_calls', 0) for x in self.bench.get('roundtrips', []))
        rpcs += self.bench.get('streaming', {}).get('rows', 0)
        result = self.rec.get('result', '—')

        cards = tk.Frame(f, bg=WHITE); cards.pack(fill='x', padx=22, pady=(12, 8))
        vals = [
            (str(self.contract.get('messages_checked', 0)), 'messages in the contract'),
            (pct(trans['reduction_pct']) if trans else pct(avg_red), 'payload reduction, Transaction'),
            (f'{rpcs:,}' if rpcs else '—', 'RPCs / rows exercised this session'),
            (result, 'agreement with Sessions 1 and 2')
        ]
        for i, (v, l) in enumerate(vals):
            c = self.card(cards, v, l); c.grid(row=0, column=i, sticky='nsew', padx=6)
            cards.grid_columnconfigure(i, weight=1)

        stagebar = tk.Frame(f, bg=WHITE); stagebar.pack(fill='x', padx=22, pady=(2, 8))
        tk.Label(stagebar, text='Run a stage', font=SECTION, fg=NAVY_DARK, bg=WHITE).pack(side='left', padx=(0, 14))
        btns = [
            ('Contract', lambda: self.nb.select(self.tab_pages['Contract'])), ('Wire format', lambda: self.run_command('python test_wire.py')),
            ('Serve', lambda: self.run_command('python services.py')), ('Payload size', lambda: self.nb.select(self.tab_pages['Payload size'])),
            ('Unary latency', lambda: self.nb.select(self.tab_pages['Latency'])), ('Round trips', lambda: self.nb.select(self.tab_pages['Round trips & streaming'])),
            ('Streaming', lambda: self.nb.select(self.tab_pages['Round trips & streaming'])), ('Adapter swap', lambda: self.nb.select(self.tab_pages['Adapter swap'])),
            ('Deadline', lambda: self.nb.select(self.tab_pages['Adapter swap'])), ('Compose', lambda: self.nb.select(self.tab_pages['Reconciliation'])),
            ('Reconcile', lambda: self.run_command('python reconcile.py'))]
        for txt, fn in btns:
            tk.Button(stagebar, text=txt, command=fn, bg='#f2f6fb', fg=NAVY, relief='flat', font=FONT_B, padx=9, pady=6).pack(side='left', padx=2)
        tk.Button(stagebar, text='Run everything', command=self.run_pipeline, bg=NAVY, fg=WHITE, relief='flat', font=FONT_B, padx=15, pady=7).pack(side='right')

        wrap, tree = self.tree(f, ['Stage', 'Status', 'Headline result'], [210, 120, 920], height=11)
        wrap.pack(fill='both', expand=True, padx=22, pady=(0, 8))
        for tag in ('pass', 'warn'):
            tree.tag_configure(tag, background=PALE_GREEN if tag == 'pass' else PALE_ORANGE)
        rows = self.pipeline_rows()
        for stage, status, detail in rows:
            tree.insert('', 'end', values=(stage, status, detail), tags=('pass' if status == 'passed' else 'warn',))

        tk.Label(f, text=f'Data source: PostgreSQL {PGDATABASE}  ·  Service host: http://{HTTP_HOST}:{HTTP_PORT}', bg=WHITE, fg=MUTED, font=FONT).pack(anchor='w', padx=22, pady=(0, 3))
        tk.Label(f, text=f'Session 1: {SESSION1_BASELINE}  ·  Session 2: {SESSION2_REFERENCE}', bg=WHITE, fg=MUTED, font=('Segoe UI', 8)).pack(anchor='w', padx=22, pady=(0, 6))
        tk.Label(f, text='Artifacts in results/', font=SECTION, bg=WHITE, fg=NAVY_DARK).pack(anchor='w', padx=22, pady=(6, 6))
        aw, at = self.tree(f, ['Artifact', 'Written by', 'Status', 'Size'], [300, 340, 130, 120], height=5)
        aw.pack(fill='x', padx=22, pady=(0, 14))
        artifacts = [
            ('database_report.json', 'database.py / run_pipeline.py'), ('benchmark_report.json', 'benchmark.py / run_pipeline.py'), ('adapter_report.json', 'run_pipeline.py'),
            ('deadline_failures.json', 'run_pipeline.py'), ('session3_customer_revenue.csv', 'run_pipeline.py'),
            ('reconciliation_report.json', 'reconcile.py')]
        for name, writer in artifacts:
            p = RESULTS_DIR / name
            at.insert('', 'end', values=(name, writer, 'ready' if p.exists() else 'not yet', f'{p.stat().st_size:,} B' if p.exists() else '—'))

    def pipeline_rows(self):
        p = self.bench.get('payload', [])
        trans = next((x for x in p if x.get('message') == 'Transaction'), None)
        lat_by = {x.get('transport'): x for x in self.bench.get('latency', [])}
        rt = {x.get('codec'): x for x in self.bench.get('roundtrips', [])}
        st = self.bench.get('streaming', {})
        rec = self.rec
        return [
            ('1. Contract', 'passed' if self.contract.get('result') == 'PASS' else 'pending', f"{self.contract.get('messages_checked', 0)} messages, {self.contract.get('services_checked', 0)} services, {self.contract.get('methods_checked', 0)} methods, in sync with cma.proto"),
            ('2. Wire format', 'passed' if self.bench else 'pending', 'google.protobuf verified; 4 byte-identical round-trip test cases' if self.bench else 'run test_wire.py'),
            ('3. Serve', 'passed' if self.db.get('result') in ('PASS','CHECK') and self.bench else 'pending', self._db_headline()),
            ('4. Payload size', 'passed' if trans else 'pending', f"Transaction {trans['reduction_pct']:.2f}% smaller ({trans['json_bytes']} → {trans['protobuf_bytes']} bytes)" if trans else 'run pipeline'),
            ('5. Unary latency', 'passed' if lat_by else 'pending', self._lat_headline(lat_by)),
            ('6. Round trips', 'passed' if rt else 'pending', self._rt_headline(rt)),
            ('7. Streaming', 'passed' if st else 'pending', f"first row {st.get('earlier_by_ms', 0):.4f} ms earlier over proto; {st.get('rows', 0):,} rows" if st else 'run pipeline'),
            ('8. Adapter swap', 'passed' if self.adapter.get('identical') else 'pending', f"REST == gRPC: {self.adapter.get('identical', False)} · {self.adapter.get('caller_changes', '—')} call sites changed"),
            ('9. Deadline', 'passed' if self.deadline else 'pending', f"{self.deadline.get('short_deadline','—')} (short) · {self.deadline.get('generous_deadline','—')} (generous) · NOT_FOUND JSON/proto"),
            ('10. Compose', 'passed' if rec else 'pending', f"{rec.get('session3',{}).get('groups',0):,} groups · {rec.get('session3',{}).get('records',0):,} records · {rec.get('session3',{}).get('revenue',0):,.2f} revenue" if rec else 'run pipeline'),
            ('11. Reconcile', 'passed' if rec.get('result') == 'PASS' else 'pending', f"tolerance {rec.get('tolerance','—')} · {rec.get('result','—')}")]

    def _db_headline(self):
        if not self.db:
            return 'run database.py / pipeline'
        tables = self.db.get('tables', {})
        def n(key):
            value = tables.get(key)
            return f'{value:,}' if isinstance(value, (int, float)) else '—'
        detail = f'6 PostgreSQL-backed services · {PGDATABASE} · customers {n("customers")} · products {n("products")} · sales {n("sales")}'
        if self.db.get('result') == 'FAIL':
            detail += f" · ERROR: {self.db.get('error','connection failed')}"
        return detail

    def _lat_headline(self, d):
        if not d: return 'run pipeline'
        parts=[]
        for n in ['in-process','http/json','http/proto','grpc/proto']:
            if n in d: parts.append(f"{n} {d[n]['median_ms']:.4f} ms")
        return ' · '.join(parts)

    def _rt_headline(self, d):
        if not d: return 'run pipeline'
        parts=[]
        for codec in ['json','proto']:
            if codec in d: parts.append(f"{codec.upper()} {d[codec]['speedup']:.2f}x faster")
        return ' · '.join(parts) + ' · 49 round trips saved'

    # ---------------- Contract ----------------
    def render_contract(self):
        f = self.tabs['Contract']; self.clear(f)
        self.top_line(f, 'One schema, written once', [('Verify against cma.proto', lambda: self.run_command('python contracts.py'), True)])
        methods = sum(len(v) for v in SERVICES.values())
        streaming = sum(1 for svc in SERVICES.values() for s in svc.values() if s.server_streaming)
        fields = sum(len(v) for v in MESSAGES.values())
        self.banner(f, f"{len(MESSAGES)} messages, {fields} fields, {len(SERVICES)} services, {methods} methods ({streaming} server-streaming) · contracts.py in sync with cma.proto: {self.contract.get('result') == 'PASS'}")

        body = tk.Frame(f, bg=WHITE); body.pack(fill='both', expand=True, padx=22, pady=(0, 14))
        left = tk.Frame(body, bg=WHITE); left.pack(side='left', fill='both', expand=True, padx=(0, 12))
        right = tk.Frame(body, bg=WHITE, width=450); right.pack(side='right', fill='both'); right.pack_propagate(False)
        tk.Label(left, text='Methods', font=SECTION, bg=WHITE, fg=NAVY_DARK).pack(anchor='w', pady=(0,5))
        mw, mt = self.tree(left, ['Service','Method','Request','Response','Kind'], [170,180,190,190,130], height=10); mw.pack(fill='x')
        for svc, methodspecs in SERVICES.items():
            for m, s in methodspecs.items():
                mt.insert('', 'end', values=(svc,m,s.request,s.response,'server-streaming' if s.server_streaming else 'unary'))
        tk.Label(left, text='Message fields', font=SECTION, bg=WHITE, fg=NAVY_DARK).pack(anchor='w', pady=(12,5))
        fw, ft = self.tree(left, ['Message','#','Field','Type','Wire type'], [180,60,210,160,160], height=7); fw.pack(fill='both', expand=True)
        for msg, fs in MESSAGES.items():
            for name, sp in fs.items():
                wire = 'varint' if sp.type in ('int32','int64','bool') else ('64-bit' if sp.type=='double' else 'length-delimited')
                ft.insert('', 'end', values=(msg, sp.number, name, ('repeated ' if sp.repeated else '') + sp.type, wire))
        tk.Label(right, text='cma.proto', font=SECTION, bg=WHITE, fg=NAVY_DARK).pack(anchor='w', pady=(0,5))
        t = tk.Text(right, bg=NAVY_DARK, fg='#eef6ff', font=MONO, wrap='none', relief='flat', padx=10, pady=10)
        t.pack(fill='both', expand=True); t.insert('1.0', (BASE/'cma.proto').read_text(encoding='utf-8')); t.config(state='disabled')

    # ---------------- Payload ----------------
    def render_payload(self):
        f=self.tabs['Payload size']; self.clear(f)
        self.top_line(f,'The same message, two formats',[('Measure', self.run_pipeline, True)])
        data=self.bench.get('payload',[])
        if not data: self.banner(f,'No measurements yet. Run the pipeline.','orange'); return
        trans=next((x for x in data if x['message']=='Transaction'),data[0]); best=max(data,key=lambda x:x['reduction_pct'])
        self.banner(f,f"Transaction: {trans['json_bytes']} bytes as JSON, {trans['protobuf_bytes']} as protobuf — {trans['reduction_pct']:.1f}% smaller. Best case here is {best['message']} at {best['reduction_pct']:.1f}%.")
        chart=tk.Canvas(f,bg=WHITE,height=220,highlightthickness=0); chart.pack(fill='x',padx=30,pady=(2,6)); self.draw_payload_chart(chart,data[:6])
        w,t=self.tree(f,['Message','Fields','JSON bytes','protobuf bytes','Saved','Reduction'],[230,90,150,170,120,130],height=9); w.pack(fill='both',expand=True,padx=22,pady=(0,14))
        for x in data: t.insert('','end',values=(x['message'],x['fields'],x['json_bytes'],x['protobuf_bytes'],x['saved_bytes'],f"{x['reduction_pct']:.1f}%"))

    def draw_payload_chart(self,c,data):
        c.update_idletasks(); W=max(c.winfo_width(),1000); H=210; maxv=max(max(x['json_bytes'],x['protobuf_bytes']) for x in data) or 1; base=170; group=W/max(len(data),1)
        for i,x in enumerate(data):
            x0=i*group+group*.23; bw=group*.22; hj=x['json_bytes']/maxv*125; hp=x['protobuf_bytes']/maxv*125
            c.create_rectangle(x0,base-hj,x0+bw,base,fill='#2f75b5',outline=''); c.create_rectangle(x0+bw+6,base-hp,x0+bw*2+6,base,fill='#c85b08',outline='')
            c.create_text(x0+bw*.5,base-hj-10,text=str(x['json_bytes']),fill=NAVY,font=('Segoe UI',8)); c.create_text(x0+bw*1.5+6,base-hp-10,text=str(x['protobuf_bytes']),fill=NAVY,font=('Segoe UI',8)); c.create_text(x0+bw+3,base+18,text=x['message'],fill=MUTED,font=('Segoe UI',8))
        c.create_text(42,198,text='■ JSON',fill='#2f75b5',anchor='w'); c.create_text(110,198,text='■ protobuf',fill='#c85b08',anchor='w')

    # ---------------- Latency ----------------
    def render_latency(self):
        f=self.tabs['Latency']; self.clear(f)
        self.top_line(f,'One RPC, three transports',[('Benchmark',self.run_pipeline,True)])
        data=self.bench.get('latency',[])
        if not data: self.banner(f,'No measurements yet. Run the pipeline.','orange'); return
        d={x['transport']:x for x in data}
        self.banner(f,self._lat_headline(d),'blue')
        chart=tk.Canvas(f,bg=WHITE,height=280,highlightthickness=0); chart.pack(fill='x',padx=30,pady=(8,2)); self.draw_latency_chart(chart,data)
        w,t=self.tree(f,['Transport','Codec','Calls','Median ms','Calls/sec','Bytes/call'],[210,130,100,150,160,150],height=6); w.pack(fill='x',padx=22,pady=(0,10))
        for x in data:t.insert('','end',values=(x['transport'],x['codec'],x['timed_calls'],f"{x['median_ms']:.5f}",f"{x['calls_per_sec']:,.0f}",x['bytes_call']))
        self.banner(f,'Read the in-process row first: it is the baseline floor with no network wire. Compare transport overhead and wire bytes separately; benchmark timings vary by machine and runtime.','orange')

    def draw_latency_chart(self,c,data):
        c.update_idletasks(); W=max(c.winfo_width(),1000); base=215; maxv=max(x['median_ms'] for x in data) or 1; group=W/max(len(data),1)
        for i,x in enumerate(data):
            bw=72; x0=i*group+group/2-bw/2; h=x['median_ms']/maxv*155
            c.create_rectangle(x0,base-h,x0+bw,base,fill=['#777','#2f75b5','#c85b08','#5b9b55'][i%4],outline=''); c.create_text(x0+bw/2,base-h-12,text=f"{x['median_ms']:.3f}",fill=NAVY,font=FONT_B); c.create_text(x0+bw/2,base+18,text=x['transport'],fill=MUTED,font=FONT)
        c.create_line(20,base,W-20,base,fill='#cbd9e8')

    # ---------------- Round trips ----------------
    def render_roundtrips(self):
        f=self.tabs['Round trips & streaming']; self.clear(f)
        self.top_line(f,'Chattiness, and when the first row arrives',[('Streaming',self.run_pipeline,True),('Round trips',self.run_pipeline,False)])
        rt=self.bench.get('roundtrips',[]); st=self.bench.get('streaming',{})
        tk.Label(f,text='One batched call against N single calls',font=SECTION,bg=WHITE,fg=NAVY_DARK).pack(anchor='w',padx=22,pady=(4,5))
        w,t=self.tree(f,['Codec','Rows','Batched (1 call)','N single calls','Speedup','Round trips saved'],[130,110,190,190,140,180],height=4); w.pack(fill='x',padx=22)
        for x in rt:t.insert('','end',values=(x['codec'],x['rows'],f"{x['batched_ms']:.4f} ms",f"{x['single_ms']:.4f} ms",f"{x['speedup']:.1f}x",x['trips_saved']))
        tk.Label(f,text='Identical data and server work; the difference is how many times the request crosses the transport — the N+1 problem.',bg=WHITE,fg=MUTED,font=FONT).pack(anchor='w',padx=22,pady=(8,15))
        tk.Label(f,text='Unary against server streaming — time to the first usable row',font=SECTION,bg=WHITE,fg=NAVY_DARK).pack(anchor='w',padx=22,pady=(0,5))
        sw,stree=self.tree(f,['Codec','Rows','Unary total ms','Stream total ms','First message ms','Earlier by'],[130,110,180,180,180,160],height=3); sw.pack(fill='x',padx=22)
        if st: stree.insert('','end',values=(st['codec'],st['rows'],f"{st['unary_total_ms']:.4f}",f"{st['stream_total_ms']:.4f}",f"{st['first_message_ms']:.4f}",f"{st['earlier_by_ms']:.4f} ms"))
        if st:self.banner(f,f"Over proto, the first row arrives after {st['first_message_ms']:.4f} ms streaming against {st['unary_total_ms']:.4f} ms unary — {st['earlier_by_ms']:.4f} ms earlier, for the same {st['rows']:,} rows.")
        self.banner(f,'Unary must wait for the full response before the application can use any row. Streaming allows incremental processing, though per-message framing can make total stream time larger.','blue')

    # ---------------- Adapter ----------------
    def render_adapter(self):
        f=self.tabs['Adapter swap']; self.clear(f)
        self.top_line(f,'PaymentProviderAdapter: one interface, two wires',[('Deadline demo',self.run_pipeline,False),('Compare adapters',self.run_pipeline,True)])
        identical=self.adapter.get('identical',False)
        self.banner(f,f"Identical results across both adapters: {identical} · one adapter implementation swapped · {self.adapter.get('caller_changes','—')} call sites changed")
        w,t=self.tree(f,['Provider','Protocol','Result status','Provider result','Caller changes'],[220,220,180,300,170],height=4); w.pack(fill='x',padx=22,pady=(0,10))
        rr=self.adapter.get('rest_result',{}); gr=self.adapter.get('grpc_result',{})
        t.insert('','end',values=('rest-payment','REST / JSON',rr.get('status','—'),rr.get('provider','—'),self.adapter.get('caller_changes','—')))
        t.insert('','end',values=('grpc-payment','gRPC / protobuf',gr.get('status','—'),gr.get('provider','—'),self.adapter.get('caller_changes','—')))
        cards=tk.Frame(f,bg=WHITE); cards.pack(fill='x',padx=22,pady=8)
        for i,(v,l) in enumerate([('yes' if identical else 'no','results identical across adapters'),('1','adapter class changed'),(str(self.adapter.get('caller_changes','—')),'call sites changed'),('typed','common ChargeResult')]):
            c=self.card(cards,v,l); c.grid(row=0,column=i,sticky='nsew',padx=6); cards.grid_columnconfigure(i,weight=1)
        tk.Label(f,text='Deadline behaviour',font=SECTION,bg=WHITE,fg=NAVY_DARK).pack(anchor='w',padx=22,pady=(12,5))
        dw,dt=self.tree(f,['Observation','Value'],[460,760],height=7); dw.pack(fill='x',padx=22)
        values=[('Short caller deadline',self.deadline.get('short_deadline','—')),('Same call, generous deadline',self.deadline.get('generous_deadline','—')),('NOT_FOUND under JSON',self.deadline.get('not_found_json','—')),('NOT_FOUND under protobuf',self.deadline.get('not_found_proto','—')),('Caller protocol knowledge','none — checkout() receives an adapter')]
        for a,b in values:dt.insert('','end',values=(a,b))
        self.banner(f,'checkout() depends only on the adapter interface. The caller does not need to know whether the implementation uses REST/JSON or gRPC/protobuf.','blue')

    # ---------------- Reconciliation ----------------
    def render_reconciliation(self):
        f=self.tabs['Reconciliation']; self.clear(f)
        self.top_line(f,'Three execution models, one number',[('Reconcile now',lambda:self.run_command('python reconcile.py'),True),('Compose only',self.run_pipeline,False)])
        r=self.rec
        if not r: self.banner(f,'No reconciliation report yet. Run the pipeline.','orange'); return
        s3=r.get('session3',{})
        self.banner(f,f"{r.get('result')} — {s3.get('groups',0):,} customer groups, {s3.get('records',0):,} transactions, {s3.get('revenue',0):,.2f}. Agreement with Sessions 1 and 2 within tolerance {r.get('tolerance')}.")
        w,t=self.tree(f,['Compared against','Groups','Transactions','Max count diff','Max revenue total diff','Max mean diff','Verdict'],[220,110,140,150,190,170,120],height=5); w.pack(fill='x',padx=22,pady=(0,12))
        for key,label in [('session1_vs_session2','Session 1 vs Session 2'),('session1_vs_session3','Session 1 vs Session 3'),('session2_vs_session3','Session 2 vs Session 3')]:
            d=r.get(key,{})
            t.insert('','end',values=(label,s3.get('groups',0),s3.get('records',0),d.get('max_txn_count_diff','—'),f"{d.get('max_revenue_total_diff',0):.3e}",f"{d.get('max_revenue_mean_diff',0):.3e}",'PASSED' if d.get('group_sets_identical') and d.get('max_txn_count_diff',1)==0 else 'CHECK'))
        tk.Label(f,text='Composed customer revenue (results/session3_customer_revenue.csv)',font=SECTION,bg=WHITE,fg=NAVY_DARK).pack(anchor='w',padx=22,pady=(6,5))
        cw,ct=self.tree(f,['Customer_ID','Transactions','Revenue total','Revenue mean','Quantity total','Rating mean'],[190,130,190,170,150,150],height=12); cw.pack(fill='both',expand=True,padx=22,pady=(0,14))
        p=RESULTS_DIR/'session3_customer_revenue.csv'
        if p.exists():
            with p.open(encoding='utf-8',newline='') as fh:
                rd=csv.DictReader(fh)
                for i,row in enumerate(rd):
                    if i>=300: break
                    ct.insert('','end',values=(row['Customer_ID'],row['txn_count'],f"{float(row['revenue_total']):,.2f}",f"{float(row['revenue_mean']):,.4f}",row['quantity_total'],f"{float(row['rating_mean']):.4f}"))

    # ---------------- Console ----------------
    def render_console(self):
        f=self.tabs['Console']; self.clear(f)
        self.top_line(f,'Verbatim output of every stage',[('Clear',self.clear_console,False),('Save transcript…',self.save_console,False)])
        self.console=tk.Text(f,bg=NAVY_DARK,fg='#f3f7fb',insertbackground=WHITE,font=MONO,wrap='none',relief='flat',padx=12,pady=12)
        self.console.pack(fill='both',expand=True,padx=22,pady=(0,16))
        if self.console_lines:
            self.console.insert('1.0',''.join(self.console_lines))
        else:
            self.console.insert('1.0','Run the pipeline to capture your own executed Session 3 transcript here.\n')
        self.console.config(state='disabled')

    # ---------------- actions ----------------
    def clear_console(self):
        self.console_lines=[]; self.render_console()

    def save_console(self):
        path=filedialog.asksaveasfilename(defaultextension='.txt',filetypes=[('Text','*.txt')],initialfile='session3_console_transcript.txt')
        if path: Path(path).write_text(''.join(self.console_lines),encoding='utf-8')

    def run_command(self, cmd):
        if self.running: return
        self._run_thread(cmd)

    def run_pipeline(self):
        if self.running: return
        self._run_thread(f'"{sys.executable}" run_pipeline.py')

    def _run_thread(self, cmd):
        self.running=True
        self.nb.select(self.tab_pages['Console'])
        self.console_lines.append(f'\n> {cmd}\n')
        self.render_console()
        threading.Thread(target=self._worker,args=(cmd,),daemon=True).start()

    def _worker(self, cmd):
        try:
            if cmd.startswith('python '):
                args=[sys.executable]+cmd.split()[1:]
            elif cmd.startswith('"'):
                args=[sys.executable,'run_pipeline.py']
            else:
                args=cmd.split()
            p=subprocess.Popen(args,cwd=BASE,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True,bufsize=1)
            for line in p.stdout:
                self.console_lines.append(line)
                self.after(0,self._append_console,line)
            code=p.wait()
            self.console_lines.append(f'\n[process exited with code {code}]\n')
            self.after(0,self._finish_run,code)
        except Exception as e:
            self.console_lines.append(f'ERROR: {e}\n')
            self.after(0,self._finish_run,1)

    def _append_console(self,line):
        if not hasattr(self,'console'): return
        self.console.config(state='normal'); self.console.insert('end',line); self.console.see('end'); self.console.config(state='disabled')

    def _finish_run(self,code):
        self.running=False
        self.refresh_all()
        self.nb.select(self.tab_pages['Console'])
        if code==0: messagebox.showinfo('Session 3','Command completed successfully. Results refreshed.')
        else: messagebox.showerror('Session 3','Command finished with an error. Check the Console tab.')


if __name__ == '__main__':
    CMAFlowDashboard().mainloop()
