"""PostgreSQL access for Session 3.

The password is read from .env / PGPASSWORD and is never hard-coded in source.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from config import BASE, RESULTS_DIR, PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD, PGSCHEMA

try:
    from dotenv import load_dotenv
    load_dotenv(BASE / '.env')
except Exception:
    pass

# Reload environment-backed values after .env is loaded.
import os
HOST = os.getenv('PGHOST', PGHOST)
PORT = int(os.getenv('PGPORT', str(PGPORT)))
DATABASE = os.getenv('PGDATABASE', PGDATABASE)
USER = os.getenv('PGUSER', PGUSER)
PASSWORD = os.getenv('PGPASSWORD', PGPASSWORD)
SCHEMA = os.getenv('PGSCHEMA', PGSCHEMA)


def _driver():
    try:
        import psycopg
        from psycopg.rows import dict_row
        from psycopg import sql
        return psycopg, dict_row, sql
    except ImportError as exc:
        raise RuntimeError(
            "psycopg is not installed. Run: python -m pip install -r requirements.txt"
        ) from exc


def connect():
    if not PASSWORD:
        raise RuntimeError(
            "PostgreSQL password is not configured. Copy .env.example to .env and set PGPASSWORD."
        )
    psycopg, dict_row, _ = _driver()
    return psycopg.connect(
        host=HOST,
        port=PORT,
        dbname=DATABASE,
        user=USER,
        password=PASSWORD,
        connect_timeout=5,
        row_factory=dict_row,
    )


def _ident(name: str):
    _, _, sql = _driver()
    return sql.Identifier(name)


def table_count(table: str) -> int:
    _, _, sql = _driver()
    with connect() as conn, conn.cursor() as cur:
        cur.execute(sql.SQL("SELECT COUNT(*) AS n FROM {}.{}").format(_ident(SCHEMA), _ident(table)))
        return int(cur.fetchone()['n'])


def fetch_all(table: str) -> list[dict[str, Any]]:
    _, _, sql = _driver()
    with connect() as conn, conn.cursor() as cur:
        cur.execute(sql.SQL("SELECT * FROM {}.{}").format(_ident(SCHEMA), _ident(table)))
        return [dict(r) for r in cur.fetchall()]


def fetch_sales(limit: int = 100, customer_id: str = '') -> list[dict[str, Any]]:
    _, _, sql = _driver()
    limit = max(int(limit), 0)
    with connect() as conn, conn.cursor() as cur:
        base = sql.SQL("SELECT * FROM {}.{}").format(_ident(SCHEMA), _ident('sales'))
        if customer_id:
            q = base + sql.SQL(' WHERE {} = %s ORDER BY {} LIMIT %s').format(
                _ident('Customer_ID'), _ident('Order_ID')
            )
            cur.execute(q, (customer_id, limit))
        else:
            q = base + sql.SQL(' ORDER BY {} LIMIT %s').format(_ident('Order_ID'))
            cur.execute(q, (limit,))
        return [dict(r) for r in cur.fetchall()]


def aggregate_customer_revenue() -> list[dict[str, Any]]:
    _, _, sql = _driver()
    q = sql.SQL('''
        SELECT
            {cid} AS customer_id,
            COUNT(*)::bigint AS txn_count,
            COALESCE(SUM({amount}), 0)::double precision AS revenue_total,
            COALESCE(AVG({amount}), 0)::double precision AS revenue_mean,
            COALESCE(SUM({qty}), 0)::bigint AS quantity_total,
            COALESCE(AVG({rating}) FILTER (WHERE {rating} IS NOT NULL), 0)::double precision AS rating_mean
        FROM {schema}.{sales}
        GROUP BY {cid}
        ORDER BY {cid}
    ''').format(
        cid=_ident('Customer_ID'), amount=_ident('Total_Amount'), qty=_ident('Quantity'),
        rating=_ident('Rating'), schema=_ident(SCHEMA), sales=_ident('sales')
    )
    with connect() as conn, conn.cursor() as cur:
        cur.execute(q)
        return [dict(r) for r in cur.fetchall()]


def validate_database(write_report: bool = True) -> dict[str, Any]:
    report = {
        'source': 'PostgreSQL',
        'host': HOST,
        'port': PORT,
        'database': DATABASE,
        'schema': SCHEMA,
        'user': USER,
        'tables': {},
        'result': 'PASS',
    }
    try:
        for table in ('customers', 'products', 'sales'):
            report['tables'][table] = table_count(table)
        expected = {'customers': 40000, 'products': 2000, 'sales': 250000}
        report['expected_rows'] = expected
        report['counts_match_expected'] = all(report['tables'][k] == v for k, v in expected.items())
        if not report['counts_match_expected']:
            report['result'] = 'CHECK'
    except Exception as exc:
        report['result'] = 'FAIL'
        report['error'] = str(exc)
    if write_report:
        (RESULTS_DIR / 'database_report.json').write_text(json.dumps(report, indent=2), encoding='utf-8')
    return report


if __name__ == '__main__':
    report = validate_database(True)
    print(json.dumps(report, indent=2))
    raise SystemExit(0 if report['result'] in ('PASS', 'CHECK') else 1)
