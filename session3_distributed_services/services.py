import time
import uuid
from database import fetch_all, fetch_sales, aggregate_customer_revenue, table_count, validate_database


class RpcError(Exception):
    def __init__(self, code, message):
        super().__init__(message)
        self.code = code
        self.message = message


class Repository:
    """PostgreSQL-backed repository used by all Session 3 services."""
    def __init__(self):
        report = validate_database(True)
        if report.get('result') == 'FAIL':
            raise RuntimeError(report.get('error', 'PostgreSQL validation failed'))
        self.database_report = report
        self.customers = fetch_all('customers')
        self.products = fetch_all('products')
        self.customer_by_id = {str(r['Customer_ID']): r for r in self.customers}
        self.sales_count = table_count('sales')
        self._revenue = None

    def aggregate_customer_revenue(self):
        if self._revenue is None:
            rows = aggregate_customer_revenue()
            self._revenue = [
                {
                    'customer_id': str(r['customer_id']),
                    'txn_count': int(r['txn_count']),
                    'revenue_total': float(r['revenue_total']),
                    'revenue_mean': float(r['revenue_mean']),
                    'quantity_total': int(r['quantity_total']),
                    'rating_mean': float(r['rating_mean']),
                }
                for r in rows
            ]
        return self._revenue

    def transactions(self, limit=100, customer_id=''):
        return fetch_sales(limit=limit, customer_id=customer_id)


class CustomerService:
    def __init__(self, r): self.r = r
    def GetCustomer(self, q):
        x = self.r.customer_by_id.get(q.get('customer_id', ''))
        if not x: raise RpcError('NOT_FOUND', 'customer not found')
        return {
            'customer_id': str(x['Customer_ID']),
            'customer_name': x.get('Customer_Name', '') or '',
            'city': x.get('City', '') or '',
            'state': x.get('State', '') or '',
            'customer_tier': x.get('Customer_Tier', '') or ''
        }
    def BatchGetCustomers(self, q):
        return {'customers': [self.GetCustomer({'customer_id': cid}) for cid in q.get('customer_ids', [])]}


class ConfigService:
    def GetConfig(self, q):
        return {'currency': 'INR', 'high_value_threshold': 5000.0, 'version': 1}


class EntitlementService:
    def __init__(self, r): self.r = r
    def GetEntitlement(self, q):
        x = self.r.customer_by_id.get(q.get('customer_id', ''))
        if not x: raise RpcError('NOT_FOUND', 'customer not found')
        return {'customer_id': str(x['Customer_ID']), 'tier': x.get('Customer_Tier', '') or 'Standard', 'active': True}


class TransactionService:
    def __init__(self, r): self.r = r
    def _one(self, x):
        return {
            'order_id': str(x['Order_ID']), 'customer_id': str(x['Customer_ID']), 'product_id': str(x['Product_ID']),
            'order_date': str(x.get('Order_Date', '') or ''), 'quantity': int(float(x.get('Quantity') or 0)),
            'total_amount': float(x.get('Total_Amount') or 0), 'rating': float(x.get('Rating') or 0),
            'payment_mode': x.get('Payment_Mode', '') or ''
        }
    def CountTransactions(self, q):
        return {'value': self.r.sales_count}
    def _select(self, q):
        limit = int(q.get('limit') or 100)
        cid = q.get('customer_id', '')
        for x in self.r.transactions(limit=limit, customer_id=cid):
            yield self._one(x)
    def ListTransactions(self, q):
        return {'transactions': list(self._select(q))}
    def StreamTransactions(self, q):
        yield from self._select(q)


class RevenueService:
    def __init__(self, r): self.r = r
    def _rows(self, q):
        cid = q.get('customer_id', '')
        rows = self.r.aggregate_customer_revenue()
        return [x for x in rows if not cid or x['customer_id'] == cid]
    def ComputeCustomerRevenue(self, q):
        rows = self._rows(q)
        return {'rows': rows, 'total_records': sum(x['txn_count'] for x in rows), 'aggregate_total': sum(x['revenue_total'] for x in rows)}
    def StreamCustomerRevenue(self, q):
        yield from self._rows(q)


class ChargeService:
    def Charge(self, q):
        delay = int(q.get('delay_ms') or 0)
        if delay: time.sleep(delay / 1000)
        key = q.get('idempotency_key') or f"{q.get('customer_id','')}:{q.get('amount_cents',0)}"
        return {
            'charge_id': 'CHG-' + uuid.uuid5(uuid.NAMESPACE_DNS, key).hex[:12].upper(),
            'status': 'approved', 'amount_cents': int(q.get('amount_cents') or 0),
            'provider': 'EcomInsightPayments'
        }


class ServiceHost:
    def __init__(self):
        r = Repository()
        self.repository = r
        self.services = {
            'CustomerService': CustomerService(r), 'ConfigService': ConfigService(),
            'EntitlementService': EntitlementService(r), 'TransactionService': TransactionService(r),
            'RevenueService': RevenueService(r), 'ChargeService': ChargeService()
        }
    def invoke(self, s, m, q): return getattr(self.services[s], m)(q)
    def stream(self, s, m, q): yield from getattr(self.services[s], m)(q)


if __name__ == '__main__':
    h = ServiceHost()
    db = h.repository.database_report
    print('source PostgreSQL', db['database'], db['schema'])
    print('services', len(h.services), 'customers', len(h.repository.customers), 'products', len(h.repository.products), 'sales', h.repository.sales_count, 'groups', len(h.repository.aggregate_customer_revenue()))
