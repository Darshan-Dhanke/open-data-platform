"""
E-commerce sample dataset generator (pure standard library).

Produces six referentially-consistent CSVs that stand in for an online store's
operational database. Volumes are realistic-but-small so the whole pipeline runs
on a laptop:

    customers   ~2,000     products ~500     orders ~8,000
    order_items ~15,000    payments ~8,000   events ~10,000

The data has the variety a real platform must handle: timestamps spread over a
90-day window (velocity), categoricals, money, and a JSON event payload. Orders
reference customers; items reference orders + products; payments reference
orders; events reference customers.

    python samples/datasets/ecommerce.py <out_dir> [scale]

`scale` (default 1.0) multiplies all row counts. CSVs are written with a header
row and ISO-8601 timestamps, ready to COPY into Postgres or land in MinIO.
"""

import csv
import json
import os
import random
import sys
from datetime import datetime, timedelta

COUNTRIES = ["US", "UK", "DE", "IN", "BR", "CA", "AU", "FR", "JP", "SG"]
SEGMENTS = ["consumer", "smb", "enterprise"]
CATEGORIES = ["Electronics", "Home", "Apparel", "Books", "Sports", "Beauty", "Toys", "Grocery"]
CHANNELS = ["web", "mobile", "store", "partner"]
ORDER_STATUS = ["placed", "shipped", "delivered", "cancelled", "returned"]
PAY_METHODS = ["card", "paypal", "bank_transfer", "wallet", "cod"]
PAY_STATUS = ["captured", "pending", "failed", "refunded"]
EVENT_TYPES = ["page_view", "search", "add_to_cart", "checkout", "purchase", "support_ticket"]

NOW = datetime(2024, 6, 1, 0, 0, 0)
WINDOW_DAYS = 90


def _iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def _rand_ts(start_days_ago: int = WINDOW_DAYS) -> datetime:
    return NOW - timedelta(
        days=random.randint(0, start_days_ago),
        seconds=random.randint(0, 86399),
    )


def _write(out_dir: str, name: str, header: list[str], rows: list) -> int:
    path = os.path.join(out_dir, f"{name}.csv")
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)
    return len(rows)


def generate(out_dir: str, scale: float = 1.0, seed: int = 42) -> dict[str, int]:
    random.seed(seed)
    os.makedirs(out_dir, exist_ok=True)

    n_customers = int(2000 * scale)
    n_products = int(500 * scale)
    n_orders = int(8000 * scale)
    n_events = int(10000 * scale)

    counts: dict[str, int] = {}

    # customers
    customers = []
    for cid in range(1, n_customers + 1):
        customers.append([
            cid, f"Customer {cid}", f"user{cid}@example.com",
            random.choice(COUNTRIES), random.choice(SEGMENTS),
            _iso(_rand_ts(365)),
        ])
    counts["customers"] = _write(out_dir, "customers", [
        "customer_id", "name", "email", "country", "segment", "signup_date"], customers)

    # products (price/cost so margin is computable)
    products = []
    for pid in range(1, n_products + 1):
        cost = round(random.uniform(2, 400), 2)
        price = round(cost * random.uniform(1.15, 2.5), 2)
        products.append([
            pid, f"Product {pid}", random.choice(CATEGORIES), price, cost])
    counts["products"] = _write(out_dir, "products", [
        "product_id", "name", "category", "unit_price", "unit_cost"], products)

    # orders + order_items + payments (kept consistent)
    orders, items, payments = [], [], []
    item_id = 0
    for oid in range(1, n_orders + 1):
        cust = random.randint(1, n_customers)
        ots = _rand_ts()
        status = random.choices(ORDER_STATUS, weights=[10, 20, 50, 12, 8])[0]
        orders.append([oid, cust, _iso(ots), random.choice(CHANNELS), status])

        order_total = 0.0
        for _ in range(random.randint(1, 4)):  # 1-4 lines per order
            item_id += 1
            pid = random.randint(1, n_products)
            qty = random.randint(1, 5)
            unit_price = products[pid - 1][3]
            order_total += qty * unit_price
            items.append([item_id, oid, pid, qty, unit_price])

        if status != "cancelled":
            pstatus = "refunded" if status == "returned" else \
                random.choices(PAY_STATUS, weights=[80, 8, 7, 5])[0]
            payments.append([
                oid, random.choice(PAY_METHODS), round(order_total, 2), pstatus,
                _iso(ots + timedelta(minutes=random.randint(0, 120)))])

    counts["orders"] = _write(out_dir, "orders", [
        "order_id", "customer_id", "order_ts", "channel", "status"], orders)
    counts["order_items"] = _write(out_dir, "order_items", [
        "item_id", "order_id", "product_id", "quantity", "unit_price"], items)
    counts["payments"] = _write(out_dir, "payments", [
        "order_id", "method", "amount", "status", "paid_ts"], payments)

    # events with a JSON payload (semi-structured variety)
    events = []
    for eid in range(1, n_events + 1):
        etype = random.choice(EVENT_TYPES)
        payload = {"session": f"s{random.randint(1, 50000)}",
                   "device": random.choice(["ios", "android", "desktop"])}
        if etype == "search":
            payload["query"] = random.choice(["shoes", "phone", "laptop", "gift", "sale"])
        elif etype in ("add_to_cart", "purchase"):
            payload["product_id"] = random.randint(1, n_products)
        events.append([
            eid, random.randint(1, n_customers), etype, _iso(_rand_ts()),
            json.dumps(payload)])
    counts["events"] = _write(out_dir, "events", [
        "event_id", "customer_id", "event_type", "event_ts", "payload"], events)

    return counts


def main() -> int:
    if len(sys.argv) < 2:
        print("usage: python samples/datasets/ecommerce.py <out_dir> [scale]")
        return 2
    out_dir = sys.argv[1]
    scale = float(sys.argv[2]) if len(sys.argv) > 2 else 1.0
    counts = generate(out_dir, scale)
    print(f"Wrote e-commerce dataset to {out_dir}:")
    for name, n in counts.items():
        print(f"  {name:12s} {n:>7,} rows")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
