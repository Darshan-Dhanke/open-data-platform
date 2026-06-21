"""
Auto-provision Metabase for the e-commerce gold marts.

  1. complete first-run setup (create an admin) or log in
  2. add Trino as a database (Starburst community driver) pointed at the
     `iceberg` catalog
  3. sync the schema
  4. build a "E-commerce Overview" dashboard with three native-SQL cards over
     the gold tables (revenue by channel, top products, LTV by segment)

So the demo's "visual output" is ready without any manual clicking.

    python samples/pipelines/ecommerce_iceberg/provision_metabase.py
"""

import json
import sys
import time
import urllib.request

BASE = "http://localhost:3002"
ADMIN_EMAIL = "admin@example.com"
ADMIN_PASSWORD = "MetabasePlatform1!"  # printed below; local demo only


def _req(method, path, token=None, data=None):
    url = BASE + path
    headers = {"Content-Type": "application/json"}
    if token:
        headers["X-Metabase-Session"] = token
    body = json.dumps(data).encode() if data is not None else None
    req = urllib.request.Request(url, data=body, headers=headers, method=method)
    with urllib.request.urlopen(req, timeout=60) as r:
        raw = r.read()
        return json.loads(raw) if raw else {}


def _wait_healthy():
    for _ in range(60):
        try:
            if _req("GET", "/api/health").get("status") == "ok":
                return
        except Exception:
            pass
        time.sleep(5)
    raise SystemExit("Metabase did not become healthy")


def _session() -> str:
    props = _req("GET", "/api/session/properties")
    token = props.get("setup-token")
    if token:  # fresh instance — run setup
        res = _req("POST", "/api/setup", data={
            "token": token,
            "user": {"first_name": "Admin", "last_name": "User",
                     "email": ADMIN_EMAIL, "password": ADMIN_PASSWORD,
                     "site_name": "Open Data Platform"},
            "prefs": {"site_name": "Open Data Platform", "allow_tracking": False},
        })
        return res["id"]
    return _req("POST", "/api/session",
                data={"username": ADMIN_EMAIL, "password": ADMIN_PASSWORD})["id"]


def _ensure_trino_db(token: str) -> int:
    for db in _req("GET", "/api/database", token).get("data", []):
        if db["name"] == "Lakehouse (Trino)":
            return db["id"]
    res = _req("POST", "/api/database", token, data={
        "name": "Lakehouse (Trino)",
        "engine": "starburst",
        "details": {"host": "trino", "port": 8080, "catalog": "iceberg",
                    "user": "metabase", "ssl": False},
    })
    return res["id"]


def _card(token, db_id, name, sql, display, viz=None):
    return _req("POST", "/api/card", token, data={
        "name": name, "display": display,
        "dataset_query": {"type": "native", "database": db_id,
                          "native": {"query": sql}},
        "visualization_settings": viz or {},
    })["id"]


def main() -> int:
    _wait_healthy()
    token = _session()
    db_id = _ensure_trino_db(token)
    _req("POST", f"/api/database/{db_id}/sync_schema", token)
    time.sleep(8)  # let the sync register tables

    cards = [
        _card(token, db_id, "Revenue by channel",
              "SELECT channel, round(sum(revenue),0) AS revenue "
              "FROM iceberg.gold.daily_revenue GROUP BY 1 ORDER BY 2 DESC", "bar"),
        _card(token, db_id, "Top 10 products by revenue",
              "SELECT name, revenue FROM iceberg.gold.top_products "
              "ORDER BY revenue DESC LIMIT 10", "row"),
        _card(token, db_id, "Customers & avg LTV by segment",
              "SELECT segment, count(*) AS customers, round(avg(lifetime_value),0) AS avg_ltv "
              "FROM iceberg.gold.customer_ltv GROUP BY 1", "bar"),
    ]

    dash = _req("POST", "/api/dashboard", token,
                data={"name": "E-commerce Overview"})
    dash_id = dash["id"]
    dashcards = [{"id": -(i + 1), "card_id": cid, "row": (i // 2) * 7,
                  "col": (i % 2) * 9, "size_x": 9, "size_y": 7}
                 for i, cid in enumerate(cards)]
    _req("PUT", f"/api/dashboard/{dash_id}/cards", token, data={"cards": dashcards})

    print("Metabase provisioned.")
    print(f"  URL:        {BASE}")
    print(f"  Login:      {ADMIN_EMAIL} / {ADMIN_PASSWORD}")
    print(f"  Dashboard:  {BASE}/dashboard/{dash_id}  (E-commerce Overview)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
