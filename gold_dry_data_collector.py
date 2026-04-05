#!/usr/bin/env python3
"""
Gold Dry - Revenue Data Collector
==================================
Haalt omzetdata op uit Shopify (B2B) en WooCommerce (B2C),
verwerkt deze (ex BTW, ex statiegeld) en genereert een JSON-databestand
dat het HTML dashboard voedt.

Configuratie: Vul de API credentials in als environment variables.
Gebruik:      python3 gold_dry_data_collector.py
Output:       gold_dry_dashboard_data.json + update index.html
"""

import json
import os
import re
import sys
from datetime import datetime, timedelta
from collections import defaultdict

# ============================================================
# CONFIG
# ============================================================
SHOPIFY_CONFIG = {
    "store_url":    os.getenv("SHOPIFY_STORE_URL", ""),
    "api_key":      os.getenv("SHOPIFY_API_KEY", ""),
    "api_secret":   os.getenv("SHOPIFY_API_SECRET", ""),
    "access_token": os.getenv("SHOPIFY_ACCESS_TOKEN", ""),
    "api_version":  "2024-01"
}

WOOCOMMERCE_CONFIG = {
    "site_url":        os.getenv("WOO_SITE_URL", ""),
    "consumer_key":    os.getenv("WOO_CONSUMER_KEY", ""),
    "consumer_secret": os.getenv("WOO_CONSUMER_SECRET", ""),
}

STATIEGELD_PER_TRAY = 1.80   # 12 blikken x 0.15
BTW_RATE = 0.21

# SKU prefixes die trays zijn (blikken met statiegeld)
TRAY_SKU_PREFIXES = ("TRAY", "tray")

OUTPUT_DIR  = os.path.dirname(os.path.abspath(__file__))
OUTPUT_JSON = os.path.join(OUTPUT_DIR, "gold_dry_dashboard_data.json")
OUTPUT_HTML = os.path.join(OUTPUT_DIR, "index.html")


# ============================================================
# SHOPIFY API CLIENT
# ============================================================
def shopify_fetch_orders(since_date, until_date=None):
    try:
        import requests
    except ImportError:
        print("ERROR: 'requests' package niet gevonden.")
        return []

    if not SHOPIFY_CONFIG["store_url"] or not SHOPIFY_CONFIG["access_token"]:
        print("WARN: Shopify credentials niet ingesteld")
        return None

    base_url = f"https://{SHOPIFY_CONFIG['store_url']}/admin/api/{SHOPIFY_CONFIG['api_version']}"
    headers = {
        "X-Shopify-Access-Token": SHOPIFY_CONFIG["access_token"],
        "Content-Type": "application/json"
    }

    orders = []
    params = {
        "status": "any",
        "financial_status": "any",
        "created_at_min": since_date.isoformat(),
        "limit": 250
    }
    if until_date:
        params["created_at_max"] = until_date.isoformat()

    url = f"{base_url}/orders.json"
    while url:
        resp = requests.get(url, headers=headers, params=params)
        if resp.status_code != 200:
            print(f"ERROR Shopify API: {resp.status_code}")
            return None
        data = resp.json()
        orders.extend(data.get("orders", []))
        link = resp.headers.get("Link", "")
        url = None
        params = None
        if 'rel="next"' in link:
            for part in link.split(","):
                if 'rel="next"' in part:
                    url = part.split("<")[1].split(">")[0]
                    break
    return orders


def process_shopify_orders(orders):
    results = []
    for order in orders:
        created = datetime.fromisoformat(order["created_at"].replace("Z", "+00:00"))
        total_price = float(order.get("total_price", 0))
        tax = float(order.get("total_tax", 0))

        # Credits / refunds verrekenen
        total_refund = 0
        for refund in order.get("refunds", []):
            for txn in refund.get("transactions", []):
                total_refund += float(txn.get("amount", 0))

        total_statiegeld = 0
        items = []
        for item in order.get("line_items", []):
            qty = item.get("quantity", 0)
            sku = item.get("sku") or "UNKNOWN"
            name = item.get("title", item.get("name", ""))
            price_ex_tax = float(item.get("price", 0)) * qty
            # Statiegeld alleen voor tray-producten (blikken), niet voor flessen
            is_tray = sku.upper().startswith("TRAY")
            tray_statiegeld = (qty * STATIEGELD_PER_TRAY) if is_tray else 0
            total_statiegeld += tray_statiegeld
            items.append({
                "sku": sku, "name": name, "quantity": qty,
                "revenue_ex": price_ex_tax - tray_statiegeld
            })

        revenue_ex = total_price - tax - total_statiegeld - total_refund
        company = (order.get("customer", {}).get("default_address", {}) or {}).get("company", "")
        customer_name = (order.get("customer", {}).get("first_name", "") + " " +
                         order.get("customer", {}).get("last_name", "")).strip()

        results.append({
            "date": created.strftime("%Y-%m-%d"),
            "month": created.strftime("%Y-%m"),
            "year": created.year,
            "order_id": order.get("name", order.get("id")),
            "customer": customer_name,
            "company": company,
            "channel": "B2B",
            "revenue_bruto": total_price,
            "tax": tax,
            "statiegeld": total_statiegeld,
            "refund": total_refund,
            "revenue_ex": max(0, revenue_ex),
            "items": items
        })
    return results


# ============================================================
# WOOCOMMERCE API CLIENT
# ============================================================
def woo_fetch_orders(since_date, until_date=None):
    try:
        import requests
    except ImportError:
        return None

    if not WOOCOMMERCE_CONFIG["site_url"] or not WOOCOMMERCE_CONFIG["consumer_key"]:
        print("WARN: WooCommerce credentials niet ingesteld")
        return None

    base_url = f"{WOOCOMMERCE_CONFIG['site_url']}/wp-json/wc/v3"
    auth = (WOOCOMMERCE_CONFIG["consumer_key"], WOOCOMMERCE_CONFIG["consumer_secret"])

    orders = []
    page = 1
    while True:
        params = {
            "status": "any",
            "after": since_date.isoformat(),
            "per_page": 100,
            "page": page,
            "orderby": "date",
            "order": "desc"
        }
        if until_date:
            params["before"] = until_date.isoformat()

        resp = requests.get(f"{base_url}/orders", auth=auth, params=params)
        if resp.status_code != 200:
            print(f"ERROR WooCommerce API: {resp.status_code}")
            return None

        batch = resp.json()
        if not batch:
            break
        orders.extend(batch)
        page += 1
        total_pages = int(resp.headers.get("X-WP-TotalPages", 1))
        if page > total_pages:
            break
    return orders


def process_woo_orders(orders):
    results = []
    for order in orders:
        # Cancelled orders uitsluiten
        if order.get("status", "").lower() == "cancelled":
            continue
        created = datetime.fromisoformat(order["date_created"])
        total_price = float(order.get("total", 0))
        tax = float(order.get("total_tax", 0))

        total_statiegeld = 0
        items = []
        for item in order.get("line_items", []):
            qty = item.get("quantity", 0)
            sku = item.get("sku") or "UNKNOWN"
            name = item.get("name", "")
            price = float(item.get("total", 0))
            # Statiegeld alleen voor tray-producten (blikken), niet voor flessen
            is_tray = sku.upper().startswith("TRAY")
            tray_statiegeld = (qty * STATIEGELD_PER_TRAY) if is_tray else 0
            total_statiegeld += tray_statiegeld
            items.append({
                "sku": sku, "name": name, "quantity": qty,
                "revenue_ex": price - tray_statiegeld
            })

        revenue_ex = total_price - tax - total_statiegeld
        customer_name = (order.get("billing", {}).get("first_name", "") + " " +
                         order.get("billing", {}).get("last_name", "")).strip()

        results.append({
            "date": created.strftime("%Y-%m-%d"),
            "month": created.strftime("%Y-%m"),
            "year": created.year,
            "order_id": str(order.get("number", order.get("id"))),
            "customer": customer_name,
            "company": order.get("billing", {}).get("company", ""),
            "channel": "B2C",
            "revenue_bruto": total_price,
            "tax": tax,
            "statiegeld": total_statiegeld,
            "revenue_ex": max(0, revenue_ex),
            "items": items
        })
    return results


# ============================================================
# DATA AGGREGATION
# ============================================================
def _top_customers(orders, limit=10):
    """Aggregeer top klanten uit een lijst orders, inclusief order details."""
    customer_rev = defaultdict(lambda: {"revenue": 0, "orders": 0, "channel": "", "orderList": []})
    for o in orders:
        key = o["company"] if o["company"] else o["customer"]
        if not key.strip():
            key = f"Klant #{o['order_id']}"
        customer_rev[key]["revenue"] += o["revenue_ex"]
        customer_rev[key]["orders"] += 1
        customer_rev[key]["channel"] = o["channel"]
        customer_rev[key]["orderList"].append({
            "id": str(o["order_id"]),
            "date": o["date"],
            "amount": round(o["revenue_ex"], 2)
        })
    top = sorted(customer_rev.items(), key=lambda x: x[1]["revenue"], reverse=True)[:limit]
    return [{"name": k, "channel": v["channel"], "revenue": round(v["revenue"], 2),
             "orders": v["orders"], "orderList": sorted(v["orderList"], key=lambda x: x["date"], reverse=True)}
            for k, v in top]


def _top_products(orders, limit=10):
    """Aggregeer top producten uit een lijst orders."""
    sku_stats = defaultdict(lambda: {"name": "", "sku": "", "units": 0, "revenue": 0})
    for o in orders:
        for item in o["items"]:
            sku = item["sku"]
            sku_stats[sku]["sku"] = sku
            sku_stats[sku]["name"] = item["name"]
            sku_stats[sku]["units"] += item["quantity"]
            sku_stats[sku]["revenue"] += item["revenue_ex"]
    top = sorted(sku_stats.values(), key=lambda x: x["revenue"], reverse=True)[:limit]
    return [{"name": p["name"], "sku": p["sku"], "units": p["units"], "revenue": round(p["revenue"], 2)}
            for p in top]


def aggregate_dashboard_data(all_orders):
    now = datetime.now()
    current_year = now.year
    current_month = now.month

    # Filter sets
    mtd_orders = [o for o in all_orders
                  if o["year"] == current_year
                  and datetime.strptime(o["date"], "%Y-%m-%d").month == current_month]
    ytd_orders = [o for o in all_orders if o["year"] == current_year]

    prev_mtd = [o for o in all_orders
                if o["year"] == current_year - 1
                and datetime.strptime(o["date"], "%Y-%m-%d").month == current_month
                and datetime.strptime(o["date"], "%Y-%m-%d").day <= now.day]
    prev_ytd = [o for o in all_orders
                if o["year"] == current_year - 1
                and (datetime.strptime(o["date"], "%Y-%m-%d").month < current_month
                     or (datetime.strptime(o["date"], "%Y-%m-%d").month == current_month
                         and datetime.strptime(o["date"], "%Y-%m-%d").day <= now.day))]

    def period_stats(orders, prev_orders):
        b2b = [o for o in orders if o["channel"] == "B2B"]
        b2c = [o for o in orders if o["channel"] == "B2C"]
        prev_b2b = [o for o in prev_orders if o["channel"] == "B2B"]
        prev_b2c = [o for o in prev_orders if o["channel"] == "B2C"]
        return {
            "total": round(sum(o["revenue_ex"] for o in orders), 2),
            "b2b":   round(sum(o["revenue_ex"] for o in b2b), 2),
            "b2c":   round(sum(o["revenue_ex"] for o in b2c), 2),
            "b2bOrders": len(b2b),
            "b2cOrders": len(b2c),
            "prevTotal": round(sum(o["revenue_ex"] for o in prev_orders), 2),
            "prevB2B":   round(sum(o["revenue_ex"] for o in prev_b2b), 2),
            "prevB2C":   round(sum(o["revenue_ex"] for o in prev_b2c), 2),
        }

    # Jaaroverzichten
    years = {}
    for year in sorted(set(o["year"] for o in all_orders)):
        year_orders = [o for o in all_orders if o["year"] == year]
        years[year] = round(sum(o["revenue_ex"] for o in year_orders), 2)

    # Maandelijks
    def monthly_revenue(year):
        monthly = [0] * 12
        for o in all_orders:
            if o["year"] == year:
                m = datetime.strptime(o["date"], "%Y-%m-%d").month - 1
                monthly[m] += o["revenue_ex"]
        return [round(v, 2) for v in monthly]

    # Top klanten & producten: ALL TIME + YTD
    return {
        "lastUpdated":               now.isoformat(),
        "mtd":                       period_stats(mtd_orders, prev_mtd),
        "ytd":                       period_stats(ytd_orders, prev_ytd),
        "years":                     years,
        f"monthly{current_year}":    monthly_revenue(current_year),
        f"monthly{current_year-1}":  monthly_revenue(current_year - 1),
        "topCustomers":              _top_customers(all_orders),
        "topCustomersYTD":           _top_customers(ytd_orders),
        "topProducts":               _top_products(all_orders),
        "topProductsYTD":            _top_products(ytd_orders),
    }


# ============================================================
# HTML UPDATER
# ============================================================
def update_dashboard_html(data):
    if not os.path.exists(OUTPUT_HTML):
        print(f"WARN: Dashboard HTML niet gevonden op {OUTPUT_HTML}")
        return

    with open(OUTPUT_HTML, "r", encoding="utf-8") as f:
        html = f.read()

    # Vervang demoData
    data_json = json.dumps(data, indent=2, ensure_ascii=True)
    pattern = r'const demoData = \{.*?\};'
    replacement = f'const demoData = {data_json};'
    new_html = re.sub(pattern, replacement, html, flags=re.DOTALL)

    # Verberg demo banner
    new_html = new_html.replace(
        '<div class="demo-banner">',
        '<div class="demo-banner" style="display:none">'
    )

    with open(OUTPUT_HTML, "w", encoding="utf-8") as f:
        f.write(new_html)
    print(f"Dashboard HTML bijgewerkt: {OUTPUT_HTML}")


# ============================================================
# MAIN
# ============================================================
def main():
    print("=" * 60)
    print("  GOLD DRY - Revenue Data Collector")
    print(f"  {datetime.now().strftime('%d %B %Y, %H:%M')}")
    print("=" * 60)

    now = datetime.now()
    since = datetime(now.year - 3, 1, 1)
    all_orders = []

    # Shopify (B2B)
    print("\n[1/4] Shopify orders ophalen (B2B)...")
    shopify_orders = shopify_fetch_orders(since)
    if shopify_orders is not None:
        processed = process_shopify_orders(shopify_orders)
        refunded = [o for o in processed if o.get("refund", 0) > 0]
        all_orders.extend(processed)
        print(f"  -> {len(processed)} B2B orders verwerkt")
        if refunded:
            total_ref = sum(o["refund"] for o in refunded)
            print(f"  -> {len(refunded)} orders met credits/refunds (totaal EUR {total_ref:,.2f} verrekend)")
    else:
        print("  -> Shopify overgeslagen (geen credentials)")

    # WooCommerce (B2C)
    print("\n[2/4] WooCommerce orders ophalen (B2C)...")
    woo_orders = woo_fetch_orders(since)
    if woo_orders is not None:
        raw_count = len(woo_orders)
        processed = process_woo_orders(woo_orders)
        cancelled_count = raw_count - len(processed)
        all_orders.extend(processed)
        print(f"  -> {len(processed)} B2C orders verwerkt")
        if cancelled_count > 0:
            print(f"  -> {cancelled_count} cancelled orders uitgesloten")
    else:
        print("  -> WooCommerce overgeslagen (geen credentials)")

    if not all_orders:
        print("\nGeen orders opgehaald. Controleer je API credentials.")
        return

    # Aggregeer
    print("\n[3/4] Data aggregeren...")
    dashboard_data = aggregate_dashboard_data(all_orders)

    # JSON opslaan
    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump(dashboard_data, f, indent=2, ensure_ascii=True)
    print(f"  -> Data opgeslagen: {OUTPUT_JSON}")

    # HTML bijwerken
    print("\n[4/4] Dashboard HTML bijwerken...")
    update_dashboard_html(dashboard_data)

    # Samenvatting
    ytd = dashboard_data["ytd"]
    mtd = dashboard_data["mtd"]
    print("\n" + "=" * 60)
    print("  SAMENVATTING")
    print("=" * 60)
    print(f"  Totaal orders: {len(all_orders)}")
    print(f"  YTD omzet:  EUR {ytd['total']:,.2f}")
    print(f"    B2B:      EUR {ytd['b2b']:,.2f}")
    print(f"    B2C:      EUR {ytd['b2c']:,.2f}")
    print(f"  MTD omzet:  EUR {mtd['total']:,.2f}")
    print("=" * 60)


if __name__ == "__main__":
    main()
