#!/usr/bin/env python3
"""
Gold Dry â Revenue Data Collector
==================================
Haalt omzetdata op uit Shopify (B2B) en WooCommerce (B2C),
verwerkt deze (ex BTW, ex statiegeld) en genereert een JSON-databestand
dat het HTML dashboard voedt.

Configuratie:
  Vul de API credentials in bij de CONFIG sectie hieronder,
  of zet ze als environment variables.

Gebruik:
  python3 gold_dry_data_collector.py

Output:
  gold_dry_dashboard_data.json â wordt ingelezen door het HTML dashboard
"""

import json
import os
import sys
from datetime import datetime, timedelta
from collections import defaultdict

# ============================================================
# CONFIG â Vul hier je API credentials in
# ============================================================

SHOPIFY_CONFIG = {
    "store_url": os.getenv("SHOPIFY_STORE_URL", ""),
    "api_key": os.getenv("SHOPIFY_API_KEY", ""),
    "api_secret": os.getenv("SHOPIFY_API_SECRET", ""),
    "access_token": os.getenv("SHOPIFY_ACCESS_TOKEN", ""),
    "api_version": "2024-01"
}

WOOCOMMERCE_CONFIG = {
    "site_url": os.getenv("WOO_SITE_URL", ""),
    "consumer_key": os.getenv("WOO_CONSUMER_KEY", ""),
    "consumer_secret": os.getenv("WOO_CONSUMER_SECRET", ""),
}

# Statiegeld configuratie
STATIEGELD_PER_TRAY = 1.80   # 12 blikken x â¬0.15
BTW_RATE = 0.21              # 21% BTW

# Output pad
OUTPUT_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_JSON = os.path.join(OUTPUT_DIR, "gold_dry_dashboard_data.json")
OUTPUT_HTML = os.path.join(OUTPUT_DIR, "index.html")


# ============================================================
# SHOPIFY API CLIENT
# ============================================================

def shopify_fetch_orders(since_date, until_date=None):
    """Haal orders op uit Shopify (B2B)."""
    try:
        import requests
    except ImportError:
        print("ERROR: 'requests' package niet gevonden. Installeer met: pip install requests")
        return []

    if not SHOPIFY_CONFIG["store_url"] or not SHOPIFY_CONFIG["access_token"]:
        print("WARN: Shopify credentials niet ingesteld â gebruik demo data")
        return None

    base_url = f"https://{SHOPIFY_CONFIG['store_url']}/admin/api/{SHOPIFY_CONFIG['api_version']}"
    headers = {
        "X-Shopify-Access-Token": SHOPIFY_CONFIG["access_token"],
        "Content-Type": "application/json"
    }

    orders = []
    params = {
        "status": "any",
        "financial_status": "paid",
        "created_at_min": since_date.isoformat(),
        "limit": 250
    }
    if until_date:
        params["created_at_max"] = until_date.isoformat()

    url = f"{base_url}/orders.json"

    while url:
        resp = requests.get(url, headers=headers, params=params)
        if resp.status_code != 200:
            print(f"ERROR Shopify API: {resp.status_code} â {resp.text[:200]}")
            return None

        data = resp.json()
        orders.extend(data.get("orders", []))

        # Pagination via Link header
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
    """Verwerk Shopify orders: bereken omzet ex BTW, ex statiegeld."""
    results = []
    for order in orders:
        # Parse datum
        created = datetime.fromisoformat(order["created_at"].replace("Z", "+00:00"))

        # Bruto omzet
        total_price = float(order.get("total_price", 0))
        tax = float(order.get("total_tax", 0))

        # Tel trays voor statiegeld aftrek
        total_statiegeld = 0
        items = []
        for item in order.get("line_items", []):
            qty = item.get("quantity", 0)
            sku = item.get("sku", "UNKNOWN")
            name = item.get("title", item.get("name", ""))
            price_ex_tax = float(item.get("price", 0)) * qty

            # Statiegeld: per tray (elke order line met blikken)
            # Aanname: elk product is 1 tray tenzij SKU anders aangeeft
            tray_statiegeld = qty * STATIEGELD_PER_TRAY
            total_statiegeld += tray_statiegeld

            items.append({
                "sku": sku,
                "name": name,
                "quantity": qty,
                "revenue_ex": price_ex_tax - tray_statiegeld
            })

        # Omzet ex BTW ex statiegeld
        revenue_ex = total_price - tax - total_statiegeld

        results.append({
            "date": created.strftime("%Y-%m-%d"),
            "month": created.strftime("%Y-%m"),
            "year": created.year,
            "order_id": order.get("name", order.get("id")),
            "customer": order.get("customer", {}).get("first_name", "") + " " +
                       order.get("customer", {}).get("last_name", ""),
            "company": (order.get("customer", {}).get("default_address", {}) or {}).get("company", ""),
            "channel": "B2B",
            "revenue_bruto": total_price,
            "tax": tax,
            "statiegeld": total_statiegeld,
            "revenue_ex": max(0, revenue_ex),
            "items": items
        })

    return results


# ============================================================
# WOOCOMMERCE API CLIENT
# ============================================================

def woo_fetch_orders(since_date, until_date=None):
    """Haal orders op uit WooCommerce (B2C)."""
    try:
        import requests
    except ImportError:
        return None

    if not WOOCOMMERCE_CONFIG["site_url"] or not WOOCOMMERCE_CONFIG["consumer_key"]:
        print("WARN: WooCommerce credentials niet ingesteld â gebruik demo data")
        return None

    base_url = f"{WOOCOMMERCE_CONFIG['site_url']}/wp-json/wc/v3"
    auth = (WOOCOMMERCE_CONFIG["consumer_key"], WOOCOMMERCE_CONFIG["consumer_secret"])

    orders = []
    page = 1

    while True:
        params = {
            "status": "completed",
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
            print(f"ERROR WooCommerce API: {resp.status_code} â {resp.text[:200]}")
            return None

        batch = resp.json()
        if not batch:
            break

        orders.extend(batch)
        page += 1

        # Check totaal pagina's
        total_pages = int(resp.headers.get("X-WP-TotalPages", 1))
        if page > total_pages:
            break

    return orders


def process_woo_orders(orders):
    """Verwerk WooCommerce orders: bereken omzet ex BTW, ex statiegeld."""
    results = []
    for order in orders:
        created = datetime.fromisoformat(order["date_created"])
        total_price = float(order.get("total", 0))
        tax = float(order.get("total_tax", 0))

        total_statiegeld = 0
        items = []
        for item in order.get("line_items", []):
            qty = item.get("quantity", 0)
            sku = item.get("sku", "UNKNOWN")
            name = item.get("name", "")
            price = float(item.get("total", 0))

            tray_statiegeld = qty * STATIEGELD_PER_TRAY
            total_statiegeld += tray_statiegeld

            items.append({
                "sku": sku,
                "name": name,
                "quantity": qty,
                "revenue_ex": price - tray_statiegeld
            })

        revenue_ex = total_price - tax - total_statiegeld
        customer_name = order.get("billing", {}).get("first_name", "") + " " + \
                       order.get("billing", {}).get("last_name", "")

        results.append({
            "date": created.strftime("%Y-%m-%d"),
            "month": created.strftime("%Y-%m"),
            "year": created.year,
            "order_id": str(order.get("number", order.get("id"))),
            "customer": customer_name.strip(),
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

def aggregate_dashboard_data(all_orders):
    """Aggregeer orders tot dashboard-data."""
    now = datetime.now()
    current_year = now.year
    current_month = now.month

    # MTD
    mtd_orders = [o for o in all_orders if o["year"] == current_year and
                  datetime.strptime(o["date"], "%Y-%m-%d").month == current_month]

    # YTD
    ytd_orders = [o for o in all_orders if o["year"] == current_year]

    # Prev year same period (voor vergelijking)
    prev_mtd = [o for o in all_orders if o["year"] == current_year - 1 and
                datetime.strptime(o["date"], "%Y-%m-%d").month == current_month and
                datetime.strptime(o["date"], "%Y-%m-%d").day <= now.day]

    prev_ytd = [o for o in all_orders if o["year"] == current_year - 1 and
                (datetime.strptime(o["date"], "%Y-%m-%d").month < current_month or
                 (datetime.strptime(o["date"], "%Y-%m-%d").month == current_month and
                  datetime.strptime(o["date"], "%Y-%m-%d").day <= now.day))]

    def period_stats(orders, prev_orders):
        b2b = [o for o in orders if o["channel"] == "B2B"]
        b2c = [o for o in orders if o["channel"] == "B2C"]
        prev_b2b = [o for o in prev_orders if o["channel"] == "B2B"]
        prev_b2c = [o for o in prev_orders if o["channel"] == "B2C"]
        return {
            "total": round(sum(o["revenue_ex"] for o in orders), 2),
            "b2b": round(sum(o["revenue_ex"] for o in b2b), 2),
            "b2c": round(sum(o["revenue_ex"] for o in b2c), 2),
            "b2bOrders": len(b2b),
            "b2cOrders": len(b2c),
            "prevTotal": round(sum(o["revenue_ex"] for o in prev_orders), 2),
            "prevB2B": round(sum(o["revenue_ex"] for o in prev_b2b), 2),
            "prevB2C": round(sum(o["revenue_ex"] for o in prev_b2c), 2),
        }

    # Jaaroverzichten
    years = {}
    for year in sorted(set(o["year"] for o in all_orders)):
        year_orders = [o for o in all_orders if o["year"] == year]
        years[year] = round(sum(o["revenue_ex"] for o in year_orders), 2)

    # Maandelijks (huidig jaar + vorig jaar)
    def monthly_revenue(year):
        monthly = [0] * 12
        for o in all_orders:
            if o["year"] == year:
                m = datetime.strptime(o["date"], "%Y-%m-%d").month - 1
                monthly[m] += o["revenue_ex"]
        return [round(v, 2) for v in monthly]

    # Top klanten
    customer_rev = defaultdict(lambda: {"revenue": 0, "orders": 0, "channel": ""})
    for o in ytd_orders:
        key = o["company"] if o["company"] else o["customer"]
        if not key.strip():
            key = f"Klant #{o['order_id']}"
        customer_rev[key]["revenue"] += o["revenue_ex"]
        customer_rev[key]["orders"] += 1
        customer_rev[key]["channel"] = o["channel"]

    top_customers = sorted(customer_rev.items(), key=lambda x: x[1]["revenue"], reverse=True)[:10]
    top_customers = [
        {"name": k, "channel": v["channel"], "revenue": round(v["revenue"], 2), "orders": v["orders"]}
        for k, v in top_customers
    ]

    # Top producten (op SKU)
    sku_stats = defaultdict(lambda: {"name": "", "sku": "", "units": 0, "revenue": 0})
    for o in ytd_orders:
        for item in o["items"]:
            sku = item["sku"]
            sku_stats[sku]["sku"] = sku
            sku_stats[sku]["name"] = item["name"]
            sku_stats[sku]["units"] += item["quantity"]
            sku_stats[sku]["revenue"] += item["revenue_ex"]

    top_products = sorted(sku_stats.values(), key=lambda x: x["revenue"], reverse=True)[:10]
    top_products = [
        {"name": p["name"], "sku": p["sku"], "units": p["units"], "revenue": round(p["revenue"], 2)}
        for p in top_products
    ]

    return {
        "lastUpdated": now.isoformat(),
        "mtd": period_stats(mtd_orders, prev_mtd),
        "ytd": period_stats(ytd_orders, prev_ytd),
        "years": years,
        f"monthly{current_year}": monthly_revenue(current_year),
        f"monthly{current_year - 1}": monthly_revenue(current_year - 1),
        "topCustomers": top_customers,
        "topProducts": top_products
    }


# ============================================================
# HTML UPDATER â Embed data in het dashboard
# ============================================================

def update_dashboard_html(data):
    """Update het HTML dashboard met verse data."""
    if not os.path.exists(OUTPUT_HTML):
        print(f"WARN: Dashboard HTML niet gevonden op {OUTPUT_HTML}")
        return

    with open(OUTPUT_HTML, "r", encoding="utf-8") as f:
        html = f.read()

    # Vervang de demoData object in de HTML
    import re
    data_json = json.dumps(data, indent=2, ensure_ascii=False)

    # Zoek het demoData blok en vervang het
    pattern = r'const demoData = \{.*?\};'
    replacement = f'const demoData = {data_json};'
    new_html = re.sub(pattern, replacement, html, flags=re.DOTALL)

    # Verwijder de demo banner als we echte data hebben
    new_html = new_html.replace(
        '<div class="demo-banner">',
        '<div class="demo-banner" style="display:none">'
    )

    with open(OUTPUT_HTML, "w", encoding="utf-8") as f:
        f.write(new_html)

    print(f"â Dashboard HTML bijgewerkt: {OUTPUT_HTML}")


# ============================================================
# MAIN
# ============================================================

def main():
    print("=" * 60)
    print("  GOLD DRY â Revenue Data Collector")
    print(f"  {datetime.now().strftime('%d %B %Y, %H:%M')}")
    print("=" * 60)

    # Bepaal datumrange: van 1 jan 3 jaar geleden tot nu
    now = datetime.now()
    since = datetime(now.year - 3, 1, 1)

    all_orders = []

    # Shopify (B2B)
    print("\nâ Shopify orders ophalen (B2B)...")
    shopify_orders = shopify_fetch_orders(since)
    if shopify_orders is not None:
        processed = process_shopify_orders(shopify_orders)
        all_orders.extend(processed)
        print(f"  â {len(processed)} B2B orders verwerkt")
    else:
        print("  â  Shopify overgeslagen (geen credentials)")

    # WooCommerce (B2C)
    print("\nâ WooCommerce orders ophalen (B2C)...")
    woo_orders = woo_fetch_orders(since)
    if woo_orders is not None:
        processed = process_woo_orders(woo_orders)
        all_orders.extend(processed)
        print(f"  â {len(processed)} B2C orders verwerkt")
    else:
        print("  â  WooCommerce overgeslagen (geen credentials)")

    if not all_orders:
        print("\nâ  Geen orders opgehaald â controleer je API credentials.")
        print("  Dashboard blijft op demo-data draaien.")
        return

    # Aggregeer
    print("\nâ Data aggregeren...")
    dashboard_data = aggregate_dashboard_data(all_orders)

    # Opslaan als JSON
    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump(dashboard_data, f, indent=2, ensure_ascii=False)
    print(f"  â Data opgeslagen: {OUTPUT_JSON}")

    # HTML bijwerken
    print("\nâ Dashboard HTML bijwerken...")
    update_dashboard_html(dashboard_data)

    # Samenvatting
    print("\n" + "=" * 60)
    print("  SAMENVATTING")
    print("=" * 60)
    print(f"  Totaal orders: {len(all_orders)}")
    print(f"  YTD omzet:     â¬{dashboard_data['ytd']['total']:,.2f}")
    print(f"    B2B:         â¬{dashboard_data['ytd']['b2b']:,.2f}")
    print(f"    B2C:         â¬{dashboard_data['ytd']['b2c']:,.2f}")
    print(f"  MTD omzet:     â¬{dashboard_data['mtd']['total']:,.2f}")
    print("=" * 60)


if __name__ == "__main__":
    main()
