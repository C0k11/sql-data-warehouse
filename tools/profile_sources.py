"""Profile raw CRM/ERP CSVs: nulls, dirt patterns, key overlaps, math errors.

Output feeds silver-layer cleansing rules and the data catalog.
"""
import csv
import io
import re
import sys
from collections import Counter
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
ROOT = Path(__file__).resolve().parent.parent / "datasets"


def load(rel):
    with open(ROOT / rel, newline="", encoding="utf-8-sig") as f:
        rows = list(csv.DictReader(f))
    print(f"\n{'='*70}\n{rel}: {len(rows)} rows, cols={list(rows[0].keys())}")
    return rows


def col_report(rows, col, top=6):
    vals = [r[col] for r in rows]
    empty = sum(1 for v in vals if v is None or v.strip() == "")
    ws = sum(1 for v in vals if v and v != v.strip())
    distinct = set(vals)
    print(f"  {col}: empty={empty} untrimmed={ws} distinct={len(distinct)}", end="")
    if len(distinct) <= top:
        print(f" values={sorted(distinct)!r}")
    else:
        print(f" sample={sorted(distinct)[:top]!r}")


cust = load("source_crm/cust_info.csv")
for c in cust[0]:
    col_report(cust, c)
ids = [r["cst_id"] for r in cust]
dup = {k: v for k, v in Counter(ids).items() if v > 1 and k.strip()}  # blanks are a separate rule
print(f"  dup cst_id: {len(dup)} ids, e.g. {list(dup.items())[:5]}")

prd = load("source_crm/prd_info.csv")
for c in prd[0]:
    col_report(prd, c)
# prd_key embeds category prefix: first 5 chars -> erp PX_CAT id (with _ vs -)
print("  prd_key prefixes:", sorted({r["prd_key"][:5] for r in prd})[:10])
end_before_start = sum(
    1 for r in prd if r["prd_end_dt"] and r["prd_end_dt"] < r["prd_start_dt"]
)
print(f"  prd_end_dt < prd_start_dt: {end_before_start}")

sales = load("source_crm/sales_details.csv")
for c in sales[0]:
    col_report(sales, c)
bad_dt = sum(
    1
    for r in sales
    for c in ("sls_order_dt", "sls_ship_dt", "sls_due_dt")
    if not re.fullmatch(r"20\d{6}", r[c])
)
math_err = 0
neg_or_zero = 0
for r in sales:
    try:
        s, q, p = int(r["sls_sales"] or 0), int(r["sls_quantity"] or 0), int(r["sls_price"] or 0)
        if s != q * p:
            math_err += 1
        if s <= 0 or q <= 0 or p <= 0:
            neg_or_zero += 1
    except ValueError:
        math_err += 1
print(f"  bad int dates: {bad_dt}, sales!=qty*price: {math_err}, nonpositive: {neg_or_zero}")

az = load("source_erp/CUST_AZ12.csv")
for c in az[0]:
    col_report(az, c)
future_bd = sum(1 for r in az if r["BDATE"] > "2026-07-07")
print(f"  future birthdates: {future_bd}")

loc = load("source_erp/LOC_A101.csv")
for c in loc[0]:
    col_report(loc, c)
print("  CNTRY variants:", sorted(Counter(r["CNTRY"].strip() for r in loc).items()))

px = load("source_erp/PX_CAT_G1V2.csv")
for c in px[0]:
    col_report(px, c)

# --- key overlap analysis ---
print(f"\n{'='*70}\nKEY OVERLAP")
crm_keys = {r["cst_key"].strip() for r in cust}
az_norm = {re.sub(r"^NAS", "", r["CID"].strip()) for r in az}
loc_norm = {r["CID"].strip().replace("-", "") for r in loc}
print(f"  crm cst_key n={len(crm_keys)}; az12 normalized match={len(crm_keys & az_norm)}; loc normalized match={len(crm_keys & loc_norm)}")
prd_cat = {r["prd_key"][:5].replace("-", "_") for r in prd}
px_ids = {r["ID"].strip() for r in px}
print(f"  prd cat prefixes n={len(prd_cat)}; px match={len(prd_cat & px_ids)}; unmatched={sorted(prd_cat - px_ids)}")
sls_prd = {r["sls_prd_key"].strip() for r in sales}
prd_model = {r["prd_key"][7:] for r in prd}
print(f"  sales prd_key n={len(sls_prd)}; match vs prd_key[7:]={len(sls_prd & prd_model)}; unmatched={len(sls_prd - prd_model)}")
sls_cust = {r["sls_cust_id"].strip() for r in sales}
print(f"  sales cust ids n={len(sls_cust)}; match vs cst_id={len(sls_cust & set(ids))}; unmatched={len(sls_cust - set(ids))}")
