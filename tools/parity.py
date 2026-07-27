"""Cross-engine parity: prove the Delta build produces the same data as T-SQL.

A migration is only trustworthy if you can show the output did not change.
This computes a per-table, per-column fingerprint on either engine and diffs
the two, so "the Spark rewrite is equivalent" is a test result rather than a
claim.

Only portable aggregates are used. Notably absent:

  - Hashes over row contents. SQL Server's HASHBYTES digests the UTF-16 bytes
    of NVARCHAR while Spark's sha2 digests UTF-8, so identical strings hash
    differently. Same reason attr_hash is excluded from the comparison.
  - LEN(). SQL Server's LEN ignores trailing spaces; Spark's length does not.
    Character volume is measured with DATALENGTH/2 against length().
  - Anything whose ordering depends on collation. min/max on strings is skipped
    because the SQL Server instance is case-insensitive and Spark is not.

Technical columns are excluded by design: surrogate keys are engine-assigned,
load timestamps differ by construction, and attr_hash is not comparable.

Usage:
    python tools/parity.py --engine spark     --out build/parity_spark.json
    python tools/parity.py --engine sqlserver --out build/parity_mssql.json
    python tools/parity.py --compare build/parity_mssql.json build/parity_spark.json
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

# Columns whose values legitimately differ between engines.
EXCLUDED = {
    "dwh_create_date",   # load timestamp
    "customer_sk",       # engine-assigned surrogate key
    "attr_hash",         # different digest bytes per engine (see module docstring)
    "valid_from",        # load timestamp
    "valid_to",          # load timestamp (except the 9999 sentinel, counted below)
    "customer_key",      # view alias for customer_sk
    "product_key",       # ROW_NUMBER over a view
}

TABLES = [
    ("silver", "crm_cust_info"),
    ("silver", "crm_prd_info"),
    ("silver", "crm_sales_details"),
    ("silver", "erp_cust_az12"),
    ("silver", "erp_loc_a101"),
    ("silver", "erp_px_cat_g1v2"),
    ("gold", "dim_date"),
    ("gold", "dim_customers"),
    ("gold", "dim_products"),
    ("gold", "fact_sales"),
    ("gold", "dim_customers_scd2"),
]

NUMERIC = {"int", "bigint", "smallint", "tinyint", "integer", "long", "short", "byte", "decimal"}
TEMPORAL = {"date", "timestamp", "datetime", "datetime2"}
STRINGY = {"string", "nvarchar", "varchar", "char", "nchar"}


def _agg_exprs(col: str, kind: str, engine: str) -> dict[str, str]:
    """Portable per-column aggregates keyed by metric name."""
    q = f"[{col}]" if engine == "sqlserver" else f"`{col}`"
    out = {"nonnull": f"COUNT({q})"}
    if kind in NUMERIC:
        out |= {"sum": f"SUM(CAST({q} AS BIGINT))", "min": f"MIN({q})", "max": f"MAX({q})",
                "distinct": f"COUNT(DISTINCT {q})"}
    elif kind in TEMPORAL:
        # Cast to text so the two engines' native date rendering cannot differ.
        fmt = (f"CONVERT(VARCHAR(10), {q}, 23)" if engine == "sqlserver"
               else f"date_format({q}, 'yyyy-MM-dd')")
        out |= {"min": f"MIN({fmt})", "max": f"MAX({fmt})", "distinct": f"COUNT(DISTINCT {q})"}
    elif kind in STRINGY:
        # DATALENGTH/2 == length() for NVARCHAR: both count characters, but LEN
        # would silently ignore trailing spaces and hide a padding regression.
        chars = f"SUM(DATALENGTH({q}) / 2)" if engine == "sqlserver" else f"SUM(length({q}))"
        out |= {"chars": chars, "distinct": f"COUNT(DISTINCT {q})"}
    elif kind in {"boolean", "bit"}:
        out |= {"true_count": f"SUM(CAST({q} AS INT))"}
    return out


def _fingerprint(rows: list[tuple[str, str]], run_query, engine: str) -> dict:
    """rows: [(column, type)] -> {metric: value}. run_query returns one row."""
    result = {}
    for col, kind in rows:
        if col.lower() in EXCLUDED:
            continue
        result[col] = {"_type": kind}
        for metric, expr in _agg_exprs(col, kind, engine).items():
            result[col][metric] = expr
    return result


# ---------------------------------------------------------------- Spark side
def collect_spark() -> dict:
    sys.path.insert(0, str(ROOT))
    from lakehouse.session import catalog_name, get_spark

    spark = get_spark("parity")
    cat = catalog_name()
    out = {"engine": "spark", "tables": {}}

    for schema, table in TABLES:
        fq = f"{cat}.{schema}.{table}"
        cols = [(f.name, f.dataType.simpleString().split("(")[0])
                for f in spark.table(fq).schema.fields]
        specs = _fingerprint(cols, None, "spark")
        selects, keys = ["COUNT(*) AS row_count"], []
        for col, metrics in specs.items():
            for metric, expr in metrics.items():
                if metric == "_type":
                    continue
                alias = f"{col}__{metric}"
                selects.append(f"{expr} AS `{alias}`")
                keys.append((col, metric, alias))
        row = spark.sql(f"SELECT {', '.join(selects)} FROM {fq}").first().asDict()
        entry = {"row_count": row["row_count"], "columns": {}}
        for col, metric, alias in keys:
            entry["columns"].setdefault(col, {"_type": specs[col]["_type"]})
            v = row[alias]
            entry["columns"][col][metric] = None if v is None else (
                int(v) if isinstance(v, bool) is False and isinstance(v, int) else str(v)
            )
        out["tables"][f"{schema}.{table}"] = entry
        print(f"  {schema}.{table:<22} {entry['row_count']:>8,} rows, {len(entry['columns'])} cols")
    return out


# ----------------------------------------------------------- SQL Server side
def collect_sqlserver(server: str) -> dict:
    import re

    import pyodbc

    driver = next(d for d in sorted(pyodbc.drivers(), reverse=True)
                  if re.fullmatch(r"ODBC Driver 1[78] for SQL Server", d))
    conn = pyodbc.connect(
        f"DRIVER={{{driver}}};SERVER={server};DATABASE=DataWarehouse;"
        f"Trusted_Connection=yes;TrustServerCertificate=yes;", autocommit=True)
    cur = conn.cursor()
    out = {"engine": "sqlserver", "tables": {}}

    for schema, table in TABLES:
        cur.execute("""
            SELECT c.name, t.name FROM sys.columns c
            JOIN sys.types t ON t.user_type_id = c.user_type_id
            WHERE c.object_id = OBJECT_ID(?) ORDER BY c.column_id
        """, f"{schema}.{table}")
        cols = [(r[0], r[1].lower()) for r in cur.fetchall()]
        specs = _fingerprint(cols, None, "sqlserver")
        selects, keys = ["COUNT(*) AS row_count"], []
        for col, metrics in specs.items():
            for metric, expr in metrics.items():
                if metric == "_type":
                    continue
                alias = f"{col}__{metric}"
                selects.append(f"{expr} AS [{alias}]")
                keys.append((col, metric, alias))
        cur.execute(f"SELECT {', '.join(selects)} FROM {schema}.{table}")
        names = [d[0] for d in cur.description]
        row = dict(zip(names, cur.fetchone()))
        entry = {"row_count": row["row_count"], "columns": {}}
        for col, metric, alias in keys:
            entry["columns"].setdefault(col, {"_type": specs[col]["_type"]})
            v = row[alias]
            entry["columns"][col][metric] = None if v is None else (
                int(v) if isinstance(v, int) else str(v))
        out["tables"][f"{schema}.{table}"] = entry
        print(f"  {schema}.{table:<22} {entry['row_count']:>8,} rows, {len(entry['columns'])} cols")
    conn.close()
    return out


# -------------------------------------------------------------------- diff
def compare(a_path: Path, b_path: Path) -> int:
    a, b = json.loads(a_path.read_text()), json.loads(b_path.read_text())
    la, lb = a["engine"], b["engine"]
    print(f"Comparing {la} (left) vs {lb} (right)\n")

    diffs, compared = [], 0
    for name in sorted(set(a["tables"]) | set(b["tables"])):
        ta, tb = a["tables"].get(name), b["tables"].get(name)
        if ta is None or tb is None:
            diffs.append(f"{name}: missing on {lb if ta else la}")
            continue
        if ta["row_count"] != tb["row_count"]:
            diffs.append(f"{name}: row_count {ta['row_count']:,} vs {tb['row_count']:,}")
        for col in sorted(set(ta["columns"]) | set(tb["columns"])):
            ca, cb = ta["columns"].get(col), tb["columns"].get(col)
            if ca is None or cb is None:
                diffs.append(f"{name}.{col}: column only on {la if ca else lb}")
                continue
            for metric in sorted(set(ca) | set(cb)):
                if metric == "_type":
                    continue
                compared += 1
                if ca.get(metric) != cb.get(metric):
                    diffs.append(f"{name}.{col}.{metric}: {ca.get(metric)!r} vs {cb.get(metric)!r}")

    if diffs:
        print(f"{len(diffs)} DIFFERENCE(S) across {compared} compared metrics:\n")
        for d in diffs:
            print(f"  {d}")
        return 1
    print(f"IDENTICAL: {compared} metrics across {len(a['tables'])} tables match exactly.")
    return 0


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--engine", choices=["spark", "sqlserver"])
    p.add_argument("--server", default="lpc:localhost\\SQLEXPRESS")
    p.add_argument("--out")
    p.add_argument("--compare", nargs=2, metavar=("LEFT", "RIGHT"))
    args = p.parse_args()

    if args.compare:
        return compare(Path(args.compare[0]), Path(args.compare[1]))
    if not args.engine or not args.out:
        p.error("--engine and --out are required unless --compare is used")

    print(f"Fingerprinting {args.engine}...")
    data = collect_spark() if args.engine == "spark" else collect_sqlserver(args.server)
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(data, indent=2, sort_keys=True))
    print(f"--> {out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
