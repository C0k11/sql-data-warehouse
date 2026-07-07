"""Orchestrator: run the medallion pipeline end-to-end against SQL Server.

Executes the layer scripts in dependency order, then the stored procedures,
then the data quality gate. Any SQL error or failed error-severity quality
check aborts with a nonzero exit code, so this is CI-friendly.

Usage:
    py pipeline/run_pipeline.py                 # auto: full build first time, incremental after
    py pipeline/run_pipeline.py --steps all     # DESTRUCTIVE full rebuild (erases SCD2 history)
    py pipeline/run_pipeline.py --steps load    # incremental load + quality gates only

Connection: Windows integrated auth via ODBC Driver 17/18 (auto-detected).
Default server uses shared memory (lpc:) so it works even when TCP is disabled
on the instance; set DWH_SA_PASSWORD for SQL auth (CI).
"""
import argparse
import os
import re
import sys
import time
from pathlib import Path

import pyodbc

ROOT = Path(__file__).resolve().parent.parent

INIT_SCRIPTS = [
    "scripts/init_database.sql",
]
DDL_SCRIPTS = [
    "scripts/bronze/ddl_bronze.sql",
    "scripts/bronze/proc_load_bronze.sql",
    "scripts/silver/ddl_silver.sql",
    "scripts/silver/proc_load_silver.sql",
    "scripts/gold/ddl_gold.sql",
    "scripts/gold/proc_load_gold.sql",
    "tests/seed_quality_checks.sql",
    "tests/proc_run_quality_checks.sql",
]
LOAD_PROCS = [
    "EXEC bronze.load_bronze;",
    "EXEC silver.load_silver;",
    "EXEC gold.load_gold;",
]

LANDING = ROOT / "db" / "landing"
FILE_TABLE_MAP = {
    "source_crm/cust_info.csv": "bronze.crm_cust_info",
    "source_crm/prd_info.csv": "bronze.crm_prd_info",
    "source_crm/sales_details.csv": "bronze.crm_sales_details",
    "source_erp/CUST_AZ12.csv": "bronze.erp_cust_az12",
    "source_erp/LOC_A101.csv": "bronze.erp_loc_a101",
    "source_erp/PX_CAT_G1V2.csv": "bronze.erp_px_cat_g1v2",
}

# Paths as seen BY THE SQL SERVER PROCESS. Locally these equal the repo paths;
# in CI the repo is bind-mounted into the container, so they differ.
DB_DATADIR = os.environ.get("DWH_DB_DATADIR", str(ROOT / "db"))
DB_LANDING = os.environ.get("DWH_DB_LANDING", str(LANDING))

SQLCMD_VARS = {
    "DATA_DIR": DB_DATADIR,
    "DATA_ROOT": DB_LANDING,
}


def normalize_to_landing() -> None:
    """Copy raw extracts into the landing zone, normalized for BULK INSERT.

    Raw files are UTF-8 with CRLF endings and sometimes no final newline;
    BULK INSERT drops the unterminated last row, leaks CR into the last
    column, and decodes UTF-8 with a legacy code page (CODEPAGE 65001 fixes
    Windows but is rejected by SQL Server on Linux). Landing copies are
    therefore rewritten as UTF-16 with LF endings — DATAFILETYPE='widechar'
    reads UTF-16 identically on both platforms. Raw files are never mutated.
    """
    for rel in FILE_TABLE_MAP:
        src = ROOT / "datasets" / rel
        dst = LANDING / rel
        dst.parent.mkdir(parents=True, exist_ok=True)
        text = src.read_bytes().decode("utf-8-sig").replace("\r\n", "\n")
        if not text.endswith("\n"):
            text += "\n"
        dst.write_bytes(text.encode("utf-16"))  # BOM + UTF-16LE
    print(f"--> landing zone refreshed: {len(FILE_TABLE_MAP)} files (UTF-16) -> {LANDING}")


def reconcile_bronze(conn: pyodbc.Connection) -> None:
    """Fail if bronze row counts do not match the RAW source files."""
    cursor = conn.cursor()
    print("--> reconciling source files vs bronze")
    mismatches = []
    for rel, table in FILE_TABLE_MAP.items():
        raw_text = (ROOT / "datasets" / rel).read_bytes().decode("utf-8-sig")
        expected = len(raw_text.splitlines()) - 1  # minus header
        cursor.execute(f"SELECT COUNT(*) FROM DataWarehouse.{table};")
        actual = cursor.fetchone()[0]
        status = "OK " if expected == actual else "MISMATCH"
        print(f"    {status} {table}: file={expected} bronze={actual}")
        if expected != actual:
            mismatches.append(table)
    cursor.close()
    if mismatches:
        raise RuntimeError(f"Bronze reconciliation failed: {mismatches}")


def connect(server: str) -> pyodbc.Connection:
    driver = next(
        d for d in sorted(pyodbc.drivers(), reverse=True)
        if re.fullmatch(r"ODBC Driver 1[78] for SQL Server", d)
    )
    sa_password = os.environ.get("DWH_SA_PASSWORD")
    auth = f"UID=sa;PWD={sa_password};" if sa_password else "Trusted_Connection=yes;"
    return pyodbc.connect(
        # TrustServerCertificate unconditionally: Driver 18 defaults to
        # Encrypt=yes and a local instance presents a self-signed certificate
        f"DRIVER={{{driver}}};SERVER={server};DATABASE=master;"
        f"{auth}TrustServerCertificate=yes;",
        autocommit=True,  # CREATE DATABASE cannot run inside a transaction
    )


def split_batches(sql_text: str) -> list[str]:
    """Split a sqlcmd-style script into batches on lines containing only GO."""
    batches, current = [], []
    for line in sql_text.splitlines():
        if re.fullmatch(r"\s*GO\s*;?\s*", line, flags=re.IGNORECASE):
            if any(l.strip() for l in current):
                batches.append("\n".join(current))
            current = []
        else:
            current.append(line)
    if any(l.strip() for l in current):
        batches.append("\n".join(current))
    return batches


def preprocess(sql_text: str, variables: dict) -> str:
    """Resolve :setvar defaults, then substitute $(VAR) occurrences."""
    resolved = dict(variables)
    out_lines = []
    for line in sql_text.splitlines():
        m = re.match(r'\s*:setvar\s+(\w+)\s+"([^"]*)"', line)
        if m:
            resolved.setdefault(m.group(1), m.group(2))
            continue  # drop the :setvar line; pyodbc has no sqlcmd mode
        out_lines.append(line)
    text = "\n".join(out_lines)
    for name, value in resolved.items():
        text = text.replace(f"$({name})", value)
    return text


def drain_messages(cursor: pyodbc.Cursor) -> None:
    """Print server PRINT/RAISERROR messages from all result sets."""
    while True:
        for _, message in cursor.messages:
            print(f"    {message.split(']')[-1].strip()}")
        if not cursor.nextset():
            break


def run_sql(conn: pyodbc.Connection, label: str, sql_text: str) -> None:
    t0 = time.perf_counter()
    print(f"--> {label}")
    for batch in split_batches(sql_text):
        cursor = conn.cursor()
        cursor.execute(batch)
        drain_messages(cursor)
        cursor.close()
    print(f"    done in {time.perf_counter() - t0:.2f}s")


def print_run_summary(conn: pyodbc.Connection) -> None:
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT r.layer, d.table_name, d.rows_loaded, d.duration_ms
        FROM DataWarehouse.etl.load_run_detail d
        JOIN DataWarehouse.etl.load_run r ON r.run_id = d.run_id
        WHERE r.run_id IN (SELECT MAX(run_id) FROM DataWarehouse.etl.load_run GROUP BY layer)
        ORDER BY d.detail_id;
        """
    )
    print("\n=== Load summary (latest run per layer) ===")
    for layer, table, rows, ms in cursor.fetchall():
        print(f"  {layer:<7} {table:<50} {rows if rows is not None else '-':>8} rows  {ms:>6} ms")
    cursor.execute(
        """
        SELECT c.check_name, q.violations, q.status
        FROM DataWarehouse.etl.quality_result q
        JOIN DataWarehouse.etl.quality_check c ON c.check_id = q.check_id
        WHERE q.checked_at >= (SELECT MAX(started_at) FROM DataWarehouse.etl.load_run)
        ORDER BY q.result_id;
        """
    )
    rows = cursor.fetchall()
    if rows:
        print(f"\n=== Quality checks ({len(rows)}) ===")
        for name, violations, status in rows:
            mark = {"pass": "PASS", "warn": "WARN", "fail": "FAIL"}[status]
            print(f"  {mark}  {name} ({violations} violations)")
    cursor.close()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--server", default="lpc:localhost\\SQLEXPRESS")
    parser.add_argument(
        "--steps", default="auto", choices=["auto", "all", "init", "load"],
        help="auto = 'all' when the DB is missing, else 'load' (DESTRUCTIVE "
             "rebuilds — which erase SCD2 history — only happen when explicitly "
             "requested or on first run); all = drop + init + ddl + load + tests; "
             "init = rebuild objects only; load = load procs + quality gates",
    )
    args = parser.parse_args()

    conn = connect(args.server)
    try:
        steps = args.steps
        if steps == "auto":
            cursor = conn.cursor()
            cursor.execute("SELECT DB_ID('DataWarehouse');")
            steps = "load" if cursor.fetchone()[0] else "all"
            cursor.close()
            print(f"--> steps=auto resolved to '{steps}'")
        if steps in ("all", "init"):
            for rel in INIT_SCRIPTS + DDL_SCRIPTS:
                text = preprocess((ROOT / rel).read_text(encoding="utf-8"), SQLCMD_VARS)
                run_sql(conn, rel, text)
        if steps in ("all", "load"):
            normalize_to_landing()
            conn.cursor().execute("USE DataWarehouse;").close()
            conn.cursor().execute(
                "UPDATE etl.config SET config_value = ? WHERE config_key = 'data_root';",
                DB_LANDING,
            ).close()
            run_sql(conn, LOAD_PROCS[0], LOAD_PROCS[0])       # bronze
            reconcile_bronze(conn)
            run_sql(conn, LOAD_PROCS[1], LOAD_PROCS[1])       # silver
            # gate silver BEFORE gold mutates the persistent SCD2 dimension
            run_sql(conn, "silver quality gate",
                    "EXEC etl.run_quality_checks @layer = 'silver';")
            run_sql(conn, LOAD_PROCS[2], LOAD_PROCS[2])       # gold
            run_sql(conn, "gold quality gate",
                    "EXEC etl.run_quality_checks @layer = 'gold';")
            print_run_summary(conn)
        print("\nPipeline finished: SUCCESS")
        return 0
    except (pyodbc.Error, RuntimeError) as exc:
        print(f"\nPipeline FAILED: {exc}", file=sys.stderr)
        try:
            print_run_summary(conn)
        except pyodbc.Error:
            pass
        return 1
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
