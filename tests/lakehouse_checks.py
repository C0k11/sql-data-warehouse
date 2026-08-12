"""Behavioural tests for the Delta build. Run after the pipeline succeeds.

Covers the two things the quality gate cannot: that non-ASCII survives
ingestion byte-for-byte, and that the Type-2 dimension actually versions
changes and stays idempotent when nothing changed.

Usage:  python -m tests.lakehouse_checks
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from lakehouse.run_pipeline import main as run_pipeline           # noqa: E402
from lakehouse.session import catalog_name, get_spark             # noqa: E402

# A plain literal is safe here because the assertion lives in a UTF-8 Python
# source file that CI executes directly. The SQL Server suite cannot do this:
# its assertion travels yml -> shell -> sqlcmd, and every hop re-encodes, so it
# has to compare raw bytes (0x4A006F007300E900) to avoid false passes.
EXPECTED_NAME = "José"          # José
PROBE_CUSTOMER = 11253
SCD2_CUSTOMER = 11000

failures: list[str] = []


def check(label: str, condition: bool, detail: str = "") -> None:
    if condition:
        print(f"  PASS  {label}")
    else:
        print(f"  FAIL  {label}{': ' + detail if detail else ''}")
        failures.append(label)


def main() -> int:
    spark = get_spark("lakehouse-checks")
    cat = catalog_name()

    print("\n=== Non-ASCII round-trip ===")
    row = spark.sql(
        f"SELECT cst_firstname FROM {cat}.silver.crm_cust_info WHERE cst_id = {PROBE_CUSTOMER}"
    ).first()
    actual = row["cst_firstname"] if row else None
    check(
        f"customer {PROBE_CUSTOMER} first name round-trips as {EXPECTED_NAME!r}",
        actual == EXPECTED_NAME,
        f"got {actual!r} (codepoints {[hex(ord(c)) for c in actual or '']})",
    )
    non_ascii = spark.sql(
        f"SELECT count(*) AS n FROM {cat}.silver.crm_cust_info "
        f"WHERE cst_firstname rlike '[^\\\\x00-\\\\x7F]' OR cst_lastname rlike '[^\\\\x00-\\\\x7F]'"
    ).first()["n"]
    check("non-ASCII names survive ingestion", non_ascii > 0, f"found {non_ascii}")
    # A mojibake decode turns one accented byte pair into two Latin-1 chars, so
    # the replacement character is the tell that the encoding option was ignored.
    mojibake = spark.sql(
        f"SELECT count(*) AS n FROM {cat}.silver.crm_cust_info "
        f"WHERE cst_firstname LIKE '%{chr(0xFFFD)}%' OR cst_lastname LIKE '%{chr(0xFFFD)}%'"
    ).first()["n"]
    check("no replacement characters in names", mojibake == 0, f"found {mojibake}")

    print("\n=== SCD2 change capture ===")
    before = spark.sql(
        f"SELECT count(*) AS n FROM {cat}.gold.dim_customers_scd2 "
        f"WHERE customer_id = {SCD2_CUSTOMER}"
    ).first()["n"]
    spark.sql(f"""
        UPDATE {cat}.bronze.crm_cust_info
        SET cst_marital_status = CASE WHEN cst_marital_status = 'S' THEN 'M' ELSE 'S' END
        WHERE cst_id = {SCD2_CUSTOMER}
    """)
    if run_pipeline(["--datasets", str(Path(__file__).resolve().parent.parent / "datasets"),
                     "--layer", "gold", "--skip-bronze"]) != 0:
        print("  FAIL  re-run after mutation")
        return 1

    versions = spark.sql(f"""
        SELECT marital_status, valid_from, valid_to, is_current
        FROM {cat}.gold.dim_customers_scd2
        WHERE customer_id = {SCD2_CUSTOMER} ORDER BY valid_from
    """).collect()
    check(f"customer {SCD2_CUSTOMER} gained a version",
          len(versions) == before + 1, f"{before} -> {len(versions)}")
    check("exactly one current version",
          sum(1 for v in versions if v["is_current"]) == 1)
    check("validity windows are contiguous (no as-of gaps)",
          all(versions[i]["valid_to"] == versions[i + 1]["valid_from"]
              for i in range(len(versions) - 1)),
          str([(str(v["valid_from"]), str(v["valid_to"])) for v in versions]))
    check("attribute actually changed between versions",
          len({v["marital_status"] for v in versions}) > 1)

    print("\n=== SCD2 idempotence ===")
    total_before = spark.table(f"{cat}.gold.dim_customers_scd2").count()
    if run_pipeline(["--datasets", str(Path(__file__).resolve().parent.parent / "datasets"),
                     "--layer", "gold", "--skip-bronze"]) != 0:
        print("  FAIL  idempotent re-run")
        return 1
    total_after = spark.table(f"{cat}.gold.dim_customers_scd2").count()
    check("re-running with unchanged source is a no-op",
          total_before == total_after, f"{total_before} -> {total_after}")

    print()
    if failures:
        print(f"{len(failures)} check(s) FAILED: {', '.join(failures)}")
        return 1
    print("All lakehouse checks passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
