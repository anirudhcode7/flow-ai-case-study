"""
FlowAI Bronze Layer Loader.

Reads all generated CSVs from data/raw/{emr,rcm,referral}/,
adds metadata columns (_source_system, _source_file, _row_hash),
creates the bronze schema (if not exists), and bulk-loads into DuckDB.

Usage:
  python ingestion/bronze/load_bronze.py
"""

import hashlib
import logging
import os
import sys

import duckdb
import pandas as pd

# Ensure project root is on sys.path when run as a script
_HERE = os.path.dirname(os.path.abspath(__file__))
_PROJECT_ROOT = os.path.dirname(os.path.dirname(_HERE))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from ingestion.generate import config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

# Map source subdirectory → source_system label → list of (csv_filename, bronze_table)
SOURCE_MAP = {
    "emr": [
        ("emr_patient.csv",   "bronze.emr_patient"),
        ("emr_encounter.csv", "bronze.emr_encounter"),
        ("emr_diagnosis.csv", "bronze.emr_diagnosis"),
        ("emr_procedure.csv", "bronze.emr_procedure"),
        ("emr_provider.csv",  "bronze.emr_provider"),
    ],
    "rcm": [
        ("rcm_patient_account.csv",   "bronze.rcm_patient_account"),
        ("rcm_claim_header.csv",      "bronze.rcm_claim_header"),
        ("rcm_claim_line.csv",        "bronze.rcm_claim_line"),
        ("rcm_remittance_835.csv",    "bronze.rcm_remittance_835"),
        ("rcm_ar_balance_snapshot.csv", "bronze.rcm_ar_balance_snapshot"),
    ],
    "referral": [
        ("referral_order.csv",                  "bronze.referral_order"),
        ("referral_order_status_history.csv",   "bronze.referral_order_status_history"),
        ("referral_document_reference.csv",     "bronze.referral_document_reference"),
    ],
}


def _md5_row(row: pd.Series) -> str:
    """Compute MD5 hash of a row's string representation."""
    row_str = "|".join(str(v) for v in row.values)
    return hashlib.md5(row_str.encode("utf-8")).hexdigest()


def _add_metadata(df: pd.DataFrame, source_system: str, source_file: str) -> pd.DataFrame:
    """Append Bronze metadata columns to DataFrame."""
    df = df.copy()
    df["_source_system"] = source_system
    df["_source_file"] = source_file
    df["_row_hash"] = df.apply(_md5_row, axis=1)
    # _ingested_at has a DEFAULT in DDL, but we set it explicitly for clarity
    from datetime import datetime, timezone
    df["_ingested_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    return df


def _load_schema(conn: duckdb.DuckDBPyConnection):
    """Execute schema.sql to create bronze schema and tables if not exists."""
    schema_path = os.path.join(_PROJECT_ROOT, "ingestion", "bronze", "schema.sql")
    if not os.path.isfile(schema_path):
        raise FileNotFoundError(f"Schema file not found: {schema_path}")
    with open(schema_path, "r") as fh:
        sql = fh.read()
    conn.execute(sql)
    logger.info("Bronze schema applied.")


def _load_table(
    conn: duckdb.DuckDBPyConnection,
    csv_path: str,
    table_name: str,
    source_system: str,
    source_file: str,
) -> int:
    """
    Load a single CSV into its bronze table.
    Returns number of rows inserted.
    Skips gracefully if the file doesn't exist.
    """
    if not os.path.isfile(csv_path):
        logger.warning("Skipping %s — file not found: %s", table_name, csv_path)
        return 0

    try:
        df = pd.read_csv(csv_path, dtype=str, low_memory=False)
    except Exception as exc:
        logger.error("Failed to read %s: %s", csv_path, exc)
        return 0

    if df.empty:
        logger.warning("Skipping %s — empty file: %s", table_name, csv_path)
        return 0

    df = _add_metadata(df, source_system, source_file)

    # Get existing table columns to handle schema drift gracefully
    try:
        existing_cols = [
            row[0]
            for row in conn.execute(
                f"SELECT column_name FROM information_schema.columns "
                f"WHERE table_schema = '{table_name.split('.')[0]}' "
                f"AND table_name = '{table_name.split('.')[1]}'"
            ).fetchall()
        ]
    except Exception:
        existing_cols = []

    if existing_cols:
        # Only keep columns that exist in the target table, in table order
        valid_cols = [c for c in existing_cols if c in df.columns]
        df = df[valid_cols]
    else:
        valid_cols = list(df.columns)

    try:
        conn.register("_staging", df)
        cols_quoted = ", ".join(f'"{c}"' for c in valid_cols)
        conn.execute(
            f"INSERT INTO {table_name} ({cols_quoted}) "
            f"SELECT {cols_quoted} FROM _staging"
        )
        conn.unregister("_staging")
    except Exception as exc:
        logger.error("Failed to insert into %s: %s", table_name, exc)
        return 0

    logger.info("Loaded %s: %d rows → %s", source_file, len(df), table_name)
    return len(df)


def main():
    print("=" * 60)
    print("FlowAI Bronze Layer Loader")
    print("=" * 60)

    os.makedirs(os.path.dirname(config.DUCKDB_PATH), exist_ok=True)

    conn = duckdb.connect(config.DUCKDB_PATH)
    logger.info("Connected to DuckDB: %s", config.DUCKDB_PATH)

    _load_schema(conn)

    total_rows = 0
    total_tables = 0

    for source_system, file_table_pairs in SOURCE_MAP.items():
        source_dir = os.path.join(config.DATA_RAW, source_system)
        print(f"\n[{source_system.upper()}]")
        for csv_filename, bronze_table in file_table_pairs:
            csv_path = os.path.join(source_dir, csv_filename)
            n = _load_table(conn, csv_path, bronze_table, source_system, csv_filename)
            total_rows += n
            if n > 0:
                total_tables += 1

    conn.close()

    print("\n" + "=" * 60)
    print(f"Bronze load complete: {total_rows} rows across {total_tables} tables")
    print(f"DuckDB file: {config.DUCKDB_PATH}")
    print("=" * 60)


if __name__ == "__main__":
    main()
