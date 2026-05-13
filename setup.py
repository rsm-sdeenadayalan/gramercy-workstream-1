"""
One-time setup script for the Gramercy sub-index pipelines.

Creates the four sub-index databases (if missing) and applies the corresponding
schemas. Safe to re-run — every step is idempotent.

Usage:
    python setup.py

Requires the SSH tunnel to be open and a user with CREATEDB privilege configured
in .env (POSTGRES_USER / POSTGRES_PASSWORD).
"""

from dotenv import load_dotenv
load_dotenv()

import os
import sys
from pathlib import Path

import psycopg2

ROOT = Path(__file__).parent

DB_CONFIG_BASE = {
    "host":     os.environ.get("POSTGRES_HOST", "localhost"),
    "port":     int(os.environ.get("POSTGRES_PORT", 5433)),
    "user":     os.environ.get("POSTGRES_USER", ""),
    "password": os.environ.get("POSTGRES_PASSWORD", ""),
}

GRAMERCY_DB = os.environ.get("POSTGRES_DB", "gramercy_workstream1")

# All schemas are applied to the single gramercy_workstream1 database.
SCHEMAS = [
    "schema.sql",
    "si3_schema.sql",
    "si3_flat_schema.sql",
    "score_schema.sql",
]


def _connect(dbname: str):
    return psycopg2.connect(**{**DB_CONFIG_BASE, "dbname": dbname})


def apply_schema(dbname: str, schema_file: Path):
    if not schema_file.exists():
        print(f"  ⚠ Schema file not found: {schema_file} — skipping.")
        return
    sql = schema_file.read_text()
    conn = _connect(dbname)
    try:
        print(f"  Applying {schema_file.name} to '{dbname}'…")
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()
        print(f"  ✓ {schema_file.name} applied.")
    except psycopg2.errors.InsufficientPrivilege:
        conn.rollback()
        print(f"  ⚠ Insufficient privilege to apply {schema_file.name}. "
              f"Ask the DB admin to GRANT ALL ON SCHEMA public TO "
              f"{DB_CONFIG_BASE['user']};")
    finally:
        conn.close()


def main():
    print(f"\n{'='*60}")
    print(f"  Gramercy Sub-Index — One-Time DB Setup")
    print(f"  Server: {DB_CONFIG_BASE['host']}:{DB_CONFIG_BASE['port']}")
    print(f"  User:   {DB_CONFIG_BASE['user']}")
    print(f"  DB:     {GRAMERCY_DB}")
    print(f"{'='*60}\n")

    # Verify connection
    print("[1/2] Verifying database connection…")
    try:
        conn = _connect(GRAMERCY_DB)
        conn.close()
        print(f"  ✓ Connected to '{GRAMERCY_DB}'.")
    except psycopg2.OperationalError as e:
        print(f"  ✗ Cannot connect to '{GRAMERCY_DB}': {e}")
        print("\nMake sure the SSH tunnel is open and the database exists on the server.")
        sys.exit(1)

    # Apply schemas
    print("\n[2/2] Applying schemas…")
    for schema_filename in SCHEMAS:
        apply_schema(GRAMERCY_DB, ROOT / schema_filename)

    print(f"\n{'='*60}")
    print("  Setup complete. You can now run:  python run_all.py")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
