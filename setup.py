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

# Per-database: (db_name, schema_file_relative_to_root)
DATABASES = [
    ("subindex_1", "schema.sql"),
    ("subindex_2", "schema.sql"),
    ("subindex_3", "si3_schema.sql"),
    ("subindex_4", "schema.sql"),
    ("csi_scores", "score_schema.sql"),
]

# Candidate admin DBs to connect to for issuing CREATE DATABASE.
# `postgres` is the conventional default but isn't always present.
ADMIN_DB_CANDIDATES = ["postgres", "template1", DB_CONFIG_BASE["user"] or "postgres"]


def _connect(dbname: str):
    return psycopg2.connect(**{**DB_CONFIG_BASE, "dbname": dbname})


def _admin_conn():
    """Open an autocommit connection to any reachable admin DB."""
    last_err = None
    for db in ADMIN_DB_CANDIDATES:
        try:
            conn = _connect(db)
            conn.autocommit = True
            return conn
        except psycopg2.OperationalError as e:
            last_err = e
            continue
    raise RuntimeError(
        f"Could not connect to any admin DB ({ADMIN_DB_CANDIDATES}). "
        f"Last error: {last_err}"
    )


def database_exists(admin_conn, dbname: str) -> bool:
    with admin_conn.cursor() as cur:
        cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (dbname,))
        return cur.fetchone() is not None


def create_database(admin_conn, dbname: str):
    print(f"  Creating database '{dbname}'…")
    with admin_conn.cursor() as cur:
        cur.execute(f'CREATE DATABASE "{dbname}"')
    print(f"  ✓ Database '{dbname}' created.")


def apply_schema(dbname: str, schema_file: Path):
    if not schema_file.exists():
        print(f"  ⚠ Schema file not found: {schema_file} — skipping.")
        return
    print(f"  Applying {schema_file.name} to '{dbname}'…")
    sql = schema_file.read_text()
    conn = _connect(dbname)
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()
    finally:
        conn.close()
    print(f"  ✓ {schema_file.name} applied to '{dbname}'.")


def main():
    print(f"\n{'='*60}")
    print(f"  Gramercy Sub-Index — One-Time DB Setup")
    print(f"  Server: {DB_CONFIG_BASE['host']}:{DB_CONFIG_BASE['port']}")
    print(f"  User:   {DB_CONFIG_BASE['user']}")
    print(f"{'='*60}\n")

    # Phase 1: ensure every database exists
    print("[1/2] Creating databases…")
    try:
        admin = _admin_conn()
    except RuntimeError as e:
        print(f"  ✗ {e}")
        print("\nMake sure the SSH tunnel is open and POSTGRES_USER has CREATEDB privilege.")
        sys.exit(1)

    try:
        for dbname, _ in DATABASES:
            if database_exists(admin, dbname):
                print(f"  ✓ Database '{dbname}' already exists.")
            else:
                try:
                    create_database(admin, dbname)
                except psycopg2.errors.InsufficientPrivilege as e:
                    print(f"  ✗ Cannot create '{dbname}': {e}")
                    print("    Ask your DB admin to: GRANT CREATEDB ON DATABASE postgres TO "
                          f"{DB_CONFIG_BASE['user']};")
                    sys.exit(1)
    finally:
        admin.close()

    # Phase 2: apply schemas
    print("\n[2/2] Applying schemas…")
    for dbname, schema_filename in DATABASES:
        apply_schema(dbname, ROOT / schema_filename)

    print(f"\n{'='*60}")
    print("  Setup complete. You can now run:  python run_all.py")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
