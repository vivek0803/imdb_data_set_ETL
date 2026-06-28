#!/usr/bin/env python3
"""
load_to_olap.py
───────────────
Loads the Snappy-compressed Parquet lake (produced by etl_job.py) into
ClickHouse using the clickhouse-connect Python client.

Datasets loaded:
  data/lake/titles/      → imdb.titles
  data/lake/people/      → imdb.people
  data/lake/principals/  → imdb.principals
  data/lake/akas/        → imdb.akas

Usage:
  python3 load_to_olap.py
  python3 load_to_olap.py --truncate          # re-run safely (truncates first)
  python3 load_to_olap.py --only titles       # load one dataset
  python3 load_to_olap.py --batch-size 200000
"""

import argparse
import logging
import os
import sys
import time
from pathlib import Path
from typing import Callable, Iterator
from urllib.parse import unquote

import pyarrow as pa
import pyarrow.parquet as pq

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

try:
    import clickhouse_connect
except ImportError:
    print("ERROR: pip install clickhouse-connect", file=sys.stderr)
    sys.exit(1)

# ── Logging ───────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)

# ── Column lists (selects only the columns ClickHouse expects) ────────────────

TITLES_COLUMNS = [
    "tconst", "titleType", "primaryTitle", "originalTitle", "isAdult",
    "startYear", "endYear", "runtimeMinutes", "genres", "primaryGenre",
    "decade", "averageRating", "numVotes",
]

PEOPLE_COLUMNS = [
    "nconst", "primaryName", "birthYear", "deathYear",
    "primaryProfession", "knownForTitles",
]

PRINCIPALS_COLUMNS = [
    "tconst", "ordering", "nconst", "category", "job", "characters",
]

AKAS_COLUMNS = [
    "titleId", "ordering", "title", "region", "language",
    "types", "attributes", "isOriginalTitle",
]

# ── PyArrow cast helpers ──────────────────────────────────────────────────────

def _cast(table: pa.Table, col: str, typ: pa.DataType) -> pa.Table:
    """Cast a single column; no-op if column not present."""
    if col not in table.schema.names:
        return table
    idx = table.schema.get_field_index(col)
    return table.set_column(idx, col, table.column(idx).cast(typ, safe=False))


def _fill_null_str(table: pa.Table, col: str, default: str = "") -> pa.Table:
    """Replace nulls in a string column with a default value."""
    if col not in table.schema.names:
        return table
    idx = table.schema.get_field_index(col)
    return table.set_column(idx, col, table.column(idx).fill_null(default))


def _ensure_list_string(table: pa.Table, col: str) -> pa.Table:
    """Ensure list column uses list<string> (not list<large_string>)."""
    if col not in table.schema.names:
        return table
    idx = table.schema.get_field_index(col)
    arr = table.column(idx)
    if pa.types.is_list(arr.type) or pa.types.is_large_list(arr.type):
        arr = arr.cast(pa.list_(pa.string()))
        table = table.set_column(idx, col, arr)
    return table


def add_hive_partition_columns(table: pa.Table, file_path: Path) -> pa.Table:
    """
    Add Spark/Hive partition columns encoded in directory names.

    Spark's `partitionBy("titleType", "decade")` writes data like:
      titles/titleType=movie/decade=1990/part-....parquet

    Those partition columns are intentionally omitted from the Parquet file
    itself. Since this loader reads individual files with `ParquetFile`, we
    must reconstruct those columns from the path before inserting into
    ClickHouse.
    """
    row_count = len(table)

    for part in file_path.parent.parts:
        if "=" not in part:
            continue

        key, value = part.split("=", 1)
        if key in table.schema.names:
            continue

        value = unquote(value)
        if value == "__HIVE_DEFAULT_PARTITION__":
            array = pa.nulls(row_count, type=pa.string())
        else:
            array = pa.array([value] * row_count, type=pa.string())

        table = table.append_column(key, array)

    return table


def cast_titles(table: pa.Table) -> pa.Table:
    for col, typ in [
        ("isAdult",        pa.uint8()),
        ("startYear",      pa.uint16()),
        ("endYear",        pa.uint16()),
        ("runtimeMinutes", pa.uint32()),
        ("decade",         pa.uint16()),
        ("averageRating",  pa.float32()),
        ("numVotes",       pa.uint32()),
    ]:
        table = _cast(table, col, typ)
    table = _ensure_list_string(table, "genres")
    table = _fill_null_str(table, "titleType", "unknown")
    table = _fill_null_str(table, "primaryGenre")
    return table.select([c for c in TITLES_COLUMNS if c in table.schema.names])


def cast_people(table: pa.Table) -> pa.Table:
    for col, typ in [
        ("birthYear", pa.uint16()),
        ("deathYear", pa.uint16()),
    ]:
        table = _cast(table, col, typ)
    table = _ensure_list_string(table, "primaryProfession")
    table = _ensure_list_string(table, "knownForTitles")
    return table.select([c for c in PEOPLE_COLUMNS if c in table.schema.names])


def cast_principals(table: pa.Table) -> pa.Table:
    table = _cast(table, "ordering", pa.uint8())
    table = _fill_null_str(table, "category", "unknown")
    return table.select([c for c in PRINCIPALS_COLUMNS if c in table.schema.names])


def cast_akas(table: pa.Table) -> pa.Table:
    table = _cast(table, "ordering",        pa.uint16())
    table = _cast(table, "isOriginalTitle", pa.uint8())
    table = _ensure_list_string(table, "types")
    table = _ensure_list_string(table, "attributes")
    table = _fill_null_str(table, "region",   "XX")
    table = _fill_null_str(table, "language", "")
    return table.select([c for c in AKAS_COLUMNS if c in table.schema.names])


# ── Dataset registry ──────────────────────────────────────────────────────────
# Each entry: (lake_subdir, clickhouse_table, cast_function)

DATASETS: list[tuple[str, str, Callable]] = [
    ("titles",     "imdb.titles",      cast_titles),
    ("people",     "imdb.people",      cast_people),
    ("principals", "imdb.principals",  cast_principals),
    ("akas",       "imdb.akas",        cast_akas),
]

# ── DDL helpers ────────────────────────────────────────────────────────────────

def split_sql_statements(sql: str) -> list[str]:
    """
    Split a SQL file into executable statements.

    The DDL file is heavily documented with `--` comments. The previous loader
    split on semicolons and skipped any statement starting with `--`, which also
    skipped the CREATE TABLE statement that followed the comment block. Strip
    line comments first, then split.
    """
    uncommented_lines = []
    for line in sql.splitlines():
        stripped = line.strip()
        if stripped.startswith("--"):
            continue
        uncommented_lines.append(line)

    statements: list[str] = []
    current: list[str] = []
    in_single_quote = False

    for char in "\n".join(uncommented_lines):
        if char == "'":
            in_single_quote = not in_single_quote

        if char == ";" and not in_single_quote:
            stmt = "".join(current).strip()
            if stmt:
                statements.append(stmt)
            current = []
            continue

        current.append(char)

    stmt = "".join(current).strip()
    if stmt:
        statements.append(stmt)

    return statements


def ensure_clickhouse_tables(client, ddl_path: Path) -> None:
    """Apply DDL if needed and verify all required tables exist."""
    required = {"titles", "people", "principals", "akas"}

    def current_tables() -> set[str]:
        try:
            return {r[0] for r in client.query("SHOW TABLES FROM imdb").result_rows}
        except Exception:
            return set()

    existing = current_tables()
    if required.issubset(existing):
        return

    if not ddl_path.exists():
        log.error("Tables missing and ddl/create_tables.sql not found.")
        sys.exit(1)

    log.info("Applying DDL …")
    for stmt in split_sql_statements(ddl_path.read_text()):
        first_line = stmt.splitlines()[0]
        try:
            client.command(stmt)
        except Exception as exc:
            # CREATE TABLE/CREATE DATABASE must succeed. Projection/view failures
            # should not block the base load path.
            upper = first_line.upper()
            if upper.startswith("CREATE TABLE") or upper.startswith("CREATE DATABASE"):
                log.error(f"DDL failed: {first_line}")
                log.error(str(exc))
                sys.exit(1)
            log.warning(f"DDL skipped/failed but non-critical: {first_line} ({exc})")

    existing = current_tables()
    missing = required - existing
    if missing:
        log.error(f"Required ClickHouse tables still missing after DDL: {sorted(missing)}")
        log.error(f"Existing tables in imdb: {sorted(existing)}")
        sys.exit(1)

# ── File discovery ────────────────────────────────────────────────────────────

def discover_parquet_files(lake_dir: Path, dataset: str) -> list[Path]:
    dataset_dir = lake_dir / dataset
    if not dataset_dir.exists():
        log.warning(f"Lake directory not found: {dataset_dir} — skipping.")
        return []
    return sorted(dataset_dir.rglob("*.parquet"))

# ── Core loader ───────────────────────────────────────────────────────────────

def load_dataset(
    client,
    lake_dir: Path,
    dataset: str,
    table_name: str,
    cast_fn: Callable,
    batch_size: int,
    truncate: bool,
) -> dict:
    files = discover_parquet_files(lake_dir, dataset)
    if not files:
        return {"files": 0, "rows": 0, "elapsed_s": 0.0}

    if truncate:
        log.info(f"  Truncating {table_name} …")
        client.command(f"TRUNCATE TABLE {table_name}")

    total_rows = 0
    t_start    = time.time()

    for file_path in files:
        pf = pq.ParquetFile(file_path)
        for batch in pf.iter_batches(batch_size=batch_size):
            table = pa.Table.from_batches([batch])
            table = add_hive_partition_columns(table, file_path)
            table = cast_fn(table)
            if len(table) == 0:
                continue
            client.insert_arrow(table_name, table)
            total_rows += len(table)

    elapsed = time.time() - t_start
    return {"files": len(files), "rows": total_rows, "elapsed_s": elapsed}

# ── Verification queries ──────────────────────────────────────────────────────

def run_verification(client) -> None:
    sql = (
        "SELECT 'titles' AS tbl, count() AS rows FROM imdb.titles "
        "UNION ALL SELECT 'people', count() FROM imdb.people "
        "UNION ALL SELECT 'principals', count() FROM imdb.principals "
        "UNION ALL SELECT 'akas', count() FROM imdb.akas"
    )
    try:
        result = client.query(sql)
    except Exception as exc:
        log.warning(f"Verification query failed: {exc}")
        return

    counts = ", ".join(f"{table}={rows:,}" for table, rows in result.result_rows)
    log.info(f"Verification row counts: {counts}")

# ── Argument parsing ──────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Load IMDb Parquet lake → ClickHouse")
    p.add_argument("--host",       default=os.getenv("CLICKHOUSE_HOST", "localhost"))
    p.add_argument("--port",       type=int, default=int(os.getenv("CLICKHOUSE_HTTP_PORT", "8123")))
    p.add_argument("--user",       default=os.getenv("CLICKHOUSE_USER", "default"))
    p.add_argument("--password",   default=os.getenv("CLICKHOUSE_PASSWORD", ""))
    p.add_argument("--database",   default=os.getenv("CLICKHOUSE_DATABASE", "imdb"))
    p.add_argument("--lake-dir",   default="./data/lake")
    p.add_argument("--batch-size", type=int, default=500_000)
    p.add_argument("--truncate",   action="store_true",
                   help="Truncate tables before loading (idempotent re-run)")
    p.add_argument("--only", nargs="+",
                   choices=["titles", "people", "principals", "akas"],
                   help="Load only specific datasets (default: all)")
    p.add_argument("--no-verify",  action="store_true",
                   help="Skip post-load verification queries")
    return p.parse_args()

# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    args     = parse_args()
    lake_dir = Path(args.lake_dir)

    log.info(
        f"Loading IMDb lake to ClickHouse: host={args.host}:{args.port}, "
        f"db={args.database}, lake={lake_dir.resolve()}"
    )

    # ── Connect ──────────────────────────────────────────────────────────
    try:
        client = clickhouse_connect.get_client(
            host=args.host,
            port=args.port,
            username=args.user,
            password=args.password,
            database=args.database,
            connect_timeout=30,
            send_receive_timeout=600,
        )
    except Exception as exc:
        log.error(f"Cannot connect: {exc}")
        log.error("Is 'docker compose up' running? Run: make up")
        sys.exit(1)

    # ── Ensure tables exist ───────────────────────────────────────────────
    ensure_clickhouse_tables(client, Path(__file__).parent / "ddl" / "create_tables.sql")

    # ── Filter to requested datasets ──────────────────────────────────────
    active = set(args.only) if args.only else {"titles", "people", "principals", "akas"}
    datasets_to_run = [(ds, tbl, fn) for ds, tbl, fn in DATASETS if ds in active]

    # ── Load ──────────────────────────────────────────────────────────────
    pipeline_start = time.time()
    stats = {}

    for dataset, table_name, cast_fn in datasets_to_run:
        stats[dataset] = load_dataset(
            client, lake_dir, dataset, table_name, cast_fn,
            args.batch_size, args.truncate,
        )

    # ── Summary ───────────────────────────────────────────────────────────
    total_elapsed = time.time() - pipeline_start
    log.info("Load summary")
    for ds, s in stats.items():
        log.info(
            f"  {ds:<12}: {s['rows']:>12,} rows | "
            f"{s['files']:>4} files | {s['elapsed_s']:.1f}s"
        )
    log.info(f"  {'TOTAL':<12}  {total_elapsed:.1f}s elapsed")

    # ── Verification ──────────────────────────────────────────────────────
    if not args.no_verify and stats:
        run_verification(client)

    log.info("Done. Run 'make benchmark' to compare ClickHouse vs Spark.")


if __name__ == "__main__":
    main()
