from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path
from typing import Any

import duckdb

from backend.app.core.config import get_settings


def get_duckdb_path() -> Path:
    return Path(get_settings().duckdb_path)


def connect(read_only: bool = False) -> duckdb.DuckDBPyConnection:
    """Create a DuckDB connection for the configured analytics database."""

    db_path = get_duckdb_path()
    db_path.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(db_path), read_only=read_only)


def database_exists() -> bool:
    return get_duckdb_path().exists()


def fetch_all(sql: str, parameters: Iterable[Any] | None = None) -> list[dict[str, Any]]:
    """Execute a read query and return rows as dictionaries."""

    with connect(read_only=True) as conn:
        result = conn.execute(sql, parameters or [])
        columns = [column[0] for column in result.description or []]
        return [dict(zip(columns, row)) for row in result.fetchall()]


def table_counts() -> list[dict[str, Any]]:
    if not database_exists():
        return []

    sql = """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'main'
        ORDER BY table_name
    """

    with connect(read_only=True) as conn:
        tables = [row[0] for row in conn.execute(sql).fetchall()]
        counts: list[dict[str, Any]] = []
        for table_name in tables:
            row_count = conn.execute(f'SELECT COUNT(*) FROM "{table_name}"').fetchone()[0]
            counts.append({"table_name": table_name, "row_count": row_count})
        return counts
