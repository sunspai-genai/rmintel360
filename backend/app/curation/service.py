from __future__ import annotations

import json
import re
import uuid
from datetime import datetime, timezone
from typing import Any

import duckdb

from backend.app.catalog.search_index import seed_search_index
from backend.app.db.connection import connect
from backend.app.sql.generator import TABLE_ALIASES


class CurationService:
    """Admin-only write service for governed semantic metadata."""

    EVENT_TABLE = "metadata_curation_event"
    BUSINESS_TERM_COLUMNS = [
        "term_id",
        "term_name",
        "term_type",
        "definition",
        "calculation",
        "primary_table",
        "primary_column",
        "certified_flag",
        "owner",
        "subject_area",
    ]
    METRIC_COLUMNS = [
        "metric_id",
        "metric_name",
        "description",
        "calculation_sql",
        "aggregation_type",
        "base_table",
        "required_columns",
        "default_time_period",
        "certified_flag",
        "subject_area",
    ]
    DIMENSION_COLUMNS = [
        "dimension_id",
        "dimension_name",
        "description",
        "table_name",
        "column_name",
        "sample_values",
        "certified_flag",
        "subject_area",
    ]
    SYNONYM_COLUMNS = [
        "synonym_id",
        "phrase",
        "target_type",
        "target_id",
        "confidence",
        "notes",
    ]

    def upsert_business_term(self, payload: dict[str, Any]) -> dict[str, Any]:
        self._authorize(payload.get("requested_by"))
        row = self._select_columns(payload, self.BUSINESS_TERM_COLUMNS)
        with connect() as conn:
            self.ensure_schema(conn)
            self._validate_business_term(conn, row)
            event = self._upsert_and_log(
                conn=conn,
                table_name="metadata_business_term",
                id_column="term_id",
                row=row,
                requested_by=payload["requested_by"],
                asset_type="business_term",
                notes=payload.get("notes"),
            )
            seed_search_index(conn)
            return event

    def upsert_metric(self, payload: dict[str, Any]) -> dict[str, Any]:
        self._authorize(payload.get("requested_by"))
        row = self._select_columns(payload, self.METRIC_COLUMNS)
        with connect() as conn:
            self.ensure_schema(conn)
            self._validate_metric(conn, row)
            event = self._upsert_and_log(
                conn=conn,
                table_name="metadata_metric",
                id_column="metric_id",
                row=row,
                requested_by=payload["requested_by"],
                asset_type="metric",
                notes=payload.get("notes"),
            )
            seed_search_index(conn)
            return event

    def upsert_dimension(self, payload: dict[str, Any]) -> dict[str, Any]:
        self._authorize(payload.get("requested_by"))
        row = self._select_columns(payload, self.DIMENSION_COLUMNS)
        with connect() as conn:
            self.ensure_schema(conn)
            self._validate_dimension(conn, row)
            event = self._upsert_and_log(
                conn=conn,
                table_name="metadata_dimension",
                id_column="dimension_id",
                row=row,
                requested_by=payload["requested_by"],
                asset_type="dimension",
                notes=payload.get("notes"),
            )
            seed_search_index(conn)
            return event

    def upsert_synonym(self, payload: dict[str, Any]) -> dict[str, Any]:
        self._authorize(payload.get("requested_by"))
        row = self._select_columns(payload, self.SYNONYM_COLUMNS)
        row["phrase"] = row["phrase"].strip().lower()
        with connect() as conn:
            self.ensure_schema(conn)
            self._validate_synonym(conn, row)
            event = self._upsert_and_log(
                conn=conn,
                table_name="metadata_synonym",
                id_column="synonym_id",
                row=row,
                requested_by=payload["requested_by"],
                asset_type="synonym",
                notes=payload.get("notes"),
            )
            seed_search_index(conn)
            return event

    def list_events(
        self,
        asset_type: str | None = None,
        asset_id: str | None = None,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        with connect() as conn:
            self.ensure_schema(conn)
            result = conn.execute(
                """
                SELECT
                    event_id,
                    created_at,
                    requested_by,
                    action,
                    asset_type,
                    asset_id,
                    status,
                    before_json,
                    after_json,
                    notes
                FROM metadata_curation_event
                WHERE (? IS NULL OR lower(asset_type) = lower(?))
                  AND (? IS NULL OR lower(asset_id) = lower(?))
                ORDER BY created_at DESC, event_id DESC
                LIMIT ?
                """,
                [asset_type, asset_type, asset_id, asset_id, limit],
            )
            columns = [column[0] for column in result.description or []]
            return [self._event_response(dict(zip(columns, row))) for row in result.fetchall()]

    def ensure_schema(self, conn: duckdb.DuckDBPyConnection) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS metadata_curation_event (
                event_id VARCHAR PRIMARY KEY,
                created_at TIMESTAMP,
                requested_by VARCHAR,
                action VARCHAR,
                asset_type VARCHAR,
                asset_id VARCHAR,
                status VARCHAR,
                before_json VARCHAR,
                after_json VARCHAR,
                notes VARCHAR
            )
            """
        )

    def _upsert_and_log(
        self,
        conn: duckdb.DuckDBPyConnection,
        table_name: str,
        id_column: str,
        row: dict[str, Any],
        requested_by: str,
        asset_type: str,
        notes: str | None,
    ) -> dict[str, Any]:
        asset_id = row[id_column]
        before = self._fetch_row(conn, table_name, id_column, asset_id)
        action = "update" if before else "create"

        if before:
            assignments = ", ".join(f"{column} = ?" for column in row)
            conn.execute(
                f"UPDATE {table_name} SET {assignments} WHERE lower({id_column}) = lower(?)",
                [row[column] for column in row] + [asset_id],
            )
        else:
            placeholders = ", ".join("?" for _ in row)
            columns = ", ".join(row)
            conn.execute(
                f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})",
                [row[column] for column in row],
            )

        after = self._fetch_row(conn, table_name, id_column, asset_id) or row
        event = {
            "event_id": f"curation.{uuid.uuid4().hex}",
            "created_at": datetime.now(timezone.utc).replace(tzinfo=None),
            "requested_by": requested_by,
            "action": action,
            "asset_type": asset_type,
            "asset_id": asset_id,
            "status": "applied",
            "before_json": self._json_or_none(before),
            "after_json": self._json_or_none(after),
            "notes": notes,
        }
        conn.execute(
            """
            INSERT INTO metadata_curation_event VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                event["event_id"],
                event["created_at"],
                event["requested_by"],
                event["action"],
                event["asset_type"],
                event["asset_id"],
                event["status"],
                event["before_json"],
                event["after_json"],
                event["notes"],
            ],
        )
        return self._event_response(event)

    def _fetch_row(
        self,
        conn: duckdb.DuckDBPyConnection,
        table_name: str,
        id_column: str,
        asset_id: str,
    ) -> dict[str, Any] | None:
        result = conn.execute(
            f"SELECT * FROM {table_name} WHERE lower({id_column}) = lower(?)",
            [asset_id],
        )
        row = result.fetchone()
        if not row:
            return None
        columns = [column[0] for column in result.description or []]
        return dict(zip(columns, row))

    def _validate_business_term(self, conn: duckdb.DuckDBPyConnection, row: dict[str, Any]) -> None:
        if row.get("primary_table") and not self._table_exists(conn, row["primary_table"]):
            raise ValueError(f"Unknown primary table: {row['primary_table']}")
        if row.get("primary_column"):
            if not row.get("primary_table"):
                raise ValueError("primary_column requires primary_table.")
            if not self._column_exists(conn, row["primary_table"], row["primary_column"]):
                raise ValueError(f"Unknown primary column: {row['primary_table']}.{row['primary_column']}")

    def _validate_metric(self, conn: duckdb.DuckDBPyConnection, row: dict[str, Any]) -> None:
        if not self._table_exists(conn, row["base_table"]):
            raise ValueError(f"Unknown metric base table: {row['base_table']}")
        required_columns = self._split_csv(row.get("required_columns"))
        missing_columns = [column for column in required_columns if not self._column_exists(conn, row["base_table"], column)]
        if missing_columns:
            raise ValueError(
                f"Metric required columns are not governed on {row['base_table']}: {', '.join(missing_columns)}"
            )
        restricted_columns = [
            column
            for column in required_columns
            if self._column_record(conn, row["base_table"], column).get("restricted_flag")
        ]
        if restricted_columns:
            raise ValueError(
                f"Metric required columns are restricted on {row['base_table']}: {', '.join(restricted_columns)}"
            )
        self._validate_metric_expression(row=row, required_columns=required_columns)

    def _validate_dimension(self, conn: duckdb.DuckDBPyConnection, row: dict[str, Any]) -> None:
        if not self._table_exists(conn, row["table_name"]):
            raise ValueError(f"Unknown dimension table: {row['table_name']}")
        if not self._column_exists(conn, row["table_name"], row["column_name"]):
            raise ValueError(f"Unknown dimension column: {row['table_name']}.{row['column_name']}")

    def _validate_synonym(self, conn: duckdb.DuckDBPyConnection, row: dict[str, Any]) -> None:
        target_type = row["target_type"]
        target_id = row["target_id"]
        if target_type == "metric":
            table_name = "metadata_metric"
            id_column = "metric_id"
        elif target_type == "dimension":
            table_name = "metadata_dimension"
            id_column = "dimension_id"
        elif target_type == "business_term":
            table_name = "metadata_business_term"
            id_column = "term_id"
        else:
            raise ValueError(f"Unsupported synonym target type: {target_type}")

        if not self._exists(conn, table_name, id_column, target_id):
            raise ValueError(f"Unknown {target_type} target: {target_id}")

    def _table_exists(self, conn: duckdb.DuckDBPyConnection, table_name: str) -> bool:
        return self._exists(conn, "metadata_table", "table_name", table_name)

    def _column_exists(self, conn: duckdb.DuckDBPyConnection, table_name: str, column_name: str) -> bool:
        count = conn.execute(
            """
            SELECT COUNT(*)
            FROM metadata_column
            WHERE lower(table_name) = lower(?)
              AND lower(column_name) = lower(?)
            """,
            [table_name, column_name],
        ).fetchone()[0]
        return count > 0

    def _column_record(self, conn: duckdb.DuckDBPyConnection, table_name: str, column_name: str) -> dict[str, Any]:
        result = conn.execute(
            """
            SELECT table_name, column_name, restricted_flag
            FROM metadata_column
            WHERE lower(table_name) = lower(?)
              AND lower(column_name) = lower(?)
            """,
            [table_name, column_name],
        )
        row = result.fetchone()
        if not row:
            return {}
        columns = [column[0] for column in result.description or []]
        return dict(zip(columns, row))

    def _validate_metric_expression(self, row: dict[str, Any], required_columns: list[str]) -> None:
        expected_alias = TABLE_ALIASES.get(row["base_table"])
        referenced = set(re.findall(r"\b([A-Za-z][A-Za-z0-9_]*)\.([A-Za-z][A-Za-z0-9_]*)\b", row["calculation_sql"]))
        for alias, column in referenced:
            if expected_alias and alias != expected_alias:
                raise ValueError(
                    f"Metric calculation_sql may only reference the base table alias `{expected_alias}`; found `{alias}.{column}`."
                )
            if column not in required_columns:
                raise ValueError(
                    f"Metric calculation_sql references `{column}` but it is not listed in required_columns."
                )

    def _exists(self, conn: duckdb.DuckDBPyConnection, table_name: str, id_column: str, asset_id: str) -> bool:
        count = conn.execute(
            f"SELECT COUNT(*) FROM {table_name} WHERE lower({id_column}) = lower(?)",
            [asset_id],
        ).fetchone()[0]
        return count > 0

    def _select_columns(self, payload: dict[str, Any], columns: list[str]) -> dict[str, Any]:
        missing = [column for column in columns if column not in payload]
        if missing:
            raise ValueError(f"Missing required curation fields: {', '.join(missing)}")
        return {column: payload[column] for column in columns}

    def _authorize(self, requested_by: str | None) -> None:
        if (requested_by or "").strip().lower() != "admin":
            raise PermissionError("Admin curation requires requested_by='admin'.")

    def _event_response(self, event: dict[str, Any]) -> dict[str, Any]:
        response = dict(event)
        created_at = response.get("created_at")
        if hasattr(created_at, "isoformat"):
            response["created_at"] = created_at.isoformat()
        response["before"] = self._parse_json(response.pop("before_json", None))
        response["after"] = self._parse_json(response.pop("after_json", None))
        return response

    def _json_or_none(self, value: dict[str, Any] | None) -> str | None:
        if value is None:
            return None
        return json.dumps(value, default=str, sort_keys=True)

    def _parse_json(self, value: str | None) -> dict[str, Any] | None:
        if not value:
            return None
        return json.loads(value)

    def _split_csv(self, value: Any) -> list[str]:
        if not value:
            return []
        if isinstance(value, list):
            return [str(item).strip() for item in value if str(item).strip()]
        return [part.strip() for part in str(value).split(",") if part.strip()]


curation_service = CurationService()
