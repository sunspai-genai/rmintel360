from __future__ import annotations

import json
import re
from datetime import datetime
from typing import Any
from uuid import uuid4

from backend.app.db.connection import connect


class ConversationStore:
    """Persist assistant conversations and turns in the local DuckDB database."""

    def ensure_schema(self) -> None:
        with connect(read_only=False) as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS assistant_conversation (
                    conversation_id VARCHAR PRIMARY KEY,
                    title VARCHAR NOT NULL,
                    created_at TIMESTAMP NOT NULL,
                    updated_at TIMESTAMP NOT NULL,
                    user_role VARCHAR NOT NULL,
                    message_count INTEGER NOT NULL,
                    last_status VARCHAR,
                    last_intent VARCHAR,
                    last_message VARCHAR
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS assistant_turn (
                    turn_id VARCHAR PRIMARY KEY,
                    conversation_id VARCHAR NOT NULL,
                    turn_index INTEGER NOT NULL,
                    created_at TIMESTAMP NOT NULL,
                    user_message VARCHAR NOT NULL,
                    request_json VARCHAR NOT NULL,
                    response_json VARCHAR NOT NULL,
                    status VARCHAR,
                    intent VARCHAR,
                    route VARCHAR,
                    answer VARCHAR,
                    requires_clarification BOOLEAN,
                    generated_sql VARCHAR,
                    chart_type VARCHAR
                )
                """
            )

    def ensure_conversation(
        self,
        conversation_id: str | None,
        first_message: str,
        user_role: str,
    ) -> str:
        self.ensure_schema()
        resolved_id = conversation_id or f"conv_{uuid4().hex}"

        with connect(read_only=False) as conn:
            existing = conn.execute(
                """
                SELECT conversation_id
                FROM assistant_conversation
                WHERE conversation_id = ?
                """,
                [resolved_id],
            ).fetchone()
            if existing:
                return resolved_id

            now = datetime.utcnow()
            conn.execute(
                """
                INSERT INTO assistant_conversation (
                    conversation_id,
                    title,
                    created_at,
                    updated_at,
                    user_role,
                    message_count,
                    last_message
                )
                VALUES (?, ?, ?, ?, ?, 0, ?)
                """,
                [
                    resolved_id,
                    self._title_from_message(first_message),
                    now,
                    now,
                    user_role,
                    first_message,
                ],
            )
        return resolved_id

    def record_turn(
        self,
        conversation_id: str,
        request_payload: dict[str, Any],
        response_payload: dict[str, Any],
    ) -> dict[str, Any]:
        self.ensure_schema()
        now = datetime.utcnow()
        chart_spec = response_payload.get("chart_spec") or {}
        result_table = response_payload.get("result_table") or {}
        chart_type = chart_spec.get("chart_type")

        with connect(read_only=False) as conn:
            max_index = conn.execute(
                """
                SELECT COALESCE(MAX(turn_index), 0)
                FROM assistant_turn
                WHERE conversation_id = ?
                """,
                [conversation_id],
            ).fetchone()[0]
            turn_index = int(max_index) + 1
            turn_id = f"turn_{uuid4().hex}"

            conn.execute(
                """
                INSERT INTO assistant_turn (
                    turn_id,
                    conversation_id,
                    turn_index,
                    created_at,
                    user_message,
                    request_json,
                    response_json,
                    status,
                    intent,
                    route,
                    answer,
                    requires_clarification,
                    generated_sql,
                    chart_type
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    turn_id,
                    conversation_id,
                    turn_index,
                    now,
                    request_payload["message"],
                    self._to_json(request_payload),
                    self._to_json(response_payload),
                    response_payload.get("status"),
                    response_payload.get("intent"),
                    response_payload.get("route"),
                    response_payload.get("answer"),
                    bool(response_payload.get("requires_clarification")),
                    response_payload.get("generated_sql"),
                    chart_type,
                ],
            )

            message_count = conn.execute(
                """
                SELECT COUNT(*)
                FROM assistant_turn
                WHERE conversation_id = ?
                """,
                [conversation_id],
            ).fetchone()[0]
            conn.execute(
                """
                UPDATE assistant_conversation
                SET updated_at = ?,
                    user_role = ?,
                    message_count = ?,
                    last_status = ?,
                    last_intent = ?,
                    last_message = ?
                WHERE conversation_id = ?
                """,
                [
                    now,
                    request_payload.get("user_role", "business_user"),
                    int(message_count),
                    response_payload.get("status"),
                    response_payload.get("intent"),
                    request_payload["message"],
                    conversation_id,
                ],
            )

        return {
            "turn_id": turn_id,
            "conversation_id": conversation_id,
            "turn_index": turn_index,
            "created_at": self._format_datetime(now),
            "user_message": request_payload["message"],
            "status": response_payload.get("status"),
            "intent": response_payload.get("intent"),
            "route": response_payload.get("route"),
            "answer": response_payload.get("answer"),
            "requires_clarification": bool(response_payload.get("requires_clarification")),
            "generated_sql": response_payload.get("generated_sql"),
            "chart_type": chart_type,
            "result_row_count": result_table.get("row_count"),
            "request": request_payload,
            "response": response_payload,
        }

    def list_conversations(self, limit: int = 25) -> list[dict[str, Any]]:
        self.ensure_schema()
        with connect(read_only=True) as conn:
            result = conn.execute(
                """
                SELECT
                    conversation_id,
                    title,
                    created_at,
                    updated_at,
                    user_role,
                    message_count,
                    last_status,
                    last_intent,
                    last_message
                FROM assistant_conversation
                ORDER BY updated_at DESC
                LIMIT ?
                """,
                [limit],
            )
            columns = [column[0] for column in result.description or []]
            return [self._format_record(dict(zip(columns, row))) for row in result.fetchall()]

    def get_conversation(self, conversation_id: str) -> dict[str, Any] | None:
        self.ensure_schema()
        with connect(read_only=True) as conn:
            conversation_result = conn.execute(
                """
                SELECT
                    conversation_id,
                    title,
                    created_at,
                    updated_at,
                    user_role,
                    message_count,
                    last_status,
                    last_intent,
                    last_message
                FROM assistant_conversation
                WHERE conversation_id = ?
                """,
                [conversation_id],
            )
            row = conversation_result.fetchone()
            if not row:
                return None
            columns = [column[0] for column in conversation_result.description or []]
            conversation = self._format_record(dict(zip(columns, row)))

            turns_result = conn.execute(
                """
                SELECT
                    turn_id,
                    conversation_id,
                    turn_index,
                    created_at,
                    user_message,
                    request_json,
                    response_json,
                    status,
                    intent,
                    route,
                    answer,
                    requires_clarification,
                    generated_sql,
                    chart_type
                FROM assistant_turn
                WHERE conversation_id = ?
                ORDER BY turn_index
                """,
                [conversation_id],
            )
            turn_columns = [column[0] for column in turns_result.description or []]
            conversation["turns"] = [
                self._turn_from_record(dict(zip(turn_columns, turn)))
                for turn in turns_result.fetchall()
            ]
            return conversation

    def _turn_from_record(self, record: dict[str, Any]) -> dict[str, Any]:
        request_payload = json.loads(record.pop("request_json"))
        response_payload = json.loads(record.pop("response_json"))
        result_table = response_payload.get("result_table") or {}
        record = self._format_record(record)
        record["requires_clarification"] = bool(record["requires_clarification"])
        record["result_row_count"] = result_table.get("row_count")
        record["request"] = request_payload
        record["response"] = response_payload
        return record

    def _format_record(self, record: dict[str, Any]) -> dict[str, Any]:
        return {
            key: self._format_datetime(value) if isinstance(value, datetime) else value
            for key, value in record.items()
        }

    def _format_datetime(self, value: datetime) -> str:
        return value.replace(microsecond=0).isoformat() + "Z"

    def _title_from_message(self, message: str) -> str:
        title = re.sub(r"\s+", " ", message).strip()
        if len(title) <= 72:
            return title
        return title[:69].rstrip() + "..."

    def _to_json(self, payload: dict[str, Any]) -> str:
        return json.dumps(payload, default=str, separators=(",", ":"))


conversation_store = ConversationStore()

