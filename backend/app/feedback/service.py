from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import uuid4

import duckdb

from backend.app.conversation.service import conversation_store
from backend.app.db.connection import connect


class FeedbackService:
    """Capture user feedback and summarize assistant quality signals."""

    VALID_REASON_CODES = {
        "helpful",
        "wrong_metric",
        "wrong_dimension",
        "wrong_sql",
        "unclear_answer",
        "bad_chart",
        "missing_context",
        "other",
    }

    def ensure_schema(self, conn: duckdb.DuckDBPyConnection | None = None) -> None:
        if conn is None:
            conversation_store.ensure_schema()
            with connect(read_only=False) as owned_conn:
                self.ensure_schema(owned_conn)
            return

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS assistant_feedback (
                feedback_id VARCHAR PRIMARY KEY,
                created_at TIMESTAMP NOT NULL,
                conversation_id VARCHAR NOT NULL,
                turn_id VARCHAR NOT NULL,
                turn_index INTEGER,
                rating VARCHAR NOT NULL,
                reason_code VARCHAR NOT NULL,
                comment VARCHAR,
                user_role VARCHAR NOT NULL,
                status VARCHAR,
                intent VARCHAR,
                route VARCHAR
            )
            """
        )

    def create_feedback(self, payload: dict[str, Any]) -> dict[str, Any]:
        conversation_store.ensure_schema()
        with connect(read_only=False) as conn:
            self.ensure_schema(conn)
            turn = self._load_turn(conn, payload["turn_id"])
            if not turn:
                raise LookupError(f"Unknown assistant turn: {payload['turn_id']}")

            requested_conversation_id = payload.get("conversation_id")
            if requested_conversation_id and requested_conversation_id != turn["conversation_id"]:
                raise ValueError("Feedback conversation_id does not match the referenced turn.")

            reason_code = self._normalize_reason_code(payload.get("reason_code"), payload["rating"])
            now = datetime.utcnow()
            record = {
                "feedback_id": f"feedback_{uuid4().hex}",
                "created_at": now,
                "conversation_id": turn["conversation_id"],
                "turn_id": turn["turn_id"],
                "turn_index": turn["turn_index"],
                "rating": payload["rating"],
                "reason_code": reason_code,
                "comment": payload.get("comment"),
                "user_role": payload.get("user_role") or "business_user",
                "status": turn.get("status"),
                "intent": turn.get("intent"),
                "route": turn.get("route"),
            }
            conn.execute(
                """
                INSERT INTO assistant_feedback VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    record["feedback_id"],
                    record["created_at"],
                    record["conversation_id"],
                    record["turn_id"],
                    record["turn_index"],
                    record["rating"],
                    record["reason_code"],
                    record["comment"],
                    record["user_role"],
                    record["status"],
                    record["intent"],
                    record["route"],
                ],
            )
            return self._format_record(record)

    def list_feedback(
        self,
        conversation_id: str | None = None,
        turn_id: str | None = None,
        rating: str | None = None,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        with connect(read_only=False) as conn:
            self.ensure_schema(conn)
            result = conn.execute(
                """
                SELECT
                    feedback_id,
                    created_at,
                    conversation_id,
                    turn_id,
                    turn_index,
                    rating,
                    reason_code,
                    comment,
                    user_role,
                    status,
                    intent,
                    route
                FROM assistant_feedback
                WHERE (? IS NULL OR conversation_id = ?)
                  AND (? IS NULL OR turn_id = ?)
                  AND (? IS NULL OR lower(rating) = lower(?))
                ORDER BY created_at DESC, feedback_id DESC
                LIMIT ?
                """,
                [conversation_id, conversation_id, turn_id, turn_id, rating, rating, limit],
            )
            columns = [column[0] for column in result.description or []]
            return [self._format_record(dict(zip(columns, row))) for row in result.fetchall()]

    def quality_summary(self, limit: int = 5) -> dict[str, Any]:
        with connect(read_only=False) as conn:
            self.ensure_schema(conn)
            total, positive, negative, neutral = conn.execute(
                """
                SELECT
                    COUNT(*),
                    SUM(CASE WHEN rating = 'positive' THEN 1 ELSE 0 END),
                    SUM(CASE WHEN rating = 'negative' THEN 1 ELSE 0 END),
                    SUM(CASE WHEN rating = 'neutral' THEN 1 ELSE 0 END)
                FROM assistant_feedback
                """
            ).fetchone()

            reason_rows = self._group_rows(
                conn,
                """
                SELECT reason_code, COUNT(*) AS feedback_count
                FROM assistant_feedback
                WHERE rating <> 'positive'
                GROUP BY reason_code
                ORDER BY feedback_count DESC, reason_code
                LIMIT ?
                """,
                [limit],
            )
            route_rows = self._group_rows(
                conn,
                """
                SELECT coalesce(route, 'unknown') AS route, rating, COUNT(*) AS feedback_count
                FROM assistant_feedback
                GROUP BY coalesce(route, 'unknown'), rating
                ORDER BY feedback_count DESC, route, rating
                LIMIT ?
                """,
                [limit],
            )
            recent = self._list_feedback_with_conn(conn=conn, limit=limit)

        total_count = int(total or 0)
        positive_count = int(positive or 0)
        negative_count = int(negative or 0)
        neutral_count = int(neutral or 0)
        positive_rate = round(positive_count / total_count, 4) if total_count else 0.0
        issue_rate = round(negative_count / total_count, 4) if total_count else 0.0

        return {
            "total_feedback": total_count,
            "positive_count": positive_count,
            "negative_count": negative_count,
            "neutral_count": neutral_count,
            "positive_rate": positive_rate,
            "issue_rate": issue_rate,
            "top_issue_reasons": reason_rows,
            "route_quality": route_rows,
            "recent_feedback": recent,
        }

    def _load_turn(self, conn: duckdb.DuckDBPyConnection, turn_id: str) -> dict[str, Any] | None:
        result = conn.execute(
            """
            SELECT
                turn_id,
                conversation_id,
                turn_index,
                status,
                intent,
                route
            FROM assistant_turn
            WHERE turn_id = ?
            """,
            [turn_id],
        )
        row = result.fetchone()
        if not row:
            return None
        columns = [column[0] for column in result.description or []]
        return dict(zip(columns, row))

    def _list_feedback_with_conn(
        self,
        conn: duckdb.DuckDBPyConnection,
        limit: int,
    ) -> list[dict[str, Any]]:
        result = conn.execute(
            """
            SELECT
                feedback_id,
                created_at,
                conversation_id,
                turn_id,
                turn_index,
                rating,
                reason_code,
                comment,
                user_role,
                status,
                intent,
                route
            FROM assistant_feedback
            ORDER BY created_at DESC, feedback_id DESC
            LIMIT ?
            """,
            [limit],
        )
        columns = [column[0] for column in result.description or []]
        return [self._format_record(dict(zip(columns, row))) for row in result.fetchall()]

    def _normalize_reason_code(self, reason_code: str | None, rating: str) -> str:
        if not reason_code:
            return "helpful" if rating == "positive" else "other"
        normalized = reason_code.strip().lower()
        if normalized not in self.VALID_REASON_CODES:
            raise ValueError(f"Unsupported feedback reason_code: {reason_code}")
        return normalized

    def _group_rows(
        self,
        conn: duckdb.DuckDBPyConnection,
        sql: str,
        parameters: list[Any],
    ) -> list[dict[str, Any]]:
        result = conn.execute(sql, parameters)
        columns = [column[0] for column in result.description or []]
        return [dict(zip(columns, row)) for row in result.fetchall()]

    def _format_record(self, record: dict[str, Any]) -> dict[str, Any]:
        formatted = dict(record)
        created_at = formatted.get("created_at")
        if isinstance(created_at, datetime):
            formatted["created_at"] = created_at.replace(microsecond=0).isoformat() + "Z"
        return formatted


feedback_service = FeedbackService()
