from __future__ import annotations

import csv
import html
import json
import re
from datetime import datetime
from io import StringIO
from typing import Any
from uuid import uuid4

from backend.app.db.connection import connect


EXPORT_FORMATS = {"json", "csv", "html"}


class ExportService:
    """Create persisted export snapshots from governed assistant responses."""

    def ensure_schema(self) -> None:
        with connect(read_only=False) as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS assistant_export (
                    export_id VARCHAR PRIMARY KEY,
                    created_at TIMESTAMP NOT NULL,
                    title VARCHAR NOT NULL,
                    export_format VARCHAR NOT NULL,
                    content_type VARCHAR NOT NULL,
                    filename VARCHAR NOT NULL,
                    conversation_id VARCHAR,
                    user_role VARCHAR NOT NULL,
                    source_message VARCHAR,
                    row_count INTEGER,
                    chart_type VARCHAR,
                    payload_json VARCHAR NOT NULL,
                    content VARCHAR NOT NULL
                )
                """
            )

    def create_export(
        self,
        chat_response: dict[str, Any],
        export_format: str = "html",
        user_role: str = "business_user",
        title: str | None = None,
    ) -> dict[str, Any]:
        normalized_format = export_format.lower()
        if normalized_format not in EXPORT_FORMATS:
            raise ValueError(f"Unsupported export format: {export_format}")

        self.ensure_schema()
        created_at = datetime.utcnow()
        export_id = f"exp_{uuid4().hex}"
        resolved_title = title or self._title(chat_response)
        filename = f"{self._slug(resolved_title)}-{export_id}.{normalized_format}"
        content_type = self._content_type(normalized_format)
        content = self._content(chat_response=chat_response, export_format=normalized_format, title=resolved_title)
        result_table = chat_response.get("result_table") or {}
        chart_spec = chat_response.get("chart_spec") or {}

        with connect(read_only=False) as conn:
            conn.execute(
                """
                INSERT INTO assistant_export (
                    export_id,
                    created_at,
                    title,
                    export_format,
                    content_type,
                    filename,
                    conversation_id,
                    user_role,
                    source_message,
                    row_count,
                    chart_type,
                    payload_json,
                    content
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    export_id,
                    created_at,
                    resolved_title,
                    normalized_format,
                    content_type,
                    filename,
                    chat_response.get("conversation_id"),
                    user_role,
                    chat_response.get("message"),
                    result_table.get("row_count"),
                    chart_spec.get("chart_type"),
                    json.dumps(chat_response, default=str, separators=(",", ":")),
                    content,
                ],
            )

        return {
            "export_id": export_id,
            "created_at": self._format_datetime(created_at),
            "title": resolved_title,
            "export_format": normalized_format,
            "content_type": content_type,
            "filename": filename,
            "conversation_id": chat_response.get("conversation_id"),
            "row_count": result_table.get("row_count"),
            "chart_type": chart_spec.get("chart_type"),
            "download_url": f"/exports/{export_id}/download",
            "view_url": f"/exports/{export_id}/view",
        }

    def get_export(self, export_id: str) -> dict[str, Any] | None:
        self.ensure_schema()
        with connect(read_only=True) as conn:
            result = conn.execute(
                """
                SELECT
                    export_id,
                    created_at,
                    title,
                    export_format,
                    content_type,
                    filename,
                    conversation_id,
                    user_role,
                    source_message,
                    row_count,
                    chart_type,
                    payload_json,
                    content
                FROM assistant_export
                WHERE export_id = ?
                """,
                [export_id],
            )
            row = result.fetchone()
            if not row:
                return None
            columns = [column[0] for column in result.description or []]
            record = dict(zip(columns, row))
            if isinstance(record["created_at"], datetime):
                record["created_at"] = self._format_datetime(record["created_at"])
            record["payload"] = json.loads(record.pop("payload_json"))
            return record

    def list_exports(self, conversation_id: str | None = None, limit: int = 25) -> list[dict[str, Any]]:
        self.ensure_schema()
        with connect(read_only=True) as conn:
            result = conn.execute(
                """
                SELECT
                    export_id,
                    created_at,
                    title,
                    export_format,
                    content_type,
                    filename,
                    conversation_id,
                    user_role,
                    source_message,
                    row_count,
                    chart_type
                FROM assistant_export
                WHERE (? IS NULL OR conversation_id = ?)
                ORDER BY created_at DESC
                LIMIT ?
                """,
                [conversation_id, conversation_id, limit],
            )
            columns = [column[0] for column in result.description or []]
            records = []
            for row in result.fetchall():
                record = dict(zip(columns, row))
                if isinstance(record["created_at"], datetime):
                    record["created_at"] = self._format_datetime(record["created_at"])
                record["download_url"] = f"/exports/{record['export_id']}/download"
                record["view_url"] = f"/exports/{record['export_id']}/view"
                records.append(record)
            return records

    def _content(self, chat_response: dict[str, Any], export_format: str, title: str) -> str:
        if export_format == "json":
            return json.dumps(
                {
                    "title": title,
                    "exported_at": self._format_datetime(datetime.utcnow()),
                    "chat_response": chat_response,
                },
                indent=2,
                default=str,
            )
        if export_format == "csv":
            return self._csv_content(chat_response)
        return self._html_content(chat_response=chat_response, title=title)

    def _csv_content(self, chat_response: dict[str, Any]) -> str:
        result_table = chat_response.get("result_table") or {}
        columns = result_table.get("columns") or []
        rows = result_table.get("rows") or []
        output = StringIO()
        if not columns:
            writer = csv.writer(output)
            writer.writerow(["message", "answer"])
            writer.writerow([chat_response.get("message", ""), chat_response.get("answer", "")])
            return output.getvalue()

        writer = csv.DictWriter(output, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
        return output.getvalue()

    def _html_content(self, chat_response: dict[str, Any], title: str) -> str:
        answer = html.escape(chat_response.get("answer") or "")
        message = html.escape(chat_response.get("message") or "")
        generated_sql = html.escape(chat_response.get("generated_sql") or "")
        key_points = chat_response.get("key_points") or []
        result_table = chat_response.get("result_table") or {}
        chart_spec = chat_response.get("chart_spec")
        audit_report = chat_response.get("audit_report") or {}
        chart_script = ""
        chart_block = ""
        if chart_spec:
            figure_json = json.dumps(chart_spec.get("plotly_json") or {}, default=str).replace("</", "<\\/")
            chart_block = '<section><h2>Chart</h2><div id="chart" style="min-height:420px;"></div></section>'
            chart_script = f"""
            <script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
            <script>
              const figure = {figure_json};
              Plotly.newPlot("chart", figure.data || [], figure.layout || {{}}, {{responsive: true}});
            </script>
            """

        return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>{html.escape(title)}</title>
  <style>
    body {{ margin: 0; padding: 32px; font-family: Inter, system-ui, sans-serif; color: #14201b; background: #f6f7f4; }}
    main {{ max-width: 1080px; margin: 0 auto; }}
    section {{ margin: 18px 0; padding: 18px; border: 1px solid #d7ded8; border-radius: 8px; background: #fff; }}
    h1 {{ margin: 0 0 8px; font-size: 2rem; }}
    h2 {{ margin: 0 0 12px; color: #31586b; font-size: 1rem; }}
    p, li {{ line-height: 1.5; }}
    table {{ width: 100%; border-collapse: collapse; }}
    th, td {{ padding: 9px 10px; border-bottom: 1px solid #d7ded8; text-align: left; }}
    th {{ background: #f0f4f1; color: #61706a; }}
    pre {{ overflow: auto; padding: 12px; background: #f0f4f1; }}
  </style>
</head>
<body>
  <main>
    <header>
      <h1>{html.escape(title)}</h1>
      <p>{message}</p>
    </header>
    <section>
      <h2>Answer</h2>
      <p>{answer}</p>
      {self._html_list(key_points)}
    </section>
    {chart_block}
    {self._html_table(result_table)}
    {self._html_sql(generated_sql)}
    <section>
      <h2>Governance Audit</h2>
      <pre>{html.escape(json.dumps(audit_report, indent=2, default=str))}</pre>
    </section>
  </main>
  {chart_script}
</body>
</html>"""

    def _html_table(self, result_table: dict[str, Any]) -> str:
        columns = result_table.get("columns") or []
        rows = result_table.get("rows") or []
        if not columns:
            return ""
        header = "".join(f"<th>{html.escape(column)}</th>" for column in columns)
        body_rows = []
        for row in rows:
            cells = "".join(f"<td>{html.escape(str(row.get(column, '')))}</td>" for column in columns)
            body_rows.append(f"<tr>{cells}</tr>")
        return f"<section><h2>Result Table</h2><table><thead><tr>{header}</tr></thead><tbody>{''.join(body_rows)}</tbody></table></section>"

    def _html_list(self, items: list[str]) -> str:
        if not items:
            return ""
        body = "".join(f"<li>{html.escape(str(item))}</li>" for item in items)
        return f"<ul>{body}</ul>"

    def _html_sql(self, generated_sql: str) -> str:
        if not generated_sql:
            return ""
        return f"<section><h2>Generated SQL</h2><pre>{generated_sql}</pre></section>"

    def _title(self, chat_response: dict[str, Any]) -> str:
        overview = chat_response.get("result_overview") or {}
        metric = overview.get("metric")
        dimensions = overview.get("dimensions") or []
        if metric and dimensions:
            return f"{metric} by {', '.join(dimensions)}"
        if metric:
            return metric
        message = chat_response.get("message") or "Governed Banking Export"
        return message[:96]

    def _slug(self, title: str) -> str:
        slug = re.sub(r"[^a-zA-Z0-9]+", "-", title.strip().lower()).strip("-")
        return slug[:60] or "governed-export"

    def _content_type(self, export_format: str) -> str:
        if export_format == "json":
            return "application/json"
        if export_format == "csv":
            return "text/csv"
        return "text/html"

    def _format_datetime(self, value: datetime) -> str:
        return value.replace(microsecond=0).isoformat() + "Z"


export_service = ExportService()
