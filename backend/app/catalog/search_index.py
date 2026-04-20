from __future__ import annotations

import json
import math
import re
from collections import Counter
from typing import Any

import duckdb


SEARCH_TABLE_NAMES = ["metadata_search_document"]

TOKEN_PATTERN = re.compile(r"[a-z0-9]+")
STOPWORDS = {
    "a",
    "an",
    "and",
    "are",
    "as",
    "at",
    "be",
    "by",
    "can",
    "for",
    "from",
    "has",
    "how",
    "i",
    "in",
    "is",
    "it",
    "me",
    "of",
    "on",
    "or",
    "show",
    "that",
    "the",
    "to",
    "use",
    "what",
    "where",
    "which",
    "with",
}


def tokenize(text: str | None) -> list[str]:
    if not text:
        return []
    tokens = []
    for token in TOKEN_PATTERN.findall(text.lower()):
        if token in STOPWORDS or len(token) <= 1:
            continue
        tokens.append(_normalize_token(token))
    return tokens


def build_sparse_vector(text: str | None) -> dict[str, float]:
    counts = Counter(tokenize(text))
    if not counts:
        return {}

    total = sum(counts.values())
    return {token: round(count / total, 6) for token, count in sorted(counts.items())}


def cosine_similarity(left: dict[str, float], right: dict[str, float]) -> float:
    if not left or not right:
        return 0.0

    shared_tokens = set(left).intersection(right)
    dot_product = sum(left[token] * right[token] for token in shared_tokens)
    left_norm = math.sqrt(sum(value * value for value in left.values()))
    right_norm = math.sqrt(sum(value * value for value in right.values()))

    if left_norm == 0 or right_norm == 0:
        return 0.0
    return dot_product / (left_norm * right_norm)


def create_search_schema(conn: duckdb.DuckDBPyConnection) -> None:
    for table_name in SEARCH_TABLE_NAMES:
        conn.execute(f"DROP TABLE IF EXISTS {table_name}")

    conn.execute(
        """
        CREATE TABLE metadata_search_document (
            document_id VARCHAR PRIMARY KEY,
            document_type VARCHAR,
            source_id VARCHAR,
            business_name VARCHAR,
            content VARCHAR,
            table_name VARCHAR,
            column_name VARCHAR,
            subject_area VARCHAR,
            certified_flag BOOLEAN,
            token_vector_json VARCHAR
        )
        """
    )


def seed_search_index(conn: duckdb.DuckDBPyConnection) -> None:
    create_search_schema(conn)
    documents = build_search_documents(conn)
    if not documents:
        return

    conn.executemany(
        """
        INSERT INTO metadata_search_document VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        documents,
    )


def build_search_documents(conn: duckdb.DuckDBPyConnection) -> list[tuple[Any, ...]]:
    synonym_map = _load_synonyms(conn)
    documents: list[tuple[Any, ...]] = []

    for row in conn.execute(
        """
        SELECT table_name, business_name, subject_area, table_type, domain, grain,
               refresh_frequency, data_owner, certified_flag
        FROM metadata_table
        """
    ).fetchall():
        (
            table_name,
            business_name,
            subject_area,
            table_type,
            domain,
            grain,
            refresh_frequency,
            data_owner,
            certified_flag,
        ) = row
        content = _join_text(
            business_name,
            table_name,
            subject_area,
            table_type,
            domain,
            grain,
            refresh_frequency,
            data_owner,
            "governed table",
        )
        documents.append(
            _document_tuple(
                document_id=f"table.{table_name}",
                document_type="table",
                source_id=table_name,
                business_name=business_name,
                content=content,
                table_name=table_name,
                column_name=None,
                subject_area=subject_area,
                certified_flag=certified_flag,
            )
        )

    for row in conn.execute(
        """
        SELECT table_name, column_name, business_name, data_type, description,
               semantic_type, pii_flag, restricted_flag, sample_values
        FROM metadata_column
        """
    ).fetchall():
        (
            table_name,
            column_name,
            business_name,
            data_type,
            description,
            semantic_type,
            pii_flag,
            restricted_flag,
            sample_values,
        ) = row
        content = _join_text(
            business_name,
            f"{table_name}.{column_name}",
            data_type,
            description,
            semantic_type,
            sample_values,
            "pii" if pii_flag else None,
            "restricted" if restricted_flag else None,
            "governed column",
        )
        documents.append(
            _document_tuple(
                document_id=f"column.{table_name}.{column_name}",
                document_type="column",
                source_id=f"{table_name}.{column_name}",
                business_name=business_name,
                content=content,
                table_name=table_name,
                column_name=column_name,
                subject_area=None,
                certified_flag=True,
            )
        )

    for row in conn.execute(
        """
        SELECT term_id, term_name, term_type, definition, calculation,
               primary_table, primary_column, certified_flag, owner, subject_area
        FROM metadata_business_term
        """
    ).fetchall():
        (
            term_id,
            term_name,
            term_type,
            definition,
            calculation,
            primary_table,
            primary_column,
            certified_flag,
            owner,
            subject_area,
        ) = row
        content = _join_text(
            term_name,
            term_id,
            term_type,
            definition,
            calculation,
            primary_table,
            primary_column,
            owner,
            subject_area,
            " ".join(synonym_map.get(term_id, [])),
            "business glossary term",
        )
        documents.append(
            _document_tuple(
                document_id=f"term.{term_id}",
                document_type="business_term",
                source_id=term_id,
                business_name=term_name,
                content=content,
                table_name=primary_table,
                column_name=primary_column,
                subject_area=subject_area,
                certified_flag=certified_flag,
            )
        )

    for row in conn.execute(
        """
        SELECT metric_id, metric_name, description, calculation_sql, aggregation_type,
               base_table, required_columns, default_time_period, certified_flag, subject_area
        FROM metadata_metric
        """
    ).fetchall():
        (
            metric_id,
            metric_name,
            description,
            calculation_sql,
            aggregation_type,
            base_table,
            required_columns,
            default_time_period,
            certified_flag,
            subject_area,
        ) = row
        content = _join_text(
            metric_name,
            metric_id,
            description,
            calculation_sql,
            aggregation_type,
            base_table,
            required_columns,
            default_time_period,
            subject_area,
            " ".join(synonym_map.get(metric_id, [])),
            "certified metric",
        )
        documents.append(
            _document_tuple(
                document_id=f"metric.{metric_id}",
                document_type="metric",
                source_id=metric_id,
                business_name=metric_name,
                content=content,
                table_name=base_table,
                column_name=None,
                subject_area=subject_area,
                certified_flag=certified_flag,
            )
        )

    for row in conn.execute(
        """
        SELECT dimension_id, dimension_name, description, table_name, column_name,
               sample_values, certified_flag, subject_area
        FROM metadata_dimension
        """
    ).fetchall():
        (
            dimension_id,
            dimension_name,
            description,
            table_name,
            column_name,
            sample_values,
            certified_flag,
            subject_area,
        ) = row
        content = _join_text(
            dimension_name,
            dimension_id,
            description,
            table_name,
            column_name,
            sample_values,
            subject_area,
            " ".join(synonym_map.get(dimension_id, [])),
            "certified dimension",
        )
        documents.append(
            _document_tuple(
                document_id=f"dimension.{dimension_id}",
                document_type="dimension",
                source_id=dimension_id,
                business_name=dimension_name,
                content=content,
                table_name=table_name,
                column_name=column_name,
                subject_area=subject_area,
                certified_flag=certified_flag,
            )
        )

    for row in conn.execute(
        """
        SELECT join_path_id, from_table, from_column, to_table, to_column,
               relationship_type, join_type, description, certified_flag
        FROM metadata_join_path
        """
    ).fetchall():
        (
            join_path_id,
            from_table,
            from_column,
            to_table,
            to_column,
            relationship_type,
            join_type,
            description,
            certified_flag,
        ) = row
        content = _join_text(
            join_path_id,
            description,
            f"{from_table}.{from_column}",
            f"{to_table}.{to_column}",
            relationship_type,
            join_type,
            "approved join path",
        )
        documents.append(
            _document_tuple(
                document_id=f"join.{join_path_id}",
                document_type="join_path",
                source_id=join_path_id,
                business_name=join_path_id,
                content=content,
                table_name=from_table,
                column_name=from_column,
                subject_area=None,
                certified_flag=certified_flag,
            )
        )

    for row in conn.execute(
        """
        SELECT lineage_id, source_system, source_object, target_table, target_column,
               transformation, refresh_frequency, data_owner
        FROM metadata_lineage
        """
    ).fetchall():
        (
            lineage_id,
            source_system,
            source_object,
            target_table,
            target_column,
            transformation,
            refresh_frequency,
            data_owner,
        ) = row
        content = _join_text(
            lineage_id,
            source_system,
            source_object,
            target_table,
            target_column,
            transformation,
            refresh_frequency,
            data_owner,
            "data lineage",
        )
        documents.append(
            _document_tuple(
                document_id=f"lineage.{lineage_id}",
                document_type="lineage",
                source_id=lineage_id,
                business_name=f"{target_table}.{target_column} lineage",
                content=content,
                table_name=target_table,
                column_name=target_column,
                subject_area=None,
                certified_flag=True,
            )
        )

    return documents


def search_documents(
    query: str,
    rows: list[dict[str, Any]],
    document_type: str | None = None,
    limit: int = 10,
    min_score: float = 0.0,
) -> list[dict[str, Any]]:
    query_vector = build_sparse_vector(query)
    query_tokens = set(query_vector)
    query_lower = f" {query.lower()} "
    normalized_query = " ".join(tokenize(query))
    results: list[dict[str, Any]] = []

    for row in rows:
        if document_type and row["document_type"] != document_type:
            continue

        document_vector = json.loads(row["token_vector_json"] or "{}")
        score = cosine_similarity(query_vector, document_vector)

        content_tokens = set(document_vector)
        matched_terms = sorted(query_tokens.intersection(content_tokens))
        content = row["content"] or ""
        normalized_content = " ".join(tokenize(content))

        if normalized_query and normalized_query in normalized_content:
            score += 0.20
        if row.get("business_name") and normalized_query == " ".join(tokenize(row["business_name"])):
            score += 0.15
        score += _intent_boost(row=row, query_lower=query_lower)
        if row["document_type"] == "column" and not _contains_any(query_lower, [" column", " field", " attribute"]):
            score *= 0.92
        if row["document_type"] == "dimension":
            business_name_tokens = set(tokenize(row.get("business_name") or ""))
            if query_tokens.intersection(business_name_tokens):
                score += 0.12
        if row.get("certified_flag"):
            score *= 1.03

        if score < min_score:
            continue

        result = dict(row)
        result["score"] = round(score, 6)
        result["matched_terms"] = matched_terms
        result.pop("token_vector_json", None)
        results.append(result)

    results.sort(key=lambda item: item["score"], reverse=True)
    return results[:limit]


def _load_synonyms(conn: duckdb.DuckDBPyConnection) -> dict[str, list[str]]:
    synonym_map: dict[str, list[str]] = {}
    for phrase, target_id in conn.execute("SELECT phrase, target_id FROM metadata_synonym").fetchall():
        synonym_map.setdefault(target_id, []).append(phrase)
    return synonym_map


def _document_tuple(
    document_id: str,
    document_type: str,
    source_id: str,
    business_name: str | None,
    content: str,
    table_name: str | None,
    column_name: str | None,
    subject_area: str | None,
    certified_flag: bool,
) -> tuple[Any, ...]:
    vector = build_sparse_vector(content)
    return (
        document_id,
        document_type,
        source_id,
        business_name,
        content,
        table_name,
        column_name,
        subject_area,
        certified_flag,
        json.dumps(vector, sort_keys=True),
    )


def _join_text(*values: Any) -> str:
    return " ".join(str(value) for value in values if value is not None and str(value).strip())


def _normalize_token(token: str) -> str:
    if token.endswith("ies") and len(token) > 4:
        return f"{token[:-3]}y"
    if token.endswith("s") and len(token) > 3:
        return token[:-1]
    return token


def _intent_boost(row: dict[str, Any], query_lower: str) -> float:
    document_type = row["document_type"]
    if document_type == "lineage" and _contains_any(query_lower, [" where", " source", " lineage", " come from", " comes from", " origin"]):
        return 0.24
    if document_type == "table" and _contains_any(query_lower, [" which table", " table ", " where is", " where are"]):
        return 0.18
    if document_type == "dimension" and _contains_any(query_lower, [" by ", " group", " segment", " dimension"]):
        return 0.22
    if document_type == "metric" and _contains_any(
        query_lower,
        [" average", " avg", " total", " rate", " balance", " utilization", " profit", " income", " amount"],
    ):
        return 0.08
    if document_type == "business_term" and _contains_any(
        query_lower,
        [" what", " define", " definition", " difference", " mean", " means", " how"],
    ):
        return 0.12
    return 0.0


def _contains_any(text: str, candidates: list[str]) -> bool:
    return any(candidate in text for candidate in candidates)
