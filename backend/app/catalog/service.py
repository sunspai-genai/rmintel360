from __future__ import annotations

from typing import Any

from backend.app.catalog.search_index import search_documents
from backend.app.db.connection import fetch_all


class CatalogService:
    """Read service for governed metadata catalog objects."""

    def list_tables(
        self,
        subject_area: str | None = None,
        table_type: str | None = None,
        certified: bool | None = None,
    ) -> list[dict[str, Any]]:
        sql = """
            SELECT
                table_name,
                business_name,
                subject_area,
                table_type,
                domain,
                grain,
                refresh_frequency,
                data_owner,
                certified_flag
            FROM metadata_table
            WHERE (? IS NULL OR lower(subject_area) = lower(?))
              AND (? IS NULL OR lower(table_type) = lower(?))
              AND (? IS NULL OR certified_flag = ?)
            ORDER BY subject_area, table_type, table_name
        """
        return fetch_all(sql, [subject_area, subject_area, table_type, table_type, certified, certified])

    def get_table(self, table_name: str) -> dict[str, Any] | None:
        table_rows = fetch_all(
            """
            SELECT
                table_name,
                business_name,
                subject_area,
                table_type,
                domain,
                grain,
                refresh_frequency,
                data_owner,
                certified_flag
            FROM metadata_table
            WHERE lower(table_name) = lower(?)
            """,
            [table_name],
        )
        if not table_rows:
            return None

        table = table_rows[0]
        table["columns"] = self.list_columns(table_name=table["table_name"])
        table["join_paths"] = self.list_join_paths(table_name=table["table_name"])
        table["lineage"] = self.list_lineage(asset_name=table["table_name"])
        return table

    def list_columns(
        self,
        table_name: str | None = None,
        semantic_type: str | None = None,
        include_restricted: bool = True,
    ) -> list[dict[str, Any]]:
        sql = """
            SELECT
                table_name,
                column_name,
                business_name,
                data_type,
                description,
                semantic_type,
                pii_flag,
                restricted_flag,
                sample_values
            FROM metadata_column
            WHERE (? IS NULL OR lower(table_name) = lower(?))
              AND (? IS NULL OR lower(semantic_type) = lower(?))
              AND (? OR restricted_flag = false)
            ORDER BY table_name, column_name
        """
        return fetch_all(sql, [table_name, table_name, semantic_type, semantic_type, include_restricted])

    def list_business_terms(
        self,
        query: str | None = None,
        term_type: str | None = None,
        certified: bool | None = None,
    ) -> list[dict[str, Any]]:
        like_query = self._like(query)
        sql = """
            SELECT
                term_id,
                term_name,
                term_type,
                definition,
                calculation,
                primary_table,
                primary_column,
                certified_flag,
                owner,
                subject_area
            FROM metadata_business_term
            WHERE (? IS NULL OR lower(term_type) = lower(?))
              AND (? IS NULL OR certified_flag = ?)
              AND (
                  ? IS NULL
                  OR lower(term_name) LIKE lower(?)
                  OR lower(definition) LIKE lower(?)
                  OR lower(coalesce(calculation, '')) LIKE lower(?)
              )
            ORDER BY term_type, term_name
        """
        return fetch_all(
            sql,
            [term_type, term_type, certified, certified, like_query, like_query, like_query, like_query],
        )

    def list_metrics(
        self,
        query: str | None = None,
        subject_area: str | None = None,
        certified: bool | None = None,
    ) -> list[dict[str, Any]]:
        like_query = self._like(query)
        sql = """
            SELECT
                metric_id,
                metric_name,
                description,
                calculation_sql,
                aggregation_type,
                base_table,
                required_columns,
                default_time_period,
                certified_flag,
                subject_area
            FROM metadata_metric
            WHERE (? IS NULL OR lower(subject_area) = lower(?))
              AND (? IS NULL OR certified_flag = ?)
              AND (
                  ? IS NULL
                  OR lower(metric_name) LIKE lower(?)
                  OR lower(description) LIKE lower(?)
                  OR lower(calculation_sql) LIKE lower(?)
              )
            ORDER BY subject_area, metric_name
        """
        return fetch_all(
            sql,
            [subject_area, subject_area, certified, certified, like_query, like_query, like_query, like_query],
        )

    def get_metric(self, metric_id: str) -> dict[str, Any] | None:
        rows = fetch_all(
            """
            SELECT
                metric_id,
                metric_name,
                description,
                calculation_sql,
                aggregation_type,
                base_table,
                required_columns,
                default_time_period,
                certified_flag,
                subject_area
            FROM metadata_metric
            WHERE lower(metric_id) = lower(?)
            """,
            [metric_id],
        )
        return rows[0] if rows else None

    def list_dimensions(
        self,
        query: str | None = None,
        subject_area: str | None = None,
        certified: bool | None = None,
    ) -> list[dict[str, Any]]:
        like_query = self._like(query)
        sql = """
            SELECT
                dimension_id,
                dimension_name,
                description,
                table_name,
                column_name,
                sample_values,
                certified_flag,
                subject_area
            FROM metadata_dimension
            WHERE (? IS NULL OR lower(subject_area) = lower(?))
              AND (? IS NULL OR certified_flag = ?)
              AND (
                  ? IS NULL
                  OR lower(dimension_name) LIKE lower(?)
                  OR lower(description) LIKE lower(?)
                  OR lower(table_name) LIKE lower(?)
                  OR lower(column_name) LIKE lower(?)
              )
            ORDER BY subject_area, dimension_name
        """
        return fetch_all(
            sql,
            [
                subject_area,
                subject_area,
                certified,
                certified,
                like_query,
                like_query,
                like_query,
                like_query,
                like_query,
            ],
        )

    def get_dimension(self, dimension_id: str) -> dict[str, Any] | None:
        rows = fetch_all(
            """
            SELECT
                dimension_id,
                dimension_name,
                description,
                table_name,
                column_name,
                sample_values,
                certified_flag,
                subject_area
            FROM metadata_dimension
            WHERE lower(dimension_id) = lower(?)
            """,
            [dimension_id],
        )
        return rows[0] if rows else None

    def list_join_paths(self, table_name: str | None = None) -> list[dict[str, Any]]:
        sql = """
            SELECT
                join_path_id,
                from_table,
                from_column,
                to_table,
                to_column,
                relationship_type,
                join_type,
                description,
                certified_flag
            FROM metadata_join_path
            WHERE (
                ? IS NULL
                OR lower(from_table) = lower(?)
                OR lower(to_table) = lower(?)
            )
            ORDER BY from_table, to_table, join_path_id
        """
        return fetch_all(sql, [table_name, table_name, table_name])

    def list_lineage(self, asset_name: str | None = None) -> list[dict[str, Any]]:
        table_name: str | None = None
        column_name: str | None = None
        if asset_name and "." in asset_name:
            table_name, column_name = asset_name.split(".", 1)

        sql = """
            SELECT
                lineage_id,
                source_system,
                source_object,
                target_table,
                target_column,
                transformation,
                refresh_frequency,
                data_owner
            FROM metadata_lineage
            WHERE (
                ? IS NULL
                OR lower(target_table) = lower(?)
                OR lower(target_column) = lower(?)
                OR lower(source_system) LIKE lower(?)
                OR lower(source_object) LIKE lower(?)
            )
              AND (? IS NULL OR lower(target_table) = lower(?))
              AND (? IS NULL OR lower(target_column) = lower(?))
            ORDER BY target_table, target_column
        """
        search_name = None if table_name and column_name else asset_name
        like_asset = self._like(search_name)
        return fetch_all(
            sql,
            [
                search_name,
                search_name,
                search_name,
                like_asset,
                like_asset,
                table_name,
                table_name,
                column_name,
                column_name,
            ],
        )

    def list_access_policies(self, role_name: str | None = None) -> list[dict[str, Any]]:
        sql = """
            SELECT
                policy_id,
                role_name,
                asset_type,
                asset_name,
                permission,
                masking_rule,
                notes
            FROM metadata_access_policy
            WHERE (? IS NULL OR lower(role_name) = lower(?))
            ORDER BY role_name, asset_type, asset_name
        """
        return fetch_all(sql, [role_name, role_name])

    def list_synonyms(self, target_type: str | None = None) -> list[dict[str, Any]]:
        sql = """
            SELECT
                synonym_id,
                phrase,
                target_type,
                target_id,
                confidence,
                notes
            FROM metadata_synonym
            WHERE (? IS NULL OR lower(target_type) = lower(?))
            ORDER BY length(phrase) DESC, confidence DESC, phrase
        """
        return fetch_all(sql, [target_type, target_type])

    def list_search_documents(self, document_type: str | None = None) -> list[dict[str, Any]]:
        sql = """
            SELECT
                document_id,
                document_type,
                source_id,
                business_name,
                content,
                table_name,
                column_name,
                subject_area,
                certified_flag,
                token_vector_json
            FROM metadata_search_document
            WHERE (? IS NULL OR lower(document_type) = lower(?))
            ORDER BY document_type, business_name, source_id
        """
        return fetch_all(sql, [document_type, document_type])

    def search_metadata(
        self,
        query: str,
        document_type: str | None = None,
        limit: int = 10,
        min_score: float = 0.0,
    ) -> list[dict[str, Any]]:
        rows = self.list_search_documents(document_type=document_type)
        return search_documents(
            query=query,
            rows=rows,
            document_type=document_type,
            limit=limit,
            min_score=min_score,
        )

    def search_governed_candidates(
        self,
        phrase: str,
        target_type: str | None = None,
        min_confidence: float = 0.0,
        exact_match: bool = True,
    ) -> list[dict[str, Any]]:
        like_phrase = self._like(phrase)
        sql = """
            SELECT
                s.synonym_id,
                s.phrase,
                s.target_type,
                s.target_id,
                s.confidence,
                s.notes,
                coalesce(m.metric_name, d.dimension_name, t.term_name) AS business_name,
                coalesce(m.description, d.description, t.definition) AS description,
                coalesce(m.calculation_sql, t.calculation) AS calculation,
                coalesce(m.base_table, d.table_name, t.primary_table) AS table_name,
                coalesce(d.column_name, t.primary_column) AS column_name,
                m.required_columns,
                coalesce(m.subject_area, d.subject_area, t.subject_area) AS subject_area,
                coalesce(m.certified_flag, d.certified_flag, t.certified_flag) AS certified_flag
            FROM metadata_synonym s
            LEFT JOIN metadata_metric m ON s.target_id = m.metric_id
            LEFT JOIN metadata_dimension d ON s.target_id = d.dimension_id
            LEFT JOIN metadata_business_term t ON s.target_id = t.term_id
            WHERE (? IS NULL OR lower(s.target_type) = lower(?))
              AND s.confidence >= ?
              AND (
                  (? AND lower(s.phrase) = lower(?))
                  OR (
                      NOT ?
                      AND (
                          lower(s.phrase) = lower(?)
                          OR lower(s.phrase) LIKE lower(?)
                          OR lower(coalesce(m.metric_name, d.dimension_name, t.term_name, '')) LIKE lower(?)
                          OR lower(coalesce(m.description, d.description, t.definition, '')) LIKE lower(?)
                      )
                  )
              )
            ORDER BY s.confidence DESC, business_name
        """
        return fetch_all(
            sql,
            [
                target_type,
                target_type,
                min_confidence,
                exact_match,
                phrase,
                exact_match,
                phrase,
                like_phrase,
                like_phrase,
                like_phrase,
            ],
        )

    @staticmethod
    def _like(value: str | None) -> str | None:
        if not value:
            return None
        return f"%{value}%"


catalog_service = CatalogService()
