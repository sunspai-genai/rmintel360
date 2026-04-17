from __future__ import annotations

from typing import Any

from backend.app.catalog.service import catalog_service


class GovernanceAuditService:
    """Build evidence packets explaining why an assistant response was allowed."""

    def build_from_chat_response(
        self,
        chat_response: dict[str, Any],
        user_role: str = "business_user",
    ) -> dict[str, Any]:
        semantic_result = chat_response.get("semantic_result") or {}
        query_plan = semantic_result.get("governed_query_plan")
        sql_validation = chat_response.get("sql_validation") or {}
        allowed_tables = sql_validation.get("allowed_tables") or []
        allowed_columns = sql_validation.get("allowed_columns") or []
        metadata_context = chat_response.get("metadata_context") or []

        if not query_plan:
            return self._non_sql_audit(
                chat_response=chat_response,
                user_role=user_role,
                metadata_context=metadata_context,
            )

        table_names = self._ordered_table_names(query_plan=query_plan, allowed_tables=allowed_tables)
        source_tables = [self._table_evidence(table_name) for table_name in table_names]
        lineage = self._lineage_evidence(allowed_columns=allowed_columns, query_plan=query_plan)
        access = self._access_evidence(
            role_name=user_role,
            allowed_tables=allowed_tables,
            allowed_columns=allowed_columns,
            sql_visible=chat_response.get("sql_visible", False),
        )

        return {
            "status": "available",
            "audit_summary": self._summary(chat_response=chat_response, query_plan=query_plan),
            "resolved_assets": {
                "metric": query_plan.get("metric"),
                "dimensions": query_plan.get("dimensions") or [],
                "filters": query_plan.get("filters") or [],
            },
            "source_tables": source_tables,
            "approved_joins": query_plan.get("joins") or [],
            "lineage": lineage,
            "access_controls": access,
            "sql_validation": sql_validation,
            "retrieval_context": metadata_context,
            "assumptions": chat_response.get("assumptions") or [],
            "warnings": chat_response.get("warnings") or [],
            "graph_trace": chat_response.get("graph_trace") or [],
        }

    def _non_sql_audit(
        self,
        chat_response: dict[str, Any],
        user_role: str,
        metadata_context: list[dict[str, Any]],
    ) -> dict[str, Any]:
        return {
            "status": "metadata_only" if metadata_context else "limited",
            "audit_summary": {
                "answer_status": chat_response.get("status"),
                "intent": chat_response.get("intent"),
                "route": chat_response.get("route"),
                "requires_clarification": chat_response.get("requires_clarification", False),
                "message": "No SQL query plan was executed; audit is based on routed metadata and clarification evidence.",
            },
            "resolved_assets": {"metric": None, "dimensions": [], "filters": []},
            "source_tables": [],
            "approved_joins": [],
            "lineage": [],
            "access_controls": self._access_evidence(
                role_name=user_role,
                allowed_tables=[],
                allowed_columns=[],
                sql_visible=chat_response.get("sql_visible", False),
            ),
            "sql_validation": chat_response.get("sql_validation"),
            "retrieval_context": metadata_context,
            "assumptions": chat_response.get("assumptions") or [],
            "warnings": chat_response.get("warnings") or [],
            "graph_trace": chat_response.get("graph_trace") or [],
        }

    def _summary(self, chat_response: dict[str, Any], query_plan: dict[str, Any]) -> dict[str, Any]:
        metric = query_plan.get("metric") or {}
        dimensions = query_plan.get("dimensions") or []
        result_table = chat_response.get("result_table") or {}
        return {
            "answer_status": chat_response.get("status"),
            "intent": chat_response.get("intent"),
            "route": chat_response.get("route"),
            "metric": metric.get("business_name"),
            "certified_metric": bool(metric.get("certified")),
            "certified_dimensions": all(bool(dimension.get("certified")) for dimension in dimensions),
            "dimension_count": len(dimensions),
            "row_count": result_table.get("row_count"),
            "sql_visible": chat_response.get("sql_visible", False),
            "message": "Response used certified semantic assets, approved joins, validated SQL, and role-aware access policy evidence.",
        }

    def _ordered_table_names(self, query_plan: dict[str, Any], allowed_tables: list[str]) -> list[str]:
        table_names = [query_plan["base_table"]]
        for join in query_plan.get("joins") or []:
            table_names.extend([join["from_table"], join["to_table"]])
        for dimension in query_plan.get("dimensions") or []:
            table_names.append(dimension["table_name"])
        table_names.extend(allowed_tables)
        return self._dedupe(table_names)

    def _table_evidence(self, table_name: str) -> dict[str, Any]:
        table = catalog_service.get_table(table_name)
        if not table:
            return {
                "table_name": table_name,
                "business_name": table_name,
                "certified": False,
                "columns": [],
                "lineage_count": 0,
            }

        columns = table.pop("columns", [])
        table["certified"] = table.pop("certified_flag", False)
        table["columns"] = [
            {
                "column_name": column["column_name"],
                "business_name": column["business_name"],
                "semantic_type": column["semantic_type"],
                "pii_flag": column["pii_flag"],
                "restricted_flag": column["restricted_flag"],
            }
            for column in columns
        ]
        table["lineage_count"] = len(table.get("lineage") or [])
        return table

    def _lineage_evidence(
        self,
        allowed_columns: list[str],
        query_plan: dict[str, Any],
    ) -> list[dict[str, Any]]:
        lineage_records: list[dict[str, Any]] = []
        metric = query_plan.get("metric") or {}
        base_table = query_plan.get("base_table")

        qualified_columns = list(allowed_columns)
        for required_column in self._split_required_columns(metric.get("required_columns")):
            if base_table:
                qualified_columns.append(f"{base_table}.{required_column}")

        for dimension in query_plan.get("dimensions") or []:
            qualified_columns.append(f"{dimension['table_name']}.{dimension['column_name']}")

        seen: set[str] = set()
        for qualified_column in self._dedupe(qualified_columns):
            for lineage in catalog_service.list_lineage(asset_name=qualified_column):
                lineage_key = lineage["lineage_id"]
                if lineage_key in seen:
                    continue
                lineage_records.append(lineage)
                seen.add(lineage_key)
        return lineage_records

    def _access_evidence(
        self,
        role_name: str,
        allowed_tables: list[str],
        allowed_columns: list[str],
        sql_visible: bool,
    ) -> dict[str, Any]:
        policies = catalog_service.list_access_policies(role_name=role_name)
        touched_assets = set(allowed_tables) | set(allowed_columns) | {"generated_sql"}
        matching_policies = [
            policy
            for policy in policies
            if policy["asset_name"] == "*"
            or policy["asset_name"] in touched_assets
            or (
                policy["asset_type"] == "table"
                and policy["asset_name"] in allowed_tables
            )
        ]
        return {
            "role_name": role_name,
            "sql_visible": sql_visible,
            "touched_tables": allowed_tables,
            "touched_columns": allowed_columns,
            "matching_policies": matching_policies,
        }

    def _split_required_columns(self, value: Any) -> list[str]:
        if not value:
            return []
        if isinstance(value, list):
            return [str(item).strip() for item in value if str(item).strip()]
        return [part.strip() for part in str(value).split(",") if part.strip()]

    def _dedupe(self, values: list[str]) -> list[str]:
        seen: set[str] = set()
        deduped: list[str] = []
        for value in values:
            if value in seen:
                continue
            seen.add(value)
            deduped.append(value)
        return deduped


governance_audit_service = GovernanceAuditService()

