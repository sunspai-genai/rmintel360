from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from typing import Any

from backend.app.catalog.service import catalog_service
from backend.app.intent.classifier import IntentType, intent_classifier


class ResolutionStatus:
    RESOLVED = "resolved"
    NEEDS_CLARIFICATION = "needs_clarification"
    INFORMATION_ONLY = "information_only"
    UNSUPPORTED = "unsupported"


ANALYTICAL_INTENTS = {IntentType.ANALYTICAL_QUERY, IntentType.CHART_QUERY}
INFORMATION_INTENTS = {
    IntentType.DEFINITION_QUESTION,
    IntentType.METADATA_QUESTION,
    IntentType.LINEAGE_QUESTION,
    IntentType.TABLE_DISCOVERY_QUESTION,
}
DATE_PHRASE_TO_FILTER = {
    "latest complete month": "latest_complete_month",
    "last month": "latest_complete_month",
    "this month": "current_month_to_date",
    "last quarter": "latest_complete_quarter",
    "this quarter": "current_quarter_to_date",
    "last 12 months": "last_12_months",
    "this year": "current_year_to_date",
    "last year": "latest_complete_year",
    "monthly": "month_trend",
    "quarterly": "quarter_trend",
    "yearly": "year_trend",
}


@dataclass(frozen=True)
class SemanticResolutionResult:
    message: str
    intent: str
    status: str
    requires_sql: bool
    confidence: float
    rationale: str
    ambiguities: list[dict[str, Any]]
    resolved: dict[str, Any]
    governed_query_plan: dict[str, Any] | None
    assumptions: list[str]
    retrieval_context: list[dict[str, Any]]

    def to_dict(self) -> dict[str, Any]:
        return {
            "message": self.message,
            "intent": self.intent,
            "status": self.status,
            "requires_sql": self.requires_sql,
            "confidence": self.confidence,
            "rationale": self.rationale,
            "ambiguities": self.ambiguities,
            "resolved": self.resolved,
            "governed_query_plan": self.governed_query_plan,
            "assumptions": self.assumptions,
            "retrieval_context": self.retrieval_context,
        }


class SemanticResolver:
    """Resolve raw analytical language into governed catalog objects."""

    def resolve(
        self,
        message: str,
        intent: str | None = None,
        selected_metric_id: str | None = None,
        selected_dimension_ids: list[str] | None = None,
        awaiting_clarification: bool = False,
        context: dict[str, Any] | None = None,
    ) -> SemanticResolutionResult:
        classification = intent_classifier.classify(
            message=message,
            awaiting_clarification=awaiting_clarification,
            context=context,
        )
        resolved_intent = intent or classification.intent
        selected_dimension_ids = selected_dimension_ids or []

        if resolved_intent in INFORMATION_INTENTS:
            return SemanticResolutionResult(
                message=message,
                intent=resolved_intent,
                status=ResolutionStatus.INFORMATION_ONLY,
                requires_sql=False,
                confidence=classification.confidence,
                rationale="The request is informational and should be answered from governed metadata without SQL.",
                ambiguities=[],
                resolved={},
                governed_query_plan=None,
                assumptions=[],
                retrieval_context=classification.retrieval_context,
            )

        if resolved_intent == IntentType.UNSUPPORTED:
            return SemanticResolutionResult(
                message=message,
                intent=resolved_intent,
                status=ResolutionStatus.UNSUPPORTED,
                requires_sql=False,
                confidence=classification.confidence,
                rationale="The request is outside the governed commercial banking analytics scope.",
                ambiguities=[],
                resolved={},
                governed_query_plan=None,
                assumptions=[],
                retrieval_context=classification.retrieval_context,
            )

        if resolved_intent not in ANALYTICAL_INTENTS:
            return SemanticResolutionResult(
                message=message,
                intent=resolved_intent,
                status=ResolutionStatus.NEEDS_CLARIFICATION,
                requires_sql=False,
                confidence=0.55,
                rationale="The request needs additional conversation context before semantic resolution can continue.",
                ambiguities=[
                    {
                        "kind": "intent",
                        "phrase": message,
                        "question": "Should I answer this as a definition, metadata lookup, or analytical query?",
                        "options": [
                            {"id": IntentType.DEFINITION_QUESTION, "label": "Definition or metadata answer"},
                            {"id": IntentType.ANALYTICAL_QUERY, "label": "Analytical query"},
                        ],
                    }
                ],
                resolved={},
                governed_query_plan=None,
                assumptions=[],
                retrieval_context=classification.retrieval_context,
            )

        metric_resolution = self._resolve_metric(
            message=message,
            selected_metric_id=selected_metric_id,
            retrieval_context=classification.retrieval_context,
        )
        dimension_resolution = self._resolve_dimensions(
            message=message,
            selected_dimension_ids=selected_dimension_ids,
            retrieval_context=classification.retrieval_context,
        )

        ambiguities = metric_resolution["ambiguities"] + dimension_resolution["ambiguities"]
        resolved = {
            "metric": metric_resolution["metric"],
            "dimensions": dimension_resolution["dimensions"],
        }

        if ambiguities:
            return SemanticResolutionResult(
                message=message,
                intent=resolved_intent,
                status=ResolutionStatus.NEEDS_CLARIFICATION,
                requires_sql=False,
                confidence=0.72,
                rationale="One or more business phrases map to multiple governed objects. SQL generation is blocked until the user selects governed options.",
                ambiguities=ambiguities,
                resolved=resolved,
                governed_query_plan=None,
                assumptions=[],
                retrieval_context=classification.retrieval_context,
            )

        if not metric_resolution["metric"]:
            return SemanticResolutionResult(
                message=message,
                intent=resolved_intent,
                status=ResolutionStatus.NEEDS_CLARIFICATION,
                requires_sql=False,
                confidence=0.58,
                rationale="No certified governed metric could be resolved from the request.",
                ambiguities=[
                    {
                        "kind": "metric",
                        "phrase": message,
                        "question": "Which governed metric should I use?",
                        "options": self._top_metric_options(classification.retrieval_context),
                    }
                ],
                resolved=resolved,
                governed_query_plan=None,
                assumptions=[],
                retrieval_context=classification.retrieval_context,
            )

        query_plan_result = self._build_query_plan(
            metric=metric_resolution["metric"],
            dimensions=dimension_resolution["dimensions"],
            intent=resolved_intent,
            classification_entities=classification.extracted_entities,
        )

        if query_plan_result["ambiguities"]:
            return SemanticResolutionResult(
                message=message,
                intent=resolved_intent,
                status=ResolutionStatus.NEEDS_CLARIFICATION,
                requires_sql=False,
                confidence=0.68,
                rationale="The metric and dimensions were resolved, but no approved governed join path was available for at least one dimension.",
                ambiguities=query_plan_result["ambiguities"],
                resolved=resolved,
                governed_query_plan=None,
                assumptions=query_plan_result["assumptions"],
                retrieval_context=classification.retrieval_context,
            )

        return SemanticResolutionResult(
            message=message,
            intent=resolved_intent,
            status=ResolutionStatus.RESOLVED,
            requires_sql=True,
            confidence=0.91,
            rationale="All required analytical semantics were resolved to governed catalog objects.",
            ambiguities=[],
            resolved=resolved,
            governed_query_plan=query_plan_result["query_plan"],
            assumptions=query_plan_result["assumptions"],
            retrieval_context=classification.retrieval_context,
        )

    def _resolve_metric(
        self,
        message: str,
        selected_metric_id: str | None,
        retrieval_context: list[dict[str, Any]],
    ) -> dict[str, Any]:
        if selected_metric_id:
            metric = catalog_service.get_metric(selected_metric_id)
            return {"metric": self._metric_object(metric), "ambiguities": []} if metric else {
                "metric": None,
                "ambiguities": [
                    {
                        "kind": "metric",
                        "phrase": selected_metric_id,
                        "question": f"The selected metric `{selected_metric_id}` is not governed. Which governed metric should I use?",
                        "options": self._top_metric_options(retrieval_context),
                    }
                ],
            }

        phrase_groups = self._matched_candidate_groups(message=message, target_type="metric")
        if phrase_groups:
            if len(phrase_groups) > 1:
                return {
                    "metric": None,
                    "ambiguities": [
                        self._ambiguity(
                            kind="metric",
                            phrase=" / ".join(group["phrase"] for group in phrase_groups),
                            question="I found multiple metric phrases. Which governed metric should I use?",
                            candidates=[candidate for group in phrase_groups for candidate in group["candidates"]],
                        )
                    ],
                }
            group = phrase_groups[0]
            if len(group["candidates"]) == 1:
                return {"metric": self._candidate_to_metric(group["candidates"][0]), "ambiguities": []}
            return {
                "metric": None,
                "ambiguities": [
                    self._ambiguity(
                        kind="metric",
                        phrase=group["phrase"],
                        question=f"Which governed meaning of `{group['phrase']}` should I use?",
                        candidates=group["candidates"],
                    )
                ],
            }

        metric_hints = [item for item in retrieval_context if item["document_type"] == "metric"]
        if metric_hints:
            top = metric_hints[0]
            second_score = metric_hints[1]["score"] if len(metric_hints) > 1 else 0.0
            if top["score"] >= 0.62 and (top["score"] - second_score >= 0.10 or len(metric_hints) == 1):
                metric = catalog_service.get_metric(top["source_id"])
                return {"metric": self._metric_object(metric), "ambiguities": []}

        return {"metric": None, "ambiguities": []}

    def _resolve_dimensions(
        self,
        message: str,
        selected_dimension_ids: list[str],
        retrieval_context: list[dict[str, Any]],
    ) -> dict[str, Any]:
        if selected_dimension_ids:
            dimensions = []
            missing = []
            for dimension_id in selected_dimension_ids:
                dimension = catalog_service.get_dimension(dimension_id)
                if dimension:
                    dimensions.append(self._dimension_object(dimension))
                else:
                    missing.append(dimension_id)
            if missing:
                return {
                    "dimensions": dimensions,
                    "ambiguities": [
                        {
                            "kind": "dimension",
                            "phrase": ", ".join(missing),
                            "question": "One or more selected dimensions are not governed. Which governed dimensions should I use?",
                            "options": self._top_dimension_options(retrieval_context),
                        }
                    ],
                }
            return {"dimensions": dimensions, "ambiguities": []}

        phrase_groups = self._matched_candidate_groups(message=message, target_type="dimension")
        dimensions = []
        ambiguities = []

        for group in phrase_groups:
            candidates = group["candidates"]
            if len(candidates) == 1:
                dimensions.append(self._candidate_to_dimension(candidates[0]))
            else:
                ambiguities.append(
                    self._ambiguity(
                        kind="dimension",
                        phrase=group["phrase"],
                        question=f"Which governed meaning of `{group['phrase']}` should I use?",
                        candidates=candidates,
                    )
                )

        return {"dimensions": self._dedupe_by_id(dimensions), "ambiguities": ambiguities}

    def _matched_candidate_groups(self, message: str, target_type: str) -> list[dict[str, Any]]:
        normalized = self._normalize(message)
        synonyms = catalog_service.list_synonyms(target_type=target_type)
        chosen_phrases: list[str] = []
        groups: list[dict[str, Any]] = []

        for synonym in synonyms:
            phrase = self._normalize(synonym["phrase"])
            if not phrase or not self._phrase_in_message(phrase=phrase, normalized_message=normalized):
                continue
            if any(phrase in chosen and phrase != chosen for chosen in chosen_phrases):
                continue
            if phrase in chosen_phrases:
                continue

            candidates = catalog_service.search_governed_candidates(
                phrase=phrase,
                target_type=target_type,
                exact_match=True,
            )
            if candidates:
                chosen_phrases.append(phrase)
                groups.append({"phrase": phrase, "candidates": candidates})

        return groups

    def _build_query_plan(
        self,
        metric: dict[str, Any],
        dimensions: list[dict[str, Any]],
        intent: str,
        classification_entities: dict[str, Any],
    ) -> dict[str, Any]:
        assumptions = []
        ambiguities = []
        joins = []
        base_table = metric["base_table"]

        for dimension in dimensions:
            path = self._find_join_path(base_table=base_table, target_table=dimension["table_name"])
            if path is None:
                ambiguities.append(
                    {
                        "kind": "join_path",
                        "phrase": f"{base_table} to {dimension['table_name']}",
                        "question": f"No approved governed join path was found from `{base_table}` to `{dimension['table_name']}`.",
                        "options": [],
                    }
                )
            else:
                joins.extend(path)

        filters = self._resolve_filters(
            metric=metric,
            dimensions=dimensions,
            classification_entities=classification_entities,
        )
        assumptions.extend(filters["assumptions"])

        return {
            "ambiguities": ambiguities,
            "assumptions": assumptions,
            "query_plan": {
                "intent": intent,
                "metric": metric,
                "dimensions": dimensions,
                "base_table": base_table,
                "joins": self._dedupe_joins(joins),
                "filters": filters["filters"],
                "assumptions": assumptions,
            },
        }

    def _resolve_filters(
        self,
        metric: dict[str, Any],
        dimensions: list[dict[str, Any]],
        classification_entities: dict[str, Any],
    ) -> dict[str, Any]:
        date_phrases = classification_entities.get("date_phrases", [])
        filters = []
        assumptions = []

        for phrase in date_phrases:
            filter_id = DATE_PHRASE_TO_FILTER.get(phrase)
            if filter_id:
                filters.append({"filter_id": filter_id, "phrase": phrase})

        if not filters and any(dimension["id"] == "dimension.year_month" for dimension in dimensions):
            filters.append({"filter_id": "last_12_months", "phrase": "monthly trend default"})
            assumptions.append("Used `last_12_months` because the request groups results by month without specifying a period.")
        elif not filters and metric.get("default_time_period"):
            filters.append({"filter_id": metric["default_time_period"], "phrase": "default"})
            assumptions.append(f"Used `{metric['default_time_period']}` because no time period was specified.")

        return {"filters": filters, "assumptions": assumptions}

    def _find_join_path(self, base_table: str, target_table: str) -> list[dict[str, Any]] | None:
        if base_table == target_table:
            return []

        join_paths = catalog_service.list_join_paths()
        graph: dict[str, list[tuple[str, dict[str, Any]]]] = {}
        for join_path in join_paths:
            from_table = join_path["from_table"]
            to_table = join_path["to_table"]
            graph.setdefault(from_table, []).append((to_table, join_path))
            graph.setdefault(to_table, []).append((from_table, join_path))

        queue = deque([(base_table, [])])
        visited = {base_table}

        while queue:
            table, path = queue.popleft()
            for next_table, join_path in graph.get(table, []):
                if next_table in visited:
                    continue
                next_path = path + [join_path]
                if next_table == target_table:
                    return next_path
                visited.add(next_table)
                queue.append((next_table, next_path))

        return None

    def _ambiguity(
        self,
        kind: str,
        phrase: str,
        question: str,
        candidates: list[dict[str, Any]],
    ) -> dict[str, Any]:
        return {
            "kind": kind,
            "phrase": phrase,
            "question": question,
            "options": [self._candidate_option(candidate) for candidate in candidates],
        }

    def _candidate_option(self, candidate: dict[str, Any]) -> dict[str, Any]:
        required_columns = self._split_columns(candidate.get("required_columns"))
        display_column = candidate.get("column_name") or self._first_measure_column(required_columns)
        return {
            "id": candidate["target_id"],
            "label": candidate["business_name"],
            "target_type": candidate["target_type"],
            "definition": candidate["description"],
            "table": candidate.get("table_name"),
            "column": display_column,
            "required_columns": required_columns,
            "calculation": candidate.get("calculation"),
            "subject_area": candidate.get("subject_area"),
            "confidence": candidate.get("confidence"),
            "certified": candidate.get("certified_flag"),
        }

    def _candidate_to_metric(self, candidate: dict[str, Any]) -> dict[str, Any] | None:
        metric = catalog_service.get_metric(candidate["target_id"])
        return self._metric_object(metric)

    def _candidate_to_dimension(self, candidate: dict[str, Any]) -> dict[str, Any] | None:
        dimension = catalog_service.get_dimension(candidate["target_id"])
        return self._dimension_object(dimension)

    def _metric_object(self, metric: dict[str, Any] | None) -> dict[str, Any] | None:
        if not metric:
            return None
        return {
            "id": metric["metric_id"],
            "business_name": metric["metric_name"],
            "description": metric["description"],
            "calculation_sql": metric["calculation_sql"],
            "aggregation_type": metric["aggregation_type"],
            "base_table": metric["base_table"],
            "required_columns": self._split_columns(metric["required_columns"]),
            "default_time_period": metric["default_time_period"],
            "subject_area": metric["subject_area"],
            "certified": metric["certified_flag"],
        }

    def _dimension_object(self, dimension: dict[str, Any] | None) -> dict[str, Any] | None:
        if not dimension:
            return None
        return {
            "id": dimension["dimension_id"],
            "business_name": dimension["dimension_name"],
            "description": dimension["description"],
            "table_name": dimension["table_name"],
            "column_name": dimension["column_name"],
            "sample_values": dimension["sample_values"],
            "subject_area": dimension["subject_area"],
            "certified": dimension["certified_flag"],
        }

    def _top_metric_options(self, retrieval_context: list[dict[str, Any]]) -> list[dict[str, Any]]:
        options = []
        for item in retrieval_context:
            if item["document_type"] != "metric":
                continue
            metric = catalog_service.get_metric(item["source_id"])
            if metric:
                options.append(
                    {
                        "id": metric["metric_id"],
                        "label": metric["metric_name"],
                        "target_type": "metric",
                        "definition": metric["description"],
                        "table": metric["base_table"],
                        "column": self._first_measure_column(self._split_columns(metric["required_columns"])),
                        "required_columns": self._split_columns(metric["required_columns"]),
                        "calculation": metric["calculation_sql"],
                        "subject_area": metric["subject_area"],
                        "confidence": item["score"],
                        "certified": metric["certified_flag"],
                    }
                )
        return options[:5]

    def _top_dimension_options(self, retrieval_context: list[dict[str, Any]]) -> list[dict[str, Any]]:
        options = []
        for item in retrieval_context:
            if item["document_type"] != "dimension":
                continue
            dimension = catalog_service.get_dimension(item["source_id"])
            if dimension:
                options.append(
                    {
                        "id": dimension["dimension_id"],
                        "label": dimension["dimension_name"],
                        "target_type": "dimension",
                        "definition": dimension["description"],
                        "table": dimension["table_name"],
                        "column": dimension["column_name"],
                        "required_columns": [dimension["column_name"]],
                        "calculation": None,
                        "subject_area": dimension["subject_area"],
                        "confidence": item["score"],
                        "certified": dimension["certified_flag"],
                    }
                )
        return options[:5]

    def _split_columns(self, columns: str | None) -> list[str]:
        if not columns:
            return []
        return [column.strip() for column in columns.split(",") if column.strip()]

    def _first_measure_column(self, columns: list[str]) -> str | None:
        for column in columns:
            if column not in {"as_of_date", "as_of_month", "account_id", "loan_id", "customer_id"}:
                return column
        return columns[0] if columns else None

    def _dedupe_by_id(self, items: list[dict[str, Any] | None]) -> list[dict[str, Any]]:
        deduped = []
        seen = set()
        for item in items:
            if not item or item["id"] in seen:
                continue
            seen.add(item["id"])
            deduped.append(item)
        return deduped

    def _dedupe_joins(self, joins: list[dict[str, Any]]) -> list[dict[str, Any]]:
        deduped = []
        seen = set()
        for join in joins:
            join_id = join["join_path_id"]
            if join_id in seen:
                continue
            seen.add(join_id)
            deduped.append(join)
        return deduped

    def _normalize(self, message: str) -> str:
        return " ".join(message.lower().strip().split())

    def _phrase_in_message(self, phrase: str, normalized_message: str) -> bool:
        padded_phrase = f" {phrase} "
        padded_message = f" {normalized_message} "
        return padded_phrase in padded_message


semantic_resolver = SemanticResolver()
