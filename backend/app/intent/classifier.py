from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

from backend.app.catalog.service import catalog_service


class IntentType:
    DEFINITION_QUESTION = "definition_question"
    METADATA_QUESTION = "metadata_question"
    LINEAGE_QUESTION = "lineage_question"
    TABLE_DISCOVERY_QUESTION = "table_discovery_question"
    ANALYTICAL_QUERY = "analytical_query"
    CHART_QUERY = "chart_query"
    CLARIFICATION_RESPONSE = "clarification_response"
    UNSUPPORTED = "unsupported"


ROUTE_BY_INTENT = {
    IntentType.DEFINITION_QUESTION: "information_flow",
    IntentType.METADATA_QUESTION: "information_flow",
    IntentType.LINEAGE_QUESTION: "information_flow",
    IntentType.TABLE_DISCOVERY_QUESTION: "information_flow",
    IntentType.ANALYTICAL_QUERY: "governed_analytics_flow",
    IntentType.CHART_QUERY: "governed_analytics_flow",
    IntentType.CLARIFICATION_RESPONSE: "clarification_flow",
    IntentType.UNSUPPORTED: "unsupported_flow",
}

SQL_INTENTS = {IntentType.ANALYTICAL_QUERY, IntentType.CHART_QUERY}

CHART_TERMS = [
    "plot",
    "chart",
    "graph",
    "visualize",
    "visualise",
    "bar chart",
    "line chart",
    "pie chart",
    "donut chart",
]
LINEAGE_TERMS = [
    "lineage",
    "source",
    "sourced",
    "come from",
    "comes from",
    "origin",
    "upstream",
    "downstream",
    "feed",
    "feeds",
]
TABLE_DISCOVERY_TERMS = [
    "which table",
    "what table",
    "where is",
    "where are",
    "table has",
    "table contains",
    "find table",
    "available tables",
]
METADATA_TERMS = [
    "schema",
    "metadata",
    "columns",
    "column",
    "field",
    "fields",
    "data type",
    "grain",
    "refresh",
    "catalog",
    "metric",
    "metrics",
    "certified metric",
    "certified metrics",
    "join",
    "joins",
    "join path",
]
DEFINITION_TERMS = [
    "what is",
    "what does",
    "define",
    "definition",
    "meaning",
    "mean",
    "difference between",
    "how is",
    "how are",
    "calculated",
    "calculation",
]
ANALYTICAL_TERMS = [
    "show",
    "give",
    "get",
    "list",
    "rank",
    "compare",
    "trend",
    "total",
    "average",
    "avg",
    "sum",
    "count",
    "rate",
    "top",
    "bottom",
    "highest",
    "lowest",
    "increase",
    "decrease",
    "growth",
    "by",
    "over time",
]
UNSUPPORTED_TERMS = [
    "weather",
    "sports",
    "stock price",
    "bitcoin",
    "restaurant",
    "hotel",
    "flight",
    "movie",
    "recipe",
    "president",
]
DATE_TERMS = [
    "today",
    "yesterday",
    "this month",
    "last month",
    "latest complete month",
    "this quarter",
    "last quarter",
    "this year",
    "last year",
    "last 12 months",
    "monthly",
    "quarterly",
    "yearly",
]


@dataclass(frozen=True)
class IntentResult:
    message: str
    intent: str
    confidence: float
    requires_sql: bool
    route: str
    rationale: str
    signals: list[str]
    extracted_entities: dict[str, Any]
    retrieval_context: list[dict[str, Any]]

    def to_dict(self) -> dict[str, Any]:
        return {
            "message": self.message,
            "intent": self.intent,
            "confidence": self.confidence,
            "requires_sql": self.requires_sql,
            "route": self.route,
            "rationale": self.rationale,
            "signals": self.signals,
            "extracted_entities": self.extracted_entities,
            "retrieval_context": self.retrieval_context,
        }


class IntentClassifier:
    """Deterministic intent classifier for governed analytics routing."""

    def classify(
        self,
        message: str,
        awaiting_clarification: bool = False,
        context: dict[str, Any] | None = None,
    ) -> IntentResult:
        normalized = self._normalize(message)
        signals: list[str] = []
        retrieval_context = catalog_service.search_metadata(message, limit=6, min_score=0.20)
        extracted_entities = self._extract_entities(message=message, retrieval_context=retrieval_context)

        if awaiting_clarification and self._looks_like_clarification(normalized):
            signals.append("awaiting_clarification")
            result = self._result(
                message=message,
                intent=IntentType.CLARIFICATION_RESPONSE,
                confidence=0.91,
                signals=signals,
                rationale="The conversation is waiting for a governed choice and the message looks like a short selection.",
                extracted_entities=extracted_entities,
                retrieval_context=retrieval_context,
            )
            return result

        if self._contains_any(normalized, UNSUPPORTED_TERMS) and not retrieval_context:
            signals.append("out_of_scope_term")
            return self._result(
                message=message,
                intent=IntentType.UNSUPPORTED,
                confidence=0.86,
                signals=signals,
                rationale="The message appears outside the commercial banking analytics scope.",
                extracted_entities=extracted_entities,
                retrieval_context=retrieval_context,
            )

        if self._contains_any(normalized, CHART_TERMS):
            signals.append("chart_term")
            return self._result(
                message=message,
                intent=IntentType.CHART_QUERY,
                confidence=self._confidence(0.88, retrieval_context),
                signals=signals,
                rationale="The message asks for a plot, chart, graph, or visualization.",
                extracted_entities=extracted_entities,
                retrieval_context=retrieval_context,
            )

        if self._is_lineage_question(normalized):
            signals.append("lineage_term")
            return self._result(
                message=message,
                intent=IntentType.LINEAGE_QUESTION,
                confidence=self._confidence(0.86, retrieval_context),
                signals=signals,
                rationale="The message asks about where data comes from or source lineage.",
                extracted_entities=extracted_entities,
                retrieval_context=retrieval_context,
            )

        if self._is_table_discovery_question(normalized):
            signals.append("table_discovery_term")
            return self._result(
                message=message,
                intent=IntentType.TABLE_DISCOVERY_QUESTION,
                confidence=self._confidence(0.84, retrieval_context),
                signals=signals,
                rationale="The message asks which governed table contains or should be used for data.",
                extracted_entities=extracted_entities,
                retrieval_context=retrieval_context,
            )

        if self._is_metadata_question(normalized):
            signals.append("metadata_term")
            return self._result(
                message=message,
                intent=IntentType.METADATA_QUESTION,
                confidence=self._confidence(0.82, retrieval_context),
                signals=signals,
                rationale="The message asks for schema, column, or catalog information.",
                extracted_entities=extracted_entities,
                retrieval_context=retrieval_context,
            )

        if self._is_definition_question(normalized):
            signals.append("definition_term")
            return self._result(
                message=message,
                intent=IntentType.DEFINITION_QUESTION,
                confidence=self._confidence(0.80, retrieval_context),
                signals=signals,
                rationale="The message asks for a business definition or calculation explanation.",
                extracted_entities=extracted_entities,
                retrieval_context=retrieval_context,
            )

        if self._is_analytical_query(normalized, retrieval_context):
            signals.append("analytical_term")
            return self._result(
                message=message,
                intent=IntentType.ANALYTICAL_QUERY,
                confidence=self._confidence(0.80, retrieval_context),
                signals=signals,
                rationale="The message asks for calculated results, grouped values, comparisons, rankings, or trends.",
                extracted_entities=extracted_entities,
                retrieval_context=retrieval_context,
            )

        if retrieval_context:
            signals.append("governed_context_found")
            return self._result(
                message=message,
                intent=IntentType.DEFINITION_QUESTION,
                confidence=0.58,
                signals=signals,
                rationale="The message matched governed metadata but did not clearly request a calculation.",
                extracted_entities=extracted_entities,
                retrieval_context=retrieval_context,
            )

        return self._result(
            message=message,
            intent=IntentType.UNSUPPORTED,
            confidence=0.54,
            signals=["no_governed_context"],
            rationale="The message did not match supported governed metadata or analytics intents.",
            extracted_entities=extracted_entities,
            retrieval_context=retrieval_context,
        )

    def _result(
        self,
        message: str,
        intent: str,
        confidence: float,
        signals: list[str],
        rationale: str,
        extracted_entities: dict[str, Any],
        retrieval_context: list[dict[str, Any]],
    ) -> IntentResult:
        return IntentResult(
            message=message,
            intent=intent,
            confidence=round(min(max(confidence, 0.0), 0.99), 2),
            requires_sql=intent in SQL_INTENTS,
            route=ROUTE_BY_INTENT[intent],
            rationale=rationale,
            signals=signals,
            extracted_entities=extracted_entities,
            retrieval_context=retrieval_context,
        )

    def _extract_entities(self, message: str, retrieval_context: list[dict[str, Any]]) -> dict[str, Any]:
        normalized = self._normalize(message)
        chart_type = self._extract_chart_type(normalized)
        date_phrases = [term for term in DATE_TERMS if term in normalized]

        metrics = [
            item
            for item in retrieval_context
            if item["document_type"] == "metric"
        ][:3]
        dimensions = [
            item
            for item in retrieval_context
            if item["document_type"] == "dimension"
        ][:3]
        tables = [
            item
            for item in retrieval_context
            if item["document_type"] == "table"
        ][:3]
        lineage = [
            item
            for item in retrieval_context
            if item["document_type"] == "lineage"
        ][:3]

        return {
            "chart_type": chart_type,
            "date_phrases": date_phrases,
            "metric_hints": self._compact_context(metrics),
            "dimension_hints": self._compact_context(dimensions),
            "table_hints": self._compact_context(tables),
            "lineage_hints": self._compact_context(lineage),
        }

    def _compact_context(self, items: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return [
            {
                "source_id": item["source_id"],
                "business_name": item["business_name"],
                "document_type": item["document_type"],
                "score": item["score"],
                "table_name": item.get("table_name"),
                "column_name": item.get("column_name"),
            }
            for item in items
        ]

    def _is_definition_question(self, normalized: str) -> bool:
        if self._contains_any(normalized, DEFINITION_TERMS):
            return not self._has_grouping_or_result_shape(normalized)
        return False

    def _is_metadata_question(self, normalized: str) -> bool:
        if self._is_table_discovery_question(normalized):
            return False
        return self._contains_any(normalized, METADATA_TERMS)

    def _is_table_discovery_question(self, normalized: str) -> bool:
        return self._contains_any(normalized, TABLE_DISCOVERY_TERMS)

    def _is_lineage_question(self, normalized: str) -> bool:
        if "where is" in normalized or "where are" in normalized:
            return False
        return self._contains_any(normalized, LINEAGE_TERMS)

    def _is_analytical_query(self, normalized: str, retrieval_context: list[dict[str, Any]]) -> bool:
        if self._contains_any(normalized, ANALYTICAL_TERMS) and self._has_metric_or_result_shape(normalized, retrieval_context):
            return True
        if self._has_grouping_or_result_shape(normalized) and retrieval_context:
            return True
        return False

    def _has_metric_or_result_shape(self, normalized: str, retrieval_context: list[dict[str, Any]]) -> bool:
        if any(item["document_type"] == "metric" for item in retrieval_context):
            return True
        return self._contains_any(
            normalized,
            ["balance", "deposit", "loan", "utilization", "delinquency", "profit", "income", "exposure", "amount"],
        )

    def _has_grouping_or_result_shape(self, normalized: str) -> bool:
        return self._contains_any(
            normalized,
            [" by ", " over time", " trend", " compare", " top ", " bottom ", " highest", " lowest", " rank"],
        )

    def _looks_like_clarification(self, normalized: str) -> bool:
        word_count = len(normalized.split())
        if re.fullmatch(r"(option\s*)?[1-9]", normalized):
            return True
        if normalized.startswith(("use ", "choose ", "select ", "pick ", "go with ")):
            return True
        if word_count <= 6 and self._contains_any(
            normalized,
            ["deposit", "ledger", "collected", "loan", "customer", "product", "risk", "segment", "metric", "dimension"],
        ):
            return True
        return False

    def _extract_chart_type(self, normalized: str) -> str | None:
        if "line" in normalized:
            return "line"
        if "bar" in normalized:
            return "bar"
        if "pie" in normalized:
            return "pie"
        if "donut" in normalized:
            return "donut"
        if self._contains_any(normalized, ["plot", "chart", "graph", "visualize", "visualise"]):
            return "auto"
        return None

    def _confidence(self, base: float, retrieval_context: list[dict[str, Any]]) -> float:
        if not retrieval_context:
            return base - 0.12
        top_score = retrieval_context[0]["score"]
        return base + min(top_score, 0.20)

    def _normalize(self, message: str) -> str:
        return re.sub(r"\s+", " ", message.strip().lower())

    def _contains_any(self, normalized: str, terms: list[str]) -> bool:
        return any(term in normalized for term in terms)


intent_classifier = IntentClassifier()
