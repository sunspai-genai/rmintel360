from __future__ import annotations

import re
from typing import Any

from backend.app.catalog.service import catalog_service


class MetadataRetriever:
    """Retrieve governed context the LLM may use for semantic and SQL decisions."""

    def retrieve(self, message: str, limit: int = 10) -> dict[str, Any]:
        search_results = catalog_service.search_metadata(message, limit=limit, min_score=0.05)
        candidate_groups = self._candidate_groups(message)
        citations = self._citations(search_results=search_results, candidate_groups=candidate_groups)

        return {
            "query": message,
            "search_results": search_results,
            "candidate_groups": candidate_groups,
            "citations": citations,
            "business_terms": [item for item in search_results if item["document_type"] == "business_term"],
            "metrics": [item for item in search_results if item["document_type"] == "metric"],
            "dimensions": [item for item in search_results if item["document_type"] == "dimension"],
            "tables": [item for item in search_results if item["document_type"] == "table"],
            "columns": [item for item in search_results if item["document_type"] == "column"],
            "lineage": [item for item in search_results if item["document_type"] == "lineage"],
        }

    def _candidate_groups(self, message: str) -> list[dict[str, Any]]:
        normalized = f" {self._normalize(message)} "
        groups = []
        seen: set[tuple[str, str]] = set()
        for synonym in catalog_service.list_synonyms():
            phrase = self._normalize(synonym["phrase"])
            if f" {phrase} " not in normalized:
                continue
            key = (phrase, synonym["target_type"])
            if key in seen:
                continue
            seen.add(key)
            candidates = catalog_service.search_governed_candidates(
                phrase=phrase,
                target_type=synonym["target_type"],
                exact_match=True,
            )
            if candidates:
                groups.append(
                    {
                        "phrase": phrase,
                        "target_type": synonym["target_type"],
                        "candidates": candidates,
                    }
                )
        return groups

    def _normalize(self, value: str) -> str:
        return re.sub(r"[^a-z0-9]+", " ", value.lower()).strip()

    def _citations(
        self,
        search_results: list[dict[str, Any]],
        candidate_groups: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        citations: list[dict[str, Any]] = []
        seen: set[str] = set()

        for item in search_results[:8]:
            key = f"{item['document_type']}:{item['source_id']}"
            if key in seen:
                continue
            seen.add(key)
            citations.append(
                {
                    "citation_id": key,
                    "source_type": item["document_type"],
                    "source_id": item["source_id"],
                    "business_name": item.get("business_name"),
                    "table_name": item.get("table_name"),
                    "column_name": item.get("column_name"),
                    "score": item.get("score"),
                }
            )

        for group in candidate_groups:
            for candidate in group["candidates"]:
                key = f"{candidate['target_type']}:{candidate['target_id']}"
                if key in seen:
                    continue
                seen.add(key)
                citations.append(
                    {
                        "citation_id": key,
                        "source_type": candidate["target_type"],
                        "source_id": candidate["target_id"],
                        "business_name": candidate.get("business_name"),
                        "table_name": candidate.get("table_name"),
                        "column_name": candidate.get("column_name"),
                        "score": candidate.get("confidence"),
                    }
                )

        return citations


metadata_retriever = MetadataRetriever()
