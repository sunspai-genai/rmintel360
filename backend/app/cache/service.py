from __future__ import annotations

import hashlib
import json
import re
import time
from typing import Any

from backend.app.core.config import get_settings
from backend.app.db.connection import fetch_all

try:  # pragma: no cover - exercised when Redis is installed/configured.
    import redis
except ModuleNotFoundError:  # pragma: no cover
    redis = None


class AssistantResponseCache:
    """Cache complete assistant responses with Redis and a local fallback."""

    def __init__(self) -> None:
        self.settings = get_settings()
        self._redis_client: Any | None = None
        self._local_cache: dict[str, tuple[float, dict[str, Any]]] = {}

    def build_key(
        self,
        *,
        message: str,
        conversation_id: str | None,
        user_role: str,
        limit: int,
        execute_sql: bool,
        selected_metric_id: str | None,
        selected_dimension_ids: list[str],
    ) -> str:
        payload = {
            "cache_scope_version": "conversation-scoped-v2",
            "conversation_id": conversation_id or "no-conversation",
            "message": self._normalize(message),
            "user_role": user_role,
            "limit": limit,
            "execute_sql": execute_sql,
            "selected_metric_id": selected_metric_id,
            "selected_dimension_ids": sorted(selected_dimension_ids),
            "app_version": self.settings.app_version,
            "bedrock_model_id": self.settings.bedrock_model_id,
            "metadata_version": self._metadata_version(),
        }
        digest = hashlib.sha256(json.dumps(payload, sort_keys=True, default=str).encode("utf-8")).hexdigest()
        return f"banking-assistant:response:{digest}"

    def build_resolved_plan_key(
        self,
        *,
        message: str,
        conversation_id: str | None,
        user_role: str,
        limit: int,
        execute_sql: bool,
        intent: str | None,
        response_mode: str | None,
        metric_id: str | None,
        dimension_ids: list[str],
        filters: list[dict[str, Any]],
        chart_requested: bool,
        chart_type: str | None,
        generated_sql: str | None,
    ) -> str:
        payload = {
            "cache_scope_version": "resolved-governed-plan-v1",
            "conversation_id": conversation_id or "no-conversation",
            "message": self._normalize(message),
            "user_role": user_role,
            "limit": limit,
            "execute_sql": execute_sql,
            "intent": intent,
            "response_mode": response_mode,
            "metric_id": metric_id,
            "dimension_ids": sorted(dimension_ids),
            "filters": filters,
            "chart_requested": chart_requested,
            "chart_type": chart_type,
            "generated_sql": generated_sql,
            "app_version": self.settings.app_version,
            "bedrock_model_id": self.settings.bedrock_model_id,
            "metadata_version": self._metadata_version(),
        }
        digest = hashlib.sha256(json.dumps(payload, sort_keys=True, default=str).encode("utf-8")).hexdigest()
        return f"banking-assistant:resolved-response:{digest}"

    def get(self, key: str) -> dict[str, Any] | None:
        cached = self._get_redis(key)
        if cached is not None:
            cached["cache"] = {"status": "hit", "backend": "redis", "key": key}
            return cached

        if not self.settings.local_cache_fallback_enabled:
            return None

        local = self._local_cache.get(key)
        if not local:
            return None
        expires_at, payload = local
        if expires_at < time.time():
            self._local_cache.pop(key, None)
            return None
        cached = dict(payload)
        cached["cache"] = {"status": "hit", "backend": "local_memory", "key": key}
        return cached

    def set(self, key: str, payload: dict[str, Any]) -> None:
        ttl = max(1, int(self.settings.redis_cache_ttl_seconds))
        serializable = self._cache_payload(payload)
        self._set_redis(key=key, payload=serializable, ttl=ttl)

        if self.settings.local_cache_fallback_enabled:
            self._local_cache[key] = (time.time() + ttl, serializable)

    def clear(self) -> None:
        self._local_cache.clear()
        client = self._redis()
        if client is None:
            return
        try:
            for key in client.scan_iter(match="banking-assistant:response:*"):
                client.delete(key)
            for key in client.scan_iter(match="banking-assistant:resolved-response:*"):
                client.delete(key)
        except Exception:
            return

    def is_cacheable_message(self, message: str) -> bool:
        normalized = self._normalize(message)
        return len(normalized.split()) > 2 and normalized not in {"yes", "no"}

    def _get_redis(self, key: str) -> dict[str, Any] | None:
        client = self._redis()
        if client is None:
            return None
        try:
            raw = client.get(key)
        except Exception:
            return None
        if not raw:
            return None
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            return None

    def _set_redis(self, key: str, payload: dict[str, Any], ttl: int) -> None:
        client = self._redis()
        if client is None:
            return
        try:
            client.setex(key, ttl, json.dumps(payload, default=str))
        except Exception:
            return

    def _redis(self) -> Any | None:
        if not self.settings.redis_enabled or redis is None:
            return None
        if self._redis_client is not None:
            return self._redis_client
        try:
            client = redis.Redis.from_url(self.settings.redis_url, decode_responses=True)
            client.ping()
        except Exception:
            return None
        self._redis_client = client
        return client

    def _cache_payload(self, payload: dict[str, Any]) -> dict[str, Any]:
        cached = dict(payload)
        cached.pop("conversation_id", None)
        cached.pop("turn_id", None)
        cached.pop("audit_report", None)
        return cached

    def _metadata_version(self) -> str:
        try:
            rows = fetch_all(
                """
                SELECT
                    (SELECT COUNT(*) FROM metadata_table) AS table_count,
                    (SELECT COUNT(*) FROM metadata_column) AS column_count,
                    (SELECT COUNT(*) FROM metadata_metric) AS metric_count,
                    (SELECT COUNT(*) FROM metadata_dimension) AS dimension_count,
                    (SELECT COUNT(*) FROM metadata_synonym) AS synonym_count,
                    (SELECT COUNT(*) FROM metadata_search_document) AS search_count
                """
            )
        except Exception:
            return "missing"
        return json.dumps(rows[0] if rows else {}, sort_keys=True, default=str)

    def _normalize(self, message: str) -> str:
        return re.sub(r"[^a-z0-9]+", " ", message.lower()).strip()


assistant_response_cache = AssistantResponseCache()
