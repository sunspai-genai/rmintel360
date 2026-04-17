from __future__ import annotations

import json
from typing import Any, Callable

from langchain_core.prompts import PromptTemplate

from backend.app.core.config import get_settings

try:  # pragma: no cover - exercised only when AWS dependencies are installed/configured.
    import boto3
except ModuleNotFoundError:  # pragma: no cover
    boto3 = None


class BedrockTitanClient:
    """Small Bedrock Titan JSON client with a local fallback for offline development."""

    def __init__(self) -> None:
        self.settings = get_settings()

    def invoke_json(
        self,
        *,
        task_name: str,
        system_prompt: str,
        input_payload: dict[str, Any],
        fallback: Callable[[], dict[str, Any]],
    ) -> dict[str, Any]:
        if not self.settings.bedrock_enabled:
            payload = fallback()
            payload.setdefault("llm_provider", "local_titan_fallback")
            payload.setdefault("llm_task", task_name)
            return payload

        if boto3 is None:
            payload = fallback()
            payload.setdefault("llm_provider", "local_titan_fallback_missing_boto3")
            payload.setdefault("llm_task", task_name)
            return payload

        try:
            text = self._invoke_bedrock(system_prompt=system_prompt, input_payload=input_payload)
            parsed = self._parse_json(text)
            parsed.setdefault("llm_provider", "aws_bedrock_titan")
            parsed.setdefault("llm_task", task_name)
            return parsed
        except Exception as exc:  # pragma: no cover - requires AWS runtime failure.
            payload = fallback()
            payload.setdefault("llm_provider", "local_titan_fallback_after_error")
            payload.setdefault("llm_task", task_name)
            payload.setdefault("llm_error", str(exc))
            return payload

    def _invoke_bedrock(self, system_prompt: str, input_payload: dict[str, Any]) -> str:
        session_kwargs = {}
        if self.settings.aws_profile:
            session_kwargs["profile_name"] = self.settings.aws_profile
        session = boto3.Session(**session_kwargs)
        client = session.client("bedrock-runtime", region_name=self.settings.aws_region)

        prompt = PromptTemplate.from_template(
            "{system_prompt}\n\nReturn only valid JSON.\n\nINPUT:\n{input_json}"
        ).format(
            system_prompt=system_prompt,
            input_json=json.dumps(input_payload, indent=2, default=str),
        )

        response = client.converse(
            modelId=self.settings.bedrock_model_id,
            messages=[{"role": "user", "content": [{"text": prompt}]}],
            inferenceConfig={
                "maxTokens": self.settings.bedrock_max_tokens,
                "temperature": self.settings.bedrock_temperature,
                "topP": 0.9,
            },
        )
        return response["output"]["message"]["content"][0]["text"]

    def _parse_json(self, text: str) -> dict[str, Any]:
        stripped = text.strip()
        if stripped.startswith("```"):
            stripped = stripped.strip("`")
            stripped = stripped.removeprefix("json").strip()
        start = stripped.find("{")
        end = stripped.rfind("}")
        if start >= 0 and end >= start:
            stripped = stripped[start : end + 1]
        return json.loads(stripped)


llm_client = BedrockTitanClient()
