"""Unified OpenAI-compatible LLM client with retry and fallback."""
from __future__ import annotations

import json
import os
import time
import urllib.request
from dataclasses import dataclass
from typing import Any, Dict, Optional


@dataclass
class LLMProviderConfig:
    name: str
    base_url: str
    api_key_env: str
    model: str
    api_key: str = ""
    timeout_seconds: int = 120
    max_retries: int = 2
    retry_backoff_seconds: float = 1.0
    response_format_json: bool = False
    max_output_tokens: int = 0


class LLMClient:
    """Calls an OpenAI-compatible endpoint, including Responses API for newer GPT-5 models."""

    def __init__(self, config: LLMProviderConfig) -> None:
        self.config = config

    def chat(
        self,
        prompt: str,
        system_prompt: str = "",
        temperature: float = 0.2,
        max_retries: Optional[int] = None,
        timeout_seconds: Optional[int] = None,
        max_output_tokens: Optional[int] = None,
    ) -> Dict[str, Any]:
        api_key = self.config.api_key or os.getenv(self.config.api_key_env, "")
        protocol = self._protocol_for_model()
        output_limit = self.config.max_output_tokens if max_output_tokens is None else max_output_tokens
        url, data = self._build_request(protocol, prompt, system_prompt, temperature, output_limit)

        retries = self.config.max_retries if max_retries is None else max_retries
        timeout = self.config.timeout_seconds if timeout_seconds is None else timeout_seconds

        last_error: Optional[Exception] = None
        for attempt in range(retries + 1):
            try:
                req = urllib.request.Request(url, data=json.dumps(data).encode("utf-8"))
                req.add_header("Content-Type", "application/json")
                if api_key:
                    req.add_header("Authorization", f"Bearer {api_key}")

                with urllib.request.urlopen(req, timeout=timeout) as response:
                    result = json.loads(response.read().decode("utf-8"))
                    content = self._extract_content(protocol, result)
                    return {
                        "provider": self.config.name,
                        "model": self.config.model,
                        "api_key_set": bool(api_key),
                        "content": content,
                    }
            except Exception as e:
                last_error = e
                if attempt < retries and self.config.retry_backoff_seconds > 0:
                    time.sleep(self.config.retry_backoff_seconds * (attempt + 1))

        # All retries exhausted — return fallback
        return {
            "provider": self.config.name,
            "model": self.config.model,
            "api_key_set": bool(api_key),
            "error": str(last_error),
            "content": f"[llm-fallback] {str(last_error)}",
        }

    def _protocol_for_model(self) -> str:
        model = (self.config.model or "").lower()
        if model.startswith("gpt-5"):
            return "responses"
        return "chat_completions"

    def _build_request(
        self,
        protocol: str,
        prompt: str,
        system_prompt: str,
        temperature: float,
        max_output_tokens: int = 0,
    ) -> tuple[str, Dict[str, Any]]:
        base = self.config.base_url.rstrip("/")
        if protocol == "responses":
            input_items = []
            if system_prompt:
                input_items.append(
                    {
                        "role": "system",
                        "content": [{"type": "input_text", "text": system_prompt}],
                    }
                )
            input_items.append(
                {
                    "role": "user",
                    "content": [{"type": "input_text", "text": prompt}],
                }
            )
            data: Dict[str, Any] = {
                "model": self.config.model,
                "input": input_items,
                "temperature": temperature,
                "text": {"format": {"type": "json_object"}},
            }
            if max_output_tokens and max_output_tokens > 0:
                data["max_output_tokens"] = max_output_tokens
            url = base if base.endswith("/responses") else f"{base}/responses"
            return url, data

        messages = []
        if system_prompt:
            messages.append({"role": "system", "content": system_prompt})
        messages.append({"role": "user", "content": prompt})
        data = {
            "model": self.config.model,
            "messages": messages,
            "temperature": temperature,
        }
        if self.config.response_format_json or "gpt" in self.config.model.lower():
            data["response_format"] = {"type": "json_object"}
        if max_output_tokens and max_output_tokens > 0:
            data["max_tokens"] = max_output_tokens
        url = base if base.endswith("/chat/completions") else f"{base}/chat/completions"
        return url, data

    @staticmethod
    def _extract_content(protocol: str, result: Dict[str, Any]) -> str:
        if protocol == "responses":
            if isinstance(result.get("output_text"), str) and result.get("output_text"):
                return result["output_text"]
            output = result.get("output", [])
            for item in output:
                for content in item.get("content", []):
                    text = content.get("text")
                    if text:
                        return text
            return ""
        return result.get("choices", [{}])[0].get("message", {}).get("content", "")
