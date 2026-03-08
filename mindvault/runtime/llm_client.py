"""Unified OpenAI-compatible LLM client with retry and fallback."""
from __future__ import annotations

import json
import os
import urllib.request
import urllib.error
from dataclasses import dataclass
from typing import Any, Dict, Optional


@dataclass
class LLMProviderConfig:
    name: str
    base_url: str
    api_key_env: str
    model: str
    api_key: str = ""


class LLMClient:
    """Calls an OpenAI-compatible chat/completions endpoint."""

    def __init__(self, config: LLMProviderConfig) -> None:
        self.config = config

    def chat(self, prompt: str, system_prompt: str = "", temperature: float = 0.2, max_retries: int = 0) -> Dict[str, Any]:
        api_key = self.config.api_key or os.getenv(self.config.api_key_env, "")

        messages = []
        if system_prompt:
            messages.append({"role": "system", "content": system_prompt})
        messages.append({"role": "user", "content": prompt})

        data: Dict[str, Any] = {
            "model": self.config.model,
            "messages": messages,
            "temperature": temperature,
        }
        if "gpt" in self.config.model.lower():
            data["response_format"] = {"type": "json_object"}

        url = self.config.base_url
        if not url.endswith("/chat/completions"):
            url = url.rstrip("/") + "/chat/completions"

        last_error: Optional[Exception] = None
        for attempt in range(max_retries + 1):
            try:
                req = urllib.request.Request(url, data=json.dumps(data).encode("utf-8"))
                req.add_header("Content-Type", "application/json")
                if api_key:
                    req.add_header("Authorization", f"Bearer {api_key}")

                with urllib.request.urlopen(req, timeout=120) as response:
                    result = json.loads(response.read().decode("utf-8"))
                    content = result.get("choices", [{}])[0].get("message", {}).get("content", "")
                    return {
                        "provider": self.config.name,
                        "model": self.config.model,
                        "api_key_set": bool(api_key),
                        "content": content,
                    }
            except Exception as e:
                last_error = e

        # All retries exhausted — return fallback
        return {
            "provider": self.config.name,
            "model": self.config.model,
            "api_key_set": bool(api_key),
            "error": str(last_error),
            "content": f"[llm-fallback] {str(last_error)}",
        }
