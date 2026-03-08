"""Unified OpenAI-compatible LLM client wrapper."""
from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Dict, Any


@dataclass
class LLMProviderConfig:
    name: str
    base_url: str
    api_key_env: str
    model: str
    api_key: str = ""


class LLMClient:
    def __init__(self, config: LLMProviderConfig) -> None:
        self.config = config

    def chat(self, prompt: str) -> Dict[str, Any]:
        api_key = self.config.api_key or os.getenv(self.config.api_key_env, "")
        import urllib.request
        import urllib.error
        import json

        data = {
            "model": self.config.model,
            "messages": [
                {"role": "system", "content": "You are a robust structured data extractor. Always output valid JSON."},
                {"role": "user", "content": prompt}
            ],
            "response_format": {"type": "json_object"} if "gpt" in self.config.model else None,
            "temperature": 0.2
        }

        # Handle openai compatibility
        url = self.config.base_url
        if not url.endswith("/chat/completions"):
            url = url.rstrip("/") + "/chat/completions"

        req = urllib.request.Request(url, data=json.dumps(data).encode("utf-8"))
        req.add_header("Content-Type", "application/json")
        if api_key:
            req.add_header("Authorization", f"Bearer {api_key}")

        try:
            with urllib.request.urlopen(req, timeout=30) as response:
                result = json.loads(response.read().decode("utf-8"))
                content = result.get("choices", [{}])[0].get("message", {}).get("content", "")
                return {
                    "provider": self.config.name,
                    "model": self.config.model,
                    "base_url": self.config.base_url,
                    "api_key_set": bool(api_key),
                    "content": content,
                }
        except Exception as e:
            return {
                "provider": self.config.name,
                "model": self.config.model,
                "base_url": self.config.base_url,
                "api_key_set": bool(api_key),
                "error": str(e),
                "content": f"[mocked-response-fallback] fallback due to {str(e)}",
            }
