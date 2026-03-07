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


class LLMClient:
    def __init__(self, config: LLMProviderConfig) -> None:
        self.config = config

    def chat(self, prompt: str) -> Dict[str, Any]:
        api_key = os.getenv(self.config.api_key_env, "")
        # Placeholder implementation for offline mode; keeps API boundary centralized.
        return {
            "provider": self.config.name,
            "model": self.config.model,
            "base_url": self.config.base_url,
            "api_key_set": bool(api_key),
            "content": f"[mocked-response] {prompt[:120]}",
        }
