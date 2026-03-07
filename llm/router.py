"""Task to model routing for MindVault agents."""
from __future__ import annotations

from pathlib import Path
from typing import Dict, Any
import json

from llm.client import LLMClient, LLMProviderConfig


class LLMRouter:
    def __init__(self, config_path: str = "config/model_config.json") -> None:
        path = Path(config_path)
        self.config = json.loads(path.read_text(encoding="utf-8")) if path.exists() else {"providers": {}, "routing": {}}

    def client_for_task(self, task_name: str) -> LLMClient | None:
        provider_name = self.config.get("routing", {}).get(task_name)
        if not provider_name:
            return None
        provider = self.config.get("providers", {}).get(provider_name)
        if not provider:
            return None
        return LLMClient(
            LLMProviderConfig(
                name=provider_name,
                base_url=provider.get("base_url", ""),
                api_key_env=provider.get("api_key_env", "OPENAI_API_KEY"),
                model=provider.get("model", "gpt-4o-mini"),
            )
        )

    def describe(self) -> Dict[str, Any]:
        return self.config
