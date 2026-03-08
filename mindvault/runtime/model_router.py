"""Model routing: maps agent task names to LLM provider configs."""
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Optional
import json

from mindvault.runtime.llm_client import LLMClient, LLMProviderConfig


class ModelRouter:
    """Reads config/model_config.json and returns per-task LLM clients."""

    def __init__(self, config_path: str = "config/model_config.json") -> None:
        path = Path(config_path)
        self.config: Dict[str, Any] = (
            json.loads(path.read_text(encoding="utf-8")) if path.exists() else {"providers": {}, "routing": {}}
        )

    def client_for_task(self, task_name: str) -> Optional[LLMClient]:
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
                api_key=provider.get("api_key", ""),
            )
        )

    def describe(self) -> Dict[str, Any]:
        return self.config
