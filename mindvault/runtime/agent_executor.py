"""Agent executor: reads YAML agent definitions, calls LLM, validates output."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Optional

import yaml

from mindvault.runtime.model_router import ModelRouter
from mindvault.runtime.trace_logger import TraceLogger


class AgentExecutor:
    """
    Generic executor that:
    1. Reads a YAML agent definition
    2. Loads the prompt template
    3. Constructs the LLM request with context
    4. Calls the model via ModelRouter
    5. Parses and returns structured JSON
    6. Logs trace
    """

    def __init__(self, model_router: ModelRouter, trace_logger: TraceLogger) -> None:
        self.router = model_router
        self.trace = trace_logger
        self._agent_cache: Dict[str, Dict[str, Any]] = {}

    def load_agent(self, agent_path: str | Path) -> Dict[str, Any]:
        """Load and cache a YAML agent definition."""
        key = str(agent_path)
        if key not in self._agent_cache:
            path = Path(agent_path)
            if not path.exists():
                raise FileNotFoundError(f"Agent definition not found: {agent_path}")
            with open(path, "r", encoding="utf-8") as f:
                self._agent_cache[key] = yaml.safe_load(f)
        return self._agent_cache[key]

    def execute(self, agent_path: str | Path, context: Dict[str, Any]) -> Dict[str, Any]:
        """Execute an agent: load definition → build prompt → call LLM → parse result."""
        agent_def = self.load_agent(agent_path)
        agent_name = agent_def.get("name", "unknown_agent")
        model_route = agent_def.get("model_route", "parse")
        prompt_path = agent_def.get("prompt_template", "")

        # Load prompt template
        prompt_template = ""
        if prompt_path and Path(prompt_path).exists():
            prompt_template = Path(prompt_path).read_text(encoding="utf-8")

        # Build the user prompt by injecting context
        user_prompt = self._build_prompt(prompt_template, context)

        # Get LLM client
        client = self.router.client_for_task(model_route)
        if client is None:
            self.trace.log("agent_skipped", agent=agent_name, reason="no_model_route")
            return {"error": f"No model route for '{model_route}'", "content": ""}

        # Call LLM
        role_prompt = agent_def.get("role", "You are a helpful assistant.")
        result = client.chat(user_prompt, system_prompt=role_prompt)

        # Try to parse JSON from content
        content = result.get("content", "")
        parsed = self._try_parse_json(content)

        self.trace.log(
            "agent_executed",
            agent=agent_name,
            task_type=model_route,
            has_error="error" in result,
            output_keys=list(parsed.keys()) if isinstance(parsed, dict) else [],
        )

        return parsed if isinstance(parsed, dict) else {"raw_content": content}

    @staticmethod
    def _build_prompt(template: str, context: Dict[str, Any]) -> str:
        """Simple variable substitution: {{key}} → context[key] as JSON."""
        result = template
        for key, value in context.items():
            placeholder = "{{" + key + "}}"
            if placeholder in result:
                if isinstance(value, (dict, list)):
                    result = result.replace(placeholder, json.dumps(value, ensure_ascii=False, indent=2))
                else:
                    result = result.replace(placeholder, str(value))
        return result

    @staticmethod
    def _try_parse_json(content: str) -> Any:
        """Attempt to extract JSON from LLM response."""
        content = content.strip()
        # Try direct parse
        try:
            return json.loads(content)
        except json.JSONDecodeError:
            pass
        # Try extracting from markdown code block
        if "```json" in content:
            start = content.index("```json") + 7
            end = content.index("```", start)
            try:
                return json.loads(content[start:end].strip())
            except (json.JSONDecodeError, ValueError):
                pass
        if "```" in content:
            start = content.index("```") + 3
            end = content.index("```", start)
            try:
                return json.loads(content[start:end].strip())
            except (json.JSONDecodeError, ValueError):
                pass
        
        print(f"DEBUG: LLM returned unparseable content:\n---START---\n{content}\n---END---")
        return content
