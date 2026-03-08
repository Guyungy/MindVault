"""Agent executor: reads YAML agent definitions, calls LLM, validates output."""
from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Dict, List, Tuple

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
        """Load and cache a YAML-like agent definition."""
        key = str(agent_path)
        if key not in self._agent_cache:
            path = Path(agent_path)
            if not path.exists():
                raise FileNotFoundError(f"Agent definition not found: {agent_path}")
            self._agent_cache[key] = self._parse_yaml_like(path.read_text(encoding="utf-8"))
        return self._agent_cache[key]

    def execute(self, agent_path: str | Path, context: Dict[str, Any]) -> Dict[str, Any]:
        """Execute an agent: load definition → build prompt → call LLM → parse result."""
        agent_def = self.load_agent(agent_path)
        agent_name = agent_def.get("name", "unknown_agent")
        model_route = agent_def.get("model_route", "parse")
        prompt_path = agent_def.get("prompt_template", "")

        prompt_template = ""
        if prompt_path and Path(prompt_path).exists():
            prompt_template = Path(prompt_path).read_text(encoding="utf-8")

        user_prompt = self._build_prompt(prompt_template, context)

        client = self.router.client_for_task(model_route)
        if client is None:
            self.trace.log("agent_skipped", agent=agent_name, reason="no_model_route")
            return {"error": f"No model route for '{model_route}'", "content": ""}

        retry_policy = agent_def.get("retry_policy", {}) or {}
        max_retries = int(retry_policy.get("max_retries", client.config.max_retries))

        role_prompt = agent_def.get("role", "You are a helpful assistant.")
        result = client.chat(user_prompt, system_prompt=role_prompt, max_retries=max_retries)

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
        text = (content or "").strip()
        if not text:
            return text

        try:
            return json.loads(text)
        except json.JSONDecodeError:
            pass

        code_block_pattern = re.compile(r"```(?:json|JSON)?\s*(.*?)\s*```", re.DOTALL)
        for block in code_block_pattern.findall(text):
            candidate = block.strip()
            if not candidate:
                continue
            try:
                return json.loads(candidate)
            except json.JSONDecodeError:
                continue

        for opener, closer in (("{", "}"), ("[", "]")):
            start = text.find(opener)
            if start == -1:
                continue
            depth = 0
            for idx in range(start, len(text)):
                ch = text[idx]
                if ch == opener:
                    depth += 1
                elif ch == closer:
                    depth -= 1
                    if depth == 0:
                        snippet = text[start:idx + 1]
                        try:
                            return json.loads(snippet)
                        except json.JSONDecodeError:
                            break

        return text

    @staticmethod
    def _parse_scalar(raw: str) -> Any:
        value = raw.strip()
        if value == "":
            return ""
        if value in {"true", "True"}:
            return True
        if value in {"false", "False"}:
            return False
        if value in {"null", "None", "~"}:
            return None
        if (value.startswith('"') and value.endswith('"')) or (value.startswith("'") and value.endswith("'")):
            return value[1:-1]
        if re.fullmatch(r"-?\d+", value):
            return int(value)
        if re.fullmatch(r"-?\d+\.\d+", value):
            return float(value)
        return value

    @classmethod
    def _collect_block_scalar(cls, lines: List[str], start_idx: int, base_indent: int) -> Tuple[str, int]:
        idx = start_idx
        collected: List[str] = []
        while idx < len(lines):
            line = lines[idx]
            if not line.strip():
                collected.append("")
                idx += 1
                continue
            indent = len(line) - len(line.lstrip(" "))
            if indent <= base_indent:
                break
            collected.append(line[indent:])
            idx += 1
        return " ".join(part.strip() for part in collected if part.strip()), idx

    @classmethod
    def _parse_yaml_like(cls, text: str) -> Dict[str, Any]:
        lines = text.splitlines()
        root: Dict[str, Any] = {}
        stack: List[Tuple[int, Any]] = [(-1, root)]
        i = 0

        while i < len(lines):
            raw_line = lines[i]
            if not raw_line.strip() or raw_line.lstrip().startswith("#"):
                i += 1
                continue

            indent = len(raw_line) - len(raw_line.lstrip(" "))
            line = raw_line.strip()

            while len(stack) > 1 and indent <= stack[-1][0]:
                stack.pop()
            parent = stack[-1][1]

            if line.startswith("- "):
                item_val = cls._parse_scalar(line[2:])
                if isinstance(parent, list):
                    parent.append(item_val)
                i += 1
                continue

            if ":" not in line:
                i += 1
                continue

            key, value = line.split(":", 1)
            key = key.strip()
            value = value.strip()

            if value == ">":
                block_text, next_idx = cls._collect_block_scalar(lines, i + 1, indent)
                if isinstance(parent, dict):
                    parent[key] = block_text
                i = next_idx
                continue

            if value == "":
                next_non_empty = ""
                j = i + 1
                while j < len(lines):
                    probe = lines[j].strip()
                    if probe:
                        next_non_empty = probe
                        break
                    j += 1
                container: Any = [] if next_non_empty.startswith("- ") else {}
                if isinstance(parent, dict):
                    parent[key] = container
                stack.append((indent, container))
                i += 1
                continue

            if isinstance(parent, dict):
                parent[key] = cls._parse_scalar(value)
            i += 1

        return root
