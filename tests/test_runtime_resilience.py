import json
import tempfile
import unittest
from pathlib import Path

from mindvault.runtime.agent_executor import AgentExecutor
from mindvault.runtime.llm_client import LLMClient, LLMProviderConfig
from mindvault.runtime.model_router import ModelRouter
from mindvault.runtime.trace_logger import TraceLogger


class _FakeClientConfig:
    def __init__(self, max_retries: int = 2):
        self.max_retries = max_retries


class _FakeClient:
    def __init__(self):
        self.config = _FakeClientConfig(max_retries=1)
        self.captured_max_retries = None

    def chat(self, prompt, system_prompt="", max_retries=None, **kwargs):
        self.captured_max_retries = max_retries
        return {"content": '{"claims": [], "entity_candidates": []}'}


class _FakeErrorClient:
    def __init__(self):
        self.config = _FakeClientConfig(max_retries=1)

    def chat(self, prompt, system_prompt="", max_retries=None, **kwargs):
        return {"content": "[llm-fallback] network error", "error": "network error"}


class _FakeRouter:
    def __init__(self, client):
        self._client = client

    def client_for_task(self, task_name):
        return self._client


class RuntimeResilienceTests(unittest.TestCase):
    def test_agent_executor_parses_json_in_mixed_response(self):
        payload = "说明文字\n```text\nnot json\n```\n```json\n{\"claims\": [], \"entity_candidates\": []}\n```"
        parsed = AgentExecutor._try_parse_json(payload)
        self.assertIsInstance(parsed, dict)
        self.assertIn("claims", parsed)

    def test_agent_executor_parses_balanced_json_slice(self):
        payload = "Result: {\"claims\": [], \"entity_candidates\": []} trailing"
        parsed = AgentExecutor._try_parse_json(payload)
        self.assertIsInstance(parsed, dict)
        self.assertIn("entity_candidates", parsed)

    def test_agent_retry_policy_is_forwarded_to_client(self):
        fake_client = _FakeClient()
        executor = AgentExecutor(_FakeRouter(fake_client), TraceLogger())

        with tempfile.TemporaryDirectory() as td:
            td_path = Path(td)
            prompt_file = td_path / "prompt.md"
            prompt_file.write_text("{{chunk_text}}", encoding="utf-8")
            agent_file = td_path / "agent.yaml"
            agent_file.write_text(
                "\n".join(
                    [
                        "name: parse_agent",
                        "model_route: parse",
                        f"prompt_template: {prompt_file}",
                        "retry_policy:",
                        "  max_retries: 4",
                    ]
                ),
                encoding="utf-8",
            )

            result = executor.execute(agent_file, {"chunk_text": "hello"})

        self.assertEqual(fake_client.captured_max_retries, 4)
        self.assertIn("claims", result)

    def test_model_router_reads_retry_timeout_config(self):
        with tempfile.TemporaryDirectory() as td:
            config_path = Path(td) / "model_config.json"
            config_path.write_text(
                json.dumps(
                    {
                        "providers": {
                            "p1": {
                                "base_url": "http://example.com/v1",
                                "api_key_env": "OPENAI_API_KEY",
                                "model": "gpt-5.2",
                                "timeout_seconds": 33,
                                "max_retries": 5,
                                "retry_backoff_seconds": 0.2,
                                "response_format_json": True,
                            }
                        },
                        "routing": {"parse": "p1"},
                    }
                ),
                encoding="utf-8",
            )
            router = ModelRouter(str(config_path))
            client = router.client_for_task("parse")

        self.assertIsNotNone(client)
        self.assertEqual(client.config.timeout_seconds, 33)
        self.assertEqual(client.config.max_retries, 5)
        self.assertAlmostEqual(client.config.retry_backoff_seconds, 0.2)
        self.assertTrue(client.config.response_format_json)

    def test_agent_executor_surfaces_llm_error_in_result(self):
        executor = AgentExecutor(_FakeRouter(_FakeErrorClient()), TraceLogger())

        with tempfile.TemporaryDirectory() as td:
            td_path = Path(td)
            prompt_file = td_path / "prompt.md"
            prompt_file.write_text("{{chunk_text}}", encoding="utf-8")
            agent_file = td_path / "agent.yaml"
            agent_file.write_text(
                "\n".join(
                    [
                        "name: parse_agent",
                        "model_route: parse",
                        f"prompt_template: {prompt_file}",
                    ]
                ),
                encoding="utf-8",
            )

            result = executor.execute(agent_file, {"chunk_text": "hello"})

        self.assertEqual(result.get("_agent_error"), "network error")

    def test_llm_client_uses_responses_api_for_gpt5_models(self):
        client = LLMClient(
            LLMProviderConfig(
                name="p1",
                base_url="https://example.com/v1",
                api_key_env="OPENAI_API_KEY",
                model="gpt-5.2",
            )
        )
        url, payload = client._build_request("responses", "hello", "system", 0.2)
        self.assertEqual(url, "https://example.com/v1/responses")
        self.assertIn("input", payload)
        self.assertEqual(payload["model"], "gpt-5.2")

    def test_llm_client_extracts_text_from_responses_api(self):
        result = {
            "output": [
                {
                    "content": [
                        {"type": "output_text", "text": "{\"claims\": []}"}
                    ]
                }
            ]
        }
        self.assertEqual(LLMClient._extract_content("responses", result), "{\"claims\": []}")

    def test_llm_client_sets_json_response_format_for_chat_completions_when_enabled(self):
        client = LLMClient(
            LLMProviderConfig(
                name="p1",
                base_url="https://example.com/v1",
                api_key_env="OPENAI_API_KEY",
                model="deepseek-ai/DeepSeek-V3.1-Terminus",
                response_format_json=True,
            )
        )
        url, payload = client._build_request("chat_completions", "hello", "system", 0.2)
        self.assertEqual(url, "https://example.com/v1/chat/completions")
        self.assertEqual(payload["response_format"], {"type": "json_object"})

    def test_agent_executor_injects_enabled_skill_context(self):
        fake_client = _FakeClient()
        executor = AgentExecutor(_FakeRouter(fake_client), TraceLogger())

        with tempfile.TemporaryDirectory() as td:
            td_path = Path(td)
            (td_path / "config").mkdir()
            (td_path / "skills" / "demo-skill").mkdir(parents=True)

            (td_path / "config" / "skills_registry.json").write_text(
                json.dumps(
                    {
                        "skills": [
                            {
                                "id": "demo-skill",
                                "title": "Demo Skill",
                                "path": "skills/demo-skill",
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )
            (td_path / "config" / "agent_skill_bindings.json").write_text(
                json.dumps({"agents": {"parse_agent": ["demo-skill"]}}),
                encoding="utf-8",
            )
            (td_path / "skills" / "demo-skill" / "SKILL.md").write_text(
                "\n".join(
                    [
                        "---",
                        "name: demo-skill",
                        "description: Demo skill description",
                        "---",
                        "",
                        "# Demo Skill",
                        "",
                        "Always extract more structure.",
                    ]
                ),
                encoding="utf-8",
            )
            prompt_file = td_path / "prompt.md"
            prompt_file.write_text("{{chunk_text}}", encoding="utf-8")
            agent_file = td_path / "agent.yaml"
            agent_file.write_text(
                "\n".join(
                    [
                        "name: parse_agent",
                        "model_route: parse",
                        f"prompt_template: {prompt_file}",
                    ]
                ),
                encoding="utf-8",
            )

            executor._project_root = td_path
            executor._skills_registry_path = td_path / "config" / "skills_registry.json"
            executor._agent_skill_bindings_path = td_path / "config" / "agent_skill_bindings.json"

            captured = {}

            def _chat(prompt, system_prompt="", max_retries=None, **kwargs):
                captured["prompt"] = prompt
                return {"content": '{"claims": [], "entity_candidates": []}'}

            fake_client.chat = _chat
            executor.execute(agent_file, {"chunk_text": "hello"})

        self.assertIn("[Enabled Skills]", captured["prompt"])
        self.assertIn("Demo Skill", captured["prompt"])
        self.assertIn("Always extract more structure.", captured["prompt"])

    def test_agent_executor_injects_group_guide_context(self):
        fake_client = _FakeClient()
        executor = AgentExecutor(_FakeRouter(fake_client), TraceLogger())

        with tempfile.TemporaryDirectory() as td:
            td_path = Path(td)
            (td_path / "config").mkdir()
            (td_path / "mindvault" / "agents" / "parsing").mkdir(parents=True)

            (td_path / "config" / "agent_groups.json").write_text(
                json.dumps(
                    {
                        "groups": [
                            {
                                "id": "parsing",
                                "label": "解析智能体",
                                "agent_dir": "mindvault/agents/parsing",
                                "soul_path": "mindvault/agents/parsing/SOUL.md",
                                "agents_path": "mindvault/agents/parsing/AGENTS.md",
                                "tools_path": "mindvault/agents/parsing/TOOLS.md",
                                "heartbeat_path": "mindvault/agents/parsing/HEARTBEAT.md",
                                "memory_path": "mindvault/agents/parsing/MEMORY.md",
                                "internal_agents": ["parse_agent"],
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )
            for filename, content in {
                "SOUL.md": "严格抽取，不要制造噪声实体。",
                "AGENTS.md": "用户只看解析智能体，不看内部成员。",
                "TOOLS.md": "输出必须是 JSON。",
                "HEARTBEAT.md": "优先正确，再考虑丰富。",
                "MEMORY.md": "长期偏好：中文字段。",
            }.items():
                (td_path / "mindvault" / "agents" / "parsing" / filename).write_text(content, encoding="utf-8")

            prompt_file = td_path / "prompt.md"
            prompt_file.write_text("{{chunk_text}}", encoding="utf-8")
            agent_file = td_path / "agent.yaml"
            agent_file.write_text(
                "\n".join(
                    [
                        "name: parse_agent",
                        "model_route: parse",
                        f"prompt_template: {prompt_file}",
                    ]
                ),
                encoding="utf-8",
            )

            executor._project_root = td_path
            executor._agent_groups_path = td_path / "config" / "agent_groups.json"

            captured = {}

            def _chat(prompt, system_prompt="", max_retries=None, **kwargs):
                captured["prompt"] = prompt
                return {"content": '{"claims": [], "entity_candidates": []}'}

            fake_client.chat = _chat
            executor.execute(agent_file, {"chunk_text": "hello"})

        self.assertIn("[Agent Workspace]", captured["prompt"])
        self.assertIn("## SOUL.md", captured["prompt"])
        self.assertIn("## AGENTS.md", captured["prompt"])
        self.assertIn("长期偏好：中文字段。", captured["prompt"])

    def test_database_agents_use_multi_db_route(self):
        executor = AgentExecutor(_FakeRouter(_FakeClient()), TraceLogger())
        database_builder = executor.load_agent("mindvault/agents/database_builder_agent.yaml")
        ontology = executor.load_agent("mindvault/agents/ontology_agent.yaml")

        self.assertEqual(database_builder.get("model_route"), "multi_db")
        self.assertEqual(ontology.get("model_route"), "multi_db")
        self.assertEqual(database_builder.get("retry_policy", {}).get("max_retries"), 0)
        self.assertEqual(ontology.get("retry_policy", {}).get("max_retries"), 1)


if __name__ == "__main__":
    unittest.main()
