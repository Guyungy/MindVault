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


if __name__ == "__main__":
    unittest.main()
