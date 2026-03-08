import json
import tempfile
import unittest
from pathlib import Path

from mindvault.runtime.agent_executor import AgentExecutor
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


if __name__ == "__main__":
    unittest.main()
