import json
from pathlib import Path
import shutil
import unittest
from datetime import datetime, timedelta
from unittest import mock

from main import run_pipeline
from mindvault.runtime.app import VaultRuntime, load_sources_from_path
from mindvault.runtime.bash_runner import BashRunner
from mindvault.runtime.renderers.wiki import WikiExporter
from mindvault.runtime.models import NormalizedChunk
from mindvault.runtime.task_monitor import summarize_task
from mindvault.runtime.task_runtime import TaskRuntime


class MindVaultV02Tests(unittest.TestCase):
    def setUp(self):
        self.workspace = "test_v02"
        self.workspace_path = Path("output/workspaces") / self.workspace
        if self.workspace_path.exists():
            shutil.rmtree(self.workspace_path)

    def tearDown(self):
        if self.workspace_path.exists():
            shutil.rmtree(self.workspace_path)

    def test_claim_extraction_and_layered_outputs(self):
        run_pipeline(workspace=self.workspace, sample_path="sample_data/benchmarks/semi_structured.json")
        claims_file = self.workspace_path / "extracted" / "claims_v1.json"
        self.assertTrue(claims_file.exists())
        claims = json.loads(claims_file.read_text(encoding="utf-8"))
        self.assertGreater(len(claims), 0)

    def test_low_confidence_claim_in_noisy_chat(self):
        run_pipeline(workspace=self.workspace, sample_path="sample_data/benchmarks/noisy_chat.json")
        kb = json.loads((self.workspace_path / "canonical" / "knowledge_base.json").read_text(encoding="utf-8"))
        low_conf = [c for c in kb.get("claims", []) if c.get("confidence", 1.0) < 0.55]
        self.assertGreater(len(low_conf), 0)

    def test_conflict_detection_for_price(self):
        run_pipeline(workspace=self.workspace, sample_path="sample_data/benchmarks/conflicting_multi_source.json")
        conflicts = json.loads((self.workspace_path / "governance" / "conflicts.json").read_text(encoding="utf-8"))
        fields = {c.get("field") for c in conflicts.get("conflicts", [])}
        self.assertIn("price", fields)

    def test_wiki_exporter_outputs_wiki_and_tables(self):
        out_dir = self.workspace_path / "wiki"
        exporter = WikiExporter(out_dir)
        result = exporter.export(
            state={
                "entities": [
                    {
                        "id": "ent_venue_blue_harbor",
                        "type": "venue",
                        "name": "Blue Harbor",
                        "attributes": {"location": "sample", "rating": 4.5},
                        "confidence": 0.9,
                        "updated_at": "2026-03-09T00:00:00",
                        "source_refs": ["demo_doc"],
                    }
                ],
                "claims": [
                    {
                        "id": "claim_1",
                        "subject": "ent_venue_blue_harbor",
                        "predicate": "rating",
                        "object": 4.5,
                        "claim_type": "fact",
                        "confidence": 0.8,
                    }
                ],
                "relations": [],
                "events": [],
                "placeholders": [],
            },
            governance={"conflicts": {"conflicts": [], "unresolved_count": 0}, "placeholders": []},
            version_meta={"version": 1},
        )
        wiki_index = out_dir / "index.md"
        tables_json = out_dir / "tables.json"
        pages_json = out_dir / "pages.json"
        self.assertTrue(wiki_index.exists())
        self.assertTrue(tables_json.exists())
        self.assertTrue(pages_json.exists())
        self.assertIn("index", result)

    def test_directory_loader_supports_md_txt_json(self):
        input_dir = self.workspace_path / "inputs"
        input_dir.mkdir(parents=True, exist_ok=True)
        (input_dir / "a.md").write_text("# Title\n\nalpha", encoding="utf-8")
        (input_dir / "b.txt").write_text("beta", encoding="utf-8")
        (input_dir / "c.json").write_text(json.dumps([
            {"source_id": "json_one", "source_type": "doc", "content": "gamma"}
        ], ensure_ascii=False), encoding="utf-8")
        (input_dir / "ignored.csv").write_text("x,y\n1,2\n", encoding="utf-8")

        sources = load_sources_from_path(input_dir)
        source_ids = {item["source_id"] for item in sources}

        self.assertEqual(len(sources), 3)
        self.assertIn("a.md", source_ids)
        self.assertIn("b.txt", source_ids)
        self.assertIn("json_one", source_ids)

    def test_json_loader_preserves_context_hints_and_metadata(self):
        input_dir = self.workspace_path / "inputs"
        input_dir.mkdir(parents=True, exist_ok=True)
        (input_dir / "c.json").write_text(
            json.dumps(
                [
                    {
                        "source_id": "json_one",
                        "source_type": "doc",
                        "content": "gamma",
                        "metadata": {"origin": "webui"},
                        "context_hints": {"target_db": "services", "note": "manual"},
                    }
                ],
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )

        sources = load_sources_from_path(input_dir / "c.json")

        self.assertEqual(sources[0]["context_hints"]["target_db"], "services")
        self.assertEqual(sources[0]["context_hints"]["note"], "manual")
        self.assertEqual(sources[0]["metadata"]["origin"], "webui")
        self.assertEqual(sources[0]["metadata"]["filename"], "c.json")

    def test_runtime_fallback_parser_extracts_entities(self):
        runtime = VaultRuntime(self.workspace)
        chunk = NormalizedChunk(
            chunk_id="chunk_1",
            source_id="safe_doc",
            chunk_type="section",
            text="广州文化中心推出周末亲子阅读活动，活动地点位于天河区，报名方式为线上预约。",
            context_hints={"source_type": "doc", "language": "zh"},
        )
        result = runtime._fallback_parse_chunk(chunk)

        self.assertGreater(len(result.get("entity_candidates", [])), 0)
        self.assertGreater(len(result.get("claims", [])), 0)

    def test_runtime_fallback_multi_db_outputs_multiple_databases(self):
        runtime = VaultRuntime(self.workspace)
        state = {
            "entities": [
                {"id": "ent_venue_a", "type": "venue", "name": "A", "attributes": {"location": "广州"}, "confidence": 0.7, "source_refs": ["s1"]},
                {"id": "ent_service_b", "type": "service", "name": "B", "attributes": {"schedule": "周末"}, "confidence": 0.6, "source_refs": ["s1"]},
            ],
            "claims": [
                {"id": "claim_1", "subject": "ent_venue_a", "predicate": "location", "object": "广州", "claim_type": "fact", "confidence": 0.8, "source_ref": "s1"}
            ],
            "relations": [
                {"source": "ent_venue_a", "relation": "hosts_service", "target": "ent_service_b", "confidence": 0.7, "source_refs": ["s1"]}
            ],
            "events": [],
        }
        plan = runtime._fallback_database_plan(state)
        multi_db = runtime._fallback_multi_db(state, plan)

        self.assertGreaterEqual(len(multi_db.get("databases", [])), 3)
        names = {db.get("name") for db in multi_db.get("databases", [])}
        self.assertIn("claims", names)
        visibility = {db.get("name"): db.get("visibility") for db in plan.get("databases", [])}
        self.assertEqual(visibility["claims"], "system")
        self.assertEqual(visibility["relations"], "system")
        self.assertEqual(visibility["sources"], "system")

    def test_finalize_multi_db_appends_fields_and_infers_relations(self):
        runtime = VaultRuntime(self.workspace)
        database_plan = {
            "databases": [
                {
                    "name": "people",
                    "title": "人物",
                    "suggested_fields": ["id", "name", "profile_city", "friend_ids"],
                    "visibility": "business",
                },
                {
                    "name": "places",
                    "title": "地点",
                    "suggested_fields": ["id", "name"],
                    "visibility": "business",
                },
            ]
        }
        multi_db = {
            "databases": [
                {
                    "name": "people",
                    "rows": [
                        {
                            "id": "p1",
                            "name": "小王",
                            "profile": {"city": "广州"},
                            "friend_ids": ["p2"],
                            "home_place_id": "place_1",
                        },
                        {
                            "id": "p2",
                            "name": "小李",
                        },
                    ],
                },
                {
                    "name": "places",
                    "rows": [
                        {
                            "id": "place_1",
                            "name": "天河",
                        }
                    ],
                },
            ]
        }

        finalized = runtime._finalize_multi_db(multi_db, database_plan)
        people = next(db for db in finalized["databases"] if db["name"] == "people")

        self.assertIn("profile_city", people["columns"])
        self.assertIn("home_place_id", people["columns"])
        self.assertEqual(people["primary_key"], "id")
        inferred = {(rel["from_db"], rel["from_field"], rel["to_db"]) for rel in finalized["relations"]}
        self.assertIn(("people", "friend_ids", "people"), inferred)
        self.assertIn(("people", "home_place_id", "places"), inferred)

    def test_task_runtime_persists_state_and_steps(self):
        task_root = self.workspace_path / "tasks"
        runtime = TaskRuntime(task_root, goal="Test goal", workspace_id=self.workspace)
        runtime.start()
        runtime.heartbeat(step="parse", agent="parse_agent", resume_hint="Parsing input.")
        runtime.log_step("parse", "ok", chunks=3)
        runtime.complete("Done.")

        task_json = json.loads((runtime.task_dir / "task.json").read_text(encoding="utf-8"))
        step_log = (runtime.task_dir / "step_log.jsonl").read_text(encoding="utf-8").strip().splitlines()

        self.assertEqual(task_json["status"], "completed")
        self.assertEqual(task_json["current_step"], "parse")
        self.assertGreaterEqual(len(step_log), 1)

    def test_bash_runner_captures_stdout_and_exit_code(self):
        runner = BashRunner(self.workspace_path / "stdout")
        result = runner.run("printf 'hello'", timeout_seconds=5, cwd=self.workspace_path)

        self.assertEqual(result["exit_code"], 0)
        self.assertFalse(result["timed_out"])
        self.assertTrue(Path(result["stdout_path"]).exists())
        self.assertIn("hello", Path(result["stdout_path"]).read_text(encoding="utf-8"))

    def test_task_monitor_detects_stale_running_task(self):
        task = {
            "status": "running",
            "last_heartbeat": "2026-03-10T00:00:00",
        }
        summary = summarize_task(task, recent_steps=[{"status": "fallback"}, {"status": "failed"}])

        self.assertEqual(summary["health"], "stale")

    def test_report_timeout_does_not_block_multi_db_outputs(self):
        runtime = VaultRuntime(self.workspace)
        runtime.executor.execute = mock.Mock(return_value={})

        with mock.patch.object(runtime, "_generate_report", side_effect=TimeoutError("The read operation timed out")):
            result = runtime.ingest([
                {
                    "source_id": "safe_doc",
                    "source_type": "doc",
                    "content": "广州文化中心在天河区提供周末亲子阅读活动，可线上预约报名。",
                }
            ])

        multi_db_path = Path(result["multi_db"]["data"])
        latest_task_path = sorted((self.workspace_path / "tasks").glob("task_*/task.json"))[-1]
        task_json = json.loads(latest_task_path.read_text(encoding="utf-8"))
        step_log_path = latest_task_path.parent / "step_log.jsonl"
        step_log = step_log_path.read_text(encoding="utf-8")

        self.assertTrue(multi_db_path.exists())
        self.assertEqual(task_json["status"], "completed")
        self.assertIn("warnings", task_json)
        self.assertEqual(task_json["warnings"][0]["step"], "report")
        self.assertIn('"action": "report"', step_log)
        self.assertIn('"status": "failed"', step_log)
        self.assertIn("multi_db", result)

    def test_database_builder_failure_falls_back_to_local_multi_db(self):
        runtime = VaultRuntime(self.workspace)
        original_execute = runtime.executor.execute

        def fake_execute(agent_path, context):
            path_text = str(agent_path)
            if path_text.endswith("parse_agent.yaml"):
                return {}
            if path_text.endswith("ontology_agent.yaml"):
                return {
                    "domain": "Test Domain",
                    "databases": [
                        {
                            "name": "venues",
                            "title": "地点",
                            "entity_types": ["venue"],
                            "suggested_fields": ["id", "name", "location"],
                            "visibility": "business",
                        }
                    ],
                    "relations": [],
                }
            if path_text.endswith("database_builder_agent.yaml"):
                return {"_agent_error": "HTTP Error 500: Internal Server Error"}
            return original_execute(agent_path, context)

        runtime.executor.execute = fake_execute
        result = runtime.ingest([
            {
                "source_id": "safe_doc",
                "source_type": "doc",
                "content": "广州文化中心在天河区提供周末亲子阅读活动，可线上预约报名。",
            }
        ])

        multi_db_payload = json.loads(Path(result["multi_db"]["data"]).read_text(encoding="utf-8"))
        latest_task_path = sorted((self.workspace_path / "tasks").glob("task_*/task.json"))[-1]
        task_json = json.loads(latest_task_path.read_text(encoding="utf-8"))

        self.assertEqual(task_json["status"], "completed")
        self.assertGreater(len(multi_db_payload.get("databases", [])), 0)

    def test_task_monitor_uses_recent_step_activity_for_running_task(self):
        now = datetime.utcnow()
        task = {
            "status": "running",
            "last_heartbeat": (now - timedelta(minutes=20)).isoformat(),
        }
        summary = summarize_task(
            task,
            recent_steps=[
                {"status": "fallback", "timestamp": (now - timedelta(minutes=10)).isoformat()},
                {"status": "ok", "timestamp": (now - timedelta(seconds=30)).isoformat()},
            ],
        )

        self.assertEqual(summary["health"], "healthy")
        self.assertFalse(summary["is_stale"])


if __name__ == "__main__":
    unittest.main()
