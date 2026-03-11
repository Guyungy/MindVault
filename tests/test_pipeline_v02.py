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

    def _mock_parse_result(self, text: str):
        if "Alice" in text:
            return {
                "claims": [
                    {"id": "claim_alice_price", "subject": "ent_technician_alice", "predicate": "price", "object": 120, "confidence": 0.92},
                    {"id": "claim_alice_rating", "subject": "ent_technician_alice", "predicate": "rating", "object": 4.5, "confidence": 0.88},
                ],
                "entity_candidates": [
                    {"id": "ent_technician_alice", "name": "Alice", "type": "technician", "attributes": {"service_hours": 2}},
                    {"id": "ent_venue_north_hub", "name": "North Hub", "type": "venue", "attributes": {}},
                ],
                "relation_candidates": [
                    {"source": "ent_technician_alice", "relation": "works_at", "target": "ent_venue_north_hub", "confidence": 0.9},
                ],
                "event_candidates": [],
            }
        if "Bob" in text:
            return {
                "claims": [
                    {"id": "claim_bob_price", "subject": "ent_technician_bob", "predicate": "price", "object": 500, "confidence": 0.4},
                ],
                "entity_candidates": [
                    {"id": "ent_technician_bob", "name": "Bob", "type": "technician", "attributes": {}},
                    {"id": "ent_venue_south_place", "name": "South Place", "type": "venue", "attributes": {}},
                ],
                "relation_candidates": [
                    {"source": "ent_technician_bob", "relation": "works_at", "target": "ent_venue_south_place", "confidence": 0.4},
                ],
                "event_candidates": [],
            }
        if "Carol" in text and "$100" in text:
            return {
                "claims": [
                    {"id": "claim_carol_price_official", "subject": "ent_technician_carol", "predicate": "price", "object": 100, "confidence": 0.9},
                ],
                "entity_candidates": [
                    {"id": "ent_technician_carol", "name": "Carol", "type": "technician", "attributes": {}},
                    {"id": "ent_venue_east_center", "name": "East Center", "type": "venue", "attributes": {}},
                ],
                "relation_candidates": [
                    {"source": "ent_technician_carol", "relation": "works_at", "target": "ent_venue_east_center", "confidence": 0.9},
                ],
                "event_candidates": [],
            }
        if "Carol" in text and "$180" in text:
            return {
                "claims": [
                    {"id": "claim_carol_price_chat", "subject": "ent_technician_carol", "predicate": "price", "object": 180, "confidence": 0.65},
                ],
                "entity_candidates": [
                    {"id": "ent_technician_carol", "name": "Carol", "type": "technician", "attributes": {}},
                    {"id": "ent_venue_east_center", "name": "East Center", "type": "venue", "attributes": {}},
                ],
                "relation_candidates": [
                    {"source": "ent_technician_carol", "relation": "works_at", "target": "ent_venue_east_center", "confidence": 0.65},
                ],
                "event_candidates": [],
            }
        return {"claims": [], "entity_candidates": [], "relation_candidates": [], "event_candidates": []}

    def _mock_database_plan(self, entities):
        entity_types = sorted({entity.get("type", "") for entity in entities if entity.get("type")})
        databases = []
        for entity_type in entity_types:
            databases.append(
                {
                    "name": f"{entity_type}s",
                    "title": entity_type,
                    "description": f"Stores {entity_type} entities.",
                    "entity_types": [entity_type],
                    "suggested_fields": ["id", "name", "type", "source_refs"],
                    "visibility": "business",
                }
            )
        databases.extend(
            [
                {"name": "claims", "title": "claims", "description": "Atomic statements", "entity_types": [], "suggested_fields": ["id", "subject", "predicate", "object"], "visibility": "system"},
                {"name": "relations", "title": "relations", "description": "Cross-record links", "entity_types": [], "suggested_fields": ["id", "source", "relation", "target"], "visibility": "system"},
                {"name": "sources", "title": "sources", "description": "Source references", "entity_types": [], "suggested_fields": ["id", "name", "mentions"], "visibility": "system"},
            ]
        )
        return {"domain": "Test Domain", "databases": databases, "relations": []}

    @staticmethod
    def _collect_columns(rows):
        columns = []
        seen = set()
        for row in rows:
            for key in row.keys():
                if key not in seen:
                    seen.add(key)
                    columns.append(key)
        return columns or ["id"]

    def _mock_multi_db(self, database_plan, entities, claims, relations):
        databases = []
        for database in database_plan.get("databases", []):
            name = database.get("name")
            entity_types = set(database.get("entity_types", []))
            if name == "claims":
                rows = [dict(item) for item in claims]
            elif name == "relations":
                rows = [dict(item, id=f"{item.get('source')}:{item.get('relation')}:{item.get('target')}") for item in relations]
            elif name == "sources":
                source_counts = {}
                for claim in claims:
                    source_ref = claim.get("source_ref")
                    if source_ref:
                        source_counts[source_ref] = source_counts.get(source_ref, 0) + 1
                rows = [{"id": source_id, "name": source_id, "mentions": count} for source_id, count in source_counts.items()]
            else:
                rows = []
                for entity in entities:
                    if entity_types and entity.get("type") not in entity_types:
                        continue
                    row = {
                        "id": entity.get("id"),
                        "name": entity.get("name"),
                        "type": entity.get("type"),
                        "source_refs": entity.get("source_refs", []),
                    }
                    row.update(entity.get("attributes", {}))
                    rows.append(row)
            databases.append(
                {
                    "name": name,
                    "title": database.get("title", name),
                    "visibility": database.get("visibility", "business"),
                    "primary_key": "id",
                    "columns": self._collect_columns(rows),
                    "rows": rows,
                }
            )
        return {"domain": database_plan.get("domain", "Test Domain"), "databases": databases, "relations": []}

    def _mock_llm_execute(self, agent_path, context):
        path_text = str(agent_path)
        if path_text.endswith("parse_agent.yaml"):
            return self._mock_parse_result(context.get("chunk_text", ""))
        if path_text.endswith("ontology_agent.yaml"):
            return self._mock_database_plan(context.get("entities", []))
        if path_text.endswith("database_builder_agent.yaml"):
            return self._mock_multi_db(
                context.get("database_plan", {}),
                context.get("entities", []),
                context.get("claims", []),
                context.get("relations", []),
            )
        if path_text.endswith("insight_agent.yaml"):
            return {
                "insights": [
                    {
                        "insight_id": "insight_1",
                        "title": "结构概览",
                        "summary": "当前知识已形成基础实体和关系。",
                        "importance": "medium",
                        "evidence": ["ent_technician_alice"],
                        "metrics": {"entity_count": len(context.get("entities", []))},
                        "recommendation": "继续补充更多上下文来源。",
                        "generated_at": "2026-03-11T00:00:00",
                    }
                ]
            }
        if path_text.endswith("report_agent.yaml"):
            return {
                "business_domain": "Test Domain",
                "generated_at": "2026-03-11T00:00:00",
                "summary": "ok",
                "key_findings": [],
                "risks": [],
                "next_actions": [],
                "table_highlights": [],
            }
        return {}

    def test_claim_extraction_and_layered_outputs(self):
        with mock.patch("mindvault.runtime.agent_executor.AgentExecutor.execute", side_effect=self._mock_llm_execute):
            run_pipeline(workspace=self.workspace, sample_path="sample_data/benchmarks/semi_structured.json")
        claims_file = self.workspace_path / "extracted" / "claims_v1.json"
        self.assertTrue(claims_file.exists())
        claims = json.loads(claims_file.read_text(encoding="utf-8"))
        self.assertGreater(len(claims), 0)

    def test_low_confidence_claim_in_noisy_chat(self):
        with mock.patch("mindvault.runtime.agent_executor.AgentExecutor.execute", side_effect=self._mock_llm_execute):
            run_pipeline(workspace=self.workspace, sample_path="sample_data/benchmarks/noisy_chat.json")
        kb = json.loads((self.workspace_path / "canonical" / "knowledge_base.json").read_text(encoding="utf-8"))
        low_conf = [c for c in kb.get("claims", []) if c.get("confidence", 1.0) < 0.55]
        self.assertGreater(len(low_conf), 0)

    def test_conflict_detection_for_price(self):
        with mock.patch("mindvault.runtime.agent_executor.AgentExecutor.execute", side_effect=self._mock_llm_execute):
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

    def test_generate_insights_uses_structured_llm_output(self):
        runtime = VaultRuntime(self.workspace)
        runtime.executor.execute = mock.Mock(
            return_value={
                "insights": [
                    {
                        "insight_id": "insight_1",
                        "title": "结构概览",
                        "summary": "知识已形成可用结构。",
                        "importance": "medium",
                        "evidence": ["claim_1"],
                        "metrics": {"entity_count": 1},
                        "recommendation": None,
                        "generated_at": "2026-03-11T00:00:00",
                    }
                ]
            }
        )

        insights = runtime._generate_insights(
            {
                "entities": [{"id": "ent_1", "type": "venue", "name": "A"}],
                "claims": [{"id": "claim_1", "subject": "ent_1", "predicate": "location", "object": "广州"}],
                "relations": [],
                "events": [],
            },
            {"conflicts": {"conflicts": []}, "placeholders": []},
        )

        self.assertEqual(len(insights), 1)
        self.assertEqual(insights[0]["title"], "结构概览")

    def test_generate_database_plan_accepts_wrapped_plan_shape(self):
        runtime = VaultRuntime(self.workspace)
        runtime.executor.execute = mock.Mock(
            return_value={
                "database_plan": {
                    "domain": "测试域",
                    "generated_at": "2026-03-11T00:00:00",
                    "databases": [
                        {
                            "name": "products",
                            "title": "产品",
                            "description": "产品实体",
                            "entity_types": ["product"],
                            "suggested_fields": ["id", "name", "type"],
                            "visibility": "business",
                        }
                    ],
                    "relations": [],
                }
            }
        )

        plan = runtime._generate_database_plan(
            {"entities": [], "claims": [], "relations": [], "events": []},
            {"conflicts": {"conflicts": []}, "placeholders": []},
        )

        self.assertEqual(plan["databases"][0]["name"], "products")
        self.assertEqual(plan["databases"][0]["row_source"], "entities")

    def test_detect_source_type_promotes_chat_like_doc_content_to_chat(self):
        runtime = VaultRuntime(self.workspace)
        source = {
            "source_id": "s1",
            "source_type": "doc",
            "content": "\n".join(
                [
                    "Alice: 你好",
                    "Bob: 在吗",
                    "Alice: 现在网络很卡",
                    "Bob: 你查一下服务",
                    "Carol: 好的",
                ]
            ),
            "metadata": {},
            "context_hints": {},
        }

        self.assertEqual(runtime._detect_source_type(source), "chat")

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

    def test_finalize_multi_db_prunes_duplicate_derived_business_tables(self):
        runtime = VaultRuntime(self.workspace)
        database_plan = {
            "databases": [
                {
                    "name": "persons",
                    "title": "人员",
                    "entity_types": ["person"],
                    "suggested_fields": ["id", "name", "role"],
                    "visibility": "business",
                    "row_source": "entities",
                },
                {
                    "name": "products",
                    "title": "产品",
                    "entity_types": ["product"],
                    "suggested_fields": ["id", "name", "category"],
                    "visibility": "business",
                    "row_source": "entities",
                },
                {
                    "name": "usage_records",
                    "title": "使用记录",
                    "entity_types": ["person", "product"],
                    "suggested_fields": ["record_id", "person_id", "product_id", "usage_type"],
                    "visibility": "business",
                    "row_source": "mixed",
                },
            ]
        }
        multi_db = {
            "databases": [
                {
                    "name": "persons",
                    "rows": [
                        {"id": "p1", "name": "张三", "type": "person"},
                    ],
                },
                {
                    "name": "products",
                    "rows": [
                        {"id": "prd1", "name": "MindVault", "type": "product"},
                    ],
                },
                {
                    "name": "usage_records",
                    "rows": [
                        {"id": "p1", "name": "张三", "type": "person"},
                        {"id": "prd1", "name": "MindVault", "type": "product"},
                    ],
                },
            ]
        }

        finalized = runtime._finalize_multi_db(multi_db, database_plan)
        names = [db["name"] for db in finalized["databases"]]

        self.assertIn("persons", names)
        self.assertIn("products", names)
        self.assertNotIn("usage_records", names)

    def test_generate_multi_db_accepts_single_table_payload_shape(self):
        runtime = VaultRuntime(self.workspace)
        runtime.executor.execute = mock.Mock(
            side_effect=[
                {
                    "name": "products",
                    "title": "产品信息表",
                    "description": "产品实体",
                    "rows": [
                        {"id": "prd_1", "name": "OpenClaw", "type": "product"},
                    ],
                }
            ]
        )
        database_plan = {
            "domain": "产品域",
            "databases": [
                {
                    "name": "products",
                    "title": "产品信息表",
                    "description": "产品实体",
                    "suggested_fields": ["id", "name", "type"],
                    "visibility": "business",
                    "row_source": "entities",
                }
            ],
            "relations": [],
        }
        state = {
            "entities": [],
            "claims": [],
            "relations": [],
            "events": [],
        }

        multi_db, warnings = runtime._generate_multi_db(state, database_plan)

        self.assertEqual(warnings, [])
        self.assertEqual(len(multi_db["databases"]), 1)
        self.assertEqual(multi_db["databases"][0]["name"], "products")
        self.assertEqual(multi_db["databases"][0]["rows"][0]["name"], "OpenClaw")

    def test_generate_multi_db_keeps_partial_success_when_one_table_fails(self):
        runtime = VaultRuntime(self.workspace)
        runtime.executor.execute = mock.Mock(
            side_effect=[
                {
                    "name": "products",
                    "rows": [{"id": "prd_1", "name": "OpenClaw", "type": "product"}],
                },
                {"raw_content": "not structured"},
            ]
        )
        database_plan = {
            "domain": "产品域",
            "databases": [
                {
                    "name": "products",
                    "title": "产品",
                    "suggested_fields": ["id", "name", "type"],
                    "visibility": "business",
                    "row_source": "entities",
                },
                {
                    "name": "organizations",
                    "title": "组织",
                    "suggested_fields": ["id", "name", "type"],
                    "visibility": "business",
                    "row_source": "entities",
                },
            ],
            "relations": [],
        }
        state = {
            "entities": [],
            "claims": [],
            "relations": [],
            "events": [],
        }

        multi_db, warnings = runtime._generate_multi_db(state, database_plan)

        self.assertEqual([db["name"] for db in multi_db["databases"]], ["products"])
        self.assertEqual(len(warnings), 1)
        self.assertEqual(warnings[0]["table"], "organizations")

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
        runtime.executor.execute = mock.Mock(side_effect=self._mock_llm_execute)
        runtime._load_runtime_settings = mock.Mock(
            return_value={
                "execution": {"profile": "full", "engine_mode": "llm_only"},
                "artifacts": {"report": True},
            }
        )

        with mock.patch.object(runtime, "_generate_report", side_effect=TimeoutError("The read operation timed out")):
            result = runtime.ingest([
                {
                    "source_id": "safe_doc",
                    "source_type": "doc",
                    "content": "广州文化中心在天河区提供周末亲子阅读活动，可线上预约报名。",
                }
            ], profile="full")

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

    def test_database_builder_failure_fails_pipeline(self):
        runtime = VaultRuntime(self.workspace)
        original_execute = runtime.executor.execute

        def fake_execute(agent_path, context):
            path_text = str(agent_path)
            if path_text.endswith("parse_agent.yaml"):
                return {
                    "claims": [
                        {"id": "claim_safe_doc_price", "subject": "ent_venue_safe_doc", "predicate": "location", "object": "天河区", "confidence": 0.8},
                    ],
                    "entity_candidates": [
                        {"id": "ent_venue_safe_doc", "name": "广州文化中心", "type": "venue", "attributes": {"location": "天河区"}},
                    ],
                    "relation_candidates": [],
                    "event_candidates": [],
                }
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
        with self.assertRaises(RuntimeError):
            runtime.ingest([
                {
                    "source_id": "safe_doc",
                    "source_type": "doc",
                    "content": "广州文化中心在天河区提供周末亲子阅读活动，可线上预约报名。",
                }
            ])

        latest_task_path = sorted((self.workspace_path / "tasks").glob("task_*/task.json"))[-1]
        task_json = json.loads(latest_task_path.read_text(encoding="utf-8"))

        self.assertEqual(task_json["status"], "failed")

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
