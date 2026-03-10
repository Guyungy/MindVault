import json
from pathlib import Path
import shutil
import unittest

from main import run_pipeline
from mindvault.runtime.app import VaultRuntime, load_sources_from_path
from mindvault.runtime.renderers.wiki import WikiExporter
from mindvault.runtime.models import NormalizedChunk


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


if __name__ == "__main__":
    unittest.main()
