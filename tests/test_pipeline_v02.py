import json
from pathlib import Path
import shutil
import unittest

from main import run_pipeline


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


if __name__ == "__main__":
    unittest.main()
