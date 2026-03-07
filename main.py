"""Main entry point: end-to-end ingestion, KB growth, versioning, insights, and visualization."""
from __future__ import annotations

from pathlib import Path
import json

from deduplicator import DeduplicatorAgent
from ingestor import IngestorAgent
from insight_generator import InsightGeneratorAgent
from knowledge_base import SelfGrowingKnowledgeBase
from parser import ParserAgent
from placeholder_manager import PlaceholderManagerAgent
from relation_builder import RelationBuilderAgent
from version_manager import VersionManagerAgent
from visualizer import VisualizerAgent


def run_pipeline(sample_path: str = "sample_data/raw_inputs.json"):
    ingestor = IngestorAgent()
    parser = ParserAgent()
    deduper = DeduplicatorAgent()
    relation_builder = RelationBuilderAgent()
    placeholder_manager = PlaceholderManagerAgent()
    kb = SelfGrowingKnowledgeBase(path="output/knowledge_base.json")
    version_mgr = VersionManagerAgent(out_dir="output")
    insight_gen = InsightGeneratorAgent()
    visualizer = VisualizerAgent(out_dir="output")

    raw_items = ingestor.load_json(sample_path)
    normalized_docs = [d.to_dict() for d in ingestor.ingest(raw_items)]

    parsed = parser.parse(normalized_docs)
    deduped = deduper.deduplicate(parsed)
    enriched = relation_builder.build(deduped)
    enriched = placeholder_manager.update(enriched)

    state = kb.merge(enriched)

    insights = insight_gen.generate(state)
    kb.append_insights(insights)
    state = kb.state

    version_meta = version_mgr.create_snapshot(state)
    kb.add_version_record(version_meta)
    state = kb.state

    report_text = insight_gen.generate_report_text(state, insights)
    report_path = Path("output/report.md")
    report_path.write_text(report_text, encoding="utf-8")

    viz_paths = visualizer.visualize(state)

    print("Pipeline completed.")
    print(json.dumps(
        {
            "knowledge_base": "output/knowledge_base.json",
            "snapshot": version_meta["snapshot_path"],
            "report": str(report_path),
            "visualizations": viz_paths,
        },
        indent=2,
        ensure_ascii=False,
    ))


if __name__ == "__main__":
    run_pipeline()
