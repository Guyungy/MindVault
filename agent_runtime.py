"""Multi-agent runtime orchestration with explicit execution plan and traces."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List
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
from workspace_manager import WorkspaceContext


@dataclass
class AgentTrace:
    agent: str
    step: str
    timestamp: str
    input_summary: Dict[str, Any] = field(default_factory=dict)
    output_summary: Dict[str, Any] = field(default_factory=dict)


class MultiAgentRuntime:
    """Drives end-to-end KB growth through modular agent stages for one workspace."""

    def __init__(self, context: WorkspaceContext) -> None:
        self.context = context
        self.traces: List[AgentTrace] = []

        self.ingestor = IngestorAgent()
        self.parser = ParserAgent()
        self.deduper = DeduplicatorAgent()
        self.relation_builder = RelationBuilderAgent()
        self.placeholder_manager = PlaceholderManagerAgent()
        self.kb = SelfGrowingKnowledgeBase(path=str(context.kb_path))
        self.version_mgr = VersionManagerAgent(out_dir=str(context.snapshot_dir))
        self.insight_gen = InsightGeneratorAgent()
        self.visualizer = VisualizerAgent(out_dir=str(context.visualization_dir))

    def run(self, raw_items: List[Dict[str, Any]]) -> Dict[str, Any]:
        normalized_docs = [d.to_dict() for d in self.ingestor.ingest(raw_items)]
        self._trace("IngestorAgent", "normalize_inputs", {"raw_items": len(raw_items)}, {"normalized_docs": len(normalized_docs)})

        parsed = self.parser.parse(normalized_docs)
        self._trace(
            "ParserAgent+SchemaDesignerAgent",
            "extract_objects",
            {"docs": len(normalized_docs)},
            {
                "entities": len(parsed.get("entities", [])),
                "events": len(parsed.get("events", [])),
                "relations": len(parsed.get("relations", [])),
                "entity_types": parsed.get("schema", {}).get("entity_types", []),
            },
        )

        deduped = self.deduper.deduplicate(parsed)
        self._trace("DeduplicatorAgent", "merge_duplicates", {}, {"entities": len(deduped.get("entities", []))})

        enriched = self.relation_builder.build(deduped)
        self._trace("RelationBuilderAgent", "build_relations", {}, {"relations": len(enriched.get("relations", []))})

        enriched = self.placeholder_manager.update(enriched)
        unresolved = sum(
            1
            for e in enriched.get("entities", [])
            for v in e.get("placeholders", {}).values()
            if v == "missing"
        )
        self._trace("PlaceholderManagerAgent", "update_placeholders", {}, {"missing_placeholders": unresolved})

        state = self.kb.merge(enriched)
        self._trace("KnowledgeBase", "merge_fragment", {}, {"kb_entities": len(state.get("entities", []))})

        insights = self.insight_gen.generate(state)
        self.kb.append_insights(insights)
        state = self.kb.state
        self._trace("InsightGeneratorAgent", "generate_insights", {}, {"insights": len(insights)})

        version_meta = self.version_mgr.create_snapshot(state)
        self.kb.add_version_record(version_meta)
        state = self.kb.state
        self._trace("VersionManagerAgent", "create_snapshot", {}, version_meta)

        report_text = self.insight_gen.generate_report_text(state, insights)
        self.context.report_path.write_text(report_text, encoding="utf-8")
        self._trace("InsightGeneratorAgent", "write_report", {}, {"report": str(self.context.report_path)})

        viz_paths = self.visualizer.visualize(state)
        self._trace("VisualizerAgent", "render_visuals", {}, viz_paths)

        trace_path = self.context.root_dir / "agent_trace.json"
        trace_path.write_text(
            json.dumps([t.__dict__ for t in self.traces], indent=2, ensure_ascii=False),
            encoding="utf-8",
        )

        return {
            "workspace": self.context.workspace_id,
            "knowledge_base": str(self.context.kb_path),
            "snapshot": version_meta["snapshot_path"],
            "report": str(self.context.report_path),
            "visualizations": viz_paths,
            "trace": str(trace_path),
        }

    def _trace(self, agent: str, step: str, input_summary: Dict[str, Any], output_summary: Dict[str, Any]) -> None:
        self.traces.append(
            AgentTrace(
                agent=agent,
                step=step,
                timestamp=datetime.utcnow().isoformat(),
                input_summary=input_summary,
                output_summary=output_summary,
            )
        )
