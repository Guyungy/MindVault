"""Multi-agent runtime orchestration with task-passing mesh."""
from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List
import json

from agent_mesh import AgentMeshRuntime, Task
from conflict_auditor import ConflictAuditor
from deduplicator import DeduplicatorAgent
from ingestor import IngestorAgent
from insight_generator import InsightGeneratorAgent
from knowledge_base import SelfGrowingKnowledgeBase
from parser import ParserAgent
from placeholder_manager import PlaceholderManagerAgent
from relation_builder import RelationBuilderAgent
from schema_evolution import SchemaEvolutionAgent
from version_manager import VersionManagerAgent
from visualizer import VisualizerAgent
from workspace_manager import WorkspaceContext


class MultiAgentRuntime:
    """Drives KB growth by letting agents pass tasks through a configurable mesh."""

    def __init__(self, context: WorkspaceContext, workflow_path: str = "workflow/default_workflow.json") -> None:
        self.context = context
        self.mesh = AgentMeshRuntime(workflow_path=workflow_path)
        self.kb = SelfGrowingKnowledgeBase(path=str(context.kb_path))

        self.ingestor = IngestorAgent()
        self.parser = ParserAgent()
        self.deduper = DeduplicatorAgent()
        self.relation_builder = RelationBuilderAgent()
        self.placeholder_manager = PlaceholderManagerAgent()
        self.insight_gen = InsightGeneratorAgent()
        self.version_mgr = VersionManagerAgent(out_dir=str(context.snapshot_dir))
        self.visualizer = VisualizerAgent(out_dir=str(context.visualization_dir))
        self.conflict_auditor = ConflictAuditor(out_path=context.governance_dir / "conflicts.json")
        self.schema_evolution = SchemaEvolutionAgent(
            canonical_schema_path=context.canonical_dir / "schema.json",
            candidates_path=context.governance_dir / "schema_candidates.json",
            taxonomy_path=context.canonical_dir / "taxonomy.json",
        )

        self._register_handlers()

    def _register_handlers(self) -> None:
        self.mesh.register("ingestor", self._handle_ingestor)
        self.mesh.register("parser", self._handle_parser)
        self.mesh.register("deduplicator", self._handle_deduplicator)
        self.mesh.register("relation_builder", self._handle_relation_builder)
        self.mesh.register("placeholder_manager", self._handle_placeholder_manager)
        self.mesh.register("knowledge_base", self._handle_knowledge_base)
        self.mesh.register("insight_generator", self._handle_insight_generator)
        self.mesh.register("version_manager", self._handle_version_manager)
        self.mesh.register("visualizer", self._handle_visualizer)

    def run(self, raw_items: List[Dict[str, Any]]) -> Dict[str, Any]:
        context: Dict[str, Any] = {
            "raw_items": raw_items,
            "workspace": self.context.workspace_id,
            "started_at": datetime.utcnow().isoformat(),
            "governance": {},
        }

        self.mesh.run(Task(task_type="ingest.start", payload={"count": len(raw_items)}), context)

        trace_path = self.context.root_dir / "agent_trace.json"
        trace_path.write_text(json.dumps(self.mesh.traces, indent=2, ensure_ascii=False), encoding="utf-8")

        result = {
            "workspace": self.context.workspace_id,
            "knowledge_base": str(self.context.kb_path),
            "snapshot": context.get("snapshot_path", ""),
            "changelog": context.get("changelog_path", ""),
            "report": str(self.context.report_path),
            "visualizations": context.get("visualizations", {}),
            "trace": str(trace_path),
            "workflow": "workflow/default_workflow.json",
        }
        return result

    def _handle_ingestor(self, task: Task, context: Dict[str, Any]) -> List[Task]:
        docs = [d.to_dict() for d in self.ingestor.ingest(context["raw_items"])]
        context["docs"] = docs
        raw_dump = self.context.raw_dir / "ingested_docs.json"
        raw_dump.write_text(json.dumps(docs, indent=2, ensure_ascii=False), encoding="utf-8")
        return [Task(task_type="parse.request", payload={"docs": len(docs)}, source_agent="ingestor")]

    def _handle_parser(self, task: Task, context: Dict[str, Any]) -> List[Task]:
        parsed = self.parser.parse(context["docs"], workspace_id=self.context.workspace_id)
        context["fragment"] = parsed
        self._dump_claims(parsed.get("claims", []))
        return [Task(task_type="dedup.request", payload={}, source_agent="parser")]

    def _handle_deduplicator(self, task: Task, context: Dict[str, Any]) -> List[Task]:
        context["fragment"] = self.deduper.deduplicate(context["fragment"])
        return [Task(task_type="relation.request", payload={}, source_agent="deduplicator")]

    def _handle_relation_builder(self, task: Task, context: Dict[str, Any]) -> List[Task]:
        context["fragment"] = self.relation_builder.build(context["fragment"])
        return [Task(task_type="placeholder.request", payload={}, source_agent="relation_builder")]

    def _handle_placeholder_manager(self, task: Task, context: Dict[str, Any]) -> List[Task]:
        context["fragment"] = self.placeholder_manager.update(context["fragment"])
        placeholders_path = self.context.governance_dir / "placeholders.json"
        placeholders_path.write_text(
            json.dumps(context["fragment"].get("placeholder_candidates", []), indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        return [Task(task_type="kb.merge.request", payload={}, source_agent="placeholder_manager")]

    def _handle_knowledge_base(self, task: Task, context: Dict[str, Any]) -> List[Task]:
        schema_info = self.schema_evolution.evolve(context["fragment"])
        context["fragment"]["schema"] = schema_info["schema"]
        state = self.kb.merge(context["fragment"])
        conflicts = self.conflict_auditor.audit(state)
        context["governance"] = {
            "conflicts": conflicts,
            "schema_candidates": schema_info["schema_candidates"],
            "placeholders": context["fragment"].get("placeholder_candidates", []),
        }
        state["governance"] = context["governance"]
        self.kb.save()
        context["state"] = state
        return [Task(task_type="insight.request", payload={}, source_agent="knowledge_base")]

    def _handle_insight_generator(self, task: Task, context: Dict[str, Any]) -> List[Task]:
        if task.task_type == "insight.request":
            insights = self.insight_gen.generate(context["state"])
            self.kb.append_insights(insights)
            context["state"] = self.kb.state
            return [Task(task_type="version.request", payload={}, source_agent="insight_generator")]

        if task.task_type == "report.request":
            report_text = self.insight_gen.generate_report_text(context["state"], context["state"].get("insights", []))
            self.context.report_path.write_text(report_text, encoding="utf-8")
            return [Task(task_type="visualize.request", payload={}, source_agent="insight_generator")]

        return []

    def _handle_version_manager(self, task: Task, context: Dict[str, Any]) -> List[Task]:
        version_meta = self.version_mgr.create_snapshot(context["state"], governance=context.get("governance"))
        self.kb.add_version_record(version_meta)
        context["state"] = self.kb.state
        context["snapshot_path"] = version_meta["snapshot_path"]
        context["changelog_path"] = version_meta["changelog_path"]
        context["version_meta"] = version_meta
        return [Task(task_type="report.request", payload={}, source_agent="version_manager")]

    def _handle_visualizer(self, task: Task, context: Dict[str, Any]) -> List[Task]:
        context["visualizations"] = self.visualizer.visualize(
            context["state"],
            governance=context.get("governance"),
            trace=self.mesh.traces,
            version_meta=context.get("version_meta"),
        )
        return []

    def _dump_claims(self, claims: List[Dict[str, Any]]) -> None:
        existing = sorted(self.context.extracted_dir.glob("claims_v*.json"))
        version = len(existing) + 1
        path = self.context.extracted_dir / f"claims_v{version}.json"
        path.write_text(json.dumps(claims, indent=2, ensure_ascii=False), encoding="utf-8")
