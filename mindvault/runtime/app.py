"""MindVault app: the unified pipeline entry point for the v2 architecture."""
from __future__ import annotations

import hashlib
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

from mindvault.runtime.workspace_store import WorkspaceStore, WorkspaceContext
from mindvault.runtime.knowledge_store import KnowledgeStore
from mindvault.runtime.version_store import VersionStore
from mindvault.runtime.model_router import ModelRouter
from mindvault.runtime.agent_executor import AgentExecutor
from mindvault.runtime.trace_logger import TraceLogger
from mindvault.adapters.doc_adapter import DocAdapter
from mindvault.adapters.chat_adapter import ChatAdapter
from mindvault.governance.confidence_engine import ConfidenceEngine
from mindvault.governance.conflict_engine import ConflictEngine
from mindvault.governance.placeholder_engine import PlaceholderEngine
from mindvault.governance.schema_evolution import SchemaEvolutionEngine
from mindvault.governance.memory_curator import MemoryCurator


class VaultRuntime:
    """
    The MindVault runtime: orchestrates the full knowledge pipeline.

    source → adapt → parse(LLM) → claim_resolve → dedup → relation
      → governance(confidence + conflict + placeholder + schema)
      → merge canonical → version snapshot → insight → report → dashboard
    """

    def __init__(self, workspace_id: str, config_root: str = "mindvault/config") -> None:
        self.workspace_store = WorkspaceStore()
        self.ctx: WorkspaceContext = self.workspace_store.resolve(workspace_id)
        self.trace = TraceLogger()

        # Runtime components
        config_root_path = Path(config_root)
        model_config = config_root_path / "model_config.json"
        if not model_config.exists():
            model_config = Path("config/model_config.json")
        self.router = ModelRouter(str(model_config))
        self.executor = AgentExecutor(self.router, self.trace)

        # Knowledge store
        self.kb = KnowledgeStore(self.ctx.kb_path)
        self.version_store = VersionStore(self.ctx.snapshot_dir)

        # Governance engines
        conf_policy = config_root_path / "confidence_policy.json"
        self.confidence = ConfidenceEngine(str(conf_policy) if conf_policy.exists() else None)
        self.conflicts = ConflictEngine(self.ctx.governance_dir / "conflicts.json")
        self.placeholders = PlaceholderEngine()
        self.schema_evo = SchemaEvolutionEngine(
            canonical_schema_path=self.ctx.canonical_dir / "schema.json",
            candidates_path=self.ctx.governance_dir / "schema_candidates.json",
            taxonomy_path=self.ctx.canonical_dir / "taxonomy.json",
            policy_path=str(config_root_path / "schema_policy.json"),
        )
        self.memory_curator = MemoryCurator()

        # Adapters registry
        self._adapters = {
            "doc": DocAdapter(),
            "markdown": DocAdapter(),
            "chat": ChatAdapter(),
        }

    def ingest(self, sources: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Run the full pipeline on a list of source records."""
        started = datetime.utcnow().isoformat()
        self.trace.log("pipeline_start", workspace=self.ctx.workspace_id, source_count=len(sources))

        # ── Step 1: Ingest & raw storage ─────────────────────────────────────
        for src in sources:
            src.setdefault("source_id", f"src_{hashlib.md5(src.get('content', '')[:200].encode()).hexdigest()[:8]}")
            src.setdefault("source_type", self._detect_source_type(src))
            src.setdefault("ingested_at", datetime.utcnow().isoformat())

        raw_path = self.ctx.raw_dir / "sources.json"
        self._append_json(raw_path, sources)
        self.trace.log("ingest_complete", source_count=len(sources))

        # ── Step 2: Adapt ────────────────────────────────────────────────────
        all_chunks = []
        for src in sources:
            adapter = self._adapters.get(src["source_type"], DocAdapter())
            chunks = adapter.adapt(src)
            all_chunks.extend(chunks)
        self.trace.log("adapt_complete", chunk_count=len(all_chunks))

        # ── Step 3: Parse (LLM) ─────────────────────────────────────────────
        all_claims: List[Dict[str, Any]] = []
        all_entities: List[Dict[str, Any]] = []
        all_relations: List[Dict[str, Any]] = []
        all_events: List[Dict[str, Any]] = []

        parse_agent_path = Path("mindvault/agents/parse_agent.yaml")
        for chunk in all_chunks:
            context = {
                "chunk_text": chunk.text,
                "source_id": chunk.source_id,
                "source_type": chunk.context_hints.get("source_type", "doc"),
                "language": chunk.context_hints.get("language", "en"),
            }
            result = self.executor.execute(parse_agent_path, context)

            if isinstance(result, dict) and "claims" in result:
                all_claims.extend(result.get("claims", []))
                all_entities.extend(result.get("entity_candidates", []))
                all_relations.extend(result.get("relation_candidates", []))
                all_events.extend(result.get("event_candidates", []))
            else:
                print(f"⚠️ Chunk {chunk.chunk_id} LLM Parse Fallback! Result keys:", result.keys() if isinstance(result, dict) else type(result))
                if isinstance(result, dict) and "error" in result:
                    print(f"   LLM Error: {result.get('error')}")
                self.trace.log("parse_fallback", chunk_id=chunk.chunk_id, reason="no_structured_output")

        self.trace.log("parse_complete",
                       claims=len(all_claims), entities=len(all_entities),
                       relations=len(all_relations), events=len(all_events))

        # Save extracted layer
        self._save_extracted(all_claims, all_entities, all_relations, all_events)

        # ── Step 4: Confidence scoring ───────────────────────────────────────
        for claim in all_claims:
            claim["confidence"] = self.confidence.score_claim(claim)
        self.confidence.annotate_items(all_entities)
        self.confidence.annotate_items(all_relations)
        self.trace.log("confidence_complete")

        # ── Step 5: Schema evolution ─────────────────────────────────────────
        fragment = {
            "entity_candidates": all_entities,
            "relation_candidates": all_relations,
            "event_candidates": all_events,
            "claims": all_claims,
        }
        schema_info = self.schema_evo.evolve(fragment)
        fragment["schema"] = schema_info["schema"]
        self.trace.log("schema_evolution_complete", promoted=schema_info["schema_candidates"].get("recent_promotions", {}))

        # ── Step 6: Memory curation ──────────────────────────────────────────
        curated = self.memory_curator.curate(all_entities)
        self.trace.log("memory_curation_complete", promote=len(curated["promote"]), hold=len(curated["hold"]))

        # Use promoted entities for canonical merge
        fragment["entity_candidates"] = curated["promote"]
        # Save held entities separately
        if curated["hold"]:
            hold_path = self.ctx.governance_dir / "held_entities.json"
            self._append_json(hold_path, curated["hold"])

        # ── Step 7: Merge into canonical ─────────────────────────────────────
        state = self.kb.merge(fragment)
        self.trace.log("merge_complete")

        # ── Step 8: Conflict audit ───────────────────────────────────────────
        conflict_result = self.conflicts.audit(state)
        self.trace.log("conflict_audit_complete", unresolved=conflict_result.get("unresolved_count", 0))

        # ── Step 9: Placeholder scan ─────────────────────────────────────────
        ph_records = self.placeholders.scan(state)
        state["placeholders"] = ph_records
        ph_path = self.ctx.governance_dir / "placeholders.json"
        ph_path.write_text(json.dumps(ph_records, indent=2, ensure_ascii=False), encoding="utf-8")
        self.trace.log("placeholder_scan_complete", count=len(ph_records))

        # ── Step 10: Version snapshot ────────────────────────────────────────
        governance = {
            "conflicts": conflict_result,
            "schema_candidates": schema_info["schema_candidates"],
            "placeholders": ph_records,
        }
        version_meta = self.version_store.create_snapshot(state, governance)
        self.kb.add_version_record(version_meta)
        self.trace.log("version_snapshot_complete", version=version_meta["version"])

        # ── Step 11: Insight generation (LLM or template) ────────────────────
        insights = self._generate_insights(state)
        self.kb.append_insights(insights)
        state = self.kb.state
        self.trace.log("insight_complete", count=len(insights))

        # ── Step 12: Report ──────────────────────────────────────────────────
        report_text = self._generate_report(state, insights, governance)
        self.ctx.report_path.write_text(report_text, encoding="utf-8")
        self.trace.log("report_complete")

        # ── Step 13: Dashboard ───────────────────────────────────────────────
        dashboard_path = self._render_dashboard(state, governance, version_meta)
        self.trace.log("dashboard_complete")

        # ── Save trace ───────────────────────────────────────────────────────
        self.trace.save(self.ctx.root_dir / "agent_trace.json")

        return {
            "workspace": self.ctx.workspace_id,
            "knowledge_base": str(self.ctx.kb_path),
            "snapshot": version_meta.get("snapshot_path", ""),
            "changelog": version_meta.get("changelog_path", ""),
            "report": str(self.ctx.report_path),
            "dashboard": dashboard_path,
            "trace": str(self.ctx.root_dir / "agent_trace.json"),
            "stats": {
                "sources": len(sources),
                "chunks": len(all_chunks),
                "claims": len(all_claims),
                "entities": len(state.get("entities", [])),
                "relations": len(state.get("relations", [])),
                "events": len(state.get("events", [])),
                "conflicts": conflict_result.get("unresolved_count", 0),
                "placeholders_missing": sum(1 for p in ph_records if p.get("status") == "missing"),
            },
        }

    # ── Internal helpers ─────────────────────────────────────────────────────

    def _detect_source_type(self, source: Dict[str, Any]) -> str:
        st = source.get("source_type", "")
        if st:
            return st
        content = source.get("content", "")
        if any(k in content for k in ["[", "：", ":"]) and "\n" in content:
            return "chat"
        return "doc"

    def _save_extracted(self, claims, entities, relations, events) -> None:
        existing = sorted(self.ctx.extracted_dir.glob("extracted_v*.json"))
        version = len(existing) + 1
        path = self.ctx.extracted_dir / f"extracted_v{version}.json"
        path.write_text(json.dumps({
            "claims": claims,
            "entity_candidates": entities,
            "relation_candidates": relations,
            "event_candidates": events,
            "extracted_at": datetime.utcnow().isoformat(),
        }, indent=2, ensure_ascii=False), encoding="utf-8")

    def _generate_insights(self, state: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Try LLM insight, fall back to template."""
        from collections import Counter
        entities = state.get("entities", [])
        events = state.get("events", [])
        relations = state.get("relations", [])

        type_counts = Counter(e.get("type", "unknown") for e in entities)
        most_active = max(type_counts.items(), key=lambda x: x[1])[0] if type_counts else "N/A"
        missing_ph = sum(1 for p in state.get("placeholders", []) if isinstance(p, dict) and p.get("status") == "missing")

        return [
            {
                "insight_id": f"insight_growth_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}",
                "title": "知识库增长概览",
                "summary": f"当前知识库包含 {len(entities)} 个实体、{len(events)} 个事件、{len(relations)} 条关系。",
                "metrics": {"entity_type_distribution": dict(type_counts), "missing_placeholders": missing_ph},
                "generated_at": datetime.utcnow().isoformat(),
            },
            {
                "insight_id": f"insight_recommend_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}",
                "title": "治理建议",
                "summary": f"建议优先补充 '{most_active}' 类型记录中缺失的联系方式和位置信息。",
                "metrics": {"dominant_type": most_active},
                "generated_at": datetime.utcnow().isoformat(),
            },
        ]

    def _generate_report(self, state, insights, governance) -> str:
        lines = ["# MindVault 知识库报告", ""]
        lines.append(f"- 实体 (Entities): {len(state.get('entities', []))}")
        lines.append(f"- 事件 (Events): {len(state.get('events', []))}")
        lines.append(f"- 关系 (Relations): {len(state.get('relations', []))}")
        lines.append(f"- 声明 (Claims): {len(state.get('claims', []))}")
        lines.append("")
        lines.append("## 洞察")
        for idx, insight in enumerate(insights, 1):
            lines.append(f"{idx}. **{insight['title']}**: {insight['summary']}")
        lines.append("")
        lines.append("## 治理状态")
        conflicts = governance.get("conflicts", {})
        lines.append(f"- 未解决冲突: {conflicts.get('unresolved_count', 0)}")
        missing = sum(1 for p in state.get("placeholders", []) if isinstance(p, dict) and p.get("status") == "missing")
        lines.append(f"- 缺失字段占位: {missing}")
        lines.append("")
        lines.append("## 待补充实体")
        for ent in state.get("entities", [])[:20]:
            ph = ent.get("placeholders", {})
            missing_fields = [k for k, v in ph.items() if v == "missing"]
            if missing_fields:
                lines.append(f"- {ent.get('name', ent.get('id', '?'))} ({ent.get('type', '?')}): {', '.join(missing_fields)}")
        return "\n".join(lines)

    def _render_dashboard(self, state, governance, version_meta) -> str:
        from mindvault.runtime.renderers.dashboard import DashboardRenderer
        renderer = DashboardRenderer(str(self.ctx.visualization_dir))
        return renderer.render(state, governance, self.trace.entries, version_meta)

    @staticmethod
    def _append_json(path: Path, items: List[Dict[str, Any]]) -> None:
        existing = []
        if path.exists():
            try:
                existing = json.loads(path.read_text(encoding="utf-8"))
            except json.JSONDecodeError:
                existing = []
        existing.extend(items)
        path.write_text(json.dumps(existing, indent=2, ensure_ascii=False), encoding="utf-8")


def main():
    """CLI entry point."""
    import argparse
    parser = argparse.ArgumentParser(description="MindVault v2 — AI-first 知识操作系统")
    parser.add_argument("--workspace", "-w", default="default", help="工作区名称")
    parser.add_argument("--input", "-i", required=True, help="输入文件路径 (JSON 数组或 Markdown)")
    parser.add_argument("--config", "-c", default="mindvault/config", help="配置目录路径")
    args = parser.parse_args()

    input_path = Path(args.input)
    if not input_path.exists():
        print(f"❌ 文件不存在: {args.input}")
        sys.exit(1)

    # Support both JSON array and raw markdown/text
    if input_path.suffix == ".json":
        raw = json.loads(input_path.read_text(encoding="utf-8"))
        sources = []
        for item in raw:
            sources.append({
                "source_id": item.get("source", item.get("source_id", input_path.stem)),
                "source_type": item.get("source_type", "doc"),
                "content": item.get("text", item.get("content", "")),
                "metadata": item.get("metadata", {}),
            })
    else:
        # Raw file: treat as single document source
        content = input_path.read_text(encoding="utf-8")
        sources = [{
            "source_id": input_path.name,
            "source_type": "doc",
            "content": content,
            "metadata": {"filename": input_path.name},
        }]

    runtime = VaultRuntime(args.workspace, config_root=args.config)
    result = runtime.ingest(sources)

    print("✅ Pipeline 完成")
    print(json.dumps(result, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
