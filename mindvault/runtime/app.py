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
from mindvault.runtime.renderers.wiki import WikiExporter
from parser import ParserAgent as RuleParserAgent


class VaultRuntime:
    """
    The MindVault runtime: orchestrates the full knowledge pipeline.

    source → adapt → parse(LLM) → claim_resolve → dedup → relation
      → governance(confidence + conflict + placeholder + schema)
      → merge canonical → version snapshot → insight → report → dashboard
    """

    def __init__(self, workspace_id: str, config_root: str = "mindvault/config", verbose: bool = False) -> None:
        self.workspace_store = WorkspaceStore()
        self.ctx: WorkspaceContext = self.workspace_store.resolve(workspace_id)
        self.trace = TraceLogger(verbose=verbose)

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
        self.memory_curator = MemoryCurator(min_confidence=0.3)
        self.rule_parser = RuleParserAgent()

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
                normalized = self._normalize_parse_result(result, chunk)
                all_claims.extend(normalized.get("claims", []))
                all_entities.extend(normalized.get("entity_candidates", []))
                all_relations.extend(normalized.get("relation_candidates", []))
                all_events.extend(normalized.get("event_candidates", []))
            else:
                fallback = self._fallback_parse_chunk(chunk)
                all_claims.extend(fallback.get("claims", []))
                all_entities.extend(fallback.get("entity_candidates", []))
                all_relations.extend(fallback.get("relation_candidates", []))
                all_events.extend(fallback.get("event_candidates", []))
                self.trace.log(
                    "parse_fallback",
                    chunk_id=chunk.chunk_id,
                    reason="no_structured_output",
                    fallback_claims=len(fallback.get("claims", [])),
                    fallback_entities=len(fallback.get("entity_candidates", [])),
                )

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
        report_data = self._generate_report(state, insights, governance)
        self.ctx.report_path.write_text(json.dumps(report_data, indent=2, ensure_ascii=False), encoding="utf-8")
        self.trace.log("report_complete")

        # ── Step 13: Dashboard ───────────────────────────────────────────────
        dashboard_path = self._render_dashboard(state, governance, version_meta)
        self.trace.log("dashboard_complete")

        # ── Step 14: Wiki export ────────────────────────────────────────────
        wiki_paths = self._export_wiki(state, governance, version_meta)
        self.trace.log("wiki_export_complete", index=wiki_paths.get("index", ""))

        # ── Save trace ───────────────────────────────────────────────────────
        self.trace.save(self.ctx.root_dir / "agent_trace.json")

        return {
            "workspace": self.ctx.workspace_id,
            "knowledge_base": str(self.ctx.kb_path),
            "snapshot": version_meta.get("snapshot_path", ""),
            "changelog": version_meta.get("changelog_path", ""),
            "report": str(self.ctx.report_path),
            "dashboard": dashboard_path,
            "wiki": wiki_paths,
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

    def _generate_report(self, state, insights, governance) -> Dict[str, Any]:
        report_agent_path = Path("mindvault/agents/report_agent.yaml")
        if report_agent_path.exists():
            context = {
                "entities": state.get("entities", []),
                "claims": state.get("claims", []),
                "relations": state.get("relations", [])
            }
            result = self.executor.execute(report_agent_path, context)
            if isinstance(result, dict) and "business_domain" in result:
                return result
            elif isinstance(result, dict) and result.get("content") and isinstance(result["content"], dict):
                return result["content"]

        # Fallback to simple dictionary
        return {
            "business_domain": "System Fallback Overview",
            "summary": "Report generation failed or returned unparseable text.",
            "tags": ["fallback", "system", "auto-generated"],
            "tables": [
                {
                    "table_name": "Knowledge Elements",
                    "columns": ["Type", "Count"],
                    "rows": [
                        {"Type": "Entities", "Count": len(state.get('entities', []))},
                        {"Type": "Events", "Count": len(state.get('events', []))},
                        {"Type": "Relations", "Count": len(state.get('relations', []))},
                        {"Type": "Claims", "Count": len(state.get('claims', []))}
                    ]
                }
            ]
        }

    def _render_dashboard(self, state, governance, version_meta) -> str:
        from mindvault.runtime.renderers.dashboard import DashboardRenderer
        renderer = DashboardRenderer(str(self.ctx.visualization_dir))
        return renderer.render(state, governance, self.trace.entries, version_meta)

    def _export_wiki(self, state, governance, version_meta) -> Dict[str, Any]:
        exporter = WikiExporter(self.ctx.wiki_dir)
        wiki_payload = self._generate_wiki_payload(state, governance, version_meta)
        return exporter.export(state, governance, version_meta, wiki_payload=wiki_payload)

    def _generate_wiki_payload(self, state, governance, version_meta) -> Dict[str, Any] | None:
        wiki_agent_path = Path("mindvault/agents/wiki_builder_agent.yaml")
        if not wiki_agent_path.exists():
            return None

        context = {
            "entities": state.get("entities", []),
            "claims": state.get("claims", []),
            "relations": state.get("relations", []),
            "events": state.get("events", []),
            "governance": governance,
            "version_meta": version_meta,
        }
        result = self.executor.execute(wiki_agent_path, context)
        if isinstance(result, dict) and isinstance(result.get("pages"), list):
            return result
        return None

    def _fallback_parse_chunk(self, chunk) -> Dict[str, Any]:
        docs = [{
            "text": chunk.text,
            "source": chunk.source_id,
            "timestamp": datetime.utcnow().isoformat(),
            "speaker": chunk.context_hints.get("author", "unknown"),
        }]
        result = self.rule_parser.parse(docs, workspace_id=self.ctx.workspace_id)
        return self._normalize_parse_result(result, chunk)

    def _normalize_parse_result(self, result: Dict[str, Any], chunk) -> Dict[str, Any]:
        source_id = chunk.source_id
        claims = []
        for claim in result.get("claims", []):
            normalized_claim = dict(claim)
            if "claim_id" in normalized_claim and "id" not in normalized_claim:
                normalized_claim["id"] = normalized_claim["claim_id"]
            normalized_claim.setdefault("source_ref", source_id)
            normalized_claim.setdefault("source_refs", [normalized_claim.get("source_ref", source_id)])
            normalized_claim.setdefault("status", "active")
            claims.append(normalized_claim)

        entities = []
        for entity in result.get("entity_candidates", []):
            normalized_entity = dict(entity)
            if "entity_id" in normalized_entity and "id" not in normalized_entity:
                normalized_entity["id"] = normalized_entity["entity_id"]
            normalized_entity.setdefault("attributes", {})
            normalized_entity.setdefault("placeholders", {})
            normalized_entity.setdefault("source_refs", [source_id])
            normalized_entity.setdefault("status", "active")
            entities.append(normalized_entity)

        relations = []
        for relation in result.get("relation_candidates", []):
            normalized_relation = dict(relation)
            if "source_entity" in normalized_relation and "source" not in normalized_relation:
                normalized_relation["source"] = normalized_relation["source_entity"]
            if "target_entity" in normalized_relation and "target" not in normalized_relation:
                normalized_relation["target"] = normalized_relation["target_entity"]
            if "relation_type" in normalized_relation and "relation" not in normalized_relation:
                normalized_relation["relation"] = normalized_relation["relation_type"]
            normalized_relation.setdefault("source_refs", [source_id])
            normalized_relation.setdefault("status", "active")
            relations.append(normalized_relation)

        events = []
        for event in result.get("event_candidates", []):
            normalized_event = dict(event)
            if "event_id" in normalized_event and "id" not in normalized_event:
                normalized_event["id"] = normalized_event["event_id"]
            if "participants" in normalized_event and "entities" not in normalized_event:
                normalized_event["entities"] = normalized_event["participants"]
            normalized_event.setdefault("source_refs", [source_id])
            normalized_event.setdefault("status", "active")
            events.append(normalized_event)

        return {
            "claims": claims,
            "entity_candidates": entities,
            "relation_candidates": relations,
            "event_candidates": events,
        }

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
    parser.add_argument("--input", "-i", required=True, help="输入文件或目录路径，支持 .md/.json/.txt")
    parser.add_argument("--config", "-c", default="mindvault/config", help="配置目录路径")
    parser.add_argument("--verbose", "-v", action="store_true", help="显示流水线细颗粒度进度信息")
    args = parser.parse_args()

    input_path = Path(args.input)
    if not input_path.exists():
        print(f"❌ 文件不存在: {args.input}")
        sys.exit(1)

    sources = load_sources_from_path(input_path)
    if not sources:
        print(f"❌ 未找到可处理文件: {args.input}")
        sys.exit(1)

    runtime = VaultRuntime(args.workspace, config_root=args.config, verbose=args.verbose)
    result = runtime.ingest(sources)

    print("✅ Pipeline 完成")
    print(json.dumps(result, indent=2, ensure_ascii=False))


def load_sources_from_path(input_path: Path) -> List[Dict[str, Any]]:
    """Load one file or all supported files from a directory."""
    path = Path(input_path)
    if path.is_dir():
        sources: List[Dict[str, Any]] = []
        for child in sorted(path.rglob("*")):
            if child.is_file() and child.suffix.lower() in {".md", ".txt", ".json"}:
                sources.extend(_load_single_input_file(child))
        return sources

    return _load_single_input_file(path)


def _load_single_input_file(path: Path) -> List[Dict[str, Any]]:
    suffix = path.suffix.lower()
    if suffix == ".json":
        raw = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(raw, list):
            return [_normalize_json_source(item, path) for item in raw]
        if isinstance(raw, dict):
            return [_normalize_json_source(raw, path)]
        raise ValueError(f"Unsupported JSON root in {path}: expected object or array.")

    content = path.read_text(encoding="utf-8")
    return [{
        "source_id": path.name,
        "source_type": "doc",
        "content": content,
        "metadata": {"filename": path.name, "relative_path": str(path)},
    }]


def _normalize_json_source(item: Dict[str, Any], path: Path) -> Dict[str, Any]:
    if not isinstance(item, dict):
        raise ValueError(f"Unsupported JSON item in {path}: expected object.")
    return {
        "source_id": item.get("source", item.get("source_id", path.stem)),
        "source_type": item.get("source_type", "doc"),
        "content": item.get("text", item.get("content", "")),
        "metadata": {
            **item.get("metadata", {}),
            "filename": path.name,
            "relative_path": str(path),
        },
    }


if __name__ == "__main__":
    main()
