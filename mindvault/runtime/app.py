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
from mindvault.runtime.task_runtime import TaskRuntime
from mindvault.adapters.doc_adapter import DocAdapter
from mindvault.adapters.chat_adapter import ChatAdapter
from mindvault.governance.confidence_engine import ConfidenceEngine
from mindvault.governance.conflict_engine import ConflictEngine
from mindvault.governance.placeholder_engine import PlaceholderEngine
from mindvault.governance.schema_evolution import SchemaEvolutionEngine
from mindvault.governance.memory_curator import MemoryCurator
from mindvault.runtime.renderers.wiki import WikiExporter
from mindvault.runtime.renderers.multi_db import MultiDBRenderer
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
        task = TaskRuntime(self.ctx.task_dir, goal="Build structured knowledge databases from input sources.", workspace_id=self.ctx.workspace_id)
        task.start()
        self.trace.log("pipeline_start", workspace=self.ctx.workspace_id, source_count=len(sources), task_id=task.task_id)
        task.log_step("pipeline_start", "ok", source_count=len(sources))

        try:
            self._mark_task(task, "ingest", resume_hint="Persisting raw sources.")
            for src in sources:
                src.setdefault("source_id", f"src_{hashlib.md5(src.get('content', '')[:200].encode()).hexdigest()[:8]}")
                src.setdefault("source_type", self._detect_source_type(src))
                src.setdefault("ingested_at", datetime.utcnow().isoformat())

            raw_path = self.ctx.raw_dir / "sources.json"
            self._append_json(raw_path, sources)
            self.trace.log("ingest_complete", source_count=len(sources))
            task.log_step("ingest", "ok", sources=len(sources), output=str(raw_path))
            task.add_artifact("sources", str(raw_path))

            self._mark_task(task, "adapt", resume_hint="Adapting sources into normalized chunks.")
            all_chunks = []
            for src in sources:
                adapter = self._adapters.get(src["source_type"], DocAdapter())
                chunks = adapter.adapt(src)
                all_chunks.extend(chunks)
            self.trace.log("adapt_complete", chunk_count=len(all_chunks))
            task.log_step("adapt", "ok", chunks=len(all_chunks))

            self._mark_task(task, "parse", agent="parse_agent", resume_hint="Parsing chunks into atomic knowledge.")
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
                    task.log_step("parse_chunk", "fallback", chunk_id=chunk.chunk_id)

            self.trace.log("parse_complete",
                           claims=len(all_claims), entities=len(all_entities),
                           relations=len(all_relations), events=len(all_events))
            task.log_step("parse", "ok", claims=len(all_claims), entities=len(all_entities), relations=len(all_relations), events=len(all_events))

            extracted_path = self._save_extracted(all_claims, all_entities, all_relations, all_events)
            task.add_artifact("extracted", str(extracted_path))

            self._mark_task(task, "confidence", resume_hint="Scoring and annotating extracted knowledge.")
            for claim in all_claims:
                claim["confidence"] = self.confidence.score_claim(claim)
            self.confidence.annotate_items(all_entities)
            self.confidence.annotate_items(all_relations)
            self.trace.log("confidence_complete")
            task.log_step("confidence", "ok")

            self._mark_task(task, "schema", agent="schema_engine", resume_hint="Designing evolving schema.")
            fragment = {
                "entity_candidates": all_entities,
                "relation_candidates": all_relations,
                "event_candidates": all_events,
                "claims": all_claims,
            }
            schema_info = self.schema_evo.evolve(fragment)
            fragment["schema"] = schema_info["schema"]
            self.trace.log("schema_evolution_complete", promoted=schema_info["schema_candidates"].get("recent_promotions", {}))
            task.log_step("schema", "ok")

            self._mark_task(task, "curation", agent="memory_curator", resume_hint="Selecting canonical entities.")
            curated = self.memory_curator.curate(all_entities)
            self.trace.log("memory_curation_complete", promote=len(curated["promote"]), hold=len(curated["hold"]))
            fragment["entity_candidates"] = curated["promote"]
            if curated["hold"]:
                hold_path = self.ctx.governance_dir / "held_entities.json"
                self._append_json(hold_path, curated["hold"])
                task.add_artifact("held_entities", str(hold_path))
            task.log_step("curation", "ok", promote=len(curated["promote"]), hold=len(curated["hold"]))

            self._mark_task(task, "merge", agent="knowledge_store", resume_hint="Merging fragment into canonical KB.")
            state = self.kb.merge(fragment)
            self.trace.log("merge_complete")
            task.add_artifact("knowledge_base", str(self.ctx.kb_path))
            task.log_step("merge", "ok", entities=len(state.get("entities", [])))

            self._mark_task(task, "governance", agent="governance", resume_hint="Auditing conflicts and placeholders.")
            conflict_result = self.conflicts.audit(state)
            self.trace.log("conflict_audit_complete", unresolved=conflict_result.get("unresolved_count", 0))

            ph_records = self.placeholders.scan(state)
            state["placeholders"] = ph_records
            ph_path = self.ctx.governance_dir / "placeholders.json"
            ph_path.write_text(json.dumps(ph_records, indent=2, ensure_ascii=False), encoding="utf-8")
            self.trace.log("placeholder_scan_complete", count=len(ph_records))
            task.add_artifact("placeholders", str(ph_path))
            task.log_step("governance", "ok", conflicts=conflict_result.get("unresolved_count", 0), placeholders=len(ph_records))

            self._mark_task(task, "versioning", agent="version_store", resume_hint="Snapshotting canonical state.")
            governance = {
                "conflicts": conflict_result,
                "schema_candidates": schema_info["schema_candidates"],
                "placeholders": ph_records,
            }
            version_meta = self.version_store.create_snapshot(state, governance)
            self.kb.add_version_record(version_meta)
            self.trace.log("version_snapshot_complete", version=version_meta["version"])
            task.add_artifact("snapshot", version_meta.get("snapshot_path", ""))
            task.add_artifact("changelog", version_meta.get("changelog_path", ""))
            task.log_step("versioning", "ok", version=version_meta.get("version"))

            self._mark_task(task, "insight", agent="insight_generator", resume_hint="Generating insight summaries.")
            insights = self._generate_insights(state)
            self.kb.append_insights(insights)
            state = self.kb.state
            self.trace.log("insight_complete", count=len(insights))
            task.log_step("insight", "ok", count=len(insights))

            self._mark_task(task, "report", agent="report_agent", resume_hint="Writing report artifact.")
            report_data = self._generate_report(state, insights, governance)
            self.ctx.report_path.write_text(json.dumps(report_data, indent=2, ensure_ascii=False), encoding="utf-8")
            self.trace.log("report_complete")
            task.add_artifact("report", str(self.ctx.report_path))
            task.log_step("report", "ok", output=str(self.ctx.report_path))

            self._mark_task(task, "dashboard", agent="dashboard_renderer", resume_hint="Rendering dashboard.")
            dashboard_path = self._render_dashboard(state, governance, version_meta)
            self.trace.log("dashboard_complete")
            task.add_artifact("dashboard", dashboard_path)
            task.log_step("dashboard", "ok", output=dashboard_path)

            self._mark_task(task, "multi_db", agent="database_builder_agent", resume_hint="Building database plan and multi-db outputs.")
            database_plan = self._generate_database_plan(state, governance)
            multi_db = self._generate_multi_db(state, database_plan)
            multi_db_paths = self._export_multi_db(database_plan, multi_db)
            self.trace.log("multi_db_export_complete", data=multi_db_paths.get("data", ""))
            for name, value in multi_db_paths.items():
                task.add_artifact(f"multi_db_{name}", value)
            task.log_step("multi_db", "ok", output=multi_db_paths.get("data", ""))

            self._mark_task(task, "wiki", agent="wiki_builder_agent", resume_hint="Rendering wiki pages.")
            wiki_paths = self._export_wiki(state, governance, version_meta)
            self.trace.log("wiki_export_complete", index=wiki_paths.get("index", ""))
            for name, value in wiki_paths.items():
                if isinstance(value, str):
                    task.add_artifact(f"wiki_{name}", value)
            task.log_step("wiki", "ok", output=wiki_paths.get("index", ""))

            trace_path = self.ctx.root_dir / "agent_trace.json"
            self.trace.save(trace_path)
            task.add_artifact("trace", str(trace_path))
            task.complete("Pipeline completed successfully.")

            return {
                "workspace": self.ctx.workspace_id,
                "task": {
                    "task_id": task.task_id,
                    "task_json": str(task.task_path),
                    "step_log": str(task.step_log_path),
                },
                "knowledge_base": str(self.ctx.kb_path),
                "snapshot": version_meta.get("snapshot_path", ""),
                "changelog": version_meta.get("changelog_path", ""),
                "report": str(self.ctx.report_path),
                "dashboard": dashboard_path,
                "multi_db": multi_db_paths,
                "wiki": wiki_paths,
                "trace": str(trace_path),
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
        except Exception as exc:
            task.log_step("pipeline", "failed", error=str(exc))
            task.fail(str(exc), step=task.state.get("current_step", ""))
            self.trace.log("pipeline_failed", error=str(exc), task_id=task.task_id)
            self.trace.save(self.ctx.root_dir / "agent_trace.json")
            raise

    # ── Internal helpers ─────────────────────────────────────────────────────

    def _detect_source_type(self, source: Dict[str, Any]) -> str:
        st = source.get("source_type", "")
        if st:
            return st
        content = source.get("content", "")
        if any(k in content for k in ["[", "：", ":"]) and "\n" in content:
            return "chat"
        return "doc"

    def _save_extracted(self, claims, entities, relations, events) -> Path:
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
        return path

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

    def _export_multi_db(self, database_plan: Dict[str, Any], multi_db: Dict[str, Any]) -> Dict[str, str]:
        renderer = MultiDBRenderer(self.ctx.root_dir / "multi_db")
        return renderer.render(database_plan, multi_db)

    def _generate_database_plan(self, state, governance) -> Dict[str, Any]:
        ontology_agent_path = Path("mindvault/agents/ontology_agent.yaml")
        if ontology_agent_path.exists():
            context = {
                "entities": state.get("entities", []),
                "claims": state.get("claims", []),
                "relations": state.get("relations", []),
                "events": state.get("events", []),
                "governance": governance,
            }
            result = self.executor.execute(ontology_agent_path, context)
            if isinstance(result, dict) and isinstance(result.get("databases"), list):
                return self._finalize_database_plan(result)
        return self._finalize_database_plan(self._fallback_database_plan(state))

    def _generate_multi_db(self, state, database_plan: Dict[str, Any]) -> Dict[str, Any]:
        database_builder_agent_path = Path("mindvault/agents/database_builder_agent.yaml")
        if database_builder_agent_path.exists():
            context = {
                "database_plan": database_plan,
                "entities": state.get("entities", []),
                "claims": state.get("claims", []),
                "relations": state.get("relations", []),
                "events": state.get("events", []),
            }
            result = self.executor.execute(database_builder_agent_path, context)
            if isinstance(result, dict) and isinstance(result.get("databases"), list):
                return self._finalize_multi_db(result, database_plan)
        return self._finalize_multi_db(self._fallback_multi_db(state, database_plan), database_plan)

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

    def _fallback_database_plan(self, state: Dict[str, Any]) -> Dict[str, Any]:
        entity_types = sorted({entity.get("type", "unknown") for entity in state.get("entities", [])})
        databases = []
        if entity_types:
            for entity_type in entity_types:
                fields = {"id", "name", "type", "confidence", "source_refs"}
                for entity in state.get("entities", []):
                    if entity.get("type") == entity_type:
                        fields.update(entity.get("attributes", {}).keys())
                databases.append({
                    "name": f"{entity_type}s",
                    "title": entity_type,
                    "description": f"Stores {entity_type} entities.",
                    "entity_types": [entity_type],
                    "suggested_fields": list(fields),
                    "visibility": "business",
                })
        for name, title, desc in [
            ("claims", "claims", "Atomic statements extracted from sources."),
            ("relations", "relations", "Cross-record links."),
            ("sources", "sources", "Source references and provenance."),
        ]:
            databases.append({
                "name": name,
                "title": title,
                "description": desc,
                "entity_types": [],
                "suggested_fields": ["id"],
                "visibility": "system",
            })
        return {
            "domain": "MindVault Multi-DB",
            "generated_at": datetime.utcnow().isoformat(),
            "databases": databases,
            "relations": self._fallback_relation_defs(state, databases),
        }

    def _fallback_multi_db(self, state: Dict[str, Any], database_plan: Dict[str, Any]) -> Dict[str, Any]:
        databases: List[Dict[str, Any]] = []
        entities = state.get("entities", [])

        for database in database_plan.get("databases", []):
            name = database.get("name", "")
            entity_types = set(database.get("entity_types", []))
            if name == "claims":
                rows = [self._flatten_claim_row(claim) for claim in state.get("claims", [])]
            elif name == "relations":
                rows = [self._flatten_relation_row(relation) for relation in state.get("relations", [])]
            elif name == "sources":
                rows = self._build_source_rows(state)
            else:
                rows = [
                    self._flatten_entity_row(entity)
                    for entity in entities
                    if not entity_types or entity.get("type") in entity_types
                ]
            columns = self._collect_columns(rows)
            databases.append({
                "name": name,
                "title": database.get("title", name),
                "description": database.get("description", ""),
                "visibility": database.get("visibility", self._infer_database_visibility(name)),
                "primary_key": "id",
                "columns": columns,
                "rows": rows,
            })

        return {
            "domain": database_plan.get("domain", "MindVault Multi-DB"),
            "generated_at": datetime.utcnow().isoformat(),
            "databases": databases,
            "relations": self._fallback_relation_defs(state, database_plan.get("databases", [])),
        }

    def _finalize_database_plan(self, database_plan: Dict[str, Any]) -> Dict[str, Any]:
        plan = dict(database_plan)
        normalized = []
        for database in plan.get("databases", []):
            row = dict(database)
            name = row.get("name", "")
            row.setdefault("visibility", self._infer_database_visibility(name))
            normalized.append(row)
        plan["databases"] = normalized
        return plan

    def _finalize_multi_db(self, multi_db: Dict[str, Any], database_plan: Dict[str, Any]) -> Dict[str, Any]:
        payload = dict(multi_db)
        plan_map = {database.get("name", ""): database for database in database_plan.get("databases", [])}
        normalized = []
        for database in payload.get("databases", []):
            row = dict(database)
            plan_row = plan_map.get(row.get("name", ""), {})
            row.setdefault("title", plan_row.get("title", row.get("name", "")))
            row.setdefault("description", plan_row.get("description", ""))
            row.setdefault("visibility", plan_row.get("visibility", self._infer_database_visibility(row.get("name", ""))))
            normalized.append(row)
        payload["databases"] = normalized
        return payload

    def _fallback_relation_defs(self, state: Dict[str, Any], databases: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        db_names = {db.get("name", "") for db in databases}
        relation_defs = []
        if "claims" in db_names:
            relation_defs.append({
                "from_db": "claims",
                "from_field": "subject",
                "to_db": "entities",
                "to_field": "id",
                "relation_type": "many_to_one",
            })
        if "relations" in db_names:
            relation_defs.append({
                "from_db": "relations",
                "from_field": "source",
                "to_db": "entities",
                "to_field": "id",
                "relation_type": "many_to_one",
            })
            relation_defs.append({
                "from_db": "relations",
                "from_field": "target",
                "to_db": "entities",
                "to_field": "id",
                "relation_type": "many_to_one",
            })
        for relation in state.get("relations", []):
            source_type = self._entity_type_for_id(state.get("entities", []), relation.get("source", ""))
            target_type = self._entity_type_for_id(state.get("entities", []), relation.get("target", ""))
            if source_type and target_type:
                relation_defs.append({
                    "from_db": f"{source_type}s",
                    "from_field": "id",
                    "to_db": f"{target_type}s",
                    "to_field": "id",
                    "relation_type": relation.get("relation", "linked_to"),
                })
        deduped = []
        seen = set()
        for row in relation_defs:
            key = tuple(row.get(k, "") for k in ["from_db", "from_field", "to_db", "to_field", "relation_type"])
            if key not in seen:
                seen.add(key)
                deduped.append(row)
        return deduped

    @staticmethod
    def _flatten_entity_row(entity: Dict[str, Any]) -> Dict[str, Any]:
        row = {
            "id": entity.get("id", ""),
            "name": entity.get("name", ""),
            "type": entity.get("type", ""),
            "confidence": entity.get("confidence", 0),
            "source_refs": entity.get("source_refs", []),
            "updated_at": entity.get("updated_at", ""),
        }
        row.update(entity.get("attributes", {}))
        return row

    @staticmethod
    def _flatten_claim_row(claim: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "id": claim.get("id", claim.get("claim_id", "")),
            "subject": claim.get("subject", ""),
            "predicate": claim.get("predicate", ""),
            "object": claim.get("object", ""),
            "claim_type": claim.get("claim_type", ""),
            "confidence": claim.get("confidence", 0),
            "source_ref": claim.get("source_ref", ""),
        }

    @staticmethod
    def _flatten_relation_row(relation: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "id": f"{relation.get('source', '')}:{relation.get('relation', '')}:{relation.get('target', '')}",
            "source": relation.get("source", ""),
            "relation": relation.get("relation", ""),
            "target": relation.get("target", ""),
            "confidence": relation.get("confidence", 0),
            "source_refs": relation.get("source_refs", []),
        }

    @staticmethod
    def _build_source_rows(state: Dict[str, Any]) -> List[Dict[str, Any]]:
        counts: Dict[str, int] = {}
        for entity in state.get("entities", []):
            for source_ref in entity.get("source_refs", []):
                counts[source_ref] = counts.get(source_ref, 0) + 1
        for claim in state.get("claims", []):
            source_ref = claim.get("source_ref", "")
            if source_ref:
                counts[source_ref] = counts.get(source_ref, 0) + 1
        return [{"id": source_ref, "name": source_ref, "mentions": count} for source_ref, count in sorted(counts.items())]

    @staticmethod
    def _collect_columns(rows: List[Dict[str, Any]]) -> List[str]:
        columns: List[str] = []
        seen = set()
        for row in rows:
            for key in row.keys():
                if key not in seen:
                    seen.add(key)
                    columns.append(key)
        return columns or ["id"]

    @staticmethod
    def _entity_type_for_id(entities: List[Dict[str, Any]], entity_id: str) -> str:
        for entity in entities:
            if entity.get("id") == entity_id:
                return entity.get("type", "")
        return ""

    @staticmethod
    def _infer_database_visibility(name: str) -> str:
        return "system" if name in {"claims", "relations", "sources"} else "business"

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

    @staticmethod
    def _mark_task(task: TaskRuntime, step: str, *, agent: str = "", resume_hint: str = "") -> None:
        task.heartbeat(step=step, agent=agent, resume_hint=resume_hint)
        task.log_step(step, "running", agent=agent, resume_hint=resume_hint)


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
    metadata = {
        **item.get("metadata", {}),
        "filename": path.name,
        "relative_path": str(path),
    }
    return {
        "source_id": item.get("source", item.get("source_id", path.stem)),
        "source_type": item.get("source_type", "doc"),
        "content": item.get("text", item.get("content", "")),
        "metadata": metadata,
        "context_hints": dict(item.get("context_hints", {})),
    }


if __name__ == "__main__":
    main()
