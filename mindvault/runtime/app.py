"""MindVault app: the unified pipeline entry point for the v2 architecture."""
from __future__ import annotations

import hashlib
import json
import re
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
from mindvault.runtime.renderers.multi_db import MultiDBRenderer
from parser import ParserAgent as RuleParserAgent


class VaultRuntime:
    """
    The MindVault runtime: orchestrates the full knowledge pipeline.

    source → adapt → parse(LLM) → claim_resolve → dedup → relation
      → governance(confidence + conflict + placeholder + schema)
      → merge canonical → version snapshot → insight(optional) → report(optional)
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
        self.runtime_config_path = config_root_path / "runtime_config.json"
        if not self.runtime_config_path.exists():
            self.runtime_config_path = Path("config/runtime_config.json")
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

    def ingest(self, sources: List[Dict[str, Any]], profile: str | None = None) -> Dict[str, Any]:
        """Run the full pipeline on a list of source records."""
        runtime_settings = self._load_runtime_settings()
        profile = (profile or runtime_settings.get("execution", {}).get("profile") or "fast").strip().lower()
        if profile not in {"full", "fast"}:
            profile = "fast"
        report_enabled = bool(runtime_settings.get("artifacts", {}).get("report"))
        task = TaskRuntime(self.ctx.task_dir, goal="Build structured knowledge databases from input sources.", workspace_id=self.ctx.workspace_id)
        task.start()
        self.trace.log("pipeline_start", workspace=self.ctx.workspace_id, source_count=len(sources), task_id=task.task_id)
        task.log_step("pipeline_start", "ok", source_count=len(sources))
        optional_failures: List[Dict[str, str]] = []

        try:
            self._mark_task(task, "ingest", resume_hint="Persisting raw sources.")
            for src in sources:
                src.setdefault("source_id", f"src_{hashlib.md5(src.get('content', '')[:200].encode()).hexdigest()[:8]}")
                detected_type = self._detect_source_type(src)
                if not src.get("source_type") or (src.get("source_type") == "doc" and detected_type != "doc"):
                    src["source_type"] = detected_type
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
                    "context_note": chunk.context_hints.get("note", ""),
                    "speakers": chunk.context_hints.get("speakers", []),
                }
                result = self.executor.execute(parse_agent_path, context)
                self._raise_on_agent_error(result, "parse_agent")

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

            insights: List[Dict[str, Any]] = []
            report_path = ""

            if profile == "full" and report_enabled:
                self._mark_task(task, "insight", agent="insight_generator", resume_hint="Generating insight summaries.")
                insights = self._generate_insights(state)
                self.kb.append_insights(insights)
                state = self.kb.state
                self.trace.log("insight_complete", count=len(insights))
                task.log_step("insight", "ok", count=len(insights))

            self._mark_task(task, "multi_db", agent="database_builder_agent", resume_hint="Building database plan and multi-db outputs.")
            database_plan = self._generate_database_plan(state, governance)
            multi_db = self._generate_multi_db(state, database_plan)
            multi_db_paths = self._export_multi_db(database_plan, multi_db)
            self.trace.log("multi_db_export_complete", data=multi_db_paths.get("data", ""))
            for name, value in multi_db_paths.items():
                task.add_artifact(f"multi_db_{name}", value)
            task.log_step("multi_db", "ok", output=multi_db_paths.get("data", ""))

            if profile == "full" and report_enabled:
                report_path = self._run_optional_stage(
                    task=task,
                    step="report",
                    agent="report_agent",
                    resume_hint="Writing report artifact.",
                    runner=lambda: self._write_report(state, insights, governance),
                    optional_failures=optional_failures,
                )

            trace_path = self.ctx.root_dir / "agent_trace.json"
            self.trace.save(trace_path)
            task.add_artifact("trace", str(trace_path))
            if optional_failures:
                task.state["warnings"] = optional_failures
                task.complete("Pipeline completed with warnings.")
            else:
                task.complete("Pipeline completed successfully.")

            return {
                "workspace": self.ctx.workspace_id,
                "profile": profile,
                "report_enabled": report_enabled,
                "task": {
                    "task_id": task.task_id,
                    "task_json": str(task.task_path),
                    "step_log": str(task.step_log_path),
                },
                "knowledge_base": str(self.ctx.kb_path),
                "snapshot": version_meta.get("snapshot_path", ""),
                "changelog": version_meta.get("changelog_path", ""),
                "report": report_path or "",
                "multi_db": multi_db_paths,
                "trace": str(trace_path),
                "warnings": optional_failures,
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

    def _load_runtime_settings(self) -> Dict[str, Any]:
        fallback = {
            "execution": {"profile": "fast", "engine_mode": "json_engine"},
            "artifacts": {"report": False},
        }
        if not self.runtime_config_path.exists():
            return fallback
        try:
            raw = json.loads(self.runtime_config_path.read_text(encoding="utf-8"))
        except Exception:
            return fallback
        execution = raw.get("execution", {}) if isinstance(raw, dict) else {}
        artifacts = raw.get("artifacts", {}) if isinstance(raw, dict) else {}
        profile = execution.get("profile", "fast")
        if profile not in {"fast", "full"}:
            profile = "fast"
        return {
            "execution": {
                "profile": profile,
                "engine_mode": "json_engine",
            },
            "artifacts": {
                "report": bool(artifacts.get("report", False)),
            },
        }

    # ── Internal helpers ─────────────────────────────────────────────────────

    def _detect_source_type(self, source: Dict[str, Any]) -> str:
        st = source.get("source_type", "")
        if st and st != "doc":
            return st
        content = source.get("content", "")
        note = json.dumps(source.get("context_hints", {}), ensure_ascii=False)
        if self._looks_like_chat(content) or "聊天" in note or "个人数据库" in note or "个人信息数据库" in note:
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
            self._raise_on_agent_error(result, "report_agent")
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

    def _write_report(self, state, insights, governance) -> str:
        report_data = self._generate_report(state, insights, governance)
        self.ctx.report_path.write_text(json.dumps(report_data, indent=2, ensure_ascii=False), encoding="utf-8")
        return str(self.ctx.report_path)

    def _export_multi_db(self, database_plan: Dict[str, Any], multi_db: Dict[str, Any]) -> Dict[str, str]:
        renderer = MultiDBRenderer(self.ctx.root_dir / "multi_db")
        return renderer.render(database_plan, multi_db, include_html=False)

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
            try:
                result = self.executor.execute(ontology_agent_path, context)
                self._raise_on_agent_error(result, "ontology_agent")
                if isinstance(result, dict) and isinstance(result.get("databases"), list):
                    return self._finalize_database_plan(result)
            except Exception as exc:
                self.trace.log("database_plan_fallback", agent="ontology_agent", error=str(exc))
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
            try:
                result = self.executor.execute(database_builder_agent_path, context)
                self._raise_on_agent_error(result, "database_builder_agent")
                if isinstance(result, dict) and isinstance(result.get("databases"), list):
                    return self._finalize_multi_db(result, database_plan)
            except Exception as exc:
                self.trace.log("multi_db_fallback", agent="database_builder_agent", error=str(exc))
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
        self._raise_on_agent_error(result, "wiki_builder_agent")
        if isinstance(result, dict) and isinstance(result.get("pages"), list):
            return result
        return None

    @staticmethod
    def _raise_on_agent_error(result: Dict[str, Any] | Any, agent_name: str) -> None:
        if isinstance(result, dict) and result.get("_agent_error"):
            raise RuntimeError(f"{agent_name} failed: {result['_agent_error']}")

    def _run_optional_stage(
        self,
        *,
        task: TaskRuntime,
        step: str,
        agent: str,
        resume_hint: str,
        runner,
        optional_failures: List[Dict[str, str]],
    ) -> Any:
        self._mark_task(task, step, agent=agent, resume_hint=resume_hint)
        try:
            result = runner()
            self.trace.log(f"{step}_complete")
            if isinstance(result, dict):
                output = result.get("index", "") or result.get("data", "")
                task.log_step(step, "ok", output=output)
            else:
                task.log_step(step, "ok", output=result or "")
            if step == "report" and isinstance(result, str):
                task.add_artifact("report", result)
            if step == "dashboard" and isinstance(result, str):
                task.add_artifact("dashboard", result)
            return result
        except Exception as exc:
            error_text = str(exc)
            optional_failures.append({
                "step": step,
                "agent": agent,
                "error": error_text,
            })
            self.trace.log("optional_stage_failed", stage=step, agent=agent, error=error_text)
            task.log_step(step, "failed", agent=agent, error=error_text)
            task.heartbeat(step=step, agent=agent, resume_hint=f"{step} failed: {error_text}")
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
            rows = [self._normalize_row_shape(item) for item in rows]
            columns = self._collect_columns(rows)
            databases.append({
                "name": name,
                "title": database.get("title", name),
                "description": database.get("description", ""),
                "visibility": database.get("visibility", self._infer_database_visibility(name)),
                "primary_key": self._infer_primary_key(columns),
                "columns": columns,
                "rows": rows,
            })

        return {
            "domain": database_plan.get("domain", "MindVault Multi-DB"),
            "generated_at": datetime.utcnow().isoformat(),
            "databases": databases,
            "relations": self._merge_relation_defs(
                self._fallback_relation_defs(state, database_plan.get("databases", [])),
                self._infer_relations_from_multi_db(databases),
            ),
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
            rows = [self._normalize_row_shape(item) for item in row.get("rows", [])]
            inferred_columns = self._collect_columns(rows)
            planned_fields = [field for field in plan_row.get("suggested_fields", []) if field not in inferred_columns]
            row.setdefault("title", plan_row.get("title", row.get("name", "")))
            row.setdefault("description", plan_row.get("description", ""))
            row.setdefault("visibility", plan_row.get("visibility", self._infer_database_visibility(row.get("name", ""))))
            row["rows"] = rows
            row["columns"] = inferred_columns + planned_fields
            row.setdefault("primary_key", self._infer_primary_key(row["columns"]))
            normalized.append(row)
        payload["databases"] = normalized
        payload["relations"] = self._merge_relation_defs(
            payload.get("relations", []),
            self._infer_relations_from_multi_db(normalized),
        )
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
            "entity_id": entity.get("id", ""),
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
            "claim_id": claim.get("id", claim.get("claim_id", "")),
            "subject": claim.get("subject", ""),
            "predicate": claim.get("predicate", ""),
            "object": claim.get("object", ""),
            "claim_text": claim.get("claim_text", ""),
            "claim_type": claim.get("claim_type", ""),
            "confidence": claim.get("confidence", 0),
            "source_ref": claim.get("source_ref", ""),
        }

    @staticmethod
    def _flatten_relation_row(relation: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "id": f"{relation.get('source', '')}:{relation.get('relation', '')}:{relation.get('target', '')}",
            "relation_id": f"{relation.get('source', '')}:{relation.get('relation', '')}:{relation.get('target', '')}",
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
    def _normalize_row_shape(row: Dict[str, Any]) -> Dict[str, Any]:
        normalized: Dict[str, Any] = {}
        for key, value in row.items():
            if isinstance(value, dict):
                flattened = {
                    f"{key}_{child_key}": child_value
                    for child_key, child_value in value.items()
                    if not isinstance(child_value, (dict, list))
                }
                if flattened:
                    normalized.update(flattened)
                else:
                    normalized[key] = value
            else:
                normalized[key] = value
        return normalized

    @staticmethod
    def _infer_primary_key(columns: List[str]) -> str:
        for candidate in ["id", "entity_id", "event_id", "claim_id", "relation_id", "source_id", "name", "title"]:
            if candidate in columns:
                return candidate
        return columns[0] if columns else "id"

    def _infer_relations_from_multi_db(self, databases: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        id_registry: Dict[str, tuple[str, str]] = {}
        for database in databases:
            db_name = database.get("name", "")
            primary_key = database.get("primary_key", "id")
            for row in database.get("rows", []):
                value = row.get(primary_key) or row.get("id") or row.get("entity_id") or row.get("event_id") or row.get("claim_id")
                if value:
                    id_registry[str(value)] = (db_name, primary_key if primary_key in row else "id")

        relation_defs: List[Dict[str, Any]] = []
        for database in databases:
            from_db = database.get("name", "")
            for row in database.get("rows", []):
                for field, value in row.items():
                    targets = value if isinstance(value, list) else [value]
                    for target in targets:
                        if not isinstance(target, str):
                            continue
                        target_meta = id_registry.get(target)
                        if not target_meta:
                            continue
                        to_db, to_field = target_meta
                        if from_db == to_db and field == to_field:
                            continue
                        relation_defs.append({
                            "from_db": from_db,
                            "from_field": field,
                            "to_db": to_db,
                            "to_field": to_field,
                            "relation_type": "many_to_many" if isinstance(value, list) else "many_to_one",
                        })
        return relation_defs

    @staticmethod
    def _merge_relation_defs(*relation_groups: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        merged: List[Dict[str, Any]] = []
        seen = set()
        for group in relation_groups:
            for row in group or []:
                key = tuple(row.get(k, "") for k in ["from_db", "from_field", "to_db", "to_field", "relation_type"])
                if key in seen:
                    continue
                seen.add(key)
                merged.append(row)
        return merged

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
        if chunk.context_hints.get("source_type") == "chat" or self._looks_like_chat(chunk.text):
            return self._fallback_parse_chat_chunk(chunk)
        docs = [{
            "text": chunk.text,
            "source": chunk.source_id,
            "timestamp": datetime.utcnow().isoformat(),
            "speaker": chunk.context_hints.get("author", "unknown"),
        }]
        result = self.rule_parser.parse(docs, workspace_id=self.ctx.workspace_id)
        return self._normalize_parse_result(result, chunk)

    @staticmethod
    def _looks_like_chat(text: str) -> bool:
        lines = [line.strip() for line in str(text).splitlines() if line.strip()]
        if len(lines) < 3:
            return False
        chat_lines = 0
        for line in lines[:30]:
            if ":" in line or "：" in line or line.startswith("["):
                prefix = re.split(r"[:：\]]", line, maxsplit=1)[0].strip("[ ")
                if 0 < len(prefix) <= 24:
                    chat_lines += 1
        return chat_lines >= max(3, len(lines) // 3)

    def _fallback_parse_chat_chunk(self, chunk) -> Dict[str, Any]:
        messages = ChatAdapter._parse_messages(chunk.text)
        source_id = chunk.source_id
        speakers = []
        for message in messages:
            author = (message.get("author") or "unknown").strip()
            if author and author not in speakers and author != "unknown":
                speakers.append(author)

        speaker_ids = {speaker: f"ent_person_{self._slug_name(speaker)}" for speaker in speakers}
        entities: List[Dict[str, Any]] = []
        claims: List[Dict[str, Any]] = []
        relations: List[Dict[str, Any]] = []
        events: List[Dict[str, Any]] = []

        speaker_stats: Dict[str, Dict[str, Any]] = {
            speaker: {"message_count": 0, "tone_tags": set(), "signals": []} for speaker in speakers
        }

        primary_target = speakers[1] if len(speakers) == 2 else ""
        for idx, message in enumerate(messages, start=1):
            speaker = (message.get("author") or "unknown").strip()
            text = (message.get("text") or "").strip()
            if not text or speaker == "unknown" or speaker not in speaker_ids:
                continue

            speaker_stats[speaker]["message_count"] += 1
            target = next((name for name in speakers if name != speaker), primary_target)
            tone_tags = self._chat_tone_tags(text)
            speaker_stats[speaker]["tone_tags"].update(tone_tags)

            for predicate, value, claim_type in self._chat_claims_for_message(text, speaker, target, source_id, idx):
                claims.append({
                    "claim_id": f"claim_chat_{idx}_{len(claims)+1:03d}",
                    "subject": speaker_ids[speaker],
                    "predicate": predicate,
                    "object": value,
                    "claim_text": text,
                    "claim_type": claim_type,
                    "confidence": 0.62 if claim_type == "fact" else 0.52,
                    "source_ref": source_id,
                    "source_refs": [source_id],
                    "status": "active",
                })

            event_type = self._chat_event_type(text)
            if event_type:
                participant_ids = [speaker_ids[speaker]]
                if target and target in speaker_ids:
                    participant_ids.append(speaker_ids[target])
                events.append({
                    "event_id": f"evt_chat_{idx:03d}",
                    "type": event_type,
                    "description": text,
                    "participants": participant_ids,
                    "timestamp": None,
                    "source_refs": [source_id],
                    "status": "active",
                })

            if target and target in speaker_ids:
                relation_type = self._chat_relation_type(text)
                if relation_type:
                    relations.append({
                        "source_entity": speaker_ids[speaker],
                        "target_entity": speaker_ids[target],
                        "relation_type": relation_type,
                        "evidence": text,
                        "source_refs": [source_id],
                        "status": "active",
                    })

        for speaker in speakers:
            stats = speaker_stats[speaker]
            attributes = {
                "message_count": stats["message_count"],
                "tone_tags": sorted(stats["tone_tags"]),
            }
            entities.append({
                "entity_id": speaker_ids[speaker],
                "type": "person",
                "name": speaker,
                "attributes": attributes,
                "source_refs": [source_id],
                "status": "active",
            })

        if len(speakers) == 2:
            relations.append({
                "source_entity": speaker_ids[speakers[0]],
                "target_entity": speaker_ids[speakers[1]],
                "relation_type": "interacts_with",
                "evidence": f"对话中双方都有发言，共 {len(messages)} 条消息。",
                "source_refs": [source_id],
                "status": "active",
            })

        return self._normalize_parse_result({
            "claims": claims,
            "entity_candidates": entities,
            "relation_candidates": relations,
            "event_candidates": events,
        }, chunk)

    @staticmethod
    def _chat_tone_tags(text: str) -> List[str]:
        tags: List[str] = []
        if any(token in text for token in ["哈哈", "笑死", "😄", "😂"]):
            tags.append("轻松")
        if any(token in text for token in ["宝宝", "宝贝", "好看", "可爱", "🥺", "👉"]):
            tags.append("亲密")
        if any(token in text for token in ["困", "睡", "累"]):
            tags.append("疲惫")
        if any(token in text for token in ["侮辱", "背刺", "骂", "攻击", "尖酸刻薄"]):
            tags.append("冲突")
        return tags

    def _chat_claims_for_message(self, text: str, speaker: str, target: str, source_id: str, idx: int):
        claims: List[tuple[str, str, str]] = []
        if any(token in text for token in ["好看", "可爱", "精致"]):
            claims.append(("positive_impression", text, "opinion"))
        if any(token in text for token in ["喜欢", "忍不住", "舍不得"]):
            claims.append(("preference_signal", text, "opinion"))
        if text.startswith("我") and any(token in text for token in ["困", "睡", "不困"]):
            claims.append(("current_state", text, "fact"))
        if any(token in text for token in ["去睡吧", "慢慢来", "没事"]):
            claims.append(("care_signal", text, "fact"))
        if any(token in text for token in ["拒绝", "不要"]):
            claims.append(("boundary_signal", text, "fact"))
        if any(token in text for token in ["侮辱", "背刺", "攻击", "骂", "尖酸刻薄"]):
            claims.append(("conflict_experience", text, "fact"))
        if any(token in text for token in ["问", "吗", "？", "?"]):
            claims.append(("question_or_confirmation", text, "uncertain"))
        if target and any(token in text for token in ["宝宝", "宝贝"]):
            claims.append(("addresses_affectionately", target, "fact"))
        return claims

    @staticmethod
    def _chat_event_type(text: str) -> str:
        if any(token in text for token in ["侮辱", "背刺", "攻击", "骂", "尖酸刻薄"]):
            return "conflict_discussion"
        if any(token in text for token in ["好看", "可爱", "精致"]):
            return "compliment"
        if any(token in text for token in ["去睡吧", "慢慢来", "没事"]):
            return "care"
        if any(token in text for token in ["困", "睡"]):
            return "rest_discussion"
        if any(token in text for token in ["问", "吗", "？", "?"]):
            return "question"
        return ""

    @staticmethod
    def _chat_relation_type(text: str) -> str:
        if any(token in text for token in ["宝宝", "宝贝", "好看", "可爱", "🥺", "👉"]):
            return "close_to"
        if any(token in text for token in ["去睡吧", "慢慢来", "没事"]):
            return "cares_about"
        if any(token in text for token in ["拒绝", "不要"]):
            return "sets_boundary_with"
        if any(token in text for token in ["侮辱", "背刺", "攻击", "骂", "尖酸刻薄"]):
            return "shares_conflict_with"
        return ""

    @staticmethod
    def _slug_name(value: str) -> str:
        return "".join(ch.lower() if ch.isalnum() else "_" for ch in value).strip("_") or "unknown"

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
    parser.add_argument("--profile", choices=["fast", "full"], default=None, help="运行档位：未指定时跟随运行设置")
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
    result = runtime.ingest(sources, profile=args.profile)

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
