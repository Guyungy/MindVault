"""MindVault app: the unified pipeline entry point for the v2 architecture."""
from __future__ import annotations

import hashlib
import json
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Tuple

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


class VaultRuntime:
    """
    Thin LLM-first runtime.

    The runtime should only orchestrate task state, artifact persistence,
    structured-output validation, and model execution. Knowledge interpretation,
    table semantics, and explanation strategy belong to the four main agents.
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
                    raise RuntimeError(f"parse_agent returned no structured output for chunk {chunk.chunk_id}")

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
                insights = self._generate_insights(state, governance)
                self.kb.append_insights(insights)
                state = self.kb.state
                self.trace.log("insight_complete", count=len(insights))
                task.log_step("insight", "ok", count=len(insights))

            self._mark_task(task, "database_plan", agent="ontology_agent", resume_hint="Planning business tables and relationships.")
            database_plan = self._generate_database_plan(state, governance)
            database_plan_path = self._write_database_plan(database_plan)
            task.add_artifact("database_plan", str(database_plan_path))
            task.log_step("database_plan", "ok", databases=len(database_plan.get("databases", [])), output=str(database_plan_path))

            self._mark_task(task, "multi_db", agent="database_builder_agent", resume_hint="Building structured tables from the approved plan.")
            multi_db, multi_db_warnings = self._generate_multi_db(state, database_plan)
            multi_db_paths = self._export_multi_db(database_plan, multi_db)
            self.trace.log("multi_db_export_complete", data=multi_db_paths.get("data", ""))
            for name, value in multi_db_paths.items():
                task.add_artifact(f"multi_db_{name}", value)
            task.log_step(
                "multi_db",
                "ok",
                output=multi_db_paths.get("data", ""),
                warnings=len(multi_db_warnings),
                tables=len(multi_db.get("databases", [])),
            )
            optional_failures.extend(multi_db_warnings)

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
            current_step = task.state.get("current_step", "")
            current_agent = task.state.get("current_agent", "")
            if current_step and current_step != "pipeline":
                task.log_step(current_step, "failed", agent=current_agent, error=str(exc))
            task.log_step("pipeline", "failed", error=str(exc))
            task.fail(str(exc), step=current_step)
            self.trace.log("pipeline_failed", error=str(exc), task_id=task.task_id)
            self.trace.save(self.ctx.root_dir / "agent_trace.json")
            raise

    def _load_runtime_settings(self) -> Dict[str, Any]:
        fallback = {
            "execution": {"profile": "fast", "engine_mode": "llm_only"},
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
                "engine_mode": "llm_only",
            },
            "artifacts": {
                "report": bool(artifacts.get("report", False)),
            },
        }

    # ── Internal helpers ─────────────────────────────────────────────────────

    def _detect_source_type(self, source: Dict[str, Any]) -> str:
        st = str(source.get("source_type", "")).strip().lower()
        if st and st != "doc":
            return st
        hints = source.get("context_hints", {}) or {}
        metadata = source.get("metadata", {}) or {}
        content = str(source.get("content", "") or "")
        hint_text = " ".join(
            [
                str(hints.get("source_type", "")),
                str(hints.get("note", "")),
                str(metadata.get("filename", "")),
                str(metadata.get("origin", "")),
            ]
        ).lower()
        if any(token in hint_text for token in ["chat", "conversation", "message", "messages", "聊天", "对话"]):
            return "chat"
        if self._looks_like_chat_content(content):
            return "chat"
        return "doc"

    @staticmethod
    def _looks_like_chat_content(content: str) -> bool:
        text = str(content or "").strip()
        if not text:
            return False
        lines = [line.strip() for line in text.splitlines() if line.strip()]
        if not lines:
            return False
        chat_like = 0
        for line in lines[:80]:
            if len(line) > 240:
                continue
            if re.match(r"^\[.+?\]\s+\S+", line):
                chat_like += 1
                continue
            if re.match(r"^[^:：\n]{1,30}[:：]\s*.+", line):
                chat_like += 1
        return chat_like >= 4

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

    def _generate_insights(self, state: Dict[str, Any], governance: Dict[str, Any]) -> List[Dict[str, Any]]:
        insight_agent_path = Path("mindvault/agents/insight_agent.yaml")
        if not insight_agent_path.exists():
            raise RuntimeError("insight_agent definition not found")
        context = {
            "entities": state.get("entities", []),
            "claims": state.get("claims", []),
            "relations": state.get("relations", []),
            "events": state.get("events", []),
            "governance": governance,
        }
        result = self.executor.execute(insight_agent_path, context)
        self._raise_on_agent_error(result, "insight_agent")
        if isinstance(result, dict) and isinstance(result.get("insights"), list):
            return result["insights"]
        if isinstance(result, dict) and isinstance(result.get("items"), list):
            return result["items"]
        raise RuntimeError("insight_agent returned no structured insights output")

    def _generate_report(self, state, insights, governance) -> Dict[str, Any]:
        report_agent_path = Path("mindvault/agents/report_agent.yaml")
        if not report_agent_path.exists():
            raise RuntimeError("report_agent definition not found")
        context = {
            "entities": state.get("entities", []),
            "claims": state.get("claims", []),
            "relations": state.get("relations", []),
            "events": state.get("events", []),
            "insights": insights,
            "governance": governance,
        }
        result = self.executor.execute(report_agent_path, context)
        self._raise_on_agent_error(result, "report_agent")
        if isinstance(result, dict) and "business_domain" in result:
            return result
        if isinstance(result, dict) and result.get("content") and isinstance(result["content"], dict):
            return result["content"]
        raise RuntimeError("report_agent returned no structured report output")

    def _write_report(self, state, insights, governance) -> str:
        report_data = self._generate_report(state, insights, governance)
        self.ctx.report_path.write_text(json.dumps(report_data, indent=2, ensure_ascii=False), encoding="utf-8")
        return str(self.ctx.report_path)

    def _write_database_plan(self, database_plan: Dict[str, Any]) -> Path:
        out_dir = self.ctx.root_dir / "multi_db"
        out_dir.mkdir(parents=True, exist_ok=True)
        path = out_dir / "database_plan.json"
        path.write_text(json.dumps(database_plan, indent=2, ensure_ascii=False), encoding="utf-8")
        return path

    def _export_multi_db(self, database_plan: Dict[str, Any], multi_db: Dict[str, Any]) -> Dict[str, str]:
        renderer = MultiDBRenderer(self.ctx.root_dir / "multi_db")
        return renderer.render(database_plan, multi_db, include_html=False)

    def _generate_database_plan(self, state, governance) -> Dict[str, Any]:
        ontology_agent_path = Path("mindvault/agents/ontology_agent.yaml")
        if not ontology_agent_path.exists():
            raise RuntimeError("ontology_agent definition not found")
        context = self._build_modeling_context(state, governance)
        result = self.executor.execute(ontology_agent_path, context)
        self._raise_on_agent_error(result, "ontology_agent")
        normalized = self._normalize_database_plan_result(result)
        if normalized:
            return self._finalize_database_plan(normalized)
        raw_preview = ""
        if isinstance(result, dict):
            raw_preview = str(result.get("raw_content", "") or result.get("_raw_content", "")).strip()[:240]
        if raw_preview:
            raise RuntimeError(f"ontology_agent returned no structured database plan: {raw_preview}")
        raise RuntimeError("ontology_agent returned no structured database plan")

    @staticmethod
    def _normalize_database_plan_result(result: Dict[str, Any] | Any) -> Dict[str, Any] | None:
        if not isinstance(result, dict):
            return None
        if isinstance(result.get("databases"), list):
            return result

        for key in ("database_plan", "plan", "result", "output", "payload"):
            candidate = result.get(key)
            if isinstance(candidate, dict) and isinstance(candidate.get("databases"), list):
                return candidate

        tables = result.get("tables")
        if isinstance(tables, list):
            return {
                "domain": result.get("domain", ""),
                "generated_at": result.get("generated_at", ""),
                "databases": tables,
                "relations": result.get("relations", []),
            }
        return None

    def _generate_multi_db(self, state, database_plan: Dict[str, Any]) -> Tuple[Dict[str, Any], List[Dict[str, str]]]:
        database_builder_agent_path = Path("mindvault/agents/database_builder_agent.yaml")
        if not database_builder_agent_path.exists():
            raise RuntimeError("database_builder_agent definition not found")
        modeling_context = self._build_modeling_context(state)
        built_databases: List[Dict[str, Any]] = []
        warnings: List[Dict[str, str]] = []

        for database_spec in database_plan.get("databases", []):
            single_plan = {
                "domain": database_plan.get("domain", ""),
                "generated_at": database_plan.get("generated_at", ""),
                "databases": [database_spec],
                "relations": [
                    relation
                    for relation in database_plan.get("relations", [])
                    if relation.get("from_db") == database_spec.get("name") or relation.get("to_db") == database_spec.get("name")
                ],
            }
            context = {
                "database_plan": single_plan,
                **modeling_context,
            }
            try:
                result = self.executor.execute(database_builder_agent_path, context)
                self._raise_on_agent_error(result, "database_builder_agent")
                normalized_tables = self._normalize_database_builder_result(result, database_spec)
                if not normalized_tables:
                    raise RuntimeError(
                        f"database_builder_agent returned no structured table output for '{database_spec.get('name', '')}'"
                    )
                built_databases.extend(normalized_tables)
            except Exception as exc:
                error_text = str(exc)
                self.trace.log(
                    "multi_db_table_failed",
                    table=database_spec.get("name", ""),
                    error=error_text,
                )
                warnings.append(
                    {
                        "step": "multi_db",
                        "agent": "database_builder_agent",
                        "error": error_text,
                        "table": database_spec.get("name", ""),
                    }
                )

        if not built_databases:
            details = "; ".join(
                f"{item.get('table', '')}: {item.get('error', '')}"
                for item in warnings
            )
            raise RuntimeError(details or "database_builder_agent produced no usable table output")

        merged_payload = {
            "domain": database_plan.get("domain", ""),
            "generated_at": datetime.utcnow().isoformat(),
            "databases": built_databases,
            "relations": database_plan.get("relations", []),
        }
        return self._finalize_multi_db(merged_payload, database_plan), warnings

    def _normalize_database_builder_result(
        self,
        result: Dict[str, Any],
        database_spec: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        if not isinstance(result, dict):
            if isinstance(result, list):
                return [self._coerce_single_database_payload({"rows": result}, database_spec)]
            return []

        databases = result.get("databases")
        if isinstance(databases, list):
            normalized = []
            for item in databases:
                if isinstance(item, dict):
                    normalized.append(self._coerce_single_database_payload(item, database_spec))
            return normalized

        database_name = str(database_spec.get("name", "")).strip()
        direct_candidate = self._extract_single_database_candidate(result, database_name)
        if direct_candidate is not None:
            return [self._coerce_single_database_payload(direct_candidate, database_spec)]
        return []

    def _extract_single_database_candidate(self, result: Dict[str, Any], database_name: str) -> Dict[str, Any] | None:
        if self._looks_like_single_database_object(result):
            return result

        for key in ("database", "table", "result", "payload"):
            candidate = result.get(key)
            if isinstance(candidate, dict) and self._looks_like_single_database_object(candidate):
                return candidate

        if database_name:
            candidate = result.get(database_name)
            if isinstance(candidate, dict):
                return candidate
            if isinstance(candidate, list):
                return {"name": database_name, "rows": candidate}

        if len(result) == 1:
            only_value = next(iter(result.values()))
            if isinstance(only_value, dict) and self._looks_like_single_database_object(only_value):
                return only_value
            if isinstance(only_value, list):
                return {"name": database_name, "rows": only_value}
        return None

    @staticmethod
    def _looks_like_single_database_object(candidate: Dict[str, Any]) -> bool:
        if not isinstance(candidate, dict):
            return False
        return any(
            key in candidate
            for key in ("rows", "columns", "primary_key", "title", "description", "records", "items")
        )

    @staticmethod
    def _coerce_single_database_payload(payload: Dict[str, Any], database_spec: Dict[str, Any]) -> Dict[str, Any]:
        row_list = payload.get("rows")
        if not isinstance(row_list, list):
            row_list = payload.get("records")
        if not isinstance(row_list, list):
            row_list = payload.get("items")
        if not isinstance(row_list, list):
            row_list = []

        columns = payload.get("columns")
        if isinstance(columns, dict):
            columns = list(columns.keys())
        if not isinstance(columns, list):
            columns = []

        return {
            "name": payload.get("name") or database_spec.get("name", ""),
            "title": payload.get("title") or database_spec.get("title", database_spec.get("name", "")),
            "description": payload.get("description") or database_spec.get("description", ""),
            "primary_key": payload.get("primary_key") or database_spec.get("primary_key") or "id",
            "columns": columns,
            "rows": row_list,
            "visibility": payload.get("visibility") or database_spec.get("visibility", "business"),
        }

    def _build_modeling_context(self, state: Dict[str, Any], governance: Dict[str, Any] | None = None) -> Dict[str, Any]:
        return {
            "entities": [self._compact_entity_for_modeling(item) for item in state.get("entities", [])],
            "claims": [self._compact_claim_for_modeling(item) for item in state.get("claims", [])],
            "relations": [self._compact_relation_for_modeling(item) for item in state.get("relations", [])],
            "events": [self._compact_event_for_modeling(item) for item in state.get("events", [])],
            "governance": self._compact_governance_for_modeling(governance or {}),
        }

    @staticmethod
    def _compact_entity_for_modeling(entity: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "id": entity.get("id", ""),
            "name": entity.get("name", ""),
            "type": entity.get("type", ""),
            "attributes": VaultRuntime._compact_mapping(entity.get("attributes", {})),
            "source_refs": list((entity.get("source_refs") or [])[:5]),
            "confidence": entity.get("confidence"),
            "status": entity.get("status", ""),
        }

    @staticmethod
    def _compact_claim_for_modeling(claim: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "id": claim.get("id", claim.get("claim_id", "")),
            "subject": claim.get("subject", ""),
            "predicate": claim.get("predicate", ""),
            "object": VaultRuntime._compact_scalar(claim.get("object")),
            "claim_type": claim.get("claim_type", ""),
            "confidence": claim.get("confidence"),
            "source_ref": claim.get("source_ref", ""),
        }

    @staticmethod
    def _compact_relation_for_modeling(relation: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "source": relation.get("source", relation.get("source_entity", "")),
            "relation": relation.get("relation", relation.get("relation_type", "")),
            "target": relation.get("target", relation.get("target_entity", "")),
            "confidence": relation.get("confidence"),
        }

    @staticmethod
    def _compact_event_for_modeling(event: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "id": event.get("id", event.get("event_id", "")),
            "type": event.get("type", ""),
            "description": VaultRuntime._compact_text(event.get("description", "")),
            "entities": list((event.get("entities") or event.get("participants") or [])[:6]),
            "timestamp": event.get("timestamp"),
            "status": event.get("status", ""),
        }

    @staticmethod
    def _compact_governance_for_modeling(governance: Dict[str, Any]) -> Dict[str, Any]:
        conflicts = ((governance or {}).get("conflicts") or {}).get("conflicts", [])
        placeholders = (governance or {}).get("placeholders", [])
        schema_candidates = (governance or {}).get("schema_candidates", {})
        return {
            "conflict_count": len(conflicts),
            "placeholder_count": len(placeholders),
            "conflict_samples": [
                {
                    "field": item.get("field", ""),
                    "entity_id": item.get("entity_id", ""),
                }
                for item in conflicts[:5]
            ],
            "placeholder_samples": [
                {
                    "entity_id": item.get("entity_id", ""),
                    "field": item.get("field", ""),
                    "status": item.get("status", ""),
                }
                for item in placeholders[:8]
            ],
            "schema_candidates": VaultRuntime._compact_mapping(schema_candidates),
        }

    @staticmethod
    def _compact_mapping(value: Any) -> Dict[str, Any]:
        if not isinstance(value, dict):
            return {}
        compacted: Dict[str, Any] = {}
        for key, item in value.items():
            if isinstance(item, (dict, list)):
                compacted[key] = VaultRuntime._compact_scalar(item)
            else:
                compacted[key] = VaultRuntime._compact_scalar(item)
        return compacted

    @staticmethod
    def _compact_scalar(value: Any) -> Any:
        if isinstance(value, str):
            return VaultRuntime._compact_text(value)
        if isinstance(value, list):
            return [VaultRuntime._compact_scalar(item) for item in value[:6]]
        if isinstance(value, dict):
            return {
                key: VaultRuntime._compact_scalar(item)
                for key, item in list(value.items())[:8]
            }
        return value

    @staticmethod
    def _compact_text(value: str, limit: int = 180) -> str:
        text = str(value or "").strip()
        if len(text) <= limit:
            return text
        return f"{text[:limit].rstrip()}..."

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

    def _finalize_database_plan(self, database_plan: Dict[str, Any]) -> Dict[str, Any]:
        plan = dict(database_plan)
        normalized = []
        for database in plan.get("databases", []):
            row = dict(database)
            name = row.get("name", "")
            entity_types = [item for item in row.get("entity_types", []) if item]
            row.setdefault("visibility", self._infer_database_visibility(name))
            if name in {"claims", "relations", "sources"}:
                row.setdefault("row_source", name)
                row.setdefault("record_granularity", name[:-1] if name.endswith("s") else name)
            elif entity_types and len(entity_types) == 1:
                row.setdefault("row_source", "entities")
                row.setdefault("record_granularity", "entity")
            else:
                row.setdefault("row_source", "mixed")
                row.setdefault("record_granularity", "mixed")
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
        payload["databases"] = self._sanitize_business_databases(normalized, plan_map)
        payload["relations"] = self._merge_relation_defs(
            payload.get("relations", []),
            self._infer_relations_from_multi_db(payload["databases"]),
        )
        return payload

    def _sanitize_business_databases(self, databases: List[Dict[str, Any]], plan_map: Dict[str, Dict[str, Any]]) -> List[Dict[str, Any]]:
        entity_table_row_ids: set[str] = set()
        for database in databases:
            plan = plan_map.get(database.get("name", ""), {})
            if plan.get("row_source") != "entities":
                continue
            entity_table_row_ids.update(self._database_row_ids(database))

        kept: List[Dict[str, Any]] = []
        seen_business_signatures: Dict[tuple[str, ...], str] = {}
        for database in databases:
            plan = plan_map.get(database.get("name", ""), {})
            if database.get("visibility") != "business":
                kept.append(database)
                continue

            row_source = plan.get("row_source", "entities")
            row_ids = tuple(sorted(self._database_row_ids(database)))
            has_specific_values = self._has_table_specific_values(database, plan)

            if row_source != "entities" and row_ids and set(row_ids) == entity_table_row_ids and not has_specific_values:
                self.trace.log(
                    "multi_db_table_pruned",
                    table=database.get("name", ""),
                    reason="duplicates_entity_pool",
                )
                continue

            if row_source != "entities" and row_ids and row_ids in seen_business_signatures and not has_specific_values:
                self.trace.log(
                    "multi_db_table_pruned",
                    table=database.get("name", ""),
                    reason="duplicates_other_business_table",
                    duplicate_of=seen_business_signatures[row_ids],
                )
                continue

            kept.append(database)
            if database.get("visibility") == "business" and row_ids:
                seen_business_signatures[row_ids] = database.get("name", "")
        return kept

    @staticmethod
    def _database_row_ids(database: Dict[str, Any]) -> List[str]:
        primary_key = database.get("primary_key", "id")
        row_ids: List[str] = []
        for row in database.get("rows", []):
            value = row.get(primary_key) or row.get("id") or row.get("entity_id") or row.get("event_id") or row.get("claim_id")
            if value is not None:
                row_ids.append(str(value))
        return row_ids

    @staticmethod
    def _has_table_specific_values(database: Dict[str, Any], plan: Dict[str, Any]) -> bool:
        generic_fields = {
            "id",
            "entity_id",
            "event_id",
            "claim_id",
            "relation_id",
            "name",
            "title",
            "type",
            "description",
            "confidence",
            "source_ref",
            "source_refs",
            "updated_at",
            "created_at",
            "status",
            "tags",
        }
        suggested = set(plan.get("suggested_fields", []))
        row_source = plan.get("row_source", "entities")
        row_specific_fields = [field for field in suggested if field not in generic_fields]
        if row_source == "entities":
            return True
        if not row_specific_fields:
            return False
        for row in database.get("rows", []):
            for field in row_specific_fields:
                value = row.get(field)
                if value not in (None, "", [], {}):
                    return True
        return False

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
    def _infer_database_visibility(name: str) -> str:
        return "system" if name in {"claims", "relations", "sources"} else "business"

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
