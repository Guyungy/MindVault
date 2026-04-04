"""MindVault app: the unified pipeline entry point for the v2 architecture."""
from __future__ import annotations

import hashlib
import json
import re
import shutil
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from threading import Lock
from typing import Any, Dict, List, Tuple

from mindvault.runtime.workspace_store import WorkspaceStore, WorkspaceContext
from mindvault.runtime.knowledge_store import KnowledgeStore
from mindvault.runtime.version_store import VersionStore
from mindvault.runtime.model_router import ModelRouter
from mindvault.runtime.agent_executor import AgentExecutor
from mindvault.runtime.trace_logger import TraceLogger
from mindvault.runtime.task_runtime import TaskRuntime
from mindvault.runtime.rule_builder import (
    build_fallback_plan,
    build_learned_database_plan,
    build_rule_database_plan,
    save_learned_schema,
    build_table_by_rule,
    can_build_by_rule,
)
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

    MODELING_ENTITY_BATCH_SIZE = 18
    MODELING_CLAIM_BATCH_SIZE = 24
    MODELING_RELATION_BATCH_SIZE = 18
    MODELING_EVENT_BATCH_SIZE = 14

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
        self._parse_cache_path = self.ctx.config_dir / "parse_cache.json"
        self._global_parse_cache_path = config_root_path / "parse_cache_global.json"
        if self._global_parse_cache_path.parent == config_root_path and not self._global_parse_cache_path.parent.exists():
            self._global_parse_cache_path = Path("config/parse_cache_global.json")
        self._parse_cache = self._load_json_cache(self._parse_cache_path)
        self._global_parse_cache = self._load_json_cache(self._global_parse_cache_path)
        self._parse_cache_lock = Lock()
        self._parse_cache_dirty = False
        self._global_parse_cache_dirty = False
        self._parse_agent_signature = self._compute_agent_signature(Path("mindvault/agents/parse_agent.yaml"))

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
            self._write_task_input_sources(task, sources)
            self._snapshot_task_artifact(task, "sources", raw_path)

            self._mark_task(task, "adapt", resume_hint="Adapting sources into normalized chunks.")
            all_chunks = []
            for src in sources:
                adapter = self._adapters.get(src["source_type"], DocAdapter())
                chunks = adapter.adapt(src)
                all_chunks.extend(chunks)
            self.trace.log("adapt_complete", chunk_count=len(all_chunks))
            task.log_step("adapt", "ok", chunks=len(all_chunks))

            parse_workers = max(1, int(runtime_settings.get("concurrency", {}).get("parse_workers", 1) or 1))
            modeling_workers = max(1, int(runtime_settings.get("concurrency", {}).get("modeling_workers", 1) or 1))

            self._mark_task(task, "parse", agent="parse_agent", resume_hint="Parsing chunks into atomic knowledge.")
            all_claims: List[Dict[str, Any]] = []
            all_entities: List[Dict[str, Any]] = []
            all_relations: List[Dict[str, Any]] = []
            all_events: List[Dict[str, Any]] = []

            parse_agent_path = Path("mindvault/agents/parse_agent.yaml")
            self.executor.load_agent(parse_agent_path)
            with ThreadPoolExecutor(max_workers=min(parse_workers, max(1, len(all_chunks)))) as pool:
                future_to_chunk = {
                    pool.submit(self._execute_parse_chunk, parse_agent_path, chunk, task): chunk
                    for chunk in all_chunks
                }
                completed_chunks = 0
                for future in as_completed(future_to_chunk):
                    chunk = future_to_chunk[future]
                    result = future.result()
                    if isinstance(result, dict) and "claims" in result:
                        normalized = self._normalize_parse_result(result, chunk)
                        all_claims.extend(normalized.get("claims", []))
                        all_entities.extend(normalized.get("entity_candidates", []))
                        all_relations.extend(normalized.get("relation_candidates", []))
                        all_events.extend(normalized.get("event_candidates", []))
                    else:
                        raise RuntimeError(f"parse_agent returned no structured output for chunk {chunk.chunk_id}")
                    completed_chunks += 1
                    task.heartbeat(
                        step="parse",
                        agent="parse_agent",
                        resume_hint=f"Parsing chunks into atomic knowledge ({completed_chunks}/{len(all_chunks)}).",
                    )

            self.trace.log("parse_complete",
                           claims=len(all_claims), entities=len(all_entities),
                           relations=len(all_relations), events=len(all_events))
            task.log_step("parse", "ok", claims=len(all_claims), entities=len(all_entities), relations=len(all_relations), events=len(all_events))
            self._save_parse_cache()

            extracted_path = self._save_extracted(all_claims, all_entities, all_relations, all_events)
            task.add_artifact("extracted", str(extracted_path))
            self._snapshot_task_artifact(task, "extracted", extracted_path)

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
            self._snapshot_task_artifact(task, "knowledge_base", self.ctx.kb_path)
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
            self._snapshot_task_artifact(task, "placeholders", ph_path)
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
            self._snapshot_task_artifact(task, "snapshot", version_meta.get("snapshot_path", ""))
            self._snapshot_task_artifact(task, "changelog", version_meta.get("changelog_path", ""))
            task.log_step("versioning", "ok", version=version_meta.get("version"))

            current_source_ids = [
                str(src.get("source_id", "")).strip()
                for src in sources
                if str(src.get("source_id", "")).strip()
            ]
            graph_paths = self._export_graph_artifacts(state, current_source_ids)
            self.trace.log("graph_export_complete", graph=graph_paths.get("graph", ""))
            for name, value in graph_paths.items():
                task.add_artifact(name, value)
                self._snapshot_task_artifact(task, name, value)

            insights: List[Dict[str, Any]] = []
            report_path = ""

            if profile == "full" and report_enabled:
                self._mark_task(task, "insight", agent="insight_generator", resume_hint="Generating insight summaries.")
                insights = self._generate_insights(state, governance, task=task)
                self.kb.append_insights(insights)
                state = self.kb.state
                self.trace.log("insight_complete", count=len(insights))
                task.log_step("insight", "ok", count=len(insights))

            change_scope = self._build_change_scope(
                entities=fragment.get("entity_candidates", []),
                claims=all_claims,
                relations=all_relations,
                events=all_events,
            )
            self._mark_task(task, "database_plan", agent="ontology_agent", resume_hint="Planning business tables and relationships.")
            database_plan_started = time.perf_counter()
            database_plan, plan_reused = self._resolve_database_plan(state, governance, change_scope, runtime_settings, task=task)
            self.trace.log(
                "perf",
                step="database_plan",
                duration_s=round(time.perf_counter() - database_plan_started, 2),
                plan_reused=plan_reused,
                built_by=database_plan.get("built_by", "llm"),
                databases=len(database_plan.get("databases", [])),
                entity_types=sorted({
                    str(entity.get("type", "")).strip()
                    for entity in state.get("entities", [])
                    if str(entity.get("type", "")).strip()
                }),
            )
            database_plan_path = self._write_database_plan(database_plan)
            task.add_artifact("database_plan", str(database_plan_path))
            self._snapshot_task_artifact(task, "database_plan", database_plan_path)
            task.log_step(
                "database_plan",
                "ok",
                databases=len(database_plan.get("databases", [])),
                output=str(database_plan_path),
                reused=plan_reused,
            )
            self._mark_task(task, "multi_db", agent="database_builder_agent", resume_hint="Building structured tables from the approved plan.")
            multi_db, multi_db_warnings = self._generate_multi_db(
                state,
                database_plan,
                workers=modeling_workers,
                change_scope=change_scope,
                task=task,
            )
            multi_db_paths = self._export_multi_db(database_plan, multi_db)
            self.trace.log("multi_db_export_complete", data=multi_db_paths.get("data", ""))
            for name, value in multi_db_paths.items():
                task.add_artifact(f"multi_db_{name}", value)
                self._snapshot_task_artifact(task, f"multi_db_{name}", value)
            task.log_step(
                "multi_db",
                "ok",
                output=multi_db_paths.get("data", ""),
                warnings=len(multi_db_warnings),
                tables=len(multi_db.get("databases", [])),
                rebuilt_tables=multi_db.get("metadata", {}).get("rebuilt_tables", []),
            )
            optional_failures.extend(multi_db_warnings)

            if profile == "full" and report_enabled:
                report_path = self._run_optional_stage(
                    task=task,
                    step="report",
                    agent="report_agent",
                    resume_hint="Writing report artifact.",
                    runner=lambda: self._write_report(state, insights, governance, task=task),
                    optional_failures=optional_failures,
                )

            trace_path = self.ctx.root_dir / "agent_trace.json"
            self.trace.save(trace_path)
            task.add_artifact("trace", str(trace_path))
            self._snapshot_task_artifact(task, "trace", trace_path)
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
                "graph": graph_paths,
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
            "planning": {"reuse_existing_plan": True},
            "artifacts": {"report": False},
            "concurrency": {"parse_workers": 3, "modeling_workers": 2},
        }
        if not self.runtime_config_path.exists():
            return fallback
        try:
            raw = json.loads(self.runtime_config_path.read_text(encoding="utf-8"))
        except Exception:
            return fallback
        execution = raw.get("execution", {}) if isinstance(raw, dict) else {}
        planning = raw.get("planning", {}) if isinstance(raw, dict) else {}
        artifacts = raw.get("artifacts", {}) if isinstance(raw, dict) else {}
        concurrency = raw.get("concurrency", {}) if isinstance(raw, dict) else {}
        profile = execution.get("profile", "fast")
        if profile not in {"fast", "full"}:
            profile = "fast"
        return {
            "execution": {
                "profile": profile,
                "engine_mode": "llm_only",
            },
            "planning": {
                "reuse_existing_plan": planning.get("reuse_existing_plan", True) is not False,
            },
            "artifacts": {
                "report": bool(artifacts.get("report", False)),
            },
            "concurrency": {
                "parse_workers": max(1, int((concurrency or {}).get("parse_workers", 3) or 3)),
                "modeling_workers": max(1, int((concurrency or {}).get("modeling_workers", 2) or 2)),
            },
        }

    def _execute_parse_chunk(self, parse_agent_path: Path, chunk, task: TaskRuntime | None = None) -> Dict[str, Any]:
        cache_key = self._fingerprint_parse_chunk(chunk)
        with self._parse_cache_lock:
            global_cached = self._global_parse_cache.get(cache_key)
            if isinstance(global_cached, dict):
                self._parse_cache[cache_key] = global_cached
                self._parse_cache_dirty = True
            cached = global_cached if isinstance(global_cached, dict) else self._parse_cache.get(cache_key)
        if isinstance(global_cached, dict):
            self.trace.log("parse_cache_hit_global", chunk_id=chunk.chunk_id, source_id=chunk.source_id)
            return global_cached
        if isinstance(cached, dict):
            self.trace.log("parse_cache_hit", chunk_id=chunk.chunk_id, source_id=chunk.source_id)
            return cached

        context = {
            "chunk_text": chunk.text,
            "source_id": chunk.source_id,
            "source_type": chunk.context_hints.get("source_type", "doc"),
            "language": chunk.context_hints.get("language", "en"),
            "context_note": chunk.context_hints.get("note", ""),
            "speakers": chunk.context_hints.get("speakers", []),
        }
        result = self.executor.execute(
            parse_agent_path,
            context,
            heartbeat=self._make_agent_progress_callback(
                task,
                step="parse",
                agent="parse_agent",
                resume_hint=f"正在解析分块 {chunk.chunk_id}，模型仍在处理中。",
            ),
        )
        self._raise_on_agent_error(result, "parse_agent")
        if isinstance(result, dict) and "claims" in result:
            cache_payload = {
                "claims": result.get("claims", []),
                "entity_candidates": result.get("entity_candidates", []),
                "relation_candidates": result.get("relation_candidates", []),
                "event_candidates": result.get("event_candidates", []),
            }
            with self._parse_cache_lock:
                self._parse_cache[cache_key] = cache_payload
                self._global_parse_cache[cache_key] = cache_payload
                self._parse_cache_dirty = True
                self._global_parse_cache_dirty = True
        return result

    @staticmethod
    def _load_json_cache(path: Path) -> Dict[str, Any]:
        if not path.exists():
            return {}
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(payload, dict):
                return payload
        except Exception:
            pass
        return {}

    def _save_parse_cache(self) -> None:
        with self._parse_cache_lock:
            save_local = self._parse_cache_dirty
            save_global = self._global_parse_cache_dirty
            if not save_local and not save_global:
                return
            local_payload = dict(self._parse_cache)
            global_payload = dict(self._global_parse_cache)
            self._parse_cache_dirty = False
            self._global_parse_cache_dirty = False
        if save_local:
            self._parse_cache_path.write_text(
                json.dumps(local_payload, indent=2, ensure_ascii=False),
                encoding="utf-8",
            )
        if save_global:
            self._global_parse_cache_path.parent.mkdir(parents=True, exist_ok=True)
            self._global_parse_cache_path.write_text(
                json.dumps(global_payload, indent=2, ensure_ascii=False),
                encoding="utf-8",
            )

    @staticmethod
    def _compute_agent_signature(agent_path: Path) -> str:
        if not agent_path.exists():
            return "missing-parse-agent"
        content_parts = [agent_path.read_text(encoding="utf-8")]
        prompt_match = re.search(r"^prompt_template:\s*(.+)$", content_parts[0], flags=re.MULTILINE)
        if prompt_match:
            prompt_path = Path(prompt_match.group(1).strip())
            if prompt_path.exists():
                content_parts.append(prompt_path.read_text(encoding="utf-8"))
        return hashlib.sha1("\n\n".join(content_parts).encode("utf-8")).hexdigest()

    def _fingerprint_parse_chunk(self, chunk) -> str:
        parts = [
            self._parse_agent_signature,
            str(chunk.context_hints.get("source_type", "")),
            str(chunk.context_hints.get("language", "")),
            str(chunk.text or ""),
        ]
        return hashlib.sha1("\n".join(parts).encode("utf-8")).hexdigest()

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

    def _task_artifacts_dir(self, task: TaskRuntime) -> Path:
        path = task.task_dir / "artifacts"
        path.mkdir(parents=True, exist_ok=True)
        return path

    def _write_task_input_sources(self, task: TaskRuntime, sources: List[Dict[str, Any]]) -> str:
        artifact_dir = self._task_artifacts_dir(task)
        path = artifact_dir / "input_sources.json"
        path.write_text(json.dumps(sources, indent=2, ensure_ascii=False), encoding="utf-8")
        task.add_artifact("task_input_sources", str(path))
        return str(path)

    def _snapshot_task_artifact(self, task: TaskRuntime | None, name: str, source_path: str | Path | None) -> str:
        if task is None or not source_path:
            return ""
        src = Path(source_path)
        if not src.exists():
            return ""
        safe_name = re.sub(r"[^a-zA-Z0-9_.-]+", "_", str(name)).strip("._") or "artifact"
        suffix = src.suffix or ".json"
        artifact_dir = self._task_artifacts_dir(task)
        dest = artifact_dir / f"{safe_name}{suffix}"
        shutil.copy2(src, dest)
        task.add_artifact(f"task_{safe_name}", str(dest))
        return str(dest)

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

    def _generate_insights(self, state: Dict[str, Any], governance: Dict[str, Any], task: TaskRuntime | None = None) -> List[Dict[str, Any]]:
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
        result = self.executor.execute(
            insight_agent_path,
            context,
            heartbeat=self._make_agent_progress_callback(
                task,
                step="insight",
                agent="insight_generator",
                resume_hint="正在生成洞察摘要，模型仍在处理中。",
            ),
        )
        self._raise_on_agent_error(result, "insight_agent")
        if isinstance(result, dict) and isinstance(result.get("insights"), list):
            return result["insights"]
        if isinstance(result, dict) and isinstance(result.get("items"), list):
            return result["items"]
        raise RuntimeError("insight_agent returned no structured insights output")

    def _generate_report(self, state, insights, governance, task: TaskRuntime | None = None) -> Dict[str, Any]:
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
        result = self.executor.execute(
            report_agent_path,
            context,
            heartbeat=self._make_agent_progress_callback(
                task,
                step="report",
                agent="report_agent",
                resume_hint="正在生成报告内容，模型仍在处理中。",
            ),
        )
        self._raise_on_agent_error(result, "report_agent")
        if isinstance(result, dict) and "business_domain" in result:
            return result
        if isinstance(result, dict) and result.get("content") and isinstance(result["content"], dict):
            return result["content"]
        raise RuntimeError("report_agent returned no structured report output")

    def _write_report(self, state, insights, governance, task: TaskRuntime | None = None) -> str:
        report_data = self._generate_report(state, insights, governance, task=task)
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

    def _export_graph_artifacts(self, state: Dict[str, Any], current_source_ids: List[str]) -> Dict[str, str]:
        self.ctx.graph_dir.mkdir(parents=True, exist_ok=True)
        graph_path = self.ctx.graph_dir / "graph.json"
        current_path = self.ctx.graph_dir / "current_ingest.json"

        graph_payload = self._build_graph_payload(state)
        current_payload = self._filter_graph_by_source_ids(graph_payload, set(current_source_ids))

        graph_path.write_text(json.dumps(graph_payload, indent=2, ensure_ascii=False), encoding="utf-8")
        current_path.write_text(json.dumps(current_payload, indent=2, ensure_ascii=False), encoding="utf-8")
        return {
            "graph": str(graph_path),
            "current_ingest_graph": str(current_path),
        }

    def _load_existing_multi_db(self) -> Dict[str, Any]:
        path = self.ctx.root_dir / "multi_db" / "multi_db.json"
        if not path.exists():
            return {"databases": [], "relations": []}
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(payload, dict):
                return payload
        except Exception:
            pass
        return {"databases": [], "relations": []}

    def _load_existing_database_plan(self) -> Dict[str, Any] | None:
        path = self.ctx.root_dir / "multi_db" / "database_plan.json"
        if not path.exists():
            return None
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(payload, dict) and isinstance(payload.get("databases"), list) and payload.get("databases"):
                return self._finalize_database_plan(payload)
        except Exception:
            pass
        return None

    def _build_change_scope(
        self,
        *,
        entities: List[Dict[str, Any]],
        claims: List[Dict[str, Any]],
        relations: List[Dict[str, Any]],
        events: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        entity_ids = {
            str(item.get("id", ""))
            for item in entities
            if item.get("id")
        }
        entity_types = {
            str(item.get("type", "")).strip()
            for item in entities
            if str(item.get("type", "")).strip()
        }
        source_refs = set()
        for record in claims + entities + events:
            if isinstance(record.get("source_ref"), str) and record.get("source_ref"):
                source_refs.add(record["source_ref"])
            for source_ref in record.get("source_refs", []) or []:
                if source_ref:
                    source_refs.add(str(source_ref))
        return {
            "entity_ids": sorted(entity_ids),
            "entity_types": sorted(entity_types),
            "claim_count": len(claims),
            "relation_count": len(relations),
            "event_count": len(events),
            "source_refs": sorted(source_refs),
            "semantic_tags": self._extract_semantic_tags(entities, claims, relations, events),
        }

    @staticmethod
    def _extract_semantic_tags(
        entities: List[Dict[str, Any]],
        claims: List[Dict[str, Any]],
        relations: List[Dict[str, Any]],
        events: List[Dict[str, Any]],
    ) -> List[str]:
        tags: set[str] = set()
        entity_types = {
            str(item.get("type", "")).strip().lower()
            for item in entities
            if str(item.get("type", "")).strip()
        }
        claim_types = {
            str(item.get("claim_type", "")).strip().lower()
            for item in claims
            if str(item.get("claim_type", "")).strip()
        }
        predicates = {
            str(item.get("predicate", "")).strip().lower()
            for item in claims
            if str(item.get("predicate", "")).strip()
        }
        event_types = {
            str(item.get("type", "")).strip().lower()
            for item in events
            if str(item.get("type", "")).strip()
        }
        relation_types = {
            str(item.get("relation", item.get("relation_type", ""))).strip().lower()
            for item in relations
            if str(item.get("relation", item.get("relation_type", ""))).strip()
        }

        for value_set, label in (
            (entity_types, "typed_entities"),
            (claim_types, "typed_claims"),
            (predicates, "predicates"),
            (event_types, "typed_events"),
            (relation_types, "typed_relations"),
        ):
            if value_set:
                tags.add(label)

        if {"topic", "opinion", "signal", "role", "resource"} & entity_types:
            tags.add("discourse_graph")
        if {"opinion", "judgment", "subjective"} & claim_types:
            tags.add("opinionated")
        if {"discusses", "mentions", "thinks", "suggests", "believes", "认为", "提到", "建议"} & predicates:
            tags.add("discussion_heavy")
        if {"discussion", "conversation", "chat"} & event_types:
            tags.add("conversation")
        if any("topic" in predicate or "话题" in predicate for predicate in predicates):
            tags.add("topic_focused")
        return sorted(tags)

    @staticmethod
    def _collect_planned_entity_types(database_plan: Dict[str, Any]) -> set[str]:
        planned_types: set[str] = set()
        for database in database_plan.get("databases", []) or []:
            for entity_type in database.get("entity_types", []) or []:
                normalized = str(entity_type).strip()
                if normalized:
                    planned_types.add(normalized)
        return planned_types

    @staticmethod
    def _collect_plan_semantic_tags(database_plan: Dict[str, Any]) -> set[str]:
        tags: set[str] = set()
        for database in database_plan.get("databases", []) or []:
            name = str(database.get("name", "")).strip().lower()
            title = str(database.get("title", "")).strip().lower()
            row_source = str(database.get("row_source", "")).strip().lower()
            record_granularity = str(database.get("record_granularity", "")).strip().lower()
            entity_types = {
                str(item).strip().lower()
                for item in (database.get("entity_types", []) or [])
                if str(item).strip()
            }
            joined = " ".join([name, title, row_source, record_granularity, " ".join(sorted(entity_types))])
            if any(token in joined for token in ["topic", "话题", "discussion", "讨论", "opinion", "观点", "signal", "信号"]):
                tags.add("discourse_graph")
            if any(token in joined for token in ["opinion", "观点"]):
                tags.add("opinionated")
            if any(token in joined for token in ["discussion", "chat", "conversation", "讨论", "对话"]):
                tags.add("conversation")
            if any(token in joined for token in ["topic", "话题"]):
                tags.add("topic_focused")
        return tags

    def _should_reuse_database_plan(
        self,
        existing_plan: Dict[str, Any] | None,
        change_scope: Dict[str, Any],
        runtime_settings: Dict[str, Any],
    ) -> bool:
        if runtime_settings.get("planning", {}).get("reuse_existing_plan", True) is False:
            return False
        if not existing_plan or not existing_plan.get("databases"):
            return False
        changed_types = {
            str(item).strip()
            for item in change_scope.get("entity_types", [])
            if str(item).strip()
        }
        if not changed_types:
            return True
        planned_types = self._collect_planned_entity_types(existing_plan)
        if not planned_types:
            return False
        if not changed_types.issubset(planned_types):
            return False

        changed_tags = {
            str(item).strip()
            for item in change_scope.get("semantic_tags", [])
            if str(item).strip()
        }
        if not changed_tags:
            return True
        planned_tags = self._collect_plan_semantic_tags(existing_plan)
        semantic_gap = {"discourse_graph", "opinionated", "conversation", "topic_focused"} & changed_tags
        if semantic_gap and not semantic_gap.issubset(planned_tags):
            return False
        return True

    def _resolve_database_plan(
        self,
        state: Dict[str, Any],
        governance: Dict[str, Any],
        change_scope: Dict[str, Any],
        runtime_settings: Dict[str, Any],
        task: TaskRuntime | None = None,
    ) -> Tuple[Dict[str, Any], bool]:
        existing_plan = self._load_existing_database_plan()
        if self._should_reuse_database_plan(existing_plan, change_scope, runtime_settings):
            self.trace.log(
                "database_plan_reused",
                changed_entity_types=sorted(change_scope.get("entity_types", [])),
                existing_tables=sorted(
                    str(item.get("name", "")).strip()
                    for item in (existing_plan or {}).get("databases", [])
                    if str(item.get("name", "")).strip()
                ),
            )
            return existing_plan or {"databases": [], "relations": []}, True
        return self._generate_database_plan(state, governance, task=task, change_scope=change_scope), False

    def _determine_affected_tables(
        self,
        database_plan: Dict[str, Any],
        existing_multi_db: Dict[str, Any],
        change_scope: Dict[str, Any],
    ) -> set[str]:
        existing_names = {
            str(item.get("name", "")).strip()
            for item in existing_multi_db.get("databases", [])
            if str(item.get("name", "")).strip()
        }
        changed_types = {
            str(item).strip()
            for item in change_scope.get("entity_types", [])
            if str(item).strip()
        }
        affected: set[str] = set()
        for database in database_plan.get("databases", []):
            name = str(database.get("name", "")).strip()
            if not name:
                continue
            if name not in existing_names:
                affected.add(name)
                continue
            row_source = str(database.get("row_source", "mixed") or "mixed").strip().lower()
            entity_types = {
                str(item).strip()
                for item in (database.get("entity_types", []) or [])
                if str(item).strip()
            }
            if row_source == "entities" and (not entity_types or entity_types & changed_types):
                affected.add(name)
                continue
            if row_source == "claims" and change_scope.get("claim_count", 0):
                affected.add(name)
                continue
            if row_source == "relations" and change_scope.get("relation_count", 0):
                affected.add(name)
                continue
            if row_source == "events" and change_scope.get("event_count", 0):
                affected.add(name)
                continue
            if row_source == "mixed" and (
                (entity_types and entity_types & changed_types)
                or change_scope.get("claim_count", 0)
                or change_scope.get("relation_count", 0)
                or change_scope.get("event_count", 0)
            ):
                affected.add(name)

        if not affected:
            affected = {
                str(item.get("name", "")).strip()
                for item in database_plan.get("databases", [])
                if str(item.get("name", "")).strip()
            }
        return affected

    @staticmethod
    def _preserve_unaffected_databases(
        existing_multi_db: Dict[str, Any],
        database_plan: Dict[str, Any],
        affected_table_names: set[str],
    ) -> List[Dict[str, Any]]:
        allowed_names = {
            str(item.get("name", "")).strip()
            for item in database_plan.get("databases", [])
            if str(item.get("name", "")).strip()
        }
        preserved: List[Dict[str, Any]] = []
        for database in existing_multi_db.get("databases", []):
            name = str(database.get("name", "")).strip()
            if not name or name in affected_table_names or name not in allowed_names:
                continue
            preserved.append(dict(database))
        return preserved

    def _generate_database_plan(
        self,
        state,
        governance,
        task: TaskRuntime | None = None,
        change_scope: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        effective_change_scope = change_scope or self._build_change_scope(
            entities=list(state.get("entities", []) or []),
            claims=list(state.get("claims", []) or []),
            relations=list(state.get("relations", []) or []),
            events=list(state.get("events", []) or []),
        )
        learned_plan = build_learned_database_plan(
            effective_change_scope.get("entity_types", []),
            effective_change_scope.get("semantic_tags", []),
        )
        if learned_plan:
            finalized = self._finalize_database_plan(learned_plan)
            self.trace.log(
                "database_plan_loaded_from_learning",
                databases=len(finalized.get("databases", [])),
                entity_types=sorted(effective_change_scope.get("entity_types", [])),
                semantic_tags=sorted(effective_change_scope.get("semantic_tags", [])),
            )
            return finalized

        rule_plan = build_rule_database_plan(state)
        if rule_plan:
            finalized = self._finalize_database_plan(rule_plan)
            self.trace.log(
                "database_plan_built_by_rule",
                databases=len(finalized.get("databases", [])),
                entity_types=sorted(
                    {
                        str(item.get("type", "")).strip()
                        for item in state.get("entities", [])
                        if str(item.get("type", "")).strip()
                    }
                ),
            )
            return finalized

        ontology_agent_path = Path("mindvault/agents/ontology_agent.yaml")
        if not ontology_agent_path.exists():
            raise RuntimeError("ontology_agent definition not found")
        context = self._build_modeling_context(state, governance)
        try:
            result = self.executor.execute(
                ontology_agent_path,
                context,
                heartbeat=self._make_agent_progress_callback(
                    task,
                    step="database_plan",
                    agent="ontology_agent",
                    resume_hint="正在规划数据表结构，模型仍在处理中。",
                ),
            )
            self._raise_on_agent_error(result, "ontology_agent")
            normalized = self._normalize_database_plan_result(result)
            if normalized:
                finalized = self._finalize_database_plan(normalized)
                if save_learned_schema(
                    effective_change_scope.get("entity_types", []),
                    effective_change_scope.get("semantic_tags", []),
                    finalized,
                ):
                    self.trace.log(
                        "database_plan_saved_to_learning",
                        databases=len(finalized.get("databases", [])),
                        entity_types=sorted(effective_change_scope.get("entity_types", [])),
                        semantic_tags=sorted(effective_change_scope.get("semantic_tags", [])),
                    )
                return finalized
            raw_preview = ""
            if isinstance(result, dict):
                raw_preview = str(result.get("raw_content", "") or result.get("_raw_content", "")).strip()[:240]
            if raw_preview:
                raise RuntimeError(f"ontology_agent returned no structured database plan: {raw_preview}")
            raise RuntimeError("ontology_agent returned no structured database plan")
        except Exception as exc:
            fallback_plan = self._finalize_database_plan(
                build_fallback_plan(state, effective_change_scope.get("entity_types", []))
            )
            self.trace.log(
                "ontology_agent_fallback",
                reason=str(exc),
                entity_types=sorted(effective_change_scope.get("entity_types", [])),
                semantic_tags=sorted(effective_change_scope.get("semantic_tags", [])),
                databases=len(fallback_plan.get("databases", [])),
            )
            return fallback_plan

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

    def _generate_multi_db(
        self,
        state,
        database_plan: Dict[str, Any],
        workers: int = 1,
        change_scope: Dict[str, Any] | None = None,
        task: TaskRuntime | None = None,
    ) -> Tuple[Dict[str, Any], List[Dict[str, str]]]:
        database_builder_agent_path = Path("mindvault/agents/database_builder_agent.yaml")
        if not database_builder_agent_path.exists():
            raise RuntimeError("database_builder_agent definition not found")
        self.executor.load_agent(database_builder_agent_path)
        built_databases: List[Dict[str, Any]] = []
        warnings: List[Dict[str, str]] = []
        existing_multi_db = self._load_existing_multi_db()
        affected_table_names = self._determine_affected_tables(
            database_plan,
            existing_multi_db,
            change_scope or {},
        )
        self.trace.log(
            "multi_db_incremental_scope",
            affected_tables=sorted(affected_table_names),
            existing_tables=sorted(item.get("name", "") for item in existing_multi_db.get("databases", [])),
        )
        database_specs = [
            spec
            for spec in database_plan.get("databases", [])
            if spec.get("name", "") in affected_table_names
        ]
        rule_specs: List[Dict[str, Any]] = []
        direct_specs: List[Dict[str, Any]] = []
        llm_specs: List[Dict[str, Any]] = []
        for spec in database_specs:
            if self._can_build_rule_database(state, spec):
                rule_specs.append(spec)
            elif self._can_build_direct_database(spec):
                direct_specs.append(spec)
            else:
                llm_specs.append(spec)

        for database_spec in rule_specs:
            table_started = time.perf_counter()
            payload = self._build_rule_database(state, database_spec, database_plan)
            built_databases.append(payload)
            self.trace.log(
                "multi_db_table_built_rule",
                table=database_spec.get("name", ""),
                row_source=database_spec.get("row_source", ""),
                rows=len(payload.get("rows", [])),
            )
            self.trace.log(
                "perf",
                step="multi_db_table",
                table=database_spec.get("name", ""),
                built_by=payload.get("built_by", "rule"),
                duration_s=round(time.perf_counter() - table_started, 2),
                rows=len(payload.get("rows", [])),
            )
            self._push_table_ready(task, payload)

        for database_spec in direct_specs:
            table_started = time.perf_counter()
            payload = self._build_direct_database(state, database_spec)
            built_databases.append(payload)
            self.trace.log(
                "multi_db_table_built_direct",
                table=database_spec.get("name", ""),
                row_source=database_spec.get("row_source", ""),
                rows=len(payload.get("rows", [])),
            )
            self.trace.log(
                "perf",
                step="multi_db_table",
                table=database_spec.get("name", ""),
                built_by=payload.get("built_by", "direct"),
                duration_s=round(time.perf_counter() - table_started, 2),
                rows=len(payload.get("rows", [])),
            )
            self._push_table_ready(task, payload)

        table_started_at: Dict[str, float] = {}
        with ThreadPoolExecutor(max_workers=min(max(1, workers), max(1, len(llm_specs)))) as pool:
            future_to_table = {
                pool.submit(
                    self._build_single_database_payloads,
                    database_builder_agent_path,
                    database_plan,
                    state,
                    database_spec,
                    task,
                ): database_spec.get("name", "")
                for database_spec in llm_specs
            }
            table_started_at = {
                database_spec.get("name", ""): time.perf_counter()
                for database_spec in llm_specs
            }
            for future in as_completed(future_to_table):
                table_name = future_to_table[future]
                try:
                    payloads = future.result()
                    built_databases.extend(payloads)
                    built_by = payloads[0].get("built_by", "llm") if payloads else "llm"
                    self.trace.log(
                        "perf",
                        step="multi_db_table",
                        table=table_name,
                        built_by=built_by,
                        duration_s=round(time.perf_counter() - table_started_at.get(table_name, time.perf_counter()), 2),
                        rows=sum(len(payload.get("rows", [])) for payload in payloads),
                    )
                    for payload in payloads:
                        self._push_table_ready(task, payload)
                except Exception as exc:
                    error_text = str(exc)
                    self.trace.log(
                        "multi_db_table_failed",
                        table=table_name,
                        error=error_text,
                    )
                    self.trace.log(
                        "perf",
                        step="multi_db_table",
                        table=table_name,
                        built_by="llm",
                        duration_s=round(time.perf_counter() - table_started_at.get(table_name, time.perf_counter()), 2),
                        status="failed",
                    )
                    warnings.append(
                        {
                            "step": "multi_db",
                            "agent": "database_builder_agent",
                            "error": error_text,
                            "table": table_name,
                        }
                    )

        if not built_databases:
            details = "; ".join(
                f"{item.get('table', '')}: {item.get('error', '')}"
                for item in warnings
            )
            raise RuntimeError(details or "database_builder_agent produced no usable table output")

        preserved_databases = self._preserve_unaffected_databases(
            existing_multi_db,
            database_plan,
            affected_table_names,
        )
        merged_payload = {
            "domain": database_plan.get("domain", ""),
            "generated_at": datetime.utcnow().isoformat(),
            "databases": preserved_databases + built_databases,
            "relations": database_plan.get("relations", []),
            "metadata": {
                "rebuilt_tables": sorted(affected_table_names),
                "preserved_tables": sorted(
                    item.get("name", "")
                    for item in preserved_databases
                ),
            },
        }
        return self._finalize_multi_db(merged_payload, database_plan), warnings

    def _can_build_rule_database(self, state: Dict[str, Any], database_spec: Dict[str, Any]) -> bool:
        row_source = str(database_spec.get("row_source", "mixed") or "mixed").strip().lower()
        if row_source in {"claims", "relations", "events", "sources"}:
            return False
        if str(database_spec.get("built_by", "")).strip().lower() == "fallback":
            return True
        entity_types = [
            str(item).strip()
            for item in (database_spec.get("entity_types", []) or [])
            if str(item).strip()
        ]
        if len(entity_types) != 1:
            return False
        entity_type = entity_types[0]
        entities = [
            entity
            for entity in state.get("entities", [])
            if str(entity.get("type", "")).strip() == entity_type
        ]
        return can_build_by_rule(entity_type, entities)

    def _build_rule_database(
        self,
        state: Dict[str, Any],
        database_spec: Dict[str, Any],
        database_plan: Dict[str, Any],
    ) -> Dict[str, Any]:
        entity_types = [
            str(item).strip()
            for item in (database_spec.get("entity_types", []) or [])
            if str(item).strip()
        ]
        entity_type = entity_types[0] if entity_types else str(database_spec.get("title", "") or database_spec.get("name", "entity"))
        scoped_entities, _, scoped_relations, _ = self._scope_modeling_records(
            entities=list(state.get("entities", []) or []),
            claims=list(state.get("claims", []) or []),
            relations=list(state.get("relations", []) or []),
            events=list(state.get("events", []) or []),
            database_spec=database_spec,
            database_plan=database_plan,
        )
        selected_entities = [
            entity for entity in scoped_entities
            if str(entity.get("type", "")).strip() == entity_type
        ]
        entity_index = {
            str(entity.get("id", entity.get("entity_id", ""))).strip(): entity
            for entity in state.get("entities", [])
            if str(entity.get("id", entity.get("entity_id", ""))).strip()
        }
        payload = build_table_by_rule(
            entity_type,
            selected_entities,
            scoped_relations,
            database_spec=database_spec,
            entity_index=entity_index,
        )
        if database_spec.get("built_by"):
            payload["built_by"] = database_spec.get("built_by")
        normalized_rows = [self._normalize_row_shape(item) for item in payload.get("rows", [])]
        columns = self._collect_columns(normalized_rows)
        planned_fields = [
            field for field in database_spec.get("suggested_fields", [])
            if field not in columns
        ]
        payload["rows"] = normalized_rows
        payload["columns"] = columns + planned_fields
        payload["primary_key"] = database_spec.get("primary_key") or payload.get("primary_key") or self._infer_primary_key(payload["columns"])
        payload["visibility"] = database_spec.get("visibility", payload.get("visibility", self._infer_database_visibility(payload.get("name", ""))))
        payload["row_count"] = len(normalized_rows)
        return payload

    @staticmethod
    def _push_table_ready(task: TaskRuntime | None, payload: Dict[str, Any]) -> None:
        if task is None:
            return
        task.push_table_ready(payload)

    @staticmethod
    def _can_build_direct_database(database_spec: Dict[str, Any]) -> bool:
        row_source = str(database_spec.get("row_source", "mixed") or "mixed").strip().lower()
        name = str(database_spec.get("name", "")).strip().lower()
        record_granularity = str(database_spec.get("record_granularity", "") or "").strip().lower()
        return (
            row_source in {"entities", "claims", "relations", "events"}
            or name == "sources"
            or record_granularity == "discussion"
            or "discussion" in name
        )

    def _build_direct_database(self, state: Dict[str, Any], database_spec: Dict[str, Any]) -> Dict[str, Any]:
        row_source = str(database_spec.get("row_source", "mixed") or "mixed").strip().lower()
        name = str(database_spec.get("name", "")).strip()
        record_granularity = str(database_spec.get("record_granularity", "") or "").strip().lower()

        if row_source == "entities":
            rows = self._build_entity_table_rows(state, database_spec)
        elif row_source == "claims":
            rows = self._build_claim_table_rows(state)
        elif row_source == "relations":
            rows = self._build_relation_table_rows(state)
        elif row_source == "events":
            rows = self._build_event_table_rows(state)
        elif name.lower() == "sources":
            rows = self._build_source_table_rows(state)
        elif record_granularity == "discussion" or "discussion" in name.lower():
            rows = self._build_discussion_table_rows(state)
        else:
            rows = []

        normalized_rows = [self._normalize_row_shape(item) for item in rows]
        columns = self._collect_columns(normalized_rows)
        planned_fields = [
            field for field in database_spec.get("suggested_fields", [])
            if field not in columns
        ]
        return {
            "name": name,
            "title": database_spec.get("title", name),
            "description": database_spec.get("description", ""),
            "primary_key": database_spec.get("primary_key") or self._infer_primary_key(columns + planned_fields),
            "columns": columns + planned_fields,
            "rows": normalized_rows,
            "visibility": database_spec.get("visibility", self._infer_database_visibility(name)),
            "built_by": "direct",
        }

    @staticmethod
    def _build_entity_table_rows(state: Dict[str, Any], database_spec: Dict[str, Any]) -> List[Dict[str, Any]]:
        entity_types = {
            str(item).strip()
            for item in (database_spec.get("entity_types", []) or [])
            if str(item).strip()
        }
        rows: List[Dict[str, Any]] = []
        for entity in state.get("entities", []):
            if entity_types and entity.get("type") not in entity_types:
                continue
            row = {
                "id": entity.get("id", entity.get("entity_id", "")),
                "name": entity.get("name", ""),
                "type": entity.get("type", ""),
                "confidence": entity.get("confidence"),
                "source_refs": entity.get("source_refs", []),
                "updated_at": entity.get("updated_at", ""),
                "status": entity.get("status", ""),
            }
            if isinstance(entity.get("attributes"), dict):
                row.update(entity.get("attributes", {}))
            rows.append(row)
        return rows

    @staticmethod
    def _build_claim_table_rows(state: Dict[str, Any]) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        for claim in state.get("claims", []):
            rows.append({
                "id": claim.get("id", claim.get("claim_id", "")),
                "subject": claim.get("subject", ""),
                "predicate": claim.get("predicate", ""),
                "object": claim.get("object"),
                "claim_type": claim.get("claim_type", ""),
                "confidence": claim.get("confidence"),
                "source_ref": claim.get("source_ref", ""),
                "source_refs": claim.get("source_refs", []),
                "updated_at": claim.get("updated_at", ""),
                "status": claim.get("status", ""),
            })
        return rows

    @staticmethod
    def _build_relation_table_rows(state: Dict[str, Any]) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        for relation in state.get("relations", []):
            source = relation.get("source", relation.get("source_entity", ""))
            target = relation.get("target", relation.get("target_entity", ""))
            rel_type = relation.get("relation", relation.get("relation_type", ""))
            rows.append({
                "id": f"{source}:{rel_type}:{target}",
                "source": source,
                "relation": rel_type,
                "target": target,
                "confidence": relation.get("confidence"),
                "source_refs": relation.get("source_refs", []),
                "updated_at": relation.get("updated_at", ""),
                "status": relation.get("status", ""),
            })
        return rows

    @staticmethod
    def _build_event_table_rows(state: Dict[str, Any]) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        for event in state.get("events", []):
            row = {
                "id": event.get("id", event.get("event_id", "")),
                "type": event.get("type", ""),
                "description": event.get("description", ""),
                "timestamp": event.get("timestamp", ""),
                "participants": event.get("participants", []),
                "confidence": event.get("confidence"),
                "source_refs": event.get("source_refs", []),
                "updated_at": event.get("updated_at", ""),
                "status": event.get("status", ""),
            }
            if isinstance(event.get("attributes"), dict):
                row.update(event.get("attributes", {}))
            rows.append(row)
        return rows

    @staticmethod
    def _build_source_table_rows(state: Dict[str, Any]) -> List[Dict[str, Any]]:
        source_map: Dict[str, Dict[str, Any]] = {}
        def touch_source(source_id: str, kind: str) -> None:
            if not source_id:
                return
            row = source_map.setdefault(source_id, {
                "id": source_id,
                "name": source_id,
                "mentions": 0,
                "kinds": [],
            })
            row["mentions"] += 1
            if kind not in row["kinds"]:
                row["kinds"].append(kind)

        for entity in state.get("entities", []):
            for source_ref in entity.get("source_refs", []) or []:
                touch_source(str(source_ref), "entity")
        for claim in state.get("claims", []):
            if claim.get("source_ref"):
                touch_source(str(claim.get("source_ref")), "claim")
            for source_ref in claim.get("source_refs", []) or []:
                touch_source(str(source_ref), "claim")
        for event in state.get("events", []):
            for source_ref in event.get("source_refs", []) or []:
                touch_source(str(source_ref), "event")
        return list(source_map.values())

    def _build_discussion_table_rows(self, state: Dict[str, Any]) -> List[Dict[str, Any]]:
        entity_by_id = {
            str(entity.get("id", entity.get("entity_id", ""))).strip(): entity
            for entity in state.get("entities", [])
            if str(entity.get("id", entity.get("entity_id", ""))).strip()
        }
        entity_name_index = {
            str(entity.get("name", "")).strip().lower(): entity
            for entity in state.get("entities", [])
            if str(entity.get("name", "")).strip()
        }

        rows: List[Dict[str, Any]] = []
        seen_ids: set[str] = set()

        def entity_from_token(token: Any) -> Dict[str, Any] | None:
            if token is None:
                return None
            text = str(token).strip()
            if not text:
                return None
            if text in entity_by_id:
                return entity_by_id[text]
            return entity_name_index.get(text.lower())

        def add_row(row: Dict[str, Any]) -> None:
            row_id = str(row.get("id", "")).strip()
            if not row_id or row_id in seen_ids:
                return
            seen_ids.add(row_id)
            rows.append(row)

        for relation in state.get("relations", []):
            source_id = str(relation.get("source", relation.get("source_entity", "")) or "").strip()
            target_id = str(relation.get("target", relation.get("target_entity", "")) or "").strip()
            relation_type = str(relation.get("relation", relation.get("relation_type", "")) or "related_to").strip() or "related_to"
            source_entity = entity_by_id.get(source_id)
            target_entity = entity_by_id.get(target_id)
            evidence = str(relation.get("evidence", "") or "").strip()
            source_refs = relation.get("source_refs", []) or []
            target_type = str((target_entity or {}).get("type", "")).strip().lower()

            add_row(
                {
                    "id": str(relation.get("id", "")) or f"{source_id}:{relation_type}:{target_id}",
                    "discussion_topic": (target_entity or {}).get("name", "") or relation_type,
                    "participant_id": source_id,
                    "participant_name": (source_entity or {}).get("name", "") or source_id,
                    "related_node_id": target_id,
                    "related_node_name": (target_entity or {}).get("name", "") or target_id,
                    "product_id": target_id if target_type == "product" else "",
                    "organization_id": target_id if target_type == "organization" else "",
                    "opinion_type": relation_type,
                    "content_summary": evidence or f"{(source_entity or {}).get('name', source_id)} {relation_type} {(target_entity or {}).get('name', target_id)}",
                    "confidence_level": relation.get("confidence"),
                    "source_ref": source_refs[0] if source_refs else "",
                    "source_refs": source_refs,
                    "discussion_date": relation.get("updated_at", ""),
                    "sentiment": self._infer_discussion_sentiment(relation_type, evidence),
                    "technical_focus": (target_entity or {}).get("name", "") if target_type in {"product", "organization", "service"} else "",
                }
            )

        for claim in state.get("claims", []):
            claim_id = str(claim.get("id", claim.get("claim_id", "")) or "").strip()
            subject = claim.get("subject", "")
            predicate = str(claim.get("predicate", "") or claim.get("claim_type", "") or "statement").strip() or "statement"
            obj = claim.get("object")
            subject_entity = entity_from_token(subject)
            object_entity = entity_from_token(obj) if not isinstance(obj, list) else None
            source_refs = claim.get("source_refs", []) or ([claim.get("source_ref")] if claim.get("source_ref") else [])
            discussion_topic = ""
            if object_entity and object_entity.get("name"):
                discussion_topic = str(object_entity.get("name", ""))
            elif subject_entity and subject_entity.get("name"):
                discussion_topic = str(subject_entity.get("name", ""))
            else:
                discussion_topic = str(obj if obj not in (None, "", []) else subject).strip() or predicate
            target_type = str((object_entity or {}).get("type", "")).strip().lower()
            add_row(
                {
                    "id": claim_id or f"claim_discussion:{hashlib.sha1(str(claim).encode('utf-8')).hexdigest()[:12]}",
                    "discussion_topic": discussion_topic,
                    "participant_id": str((subject_entity or {}).get("id", "")),
                    "participant_name": str((subject_entity or {}).get("name", "")),
                    "related_node_id": str((object_entity or {}).get("id", "")),
                    "related_node_name": str((object_entity or {}).get("name", "")) or ("" if isinstance(obj, list) else str(obj or "")),
                    "product_id": str((object_entity or {}).get("id", "")) if target_type == "product" else "",
                    "organization_id": str((object_entity or {}).get("id", "")) if target_type == "organization" else "",
                    "opinion_type": str(claim.get("claim_type", "")) or predicate,
                    "content_summary": str(claim.get("claim_text", "") or f"{subject} {predicate} {obj}").strip(),
                    "confidence_level": claim.get("confidence"),
                    "source_ref": claim.get("source_ref", "") or (source_refs[0] if source_refs else ""),
                    "source_refs": source_refs,
                    "discussion_date": claim.get("updated_at", ""),
                    "sentiment": self._infer_discussion_sentiment(str(claim.get("claim_type", "")) or predicate, str(claim.get("claim_text", "") or "")),
                    "technical_focus": discussion_topic,
                }
            )

        return rows

    @staticmethod
    def _infer_discussion_sentiment(opinion_type: str, content: str) -> str:
        text = f"{opinion_type} {content}".lower()
        positive_markers = ("recommend", "use", "support", "promote", "good", "方便", "欢迎", "优化", "支持")
        negative_markers = ("issue", "problem", "fail", "error", "useless", "没用", "用不了", "风险", "bug", "超时")
        if any(marker in text for marker in negative_markers):
            return "negative"
        if any(marker in text for marker in positive_markers):
            return "positive"
        return "neutral"

    def _build_graph_payload(self, state: Dict[str, Any]) -> Dict[str, Any]:
        nodes: Dict[str, Dict[str, Any]] = {}
        edges: List[Dict[str, Any]] = []
        name_index: Dict[str, str] = {}

        def register_node(node: Dict[str, Any]) -> None:
            node_id = str(node.get("id", "")).strip()
            if not node_id:
                return
            existing = nodes.get(node_id)
            if existing:
                existing_sources = set(existing.get("source_refs", []) or [])
                existing_sources.update(node.get("source_refs", []) or [])
                existing["source_refs"] = sorted(existing_sources)
                attributes = dict(existing.get("attributes", {}) or {})
                attributes.update(node.get("attributes", {}) or {})
                existing["attributes"] = attributes
                if not existing.get("label") and node.get("label"):
                    existing["label"] = node["label"]
                return
            normalized = {
                "id": node_id,
                "name": str(node.get("label", "") or node_id),
                "label": str(node.get("label", "") or node_id),
                "type": str(node.get("category", "") or "concept"),
                "kind": str(node.get("kind", "") or "entity"),
                "category": str(node.get("category", "") or "concept"),
                "table": str(node.get("table_name", "") or "concepts"),
                "table_name": str(node.get("table_name", "") or "concepts"),
                "source_refs": sorted({str(item) for item in (node.get("source_refs", []) or []) if str(item).strip()}),
                "attributes": dict(node.get("attributes", {}) or {}),
            }
            nodes[node_id] = normalized
            lowered = normalized["label"].strip().lower()
            if lowered and lowered not in name_index:
                name_index[lowered] = node_id

        def resolve_value_to_node(value: Any, *, claim: Dict[str, Any] | None = None, fallback_prefix: str = "literal") -> str:
            if value is None:
                return ""
            if isinstance(value, (int, float, bool)):
                literal_value = str(value)
            else:
                literal_value = str(value).strip()
            if not literal_value:
                return ""
            if literal_value in nodes:
                return literal_value
            if literal_value.lower() in name_index:
                return name_index[literal_value.lower()]

            literal_id = f"lit_{hashlib.sha1(f'{fallback_prefix}:{literal_value}'.encode('utf-8')).hexdigest()[:16]}"
            register_node(
                {
                    "id": literal_id,
                    "label": literal_value,
                    "kind": "literal",
                    "category": fallback_prefix,
                    "table_name": "concepts",
                    "source_refs": (claim or {}).get("source_refs", []) or ([claim.get("source_ref")] if claim and claim.get("source_ref") else []),
                    "attributes": {
                        "value": literal_value,
                    },
                }
            )
            return literal_id

        for entity in state.get("entities", []):
            entity_id = str(entity.get("id", entity.get("entity_id", ""))).strip()
            if not entity_id:
                continue
            entity_type = str(entity.get("type", "") or "entity").strip() or "entity"
            attributes = dict(entity.get("attributes", {}) or {})
            for key in ("confidence", "status", "updated_at"):
                if entity.get(key) not in (None, "", []):
                    attributes.setdefault(key, entity.get(key))
            register_node(
                {
                    "id": entity_id,
                    "label": str(entity.get("name", "") or entity_id),
                    "kind": "entity",
                    "category": entity_type,
                    "table_name": self._default_table_name_for_entity_type(entity_type),
                    "source_refs": entity.get("source_refs", []) or [],
                    "attributes": attributes,
                }
            )

        for event in state.get("events", []):
            event_id = str(event.get("id", event.get("event_id", ""))).strip()
            if not event_id:
                continue
            event_type = str(event.get("type", "") or "event").strip() or "event"
            attributes = dict(event.get("attributes", {}) or {})
            for key in ("description", "timestamp", "confidence", "status", "updated_at"):
                if event.get(key) not in (None, "", []):
                    attributes.setdefault(key, event.get(key))
            register_node(
                {
                    "id": event_id,
                    "label": str(event.get("description", "") or event_type),
                    "kind": "event",
                    "category": event_type,
                    "table_name": "events",
                    "source_refs": event.get("source_refs", []) or [],
                    "attributes": attributes,
                }
            )
            for participant in event.get("participants", []) or []:
                source_id = resolve_value_to_node(participant, claim={"source_refs": event.get("source_refs", []) or []}, fallback_prefix="participant")
                if not source_id:
                    continue
                edges.append(
                    {
                        "id": f"{source_id}:participates_in:{event_id}",
                        "source": source_id,
                        "target": event_id,
                        "type": "participates_in",
                        "label": "participates_in",
                        "relation": "participates_in",
                        "kind": "event_participation",
                        "source_refs": event.get("source_refs", []) or [],
                        "confidence": event.get("confidence"),
                    }
                )

        for relation in state.get("relations", []):
            source_id = resolve_value_to_node(relation.get("source", relation.get("source_entity", "")), claim=relation, fallback_prefix="relation_source")
            target_id = resolve_value_to_node(relation.get("target", relation.get("target_entity", "")), claim=relation, fallback_prefix="relation_target")
            if not source_id or not target_id:
                continue
            relation_type = str(relation.get("relation", relation.get("relation_type", "")) or "related_to").strip() or "related_to"
            edges.append(
                {
                    "id": f"{source_id}:{relation_type}:{target_id}",
                    "source": source_id,
                    "target": target_id,
                    "type": relation_type,
                    "label": relation_type,
                    "relation": relation_type,
                    "kind": "relation",
                    "source_refs": relation.get("source_refs", []) or [],
                    "confidence": relation.get("confidence"),
                }
            )

        for claim in state.get("claims", []):
            predicate = str(claim.get("predicate", claim.get("claim_type", "")) or "claims").strip() or "claims"
            subject_id = resolve_value_to_node(claim.get("subject", ""), claim=claim, fallback_prefix="claim_subject")
            objects = claim.get("object")
            targets = objects if isinstance(objects, list) else [objects]
            for target in targets:
                target_id = resolve_value_to_node(target, claim=claim, fallback_prefix="claim_object")
                if not subject_id or not target_id:
                    continue
                edges.append(
                    {
                        "id": f"{subject_id}:{predicate}:{target_id}",
                        "source": subject_id,
                        "target": target_id,
                        "type": predicate,
                        "label": predicate,
                        "relation": predicate,
                        "kind": "claim",
                        "source_refs": claim.get("source_refs", []) or ([claim.get("source_ref")] if claim.get("source_ref") else []),
                        "confidence": claim.get("confidence"),
                    }
                )

        deduped_edges: List[Dict[str, Any]] = []
        seen_edges: set[str] = set()
        for edge in edges:
            edge_id = str(edge.get("id", "")).strip()
            if not edge_id or edge_id in seen_edges:
                continue
            seen_edges.add(edge_id)
            deduped_edges.append(edge)

        return {
            "generated_at": datetime.utcnow().isoformat(),
            "domain": self._guess_graph_domain(state),
            "nodes": sorted(nodes.values(), key=lambda item: (str(item.get("table_name", "")), str(item.get("label", "")))),
            "edges": deduped_edges,
            "metadata": {
                "node_count": len(nodes),
                "edge_count": len(deduped_edges),
                "entity_count": len(state.get("entities", [])),
                "event_count": len(state.get("events", [])),
                "claim_count": len(state.get("claims", [])),
                "relation_count": len(state.get("relations", [])),
            },
        }

    @staticmethod
    def _guess_graph_domain(state: Dict[str, Any]) -> str:
        types: Dict[str, int] = {}
        for entity in state.get("entities", []):
            entity_type = str(entity.get("type", "") or "entity").strip() or "entity"
            types[entity_type] = types.get(entity_type, 0) + 1
        if not types:
            return "全景关系图"
        top_types = sorted(types.items(), key=lambda item: (-item[1], item[0]))[:3]
        return " / ".join(item[0] for item in top_types)

    @staticmethod
    def _default_table_name_for_entity_type(entity_type: str) -> str:
        normalized = str(entity_type or "entity").strip().lower()
        mapping = {
            "person": "persons",
            "people": "persons",
            "organization": "organizations",
            "company": "organizations",
            "product": "products",
            "service": "services",
            "venue": "venues",
            "area": "areas",
            "event": "events",
            "topic": "topics",
            "opinion": "opinions",
            "signal": "signals",
            "resource": "resources",
            "project": "projects",
            "role": "roles",
        }
        return mapping.get(normalized, f"{normalized}s" if not normalized.endswith("s") else normalized)

    def _filter_graph_by_source_ids(self, graph_payload: Dict[str, Any], source_ids: set[str]) -> Dict[str, Any]:
        if not source_ids:
            return {
                **graph_payload,
                "nodes": [],
                "edges": [],
                "metadata": {
                    **dict(graph_payload.get("metadata", {}) or {}),
                    "scope": "current_ingest",
                    "source_ids": [],
                    "node_count": 0,
                    "edge_count": 0,
                },
            }

        touched_node_ids = {
            str(node.get("id", "")).strip()
            for node in graph_payload.get("nodes", [])
            if set(str(item) for item in (node.get("source_refs", []) or [])).intersection(source_ids)
        }
        touched_edges = []
        for edge in graph_payload.get("edges", []):
            edge_sources = {str(item) for item in (edge.get("source_refs", []) or []) if str(item).strip()}
            if edge_sources.intersection(source_ids):
                touched_edges.append(edge)
                touched_node_ids.add(str(edge.get("source", "")).strip())
                touched_node_ids.add(str(edge.get("target", "")).strip())

        nodes = [
            node
            for node in graph_payload.get("nodes", [])
            if str(node.get("id", "")).strip() in touched_node_ids
        ]
        node_id_set = {str(node.get("id", "")).strip() for node in nodes}
        edges = [
            edge
            for edge in touched_edges
            if str(edge.get("source", "")).strip() in node_id_set and str(edge.get("target", "")).strip() in node_id_set
        ]
        return {
            "generated_at": graph_payload.get("generated_at", ""),
            "domain": graph_payload.get("domain", ""),
            "nodes": nodes,
            "edges": edges,
            "metadata": {
                **dict(graph_payload.get("metadata", {}) or {}),
                "scope": "current_ingest",
                "source_ids": sorted(source_ids),
                "node_count": len(nodes),
                "edge_count": len(edges),
            },
        }

    def _build_single_database_payloads(
        self,
        database_builder_agent_path: Path,
        database_plan: Dict[str, Any],
        state: Dict[str, Any],
        database_spec: Dict[str, Any],
        task: TaskRuntime | None = None,
    ) -> List[Dict[str, Any]]:
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
        modeling_context = self._build_modeling_context(
            state,
            database_spec=database_spec,
            database_plan=database_plan,
        )
        self.trace.log(
            "multi_db_table_context",
            table=database_spec.get("name", ""),
            entities=len(modeling_context.get("entities", [])),
            claims=len(modeling_context.get("claims", [])),
            relations=len(modeling_context.get("relations", [])),
            events=len(modeling_context.get("events", [])),
        )
        context_batches = self._split_modeling_context_into_batches(modeling_context)
        table_payloads: List[Dict[str, Any]] = []
        for batch_index, batch_context in enumerate(context_batches, start=1):
            batch_started = time.perf_counter()
            if task is not None:
                task.log_step(
                    "multi_db",
                    "running",
                    agent="database_builder_agent",
                    resume_hint=f"正在生成数据表 {database_spec.get('title', database_spec.get('name', ''))}（第 {batch_index}/{len(context_batches)} 批）。",
                    table=database_spec.get("name", ""),
                    batch=batch_index,
                    total_batches=len(context_batches),
                )
            self.trace.log(
                "multi_db_table_batch",
                table=database_spec.get("name", ""),
                batch=batch_index,
                total_batches=len(context_batches),
                entities=len(batch_context.get("entities", [])),
                claims=len(batch_context.get("claims", [])),
                relations=len(batch_context.get("relations", [])),
                events=len(batch_context.get("events", [])),
            )
            context = {
                "database_plan": single_plan,
                **batch_context,
            }
            result = self.executor.execute(
                database_builder_agent_path,
                context,
                heartbeat=self._make_agent_progress_callback(
                    task,
                    step="multi_db",
                    agent="database_builder_agent",
                    resume_hint=f"正在生成数据表 {database_spec.get('title', database_spec.get('name', ''))}（第 {batch_index}/{len(context_batches)} 批），模型仍在处理中。",
                ),
            )
            self._raise_on_agent_error(result, "database_builder_agent")
            normalized_tables = self._normalize_database_builder_result(result, database_spec)
            if not normalized_tables:
                raise RuntimeError(
                    f"database_builder_agent returned no structured table output for '{database_spec.get('name', '')}'"
                )
            table_payloads.extend(normalized_tables)
            self.trace.log(
                "perf",
                step="multi_db_batch",
                table=database_spec.get("name", ""),
                batch=batch_index,
                total_batches=len(context_batches),
                duration_s=round(time.perf_counter() - batch_started, 2),
                rows=sum(len(item.get("rows", [])) for item in normalized_tables),
            )
        return self._merge_database_payloads(table_payloads, database_spec)

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

    def _build_modeling_context(
        self,
        state: Dict[str, Any],
        governance: Dict[str, Any] | None = None,
        database_spec: Dict[str, Any] | None = None,
        database_plan: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        entities = state.get("entities", [])
        claims = state.get("claims", [])
        relations = state.get("relations", [])
        events = state.get("events", [])

        if database_spec is not None:
            entities, claims, relations, events = self._scope_modeling_records(
                entities=entities,
                claims=claims,
                relations=relations,
                events=events,
                database_spec=database_spec,
                database_plan=database_plan or {},
            )

        return {
            "entities": [self._compact_entity_for_modeling(item) for item in entities],
            "claims": [self._compact_claim_for_modeling(item) for item in claims],
            "relations": [self._compact_relation_for_modeling(item) for item in relations],
            "events": [self._compact_event_for_modeling(item) for item in events],
            "governance": self._compact_governance_for_modeling(governance or {}),
        }

    def _scope_modeling_records(
        self,
        *,
        entities: List[Dict[str, Any]],
        claims: List[Dict[str, Any]],
        relations: List[Dict[str, Any]],
        events: List[Dict[str, Any]],
        database_spec: Dict[str, Any],
        database_plan: Dict[str, Any],
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
        entity_by_id = {
            str(item.get("id", "")): item
            for item in entities
            if item.get("id")
        }
        database_name = str(database_spec.get("name", "")).strip()
        row_source = str(database_spec.get("row_source", "mixed") or "mixed").strip().lower()
        target_types = self._collect_relevant_entity_types(database_spec, database_plan)

        selected_entities = [
            item for item in entities
            if item.get("type") in target_types
        ] if target_types else []
        if row_source == "entities" and not selected_entities:
            selected_entities = list(entities)

        seed_ids = {
            str(item.get("id", ""))
            for item in selected_entities
            if item.get("id")
        }

        selected_claims = [
            item for item in claims
            if self._claim_matches_seed_ids(item, seed_ids)
        ]
        selected_relations = [
            item for item in relations
            if self._relation_matches_seed_ids(item, seed_ids)
        ]
        selected_events = [
            item for item in events
            if self._event_matches_seed_ids(item, seed_ids)
        ]

        if row_source == "entities":
            selected_events = []

        if row_source in {"claims", "mixed"} and not selected_claims:
            selected_claims = list(claims)
        if row_source in {"relations", "mixed"} and not selected_relations:
            selected_relations = list(relations)
        if row_source in {"events", "mixed"} and not selected_events:
            selected_events = list(events)

        referenced_ids = set(seed_ids)
        for claim in selected_claims:
            referenced_ids.update(self._extract_claim_entity_ids(claim))
        for relation in selected_relations:
            referenced_ids.update(self._extract_relation_entity_ids(relation))
        for event in selected_events:
            referenced_ids.update(self._extract_event_entity_ids(event))

        if referenced_ids:
            selected_entities = self._unique_records_by_key(
                selected_entities + [
                    entity_by_id[item_id]
                    for item_id in referenced_ids
                    if item_id in entity_by_id
                ],
                "id",
            )

        allowed_entity_ids = {
            str(item.get("id", ""))
            for item in selected_entities
            if item.get("id")
        }

        if allowed_entity_ids:
            selected_claims = [
                item for item in selected_claims
                if self._claim_matches_seed_ids(item, allowed_entity_ids)
            ]
            selected_relations = [
                item for item in selected_relations
                if self._relation_matches_seed_ids(item, allowed_entity_ids)
            ]
            selected_events = [
                item for item in selected_events
                if self._event_matches_seed_ids(item, allowed_entity_ids)
            ]

        self.trace.log(
            "multi_db_scope_applied",
            table=database_name,
            row_source=row_source,
            target_types=sorted(target_types),
            entities=len(selected_entities),
            claims=len(selected_claims),
            relations=len(selected_relations),
            events=len(selected_events),
        )
        return selected_entities, selected_claims, selected_relations, selected_events

    @staticmethod
    def _collect_relevant_entity_types(database_spec: Dict[str, Any], database_plan: Dict[str, Any]) -> set[str]:
        types = {
            str(item).strip()
            for item in (database_spec.get("entity_types", []) or [])
            if str(item).strip()
        }
        database_name = str(database_spec.get("name", "")).strip()
        databases = {
            str(item.get("name", "")).strip(): item
            for item in database_plan.get("databases", [])
            if isinstance(item, dict)
        }
        for relation in database_plan.get("relations", []) or []:
            if not isinstance(relation, dict):
                continue
            if relation.get("from_db") == database_name:
                related = databases.get(str(relation.get("to_db", "")).strip(), {})
                types.update(
                    str(item).strip()
                    for item in (related.get("entity_types", []) or [])
                    if str(item).strip()
                )
            elif relation.get("to_db") == database_name:
                related = databases.get(str(relation.get("from_db", "")).strip(), {})
                types.update(
                    str(item).strip()
                    for item in (related.get("entity_types", []) or [])
                    if str(item).strip()
                )
        return types

    @staticmethod
    def _claim_matches_seed_ids(claim: Dict[str, Any], seed_ids: set[str]) -> bool:
        if not seed_ids:
            return False
        return bool(VaultRuntime._extract_claim_entity_ids(claim) & seed_ids)

    @staticmethod
    def _relation_matches_seed_ids(relation: Dict[str, Any], seed_ids: set[str]) -> bool:
        if not seed_ids:
            return False
        return bool(VaultRuntime._extract_relation_entity_ids(relation) & seed_ids)

    @staticmethod
    def _event_matches_seed_ids(event: Dict[str, Any], seed_ids: set[str]) -> bool:
        if not seed_ids:
            return False
        return bool(VaultRuntime._extract_event_entity_ids(event) & seed_ids)

    @staticmethod
    def _extract_claim_entity_ids(claim: Dict[str, Any]) -> set[str]:
        ids: set[str] = set()
        subject = claim.get("subject")
        if isinstance(subject, str) and subject.strip():
            ids.add(subject.strip())
        obj = claim.get("object")
        if isinstance(obj, str) and obj.strip().startswith("ent_"):
            ids.add(obj.strip())
        if isinstance(obj, list):
            ids.update(
                str(item).strip()
                for item in obj
                if isinstance(item, str) and str(item).strip().startswith("ent_")
            )
        return ids

    @staticmethod
    def _extract_relation_entity_ids(relation: Dict[str, Any]) -> set[str]:
        ids: set[str] = set()
        for key in ("source", "target", "source_entity", "target_entity"):
            value = relation.get(key)
            if isinstance(value, str) and value.strip():
                ids.add(value.strip())
        return ids

    @staticmethod
    def _extract_event_entity_ids(event: Dict[str, Any]) -> set[str]:
        ids: set[str] = set()
        for key in ("entities", "participants"):
            value = event.get(key)
            if isinstance(value, list):
                ids.update(
                    str(item).strip()
                    for item in value
                    if isinstance(item, str) and str(item).strip()
                )
        return ids

    @staticmethod
    def _unique_records_by_key(records: List[Dict[str, Any]], key: str) -> List[Dict[str, Any]]:
        result: List[Dict[str, Any]] = []
        seen: set[str] = set()
        for record in records:
            value = str(record.get(key, "")).strip()
            if not value or value in seen:
                continue
            seen.add(value)
            result.append(record)
        return result

    def _split_modeling_context_into_batches(self, context: Dict[str, Any]) -> List[Dict[str, Any]]:
        entities = list(context.get("entities", []))
        claims = list(context.get("claims", []))
        relations = list(context.get("relations", []))
        events = list(context.get("events", []))

        total_batches = max(
            1,
            self._ceil_div(len(entities), self.MODELING_ENTITY_BATCH_SIZE),
            self._ceil_div(len(claims), self.MODELING_CLAIM_BATCH_SIZE),
            self._ceil_div(len(relations), self.MODELING_RELATION_BATCH_SIZE),
            self._ceil_div(len(events), self.MODELING_EVENT_BATCH_SIZE),
        )

        batches: List[Dict[str, Any]] = []
        for batch_index in range(total_batches):
            batch = {
                "entities": self._slice_batch(
                    entities,
                    batch_index,
                    self.MODELING_ENTITY_BATCH_SIZE,
                ),
                "claims": self._slice_batch(
                    claims,
                    batch_index,
                    self.MODELING_CLAIM_BATCH_SIZE,
                ),
                "relations": self._slice_batch(
                    relations,
                    batch_index,
                    self.MODELING_RELATION_BATCH_SIZE,
                ),
                "events": self._slice_batch(
                    events,
                    batch_index,
                    self.MODELING_EVENT_BATCH_SIZE,
                ),
                "governance": context.get("governance", {}),
            }
            if any(batch[key] for key in ("entities", "claims", "relations", "events")):
                batches.append(batch)

        return batches or [context]

    @staticmethod
    def _ceil_div(value: int, chunk: int) -> int:
        if not value or not chunk:
            return 0
        return (value + chunk - 1) // chunk

    @staticmethod
    def _slice_batch(items: List[Dict[str, Any]], batch_index: int, batch_size: int) -> List[Dict[str, Any]]:
        if not items:
            return []
        start = batch_index * batch_size
        end = start + batch_size
        return items[start:end]

    def _merge_database_payloads(
        self,
        payloads: List[Dict[str, Any]],
        database_spec: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        if not payloads:
            return []

        merged_by_name: Dict[str, Dict[str, Any]] = {}
        for payload in payloads:
            name = payload.get("name") or database_spec.get("name", "")
            if name not in merged_by_name:
                merged_by_name[name] = {
                    "name": name,
                    "title": payload.get("title") or database_spec.get("title", name),
                    "description": payload.get("description") or database_spec.get("description", ""),
                    "primary_key": payload.get("primary_key") or database_spec.get("primary_key") or "id",
                    "columns": list(payload.get("columns", []) or []),
                    "rows": [],
                    "visibility": payload.get("visibility") or database_spec.get("visibility", "business"),
                }
            target = merged_by_name[name]
            target["columns"] = self._merge_columns(target.get("columns", []), payload.get("columns", []) or [])
            target["rows"] = self._merge_rows(
                target.get("rows", []),
                payload.get("rows", []) or [],
                target.get("primary_key", "id"),
            )

        return list(merged_by_name.values())

    @staticmethod
    def _merge_columns(existing: List[str], incoming: List[str]) -> List[str]:
        columns: List[str] = []
        seen = set()
        for key in list(existing) + list(incoming):
            if key not in seen:
                seen.add(key)
                columns.append(key)
        return columns

    @staticmethod
    def _merge_rows(existing: List[Dict[str, Any]], incoming: List[Dict[str, Any]], primary_key: str) -> List[Dict[str, Any]]:
        merged: List[Dict[str, Any]] = []
        index_by_id: Dict[str, int] = {}

        def row_key(row: Dict[str, Any]) -> str:
            value = row.get(primary_key) or row.get("id") or row.get("entity_id") or row.get("event_id") or row.get("claim_id")
            return str(value or "")

        for row in existing:
            normalized = dict(row)
            key = row_key(normalized)
            if key:
                index_by_id[key] = len(merged)
            merged.append(normalized)

        for row in incoming:
            normalized = dict(row)
            key = row_key(normalized)
            if key and key in index_by_id:
                target = merged[index_by_id[key]]
                for field, value in normalized.items():
                    if value not in (None, "", [], {}):
                        target[field] = value
                continue
            if key:
                index_by_id[key] = len(merged)
            merged.append(normalized)

        return merged

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
        plan.setdefault("semantic_tags", sorted(self._collect_plan_semantic_tags(plan)))
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

    @staticmethod
    def _make_agent_progress_callback(
        task: TaskRuntime | None,
        *,
        step: str,
        agent: str,
        resume_hint: str,
    ):
        if task is None:
            return None

        def _callback() -> None:
            task.heartbeat(step=step, agent=agent, resume_hint=resume_hint)
            task.log_step(step, "running", agent=agent, resume_hint=resume_hint)

        return _callback


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
