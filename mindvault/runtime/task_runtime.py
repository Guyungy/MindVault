"""Task runtime: persistent run state for interruption recovery and monitoring."""
from __future__ import annotations

from datetime import datetime
from pathlib import Path
from threading import Lock
from typing import Any, Dict
import json
import time


class TaskRuntime:
    """Writes task.json and step_log.jsonl for each run."""

    def __init__(self, task_root: str | Path, goal: str, workspace_id: str) -> None:
        timestamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")
        self.task_id = f"task_{timestamp}"
        self.task_dir = Path(task_root) / self.task_id
        self.task_dir.mkdir(parents=True, exist_ok=True)
        self.task_path = self.task_dir / "task.json"
        self.step_log_path = self.task_dir / "step_log.jsonl"
        self.stdout_dir = self.task_dir / "stdout"
        self.stdout_dir.mkdir(parents=True, exist_ok=True)
        self._lock = Lock()
        self.state: Dict[str, Any] = {
            "task_id": self.task_id,
            "workspace": workspace_id,
            "goal": goal,
            "status": "queued",
            "current_step": "",
            "current_agent": "",
            "last_heartbeat": datetime.utcnow().isoformat(),
            "resume_hint": "Task created.",
            "started_at": datetime.utcnow().isoformat(),
            "ended_at": "",
            "artifacts": {},
        }
        self._save()

    def start(self) -> None:
        self.state["status"] = "running"
        self.state["resume_hint"] = "Pipeline started."
        self.heartbeat()

    def heartbeat(self, *, step: str | None = None, agent: str | None = None, resume_hint: str | None = None) -> None:
        with self._lock:
            if step is not None:
                self.state["current_step"] = step
            if agent is not None:
                self.state["current_agent"] = agent
            if resume_hint is not None:
                self.state["resume_hint"] = resume_hint
            self.state["last_heartbeat"] = datetime.utcnow().isoformat()
            self._save()

    def log_step(self, action: str, status: str, **extra: Any) -> None:
        with self._lock:
            timestamp = datetime.utcnow().isoformat()
            if self.state.get("status") == "running":
                self.state["last_heartbeat"] = timestamp
                self._save()
            entry = {
                "timestamp": timestamp,
                "task_id": self.task_id,
                "action": action,
                "status": status,
            }
            entry.update(extra)
            self._append_step_entry(entry)

    def push_table_ready(self, table: Dict[str, Any]) -> None:
        with self._lock:
            timestamp = datetime.utcnow().isoformat()
            if self.state.get("status") == "running":
                self.state["last_heartbeat"] = timestamp
                self.state["resume_hint"] = f"Table ready: {table.get('name', '')}"
                self._save()
            self._append_step_entry(
                {
                    "timestamp": timestamp,
                    "task_id": self.task_id,
                    "action": "multi_db",
                    "status": "table_ready",
                    "event": "table_ready",
                    "table_name": table.get("name", ""),
                    "built_by": table.get("built_by", "unknown"),
                    "columns": table.get("columns", []),
                    "row_count": table.get("row_count", len(table.get("rows", []) or [])),
                    "ts": time.time(),
                }
            )

    def add_artifact(self, name: str, path: str) -> None:
        with self._lock:
            self.state.setdefault("artifacts", {})[name] = path
            self.state["last_heartbeat"] = datetime.utcnow().isoformat()
            self.state["resume_hint"] = f"Artifact updated: {name}"
            self._save()

    def complete(self, summary: str = "Task completed.") -> None:
        with self._lock:
            self.state["status"] = "completed"
            self.state["ended_at"] = datetime.utcnow().isoformat()
            self.state["resume_hint"] = summary
            self.state["last_heartbeat"] = datetime.utcnow().isoformat()
            self._save()

    def fail(self, error: str, step: str = "") -> None:
        with self._lock:
            self.state["status"] = "failed"
            self.state["ended_at"] = datetime.utcnow().isoformat()
            if step:
                self.state["current_step"] = step
            self.state["resume_hint"] = error
            self.state["last_heartbeat"] = datetime.utcnow().isoformat()
            self._save()

    def block(self, reason: str, step: str = "") -> None:
        with self._lock:
            self.state["status"] = "blocked"
            self.state["ended_at"] = datetime.utcnow().isoformat()
            if step:
                self.state["current_step"] = step
            self.state["resume_hint"] = reason
            self.state["last_heartbeat"] = datetime.utcnow().isoformat()
            self._save()

    def _save(self) -> None:
        self.task_path.write_text(json.dumps(self.state, indent=2, ensure_ascii=False), encoding="utf-8")

    def _append_step_entry(self, entry: Dict[str, Any]) -> None:
        with self.step_log_path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(entry, ensure_ascii=False) + "\n")
