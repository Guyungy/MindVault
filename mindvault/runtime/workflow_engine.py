"""Workflow engine: queue-based task mesh with configurable routing."""
from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Deque, Dict, List
import json


@dataclass
class Task:
    """A typed unit of work passed between agents."""
    task_type: str
    payload: Dict[str, Any] = field(default_factory=dict)
    source_agent: str = "system"
    created_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())


AgentHandler = Callable[[Task, Dict[str, Any]], List[Task]]


class WorkflowEngine:
    """Queue-based runtime. Agents process tasks and can emit downstream tasks."""

    def __init__(self, workflow_path: str | Path = "config/workflow.json") -> None:
        self.workflow_path = Path(workflow_path)
        self.workflow = self._load_workflow(self.workflow_path)
        self.registry: Dict[str, AgentHandler] = {}
        self.traces: List[Dict[str, Any]] = []

    def register(self, agent_name: str, handler: AgentHandler) -> None:
        self.registry[agent_name] = handler

    def run(self, initial_task: Task, context: Dict[str, Any]) -> List[Dict[str, Any]]:
        queue: Deque[Task] = deque([initial_task])

        while queue:
            task = queue.popleft()
            routed_agents = self.workflow.get(task.task_type, [])

            if not routed_agents:
                self.traces.append({
                    "event": "unrouted_task",
                    "task_type": task.task_type,
                    "source": task.source_agent,
                    "timestamp": datetime.utcnow().isoformat(),
                })
                continue

            for agent_name in routed_agents:
                handler = self.registry.get(agent_name)
                if handler is None:
                    self.traces.append({
                        "event": "missing_agent_handler",
                        "agent": agent_name,
                        "task_type": task.task_type,
                        "timestamp": datetime.utcnow().isoformat(),
                    })
                    continue

                emitted = handler(task, context)
                queue.extend(emitted)
                self.traces.append({
                    "event": "agent_executed",
                    "agent": agent_name,
                    "task_type": task.task_type,
                    "emitted": [t.task_type for t in emitted],
                    "timestamp": datetime.utcnow().isoformat(),
                })

        return self.traces

    @staticmethod
    def _load_workflow(path: Path) -> Dict[str, List[str]]:
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8"))
        # Fallback: default workflow if file missing
        return {
            "ingest.start": ["ingestor"],
            "adapt.request": ["adapter"],
            "parse.request": ["parser"],
            "claim_resolve.request": ["claim_resolver"],
            "dedup.request": ["deduplicator"],
            "relation.request": ["relation_builder"],
            "schema.request": ["schema_designer"],
            "governance.request": ["governance_pipeline"],
            "merge.request": ["knowledge_store"],
            "version.request": ["version_manager"],
            "insight.request": ["insight_generator"],
            "report.request": ["report_generator"],
            "visualize.request": ["visualizer"],
        }
