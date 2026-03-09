"""Trace logger: records all agent execution events."""
from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List
import json


class TraceLogger:
    """Append-only trace recorder for auditing agent pipeline runs."""

    def __init__(self, verbose: bool = False) -> None:
        self.entries: List[Dict[str, Any]] = []
        self.verbose = verbose

    def log(self, event: str, agent: str = "", task_type: str = "", **extra: Any) -> None:
        entry = {
            "event": event,
            "agent": agent,
            "task_type": task_type,
            "timestamp": datetime.utcnow().isoformat(),
        }
        entry.update(extra)
        self.entries.append(entry)
        
        if self.verbose:
            print(f"🔄 [{datetime.now().strftime('%H:%M:%S')}] Event: {event}{' | Agent: ' + agent if agent else ''}")

    def save(self, path: Path) -> None:
        path.write_text(json.dumps(self.entries, indent=2, ensure_ascii=False), encoding="utf-8")

    def reset(self) -> None:
        self.entries = []
