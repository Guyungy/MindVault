"""Knowledge base object and orchestration for merge/growth behavior."""
from __future__ import annotations

from pathlib import Path
from typing import Dict, Any
import json


class SelfGrowingKnowledgeBase:
    """JSON-backed knowledge base supporting incremental merge operations."""

    def __init__(self, path: str = "output/knowledge_base.json") -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.state = self._load_or_init()

    def _load_or_init(self) -> Dict[str, Any]:
        if self.path.exists():
            return json.loads(self.path.read_text(encoding="utf-8"))
        return {
            "entities": [],
            "events": [],
            "relations": [],
            "insights": [],
            "placeholders": {},
            "schema": {},
            "version_history": [],
        }

    def merge(self, fragment: Dict[str, Any]) -> Dict[str, Any]:
        self._merge_unique("entities", fragment.get("entities", []), key="id")
        self._merge_unique("events", fragment.get("events", []), key="id")
        self._merge_unique("relations", fragment.get("relations", []), key=None)
        self.state["schema"] = fragment.get("schema", self.state.get("schema", {}))
        self._aggregate_placeholders()
        self.save()
        return self.state

    def append_insights(self, insights):
        self.state["insights"] = insights
        self.save()

    def add_version_record(self, version_meta: Dict[str, Any]):
        self.state.setdefault("version_history", []).append(version_meta)
        self.save()

    def save(self):
        self.path.write_text(json.dumps(self.state, indent=2, ensure_ascii=False), encoding="utf-8")

    def _merge_unique(self, section: str, records, key: str | None):
        if key is None:
            existing = {json.dumps(x, sort_keys=True) for x in self.state.get(section, [])}
            for rec in records:
                marker = json.dumps(rec, sort_keys=True)
                if marker not in existing:
                    self.state[section].append(rec)
                    existing.add(marker)
            return

        existing_map = {x[key]: x for x in self.state.get(section, [])}
        for rec in records:
            if rec[key] in existing_map:
                existing_map[rec[key]].update(rec)
            else:
                self.state[section].append(rec)

    def _aggregate_placeholders(self):
        placeholder_index = {}
        for ent in self.state.get("entities", []):
            placeholder_index[ent["id"]] = ent.get("placeholders", {})
        self.state["placeholders"] = placeholder_index
