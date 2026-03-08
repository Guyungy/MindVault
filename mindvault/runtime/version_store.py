"""Version store: creates snapshots and changelogs for knowledge state."""
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict
import json


class VersionStore:
    """Manages immutable snapshots and diff-based changelogs."""

    def __init__(self, out_dir: str | Path) -> None:
        self.out_dir = Path(out_dir)
        self.out_dir.mkdir(parents=True, exist_ok=True)

    def create_snapshot(self, state: Dict[str, Any], governance: Dict[str, Any] | None = None) -> Dict[str, Any]:
        version = self._next_version()
        snapshot_path = self.out_dir / f"kb_snapshot_v{version}.json"
        changelog_path = self.out_dir / f"changelog_v{version}.json"

        # Snapshot
        snapshot = {
            "version": version,
            "entities": state.get("entities", []),
            "events": state.get("events", []),
            "relations": state.get("relations", []),
            "claims": state.get("claims", []),
            "insights": state.get("insights", []),
            "placeholders": state.get("placeholders", []),
            "schema": state.get("schema", {}),
            "governance": governance or {},
        }
        snapshot_path.write_text(json.dumps(snapshot, indent=2, ensure_ascii=False), encoding="utf-8")

        # Changelog (diff from previous)
        prev_snapshot = self._load_previous(version)
        changelog = self._build_changelog(prev_snapshot, snapshot)
        changelog_path.write_text(json.dumps(changelog, indent=2, ensure_ascii=False), encoding="utf-8")

        return {
            "version": version,
            "snapshot_path": str(snapshot_path),
            "changelog_path": str(changelog_path),
            "diff": changelog,
        }

    def _next_version(self) -> int:
        existing = sorted(self.out_dir.glob("kb_snapshot_v*.json"))
        return len(existing) + 1

    def _load_previous(self, current_version: int) -> Dict[str, Any]:
        prev = current_version - 1
        if prev < 1:
            return {}
        path = self.out_dir / f"kb_snapshot_v{prev}.json"
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8"))
        return {}

    @staticmethod
    def _build_changelog(prev: Dict[str, Any], curr: Dict[str, Any]) -> Dict[str, Any]:
        def _ids(items):
            if isinstance(items, list):
                return {item.get("id", item.get("entity_id", item.get("event_id", item.get("claim_id", "")))) for item in items}
            elif isinstance(items, dict):
                return set(items.keys())
            return set()

        changelog: Dict[str, Any] = {}
        for key in ["entities", "events", "relations", "claims"]:
            prev_ids = _ids(prev.get(key, []))
            curr_ids = _ids(curr.get(key, []))
            changelog[key] = {
                "added": len(curr_ids - prev_ids),
                "removed": len(prev_ids - curr_ids),
                "total": len(curr_ids),
            }

        # Placeholder handling (list or dict compat)
        prev_ph = prev.get("placeholders", [])
        curr_ph = curr.get("placeholders", [])
        if isinstance(prev_ph, dict):
            prev_ph = list(prev_ph.values()) if prev_ph else []
        if isinstance(curr_ph, dict):
            curr_ph = list(curr_ph.values()) if curr_ph else []
        changelog["placeholders"] = {
            "previous_count": len(prev_ph),
            "current_count": len(curr_ph),
        }

        return changelog
