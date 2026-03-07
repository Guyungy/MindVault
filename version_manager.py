"""Version manager agent: creates immutable snapshots and changelogs."""
from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List
import json


class VersionManagerAgent:
    """Persists versioned KB snapshots and computes changelog metadata."""

    def __init__(self, out_dir: str = "output") -> None:
        self.out_dir = Path(out_dir)
        self.out_dir.mkdir(parents=True, exist_ok=True)

    def create_snapshot(self, kb: Dict[str, Any], governance: Dict[str, Any] | None = None) -> Dict[str, Any]:
        snapshots = sorted(self.out_dir.glob("kb_snapshot_v*.json"))
        version = len(snapshots) + 1
        timestamp = datetime.utcnow().isoformat()

        payload = {
            "version": version,
            "timestamp": timestamp,
            "knowledge_base": kb,
        }
        snapshot_path = self.out_dir / f"kb_snapshot_v{version}.json"
        snapshot_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")

        prev_payload = None
        if version > 1:
            prev_path = self.out_dir / f"kb_snapshot_v{version-1}.json"
            prev_payload = json.loads(prev_path.read_text(encoding="utf-8"))

        changelog = self._build_changelog(prev_payload, payload, governance or {})
        changelog_path = self.out_dir / f"changelog_v{version}.json"
        changelog_path.write_text(json.dumps(changelog, indent=2, ensure_ascii=False), encoding="utf-8")

        return {
            "snapshot_path": str(snapshot_path),
            "changelog_path": str(changelog_path),
            "version": version,
            "diff": changelog.get("counts", {}),
        }

    def _build_changelog(self, prev: Dict[str, Any] | None, curr: Dict[str, Any], governance: Dict[str, Any]) -> Dict[str, Any]:
        current_kb = curr["knowledge_base"]
        prev_kb = prev["knowledge_base"] if prev else {"entities": [], "events": [], "relations": [], "claims": []}

        prev_entity_map = {e.get("id"): e for e in prev_kb.get("entities", [])}
        curr_entity_map = {e.get("id"): e for e in current_kb.get("entities", [])}

        entities_added = [eid for eid in curr_entity_map if eid not in prev_entity_map]
        entities_updated = [eid for eid, entity in curr_entity_map.items() if eid in prev_entity_map and entity != prev_entity_map[eid]]

        relations_added = self._diff_list(prev_kb.get("relations", []), current_kb.get("relations", []))
        claims_added = self._diff_list(prev_kb.get("claims", []), current_kb.get("claims", []))
        placeholders_prev = {f"{p.get('target_id')}::{p.get('field')}::{p.get('status')}" for p in prev_kb.get("placeholders", [])}
        placeholders_curr = current_kb.get("placeholders", [])
        placeholders_filled = [p for p in placeholders_curr if p.get("status") == "filled" and f"{p.get('target_id')}::{p.get('field')}::filled" not in placeholders_prev]

        conflicts = governance.get("conflicts", {})
        schema_changes = governance.get("schema_candidates", {}).get("recent_promotions", {})

        return {
            "timestamp": curr.get("timestamp"),
            "entities_added": entities_added,
            "entities_updated": entities_updated,
            "relations_added": relations_added,
            "claims_added": claims_added,
            "placeholders_filled": placeholders_filled,
            "conflicts_opened": conflicts.get("unresolved_count", 0),
            "conflicts_resolved": 0,
            "schema_changes": schema_changes,
            "counts": {
                "entities_added": len(entities_added),
                "entities_updated": len(entities_updated),
                "relations_added": len(relations_added),
                "claims_added": len(claims_added),
                "placeholders_filled": len(placeholders_filled),
                "conflicts_opened": conflicts.get("unresolved_count", 0),
            },
        }

    @staticmethod
    def _diff_list(prev_list: List[Dict[str, Any]], curr_list: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        prev_set = {json.dumps(x, sort_keys=True) for x in prev_list}
        out = []
        for item in curr_list:
            marker = json.dumps(item, sort_keys=True)
            if marker not in prev_set:
                out.append(item)
        return out
