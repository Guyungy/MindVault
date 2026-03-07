"""Version manager agent: creates immutable snapshots and computes diffs."""
from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Dict, Any
import json


class VersionManagerAgent:
    """Persists versioned KB snapshots and computes simple metadata diffs."""

    def __init__(self, out_dir: str = "output") -> None:
        self.out_dir = Path(out_dir)
        self.out_dir.mkdir(parents=True, exist_ok=True)

    def create_snapshot(self, kb: Dict[str, Any]) -> Dict[str, Any]:
        snapshots = sorted(self.out_dir.glob("kb_snapshot_v*.json"))
        version = len(snapshots) + 1
        timestamp = datetime.utcnow().isoformat()

        payload = {
            "version": version,
            "timestamp": timestamp,
            "knowledge_base": kb,
        }
        path = self.out_dir / f"kb_snapshot_v{version}.json"
        path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")

        diff = self.diff_with_previous(payload)
        return {"snapshot_path": str(path), "version": version, "diff": diff}

    def diff_with_previous(self, current_payload: Dict[str, Any]) -> Dict[str, int]:
        version = current_payload["version"]
        if version <= 1:
            return {"entities_added": len(current_payload["knowledge_base"].get("entities", [])), "events_added": len(current_payload["knowledge_base"].get("events", [])), "relations_added": len(current_payload["knowledge_base"].get("relations", []))}

        prev_path = self.out_dir / f"kb_snapshot_v{version-1}.json"
        prev = json.loads(prev_path.read_text(encoding="utf-8"))
        curr = current_payload

        return {
            "entities_added": len(curr["knowledge_base"].get("entities", [])) - len(prev["knowledge_base"].get("entities", [])),
            "events_added": len(curr["knowledge_base"].get("events", [])) - len(prev["knowledge_base"].get("events", [])),
            "relations_added": len(curr["knowledge_base"].get("relations", [])) - len(prev["knowledge_base"].get("relations", [])),
        }
