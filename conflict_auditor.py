"""Detects canonical conflicts and writes governance conflict reports."""
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List
import json


class ConflictAuditor:
    KEY_FIELDS = ["name", "aliases", "belongs_to", "time", "location", "price", "phone", "email", "code"]

    def __init__(self, out_path: Path) -> None:
        self.out_path = out_path
        self.out_path.parent.mkdir(parents=True, exist_ok=True)

    def audit(self, kb_state: Dict[str, Any]) -> Dict[str, Any]:
        conflicts: List[Dict[str, Any]] = []
        for ent in kb_state.get("entities", []):
            field_claims = ent.get("field_claims", {})
            for field in self.KEY_FIELDS:
                values = field_claims.get(field, [])
                distinct = {json.dumps(v.get("value"), sort_keys=True) for v in values}
                if len(distinct) > 1:
                    conflicts.append(
                        {
                            "entity_id": ent.get("id"),
                            "field": field,
                            "values": [v.get("value") for v in values],
                            "supporting_claims": [v.get("claim_id") for v in values],
                            "selected_value": ent.get("attributes", {}).get(field) if field != "name" else ent.get("name"),
                            "resolution_status": "unresolved",
                        }
                    )

        payload = {"conflicts": conflicts, "unresolved_count": len(conflicts)}
        self.out_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
        return payload
