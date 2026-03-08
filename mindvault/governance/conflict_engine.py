"""Conflict engine: detects and tracks knowledge conflicts."""
from __future__ import annotations

from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List
import json


class ConflictEngine:
    """Detects field-level conflicts across entities backed by multiple claims."""

    def __init__(self, out_path: Path | str | None = None) -> None:
        self.out_path = Path(out_path) if out_path else None

    def audit(self, kb_state: Dict[str, Any]) -> Dict[str, Any]:
        conflicts: List[Dict[str, Any]] = []

        for entity in kb_state.get("entities", []):
            field_claims = entity.get("field_claims", {})
            for field_name, claim_list in field_claims.items():
                values = defaultdict(list)
                for claim in claim_list:
                    val_key = json.dumps(claim.get("value"), sort_keys=True, ensure_ascii=False)
                    values[val_key].append(claim)

                if len(values) > 1:
                    sorted_vals = sorted(values.items(), key=lambda x: max(c.get("confidence", 0) for c in x[1]), reverse=True)
                    conflicts.append({
                        "entity_id": entity.get("id"),
                        "entity_name": entity.get("name", ""),
                        "field": field_name,
                        "values": [{"value": json.loads(k), "claims": v} for k, v in sorted_vals],
                        "selected_value": json.loads(sorted_vals[0][0]),
                        "resolution_status": "auto_highest_confidence",
                    })

        result = {
            "unresolved_count": len(conflicts),
            "conflicts": conflicts,
        }

        if self.out_path:
            self.out_path.parent.mkdir(parents=True, exist_ok=True)
            self.out_path.write_text(json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8")

        return result
