"""Placeholder manager agent: structured lifecycle for missing information."""
from __future__ import annotations

from datetime import datetime
from typing import Dict, Any, List


class PlaceholderManagerAgent:
    """Consolidates placeholders and resolves them when attributes appear."""

    def update(self, kb_fragment: Dict[str, Any]) -> Dict[str, Any]:
        placeholders: List[Dict[str, Any]] = kb_fragment.setdefault("placeholder_candidates", [])
        now = datetime.utcnow().isoformat()
        for entity in kb_fragment.get("entity_candidates", kb_fragment.get("entities", [])):
            attrs = entity.get("attributes", {})
            entity_placeholders = entity.setdefault("placeholders", {})

            for field in ["phone", "email", "location"]:
                if field in attrs and attrs[field]:
                    status = "filled"
                    entity_placeholders[field] = "filled"
                    fill_confidence = entity.get("confidence", 0.6)
                elif field in entity_placeholders and entity_placeholders[field] in {"inferred", "pending_verification"}:
                    status = entity_placeholders[field]
                    fill_confidence = 0.4
                else:
                    status = "missing"
                    entity_placeholders.setdefault(field, "missing")
                    fill_confidence = 0.0

                placeholders.append(
                    {
                        "target_type": "entity",
                        "target_id": entity.get("id"),
                        "field": field,
                        "status": status,
                        "first_detected_at": now,
                        "last_updated_at": now,
                        "fill_confidence": fill_confidence,
                        "supporting_claims": [c.get("id") for c in kb_fragment.get("claims", []) if c.get("subject") == entity.get("id") and c.get("predicate") == field],
                    }
                )
        kb_fragment["placeholder_candidates"] = placeholders
        return kb_fragment
