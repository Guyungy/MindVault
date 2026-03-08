"""Placeholder engine: tracks missing but important information lifecycle."""
from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List


# Fields that should exist on common entity types
EXPECTED_FIELDS: Dict[str, List[str]] = {
    "person": ["phone", "email", "location", "role"],
    "venue": ["phone", "address", "city", "district", "category"],
    "technician": ["phone", "affiliation", "location"],
    "product": ["price", "category", "manufacturer"],
    "organization": ["phone", "address", "website"],
    "_default": ["phone", "email", "location"],
}


class PlaceholderEngine:
    """Detects and manages the lifecycle of missing fields on entities."""

    def scan(self, kb_state: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Scan entities for missing expected fields and return placeholder records."""
        placeholders: List[Dict[str, Any]] = []
        now = datetime.utcnow().isoformat()

        for entity in kb_state.get("entities", []):
            etype = entity.get("type", "_default")
            expected = EXPECTED_FIELDS.get(etype, EXPECTED_FIELDS["_default"])
            attrs = entity.get("attributes", {})

            for field in expected:
                existing_val = attrs.get(field)
                ph_status = entity.get("placeholders", {}).get(field, "")

                if existing_val and ph_status != "missing":
                    status = "filled"
                    fill_conf = 0.8
                elif ph_status == "missing" or not existing_val:
                    status = "missing"
                    fill_conf = 0.0
                else:
                    status = "pending_verification"
                    fill_conf = 0.3

                placeholders.append({
                    "target_id": entity.get("id", entity.get("entity_id", "")),
                    "target_type": "entity",
                    "field": field,
                    "status": status,
                    "fill_confidence": fill_conf,
                    "first_detected_at": now,
                    "last_updated_at": now,
                })

        return placeholders

    def update_fragment(self, fragment: Dict[str, Any]) -> Dict[str, Any]:
        """Attach placeholder candidates to a parse fragment (legacy compat)."""
        entities = fragment.get("entity_candidates", fragment.get("entities", []))
        candidates: List[Dict[str, Any]] = []
        now = datetime.utcnow().isoformat()

        for entity in entities:
            etype = entity.get("type", "_default")
            expected = EXPECTED_FIELDS.get(etype, EXPECTED_FIELDS["_default"])
            attrs = entity.get("attributes", {})

            for field in expected:
                val = attrs.get(field)
                status = "filled" if val else "missing"
                candidates.append({
                    "target_id": entity.get("id", entity.get("entity_id", "")),
                    "field": field,
                    "status": status,
                    "fill_confidence": 0.8 if val else 0.0,
                    "last_updated_at": now,
                })

        fragment["placeholder_candidates"] = candidates
        return fragment
