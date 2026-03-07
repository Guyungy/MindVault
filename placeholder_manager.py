"""Placeholder manager agent: tracks and updates missing information fields."""
from __future__ import annotations

from typing import Dict, Any


class PlaceholderManagerAgent:
    """Consolidates placeholders and resolves them when attributes appear."""

    def update(self, kb_fragment: Dict[str, Any]) -> Dict[str, Any]:
        for entity in kb_fragment.get("entities", []):
            attrs = entity.get("attributes", {})
            placeholders = entity.setdefault("placeholders", {})

            for field in ["phone", "email", "location"]:
                if field in attrs and attrs[field]:
                    placeholders[field] = "resolved"
                else:
                    placeholders.setdefault(field, "missing")

        return kb_fragment
