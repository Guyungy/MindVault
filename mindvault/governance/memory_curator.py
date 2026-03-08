"""Memory curator: decides what enters long-term canonical knowledge vs temporary context."""
from __future__ import annotations

from typing import Any, Dict, List


class MemoryCurator:
    """
    Filters knowledge candidates to decide what becomes permanent
    vs what stays as short-term extracted context.

    Criteria:
    - Confidence above threshold → canonical
    - Multiple source confirmations → canonical
    - Single low-confidence claim → stays in extracted layer
    - Conflicting unresolved claims → held in governance
    """

    def __init__(self, min_confidence: float = 0.4, min_sources: int = 0) -> None:
        self.min_confidence = min_confidence
        self.min_sources = min_sources

    def curate(self, candidates: List[Dict[str, Any]], conflicts: Dict[str, Any] | None = None) -> Dict[str, List[Dict[str, Any]]]:
        """Split candidates into 'promote' (→ canonical) and 'hold' (→ stays extracted)."""
        conflicts = conflicts or {}
        conflicting_ids = {c.get("entity_id") for c in conflicts.get("conflicts", [])}

        promote: List[Dict[str, Any]] = []
        hold: List[Dict[str, Any]] = []

        for item in candidates:
            item_id = item.get("id", item.get("entity_id", ""))
            confidence = item.get("confidence", 0.6)  # LLM entities may lack this field
            n_sources = len(item.get("source_refs", []))

            if item_id in conflicting_ids:
                hold.append(item)
            elif confidence >= self.min_confidence and n_sources >= self.min_sources:
                promote.append(item)
            else:
                hold.append(item)

        return {"promote": promote, "hold": hold}
