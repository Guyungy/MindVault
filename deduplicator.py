"""Deduplicator agent: merge duplicate entities/events and resolve conflicts."""
from __future__ import annotations

from typing import Dict, List, Any


class DeduplicatorAgent:
    """Merges entities by normalized (type, name)."""

    def deduplicate(self, parsed: Dict[str, List[Dict[str, Any]]]) -> Dict[str, List[Dict[str, Any]]]:
        entities = parsed["entities"]
        events = parsed["events"]
        relations = parsed["relations"]

        merged: Dict[str, Dict[str, Any]] = {}
        id_map: Dict[str, str] = {}

        for ent in entities:
            key = f"{ent['type']}::{ent['name'].strip().lower()}"
            if key not in merged:
                merged[key] = ent
            else:
                merged[key]["attributes"] = {
                    **merged[key].get("attributes", {}),
                    **ent.get("attributes", {}),
                }
                merged[key]["placeholders"] = {
                    **merged[key].get("placeholders", {}),
                    **ent.get("placeholders", {}),
                }
            id_map[ent["id"]] = merged[key]["id"]

        dedup_entities = list(merged.values())

        for ev in events:
            ev["entities"] = [id_map.get(eid, eid) for eid in ev.get("entities", [])]

        for rel in relations:
            rel["source"] = id_map.get(rel["source"], rel["source"])
            rel["target"] = id_map.get(rel["target"], rel["target"])

        parsed["entities"] = dedup_entities
        parsed["events"] = events
        parsed["relations"] = relations
        return parsed
