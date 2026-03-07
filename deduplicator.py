"""Deduplicator agent: merge duplicate entities/events and resolve conflicts."""
from __future__ import annotations

from typing import Dict, List, Any


class DeduplicatorAgent:
    """Merges entities by normalized (type, name)."""

    def deduplicate(self, parsed: Dict[str, List[Dict[str, Any]]]) -> Dict[str, List[Dict[str, Any]]]:
        entities = parsed.get("entity_candidates", parsed.get("entities", []))
        events = parsed.get("event_candidates", parsed.get("events", []))
        relations = parsed.get("relation_candidates", parsed.get("relations", []))

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
                merged[key]["source_refs"] = sorted(set(merged[key].get("source_refs", []) + ent.get("source_refs", [])))
                merged[key]["confidence"] = round(max(merged[key].get("confidence", 0.6), ent.get("confidence", 0.6)), 3)
            id_map[ent["id"]] = merged[key]["id"]

        dedup_entities = list(merged.values())

        for ev in events:
            ev["entities"] = [id_map.get(eid, eid) for eid in ev.get("entities", [])]

        for rel in relations:
            rel["source"] = id_map.get(rel["source"], rel["source"])
            rel["target"] = id_map.get(rel["target"], rel["target"])

        parsed["entity_candidates"] = dedup_entities
        parsed["event_candidates"] = events
        parsed["relation_candidates"] = relations
        return parsed
