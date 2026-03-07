"""Relation builder agent: infer additional semantic links from entities/events."""
from __future__ import annotations

from datetime import datetime
from typing import Dict, Any


class RelationBuilderAgent:
    """Infers common relations from entity types and event participation."""

    def build(self, kb_fragment: Dict[str, Any]) -> Dict[str, Any]:
        entities = kb_fragment.get("entity_candidates", kb_fragment.get("entities", []))
        events = kb_fragment.get("event_candidates", kb_fragment.get("events", []))
        relations = kb_fragment.get("relation_candidates", kb_fragment.get("relations", []))

        techs = [e for e in entities if e.get("type") == "technician"]
        venues = [e for e in entities if e.get("type") == "venue"]

        for tech in techs:
            for venue in venues:
                relations.append(
                    {
                        "source": tech["id"],
                        "target": venue["id"],
                        "relation": "belongs_to_venue",
                        "evidence": "Inferred from co-occurrence in ingestion batch.",
                        "confidence": 0.52,
                        "source_refs": sorted(set(tech.get("source_refs", []) + venue.get("source_refs", []))),
                        "updated_at": datetime.utcnow().isoformat(),
                        "status": "active",
                    }
                )

        for ev in events:
            involved = ev.get("entities", [])
            for ent_id in involved:
                relations.append(
                    {
                        "source": ent_id,
                        "target": ev["id"],
                        "relation": "participated_in",
                        "evidence": "Entity referenced in event.",
                        "confidence": 0.6,
                        "source_refs": ev.get("source_refs", []),
                        "updated_at": datetime.utcnow().isoformat(),
                        "status": "active",
                    }
                )

        kb_fragment["relation_candidates"] = self._unique_relations(relations)
        return kb_fragment

    @staticmethod
    def _unique_relations(relations):
        seen = set()
        unique = []
        for r in relations:
            key = (r["source"], r["target"], r["relation"])
            if key in seen:
                continue
            seen.add(key)
            unique.append(r)
        return unique
