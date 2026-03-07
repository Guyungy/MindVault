"""Knowledge base object and orchestration for merge/growth behavior."""
from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List
import json


class SelfGrowingKnowledgeBase:
    """JSON-backed knowledge base supporting claim-first merge operations."""

    def __init__(self, path: str = "output/knowledge_base.json") -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.state = self._load_or_init()

    def _load_or_init(self) -> Dict[str, Any]:
        if self.path.exists():
            return json.loads(self.path.read_text(encoding="utf-8"))
        return {
            "entities": [],
            "events": [],
            "relations": [],
            "claims": [],
            "insights": [],
            "placeholders": [],
            "schema": {},
            "version_history": [],
            "governance": {},
        }

    def merge(self, fragment: Dict[str, Any]) -> Dict[str, Any]:
        claims = fragment.get("claims", [])
        self._merge_unique("claims", claims, key="id")

        entity_candidates = fragment.get("entity_candidates", fragment.get("entities", []))
        event_candidates = fragment.get("event_candidates", fragment.get("events", []))
        relation_candidates = fragment.get("relation_candidates", fragment.get("relations", []))

        canonical_entities = self._derive_entities_from_claims(entity_candidates, claims)
        canonical_events = self._derive_events_from_claims(event_candidates)
        canonical_relations = self._derive_relations_from_claims(relation_candidates, claims)

        self._merge_unique("entities", canonical_entities, key="id")
        self._merge_unique("events", canonical_events, key="id")
        self._merge_unique("relations", canonical_relations, key=None)

        self.state["schema"] = fragment.get("schema", self.state.get("schema", {}))
        self.state["placeholders"] = fragment.get("placeholder_candidates", self.state.get("placeholders", []))
        self.save()
        return self.state

    def append_insights(self, insights):
        self.state["insights"] = insights
        self.save()

    def add_version_record(self, version_meta: Dict[str, Any]):
        self.state.setdefault("version_history", []).append(version_meta)
        self.save()

    def save(self):
        self.path.write_text(json.dumps(self.state, indent=2, ensure_ascii=False), encoding="utf-8")

    def _derive_entities_from_claims(self, entities: List[Dict[str, Any]], claims: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        claim_index: Dict[str, List[Dict[str, Any]]] = {}
        for claim in claims:
            claim_index.setdefault(claim.get("subject", ""), []).append(claim)

        enriched: List[Dict[str, Any]] = []
        for entity in entities:
            ent_claims = claim_index.get(entity.get("id", ""), [])
            field_claims: Dict[str, List[Dict[str, Any]]] = {}
            for claim in ent_claims:
                field_claims.setdefault(claim.get("predicate", "unknown"), []).append(
                    {
                        "claim_id": claim.get("id"),
                        "value": claim.get("object"),
                        "source_ref": claim.get("source_ref"),
                        "confidence": claim.get("confidence", 0.5),
                    }
                )

            source_refs = sorted({c.get("source_ref", "") for c in ent_claims if c.get("source_ref")})
            confidence_values = [c.get("confidence", 0.5) for c in ent_claims]
            entity["supporting_claim_ids"] = [c.get("id") for c in ent_claims]
            entity["field_claims"] = field_claims
            entity["source_refs"] = source_refs or entity.get("source_refs", [])
            entity["updated_at"] = datetime.utcnow().isoformat()
            entity["confidence"] = round(sum(confidence_values) / len(confidence_values), 3) if confidence_values else entity.get("confidence", 0.6)
            entity.setdefault("status", "active")
            enriched.append(entity)
        return enriched

    def _derive_events_from_claims(self, events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        for event in events:
            event.setdefault("updated_at", datetime.utcnow().isoformat())
            event.setdefault("status", "active")
            event.setdefault("source_refs", [])
            event.setdefault("confidence", 0.6)
        return events

    def _derive_relations_from_claims(self, relations: List[Dict[str, Any]], claims: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        for rel in relations:
            supporting = [c for c in claims if c.get("subject") == rel.get("source") and c.get("object") == rel.get("target")]
            rel["supporting_claim_ids"] = [c.get("id") for c in supporting]
            rel["source_refs"] = sorted({c.get("source_ref", "") for c in supporting if c.get("source_ref")}) or rel.get("source_refs", [])
            if supporting:
                rel["confidence"] = round(sum(c.get("confidence", 0.5) for c in supporting) / len(supporting), 3)
            rel.setdefault("updated_at", datetime.utcnow().isoformat())
            rel.setdefault("status", "active")
        return relations

    def _merge_unique(self, section: str, records, key: str | None):
        if key is None:
            existing = {json.dumps(x, sort_keys=True) for x in self.state.get(section, [])}
            for rec in records:
                marker = json.dumps(rec, sort_keys=True)
                if marker not in existing:
                    self.state[section].append(rec)
                    existing.add(marker)
            return

        existing_map = {x[key]: x for x in self.state.get(section, [])}
        for rec in records:
            if rec[key] in existing_map:
                existing_map[rec[key]].update(rec)
            else:
                self.state[section].append(rec)
