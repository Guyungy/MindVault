"""Parser and schema designer agents for extracting structured objects."""
from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Dict, Any, List, Tuple
import re


@dataclass
class Entity:
    id: str
    type: str
    name: str
    attributes: Dict[str, Any]
    placeholders: Dict[str, str]

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class Event:
    id: str
    type: str
    description: str
    timestamp: str
    entities: List[str]
    attributes: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class Relation:
    source: str
    target: str
    relation: str
    evidence: str

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class ParserAgent:
    """Rule-based parser simulating AI extraction across heterogeneous text."""

    entity_patterns: List[Tuple[str, str]] = [
        (r"technician\s+([A-Z][a-z]+)", "technician"),
        (r"venue\s+([A-Z][A-Za-z0-9\-\s]+)", "venue"),
        (r"product\s+([A-Z][A-Za-z0-9\-\s]+)", "product"),
        (r"team\s+member\s+([A-Z][a-z]+)", "person"),
    ]

    def parse(self, docs: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
        entities: List[Entity] = []
        events: List[Event] = []
        relations: List[Relation] = []

        for idx, doc in enumerate(docs, start=1):
            text = doc["text"]
            found_entity_ids: List[str] = []

            for pattern, entity_type in self.entity_patterns:
                for match in re.finditer(pattern, text, flags=re.IGNORECASE):
                    name = match.group(1).strip().title()
                    entity_id = f"ent_{entity_type}_{self._slug(name)}"
                    placeholders = self._extract_placeholders(text)
                    attributes = self._extract_attributes(text)
                    entities.append(
                        Entity(
                            id=entity_id,
                            type=entity_type,
                            name=name,
                            attributes=attributes,
                            placeholders=placeholders,
                        )
                    )
                    found_entity_ids.append(entity_id)

            if any(k in text.lower() for k in ["appointment", "update", "scheduled", "maintenance"]):
                event_id = f"evt_{idx}"
                events.append(
                    Event(
                        id=event_id,
                        type="update",
                        description=text,
                        timestamp=doc["timestamp"],
                        entities=found_entity_ids,
                        attributes=self._extract_attributes(text),
                    )
                )
                if len(found_entity_ids) >= 2:
                    relations.append(
                        Relation(
                            source=found_entity_ids[0],
                            target=found_entity_ids[1],
                            relation="mentioned_within_event",
                            evidence=text,
                        )
                    )

        return {
            "entities": [e.to_dict() for e in entities],
            "events": [e.to_dict() for e in events],
            "relations": [r.to_dict() for r in relations],
            "schema": SchemaDesignerAgent().design(entities, events, relations),
        }

    @staticmethod
    def _extract_attributes(text: str) -> Dict[str, Any]:
        attrs: Dict[str, Any] = {"tags": []}
        price = re.search(r"\$(\d+(?:\.\d+)?)", text)
        duration = re.search(r"(\d+)\s*(?:hours?|hrs?)", text, flags=re.IGNORECASE)
        rating = re.search(r"rating\s*(\d(?:\.\d)?)", text, flags=re.IGNORECASE)

        if price:
            attrs["price"] = float(price.group(1))
        if duration:
            attrs["duration_hours"] = int(duration.group(1))
        if rating:
            attrs["rating"] = float(rating.group(1))

        if "urgent" in text.lower():
            attrs["tags"].append("urgent")
        if "recommended" in text.lower() or "recommend" in text.lower():
            attrs["tags"].append("recommended")
        return attrs

    @staticmethod
    def _extract_placeholders(text: str) -> Dict[str, str]:
        placeholders: Dict[str, str] = {}
        if "phone" not in text.lower():
            placeholders["phone"] = "missing"
        if "email" not in text.lower():
            placeholders["email"] = "missing"
        if "location" not in text.lower() and "venue" not in text.lower():
            placeholders["location"] = "missing"
        return placeholders

    @staticmethod
    def _slug(name: str) -> str:
        return re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")


class SchemaDesignerAgent:
    """Designs dynamic schema summary from extracted records."""

    def design(self, entities: List[Entity], events: List[Event], relations: List[Relation]) -> Dict[str, Any]:
        entity_types = sorted({e.type for e in entities})
        event_types = sorted({e.type for e in events})
        relation_types = sorted({r.relation for r in relations})
        return {
            "entity_types": entity_types,
            "event_types": event_types,
            "relation_types": relation_types,
            "fields": {
                "entity": ["id", "type", "name", "attributes", "placeholders"],
                "event": ["id", "type", "description", "timestamp", "entities", "attributes"],
                "relation": ["source", "target", "relation", "evidence"],
            },
        }
