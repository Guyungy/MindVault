"""Parser and schema designer agents for extracting structured objects and claims."""
from __future__ import annotations

from dataclasses import dataclass, asdict
from datetime import datetime
from typing import Dict, Any, List, Tuple
import re

from claim_model import Claim
from confidence_engine import ConfidenceEngine


@dataclass
class Entity:
    id: str
    type: str
    name: str
    attributes: Dict[str, Any]
    placeholders: Dict[str, str]
    confidence: float = 0.6
    source_refs: List[str] | None = None
    updated_at: str = ""
    status: str = "active"

    def to_dict(self) -> Dict[str, Any]:
        payload = asdict(self)
        payload["source_refs"] = payload.get("source_refs") or []
        payload["updated_at"] = payload.get("updated_at") or datetime.utcnow().isoformat()
        return payload


@dataclass
class Event:
    id: str
    type: str
    description: str
    timestamp: str
    entities: List[str]
    attributes: Dict[str, Any]
    confidence: float = 0.6
    source_refs: List[str] | None = None
    updated_at: str = ""
    status: str = "active"

    def to_dict(self) -> Dict[str, Any]:
        payload = asdict(self)
        payload["source_refs"] = payload.get("source_refs") or []
        payload["updated_at"] = payload.get("updated_at") or datetime.utcnow().isoformat()
        return payload


@dataclass
class Relation:
    source: str
    target: str
    relation: str
    evidence: str
    confidence: float = 0.6
    source_refs: List[str] | None = None
    updated_at: str = ""
    status: str = "active"

    def to_dict(self) -> Dict[str, Any]:
        payload = asdict(self)
        payload["source_refs"] = payload.get("source_refs") or []
        payload["updated_at"] = payload.get("updated_at") or datetime.utcnow().isoformat()
        return payload


class ParserAgent:
    """Rule-based parser simulating AI extraction across heterogeneous text."""

    entity_patterns: List[Tuple[str, str]] = [
        (r"technician\s+([A-Z][a-z]+)", "technician"),
        (r"venue\s+([A-Z][A-Za-z0-9\-\s]+)", "venue"),
        (r"product\s+([A-Z][A-Za-z0-9\-\s]+)", "product"),
        (r"team\s+member\s+([A-Z][a-z]+)", "person"),
        (r"\b(\d{3,5})\b", "technician"),
        (r"([A-Za-z0-9\u4e00-\u9fa5]+水[汇疗])", "venue"),
        (r"([A-Za-z0-9\u4e00-\u9fa5]+会所)", "venue"),
        (r"([A-Za-z0-9\u4e00-\u9fa5]+公寓)", "venue"),
        (r"([A-Za-z0-9\u4e00-\u9fa5]+酒店)", "venue"),
        (r"([A-Za-z0-9\u4e00-\u9fa5]+图书馆)", "venue"),
        (r"([A-Za-z0-9\u4e00-\u9fa5]+体育中心)", "venue"),
        (r"([A-Za-z0-9\u4e00-\u9fa5]+文化中心)", "venue"),
        (r"([A-Za-z0-9\u4e00-\u9fa5]+活动站)", "venue"),
        (r"([A-Za-z0-9\u4e00-\u9fa5]+社区)", "organization"),
        (r"(佛山|广州|南海区|天河区|白云区|顺德区|禅城区)", "area"),
        (r"(亲子阅读活动|健康讲座|夜间服务|预约制)", "service"),
    ]

    def __init__(self) -> None:
        self.confidence_engine = ConfidenceEngine()

    def parse(self, docs: List[Dict[str, Any]], workspace_id: str = "default") -> Dict[str, List[Dict[str, Any]]]:
        entities: List[Entity] = []
        events: List[Event] = []
        relations: List[Relation] = []
        claims: List[Dict[str, Any]] = []

        for idx, doc in enumerate(docs, start=1):
            text = doc["text"]
            source_ref = doc.get("source", f"doc_{idx}")
            found_entity_ids: List[str] = []
            entity_name_map: Dict[str, str] = {}

            for pattern, entity_type in self.entity_patterns:
                for match in re.finditer(pattern, text, flags=re.IGNORECASE):
                    raw_name = match.group(1).strip()
                    name = raw_name if self._contains_cjk(raw_name) else raw_name.title()
                    entity_id = f"ent_{entity_type}_{self._slug(name)}"
                    if entity_id in entity_name_map:
                        continue
                    placeholders = self._extract_placeholders(text)
                    attributes = self._extract_attributes(text)
                    entity = Entity(
                        id=entity_id,
                        type=entity_type,
                        name=name,
                        attributes=attributes,
                        placeholders=placeholders,
                        source_refs=[source_ref],
                    )
                    entities.append(entity)
                    found_entity_ids.append(entity_id)
                    entity_name_map[entity_id] = name
                    claims.extend(self._claims_for_entity(entity, text, workspace_id, source_ref, doc))

            relations.extend(self._infer_relations(text, found_entity_ids, entity_name_map, source_ref))

            if any(k in text.lower() for k in ["appointment", "update", "scheduled", "maintenance"]):
                event_id = f"evt_{idx}"
                event = Event(
                    id=event_id,
                    type="update",
                    description=text,
                    timestamp=doc["timestamp"],
                    entities=found_entity_ids,
                    attributes=self._extract_attributes(text),
                    source_refs=[source_ref],
                )
                events.append(event)
                claims.append(
                    Claim(
                        id=f"claim_event_{event_id}",
                        workspace_id=workspace_id,
                        subject=event_id,
                        predicate="description",
                        object=text,
                        claim_text=text,
                        claim_type=self._classify_claim(text),
                        source_ref=source_ref,
                        speaker=doc.get("speaker", "unknown"),
                        claim_time=doc.get("timestamp", datetime.utcnow().isoformat()),
                    ).to_dict()
                )
                if len(found_entity_ids) >= 2:
                    relation = Relation(
                        source=found_entity_ids[0],
                        target=found_entity_ids[1],
                        relation="mentioned_within_event",
                        evidence=text,
                        source_refs=[source_ref],
                    )
                    relations.append(relation)
                    claims.append(
                        Claim(
                            id=f"claim_rel_{idx}_{self._slug(found_entity_ids[0])}",
                            workspace_id=workspace_id,
                            subject=relation.source,
                            predicate=relation.relation,
                            object=relation.target,
                            claim_text=text,
                            claim_type=self._classify_claim(text),
                            source_ref=source_ref,
                            speaker=doc.get("speaker", "unknown"),
                            claim_time=doc.get("timestamp", datetime.utcnow().isoformat()),
                        ).to_dict()
                    )

        self.confidence_engine.annotate_items([e.__dict__ for e in entities])
        self.confidence_engine.annotate_items([e.__dict__ for e in events])
        self.confidence_engine.annotate_items([r.__dict__ for r in relations])
        for claim in claims:
            claim["confidence"] = self.confidence_engine.score_claim(claim)
            claim.setdefault("status", "active")
            claim.setdefault("updated_at", datetime.utcnow().isoformat())
            claim.setdefault("source_refs", [claim.get("source_ref", "")])

        return {
            "entity_candidates": [e.to_dict() for e in entities],
            "event_candidates": [e.to_dict() for e in events],
            "relation_candidates": [r.to_dict() for r in relations],
            "claims": claims,
            "schema": SchemaDesignerAgent().design(entities, events, relations),
        }

    def _claims_for_entity(self, entity: Entity, text: str, workspace_id: str, source_ref: str, doc: Dict[str, Any]) -> List[Dict[str, Any]]:
        claims: List[Dict[str, Any]] = []
        claim_type = self._classify_claim(text)
        claims.append(
            Claim(
                id=f"claim_{entity.id}_name",
                workspace_id=workspace_id,
                subject=entity.id,
                predicate="name",
                object=entity.name,
                claim_text=text,
                claim_type=claim_type,
                source_ref=source_ref,
                speaker=doc.get("speaker", "unknown"),
                claim_time=doc.get("timestamp", datetime.utcnow().isoformat()),
            ).to_dict()
        )
        for key, value in entity.attributes.items():
            claims.append(
                Claim(
                    id=f"claim_{entity.id}_{self._slug(key)}",
                    workspace_id=workspace_id,
                    subject=entity.id,
                    predicate=key,
                    object=value,
                    claim_text=text,
                    claim_type=claim_type,
                    source_ref=source_ref,
                    speaker=doc.get("speaker", "unknown"),
                    claim_time=doc.get("timestamp", datetime.utcnow().isoformat()),
                ).to_dict()
            )
        return claims

    @staticmethod
    def _classify_claim(text: str) -> str:
        lowered = text.lower()
        if any(token in lowered for token in ["广告", "promotion", "buy now", "limited offer"]):
            return "ad"
        if any(token in lowered for token in ["听说", "rumor", "据说"]):
            return "rumor"
        if any(token in lowered for token in ["觉得", "i think", "opinion"]):
            return "opinion"
        if any(token in lowered for token in ["好像", "maybe", "uncertain"]):
            return "uncertain"
        if any(token in lowered for token in ["曾经", "historical", "以前"]):
            return "historical"
        return "fact"

    @staticmethod
    def _extract_attributes(text: str) -> Dict[str, Any]:
        attrs: Dict[str, Any] = {"tags": []}
        price = re.search(r"\$(\d+(?:\.\d+)?)", text)
        duration = re.search(r"(\d+)\s*(?:hours?|hrs?)", text, flags=re.IGNORECASE)
        rating = re.search(r"rating\s*(\d(?:\.\d)?)", text, flags=re.IGNORECASE)
        zh_location = re.search(r"(位于|在)([\u4e00-\u9fa5]{2,12}(?:区|市|镇|街道))", text)
        zh_schedule = re.search(r"(每周[一二三四五六日天][上下]午|周末|夜间服务时段|线上预约)", text)

        if price:
            attrs["price"] = float(price.group(1))
        if duration:
            attrs["duration_hours"] = int(duration.group(1))
        if rating:
            attrs["rating"] = float(rating.group(1))
        if zh_location:
            attrs["location"] = zh_location.group(2)
        if zh_schedule:
            attrs["schedule"] = zh_schedule.group(1)

        if "urgent" in text.lower():
            attrs["tags"].append("urgent")
        if "recommended" in text.lower() or "recommend" in text.lower():
            attrs["tags"].append("recommended")
        if any(token in text for token in ["图书馆", "阅读", "讲座"]):
            attrs["tags"].append("public_service")
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
        slug = re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")
        return slug or re.sub(r"\W+", "_", name).strip("_").lower() or "item"

    @staticmethod
    def _contains_cjk(text: str) -> bool:
        return any("\u4e00" <= ch <= "\u9fff" for ch in text)

    def _infer_relations(
        self,
        text: str,
        found_entity_ids: List[str],
        entity_name_map: Dict[str, str],
        source_ref: str,
    ) -> List[Relation]:
        id_to_type = {entity_id: entity_id.split("_")[1] if "_" in entity_id else "" for entity_id in found_entity_ids}
        areas = [entity_id for entity_id, entity_type in id_to_type.items() if entity_type == "area"]
        venues = [entity_id for entity_id, entity_type in id_to_type.items() if entity_type == "venue"]
        services = [entity_id for entity_id, entity_type in id_to_type.items() if entity_type == "service"]
        relations: List[Relation] = []

        if areas:
            primary_area = areas[0]
            for venue_id in venues:
                relations.append(Relation(
                    source=venue_id,
                    target=primary_area,
                    relation="located_in",
                    evidence=text,
                    source_refs=[source_ref],
                ))
            for service_id in services:
                relations.append(Relation(
                    source=service_id,
                    target=primary_area,
                    relation="offered_in",
                    evidence=text,
                    source_refs=[source_ref],
                ))

        if venues and services:
            primary_venue = venues[0]
            for service_id in services:
                relations.append(Relation(
                    source=primary_venue,
                    target=service_id,
                    relation="hosts_service",
                    evidence=text,
                    source_refs=[source_ref],
                ))

        return relations


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
                "entity": ["id", "type", "name", "attributes", "placeholders", "confidence", "source_refs", "updated_at", "status"],
                "event": ["id", "type", "description", "timestamp", "entities", "attributes", "confidence", "source_refs", "updated_at", "status"],
                "relation": ["source", "target", "relation", "evidence", "confidence", "source_refs", "updated_at", "status"],
                "claim": ["id", "subject", "predicate", "object", "claim_type", "source_ref", "confidence", "status"],
            },
        }
