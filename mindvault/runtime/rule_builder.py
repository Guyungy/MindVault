"""Rule-based database planning and table building helpers."""
from __future__ import annotations

from collections import defaultdict
import json
from pathlib import Path
from typing import Any, Dict, List


GENERIC_ENTITY_FIELDS = [
    "id",
    "name",
    "type",
    "confidence",
    "source_refs",
    "updated_at",
    "status",
]

DISCOURSE_ENTITY_TYPES = {
    "topic",
    "opinion",
    "signal",
    "resource",
    "role",
}

TABLE_NAME_OVERRIDES = {
    "person": "persons",
    "people": "persons",
    "人物": "persons",
    "organization": "organizations",
    "company": "organizations",
    "组织": "organizations",
    "product": "products",
    "产品": "products",
    "service": "services",
    "服务": "services",
    "venue": "venues",
    "地点": "venues",
    "meeting": "meetings",
    "会议": "meetings",
    "decision": "decisions",
    "决策": "decisions",
    "task": "tasks",
    "任务": "tasks",
    "project": "projects",
    "项目": "projects",
    "document": "documents",
    "doc": "documents",
    "文档": "documents",
    "event": "events",
    "事件": "events",
    "area": "areas",
    "region": "areas",
}

KNOWN_ENTITY_SCHEMAS = {
    "人物": {"table": "persons", "fields": ["name", "role", "department", "email"]},
    "会议": {"table": "meetings", "fields": ["title", "date", "participants", "summary", "location"]},
    "决策": {"table": "decisions", "fields": ["content", "decided_by", "date", "meeting_id"]},
    "任务": {"table": "tasks", "fields": ["title", "owner", "due_date", "status", "meeting_id"]},
    "项目": {"table": "projects", "fields": ["name", "owner", "status", "deadline"]},
    "文档": {"table": "documents", "fields": ["title", "author", "date", "summary", "tags"]},
    "事件": {"table": "events", "fields": ["title", "date", "participants", "outcome"]},
    "人物/person": {"table": "persons", "fields": ["name", "role", "department", "email"]},
    "person": {"table": "persons", "fields": ["name", "role", "department", "email"]},
    "people": {"table": "persons", "fields": ["name", "role", "department", "email"]},
    "organization": {"table": "organizations", "fields": ["name", "industry", "owner", "status"]},
    "company": {"table": "organizations", "fields": ["name", "industry", "owner", "status"]},
    "product": {"table": "products", "fields": ["name", "category", "status", "owner"]},
    "service": {"table": "services", "fields": ["name", "category", "owner", "status"]},
    "meeting": {"table": "meetings", "fields": ["title", "date", "participants", "summary", "location"]},
    "decision": {"table": "decisions", "fields": ["content", "decided_by", "date", "meeting_id"]},
    "task": {"table": "tasks", "fields": ["title", "owner", "due_date", "status", "meeting_id"]},
    "project": {"table": "projects", "fields": ["name", "owner", "status", "deadline"]},
    "document": {"table": "documents", "fields": ["title", "author", "date", "summary", "tags"]},
    "event": {"table": "events", "fields": ["title", "date", "participants", "outcome"]},
}

LEARNED_CACHE_PATH = Path(__file__).resolve().parents[2] / "config" / "learned_schemas.json"


def normalize_entity_type(entity_type: str) -> str:
    return str(entity_type or "").strip().lower().replace(" ", "_")


def normalize_schema_key(entity_types: List[str], semantic_tags: List[str] | None = None) -> str:
    normalized_types = sorted(
        {
            normalize_entity_type(item)
            for item in (entity_types or [])
            if normalize_entity_type(item)
        }
    )
    normalized_tags = sorted(
        {
            normalize_entity_type(item)
            for item in (semantic_tags or [])
            if normalize_entity_type(item)
        }
    )
    return json.dumps(
        {
            "entity_types": normalized_types,
            "semantic_tags": normalized_tags,
        },
        ensure_ascii=False,
        sort_keys=True,
    )


def load_learned_schemas() -> Dict[str, Dict[str, Any]]:
    try:
        if LEARNED_CACHE_PATH.exists():
            payload = json.loads(LEARNED_CACHE_PATH.read_text(encoding="utf-8"))
            if isinstance(payload, dict):
                return {
                    str(key): value
                    for key, value in payload.items()
                    if isinstance(value, dict)
                }
    except Exception:
        pass
    return {}


def load_learned_schema(entity_types: List[str], semantic_tags: List[str] | None = None) -> Dict[str, Any] | None:
    cache = load_learned_schemas()
    key = normalize_schema_key(entity_types, semantic_tags)
    plan = cache.get(key)
    if not isinstance(plan, dict):
        return None
    payload = json.loads(json.dumps(plan, ensure_ascii=False))
    payload["built_by"] = "learned"
    return payload


def save_learned_schema(
    entity_types: List[str],
    semantic_tags: List[str] | None,
    plan: Dict[str, Any],
) -> bool:
    key = normalize_schema_key(entity_types, semantic_tags)
    if key == normalize_schema_key([], []):
        return False
    if not isinstance(plan, dict) or not isinstance(plan.get("databases"), list) or not plan.get("databases"):
        return False
    cache = load_learned_schemas()
    payload = json.loads(json.dumps(plan, ensure_ascii=False))
    payload["built_by"] = "learned"
    cache[key] = payload
    LEARNED_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    LEARNED_CACHE_PATH.write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8")
    return True


def build_learned_database_plan(entity_types: List[str], semantic_tags: List[str] | None = None) -> Dict[str, Any] | None:
    return load_learned_schema(entity_types, semantic_tags)


def build_fallback_plan(state: Dict[str, Any], entity_types: List[str]) -> Dict[str, Any]:
    normalized_types = [
        str(item).strip()
        for item in (entity_types or [])
        if str(item).strip()
    ]
    entities_by_type: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for entity in state.get("entities", []) or []:
        entity_type = str(entity.get("type", "")).strip()
        if entity_type:
            entities_by_type[entity_type].append(entity)

    plans: List[Dict[str, Any]] = []
    for entity_type in normalized_types:
        plans.append(build_fallback_database_spec(entity_type, entities_by_type.get(entity_type, [])))

    if not plans:
        for entity_type, items in sorted(entities_by_type.items()):
            plans.append(build_fallback_database_spec(entity_type, items))

    plans.extend(_system_database_specs(state, built_by="fallback"))
    return {
        "domain": " / ".join(normalized_types[:3]) or "Fallback Knowledge",
        "databases": plans,
        "relations": [],
        "built_by": "fallback",
    }


def default_table_name_for_entity_type(entity_type: str) -> str:
    raw = str(entity_type or "").strip()
    if not raw:
        return "entities"
    override = TABLE_NAME_OVERRIDES.get(raw) or TABLE_NAME_OVERRIDES.get(normalize_entity_type(raw))
    if override:
        return override
    normalized = normalize_entity_type(raw)
    return normalized if normalized.endswith("s") else f"{normalized}s"


def can_build_by_rule(entity_type: str, entities: List[Dict[str, Any]], confidence_threshold: float = 0.7) -> bool:
    raw = str(entity_type or "").strip()
    normalized = normalize_entity_type(raw)
    if not raw or normalized in DISCOURSE_ENTITY_TYPES:
        return False
    if raw in KNOWN_ENTITY_SCHEMAS or normalized in KNOWN_ENTITY_SCHEMAS:
        return True
    if not entities:
        return False
    return all(float(entity.get("confidence") or 0.0) >= confidence_threshold for entity in entities)


def build_rule_database_plan(state: Dict[str, Any], confidence_threshold: float = 0.7) -> Dict[str, Any] | None:
    entities = list(state.get("entities", []) or [])
    if not entities:
        return None

    entities_by_type: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for entity in entities:
        entity_type = str(entity.get("type", "")).strip()
        if entity_type:
            entities_by_type[entity_type].append(entity)

    if not entities_by_type:
        return None

    plans: List[Dict[str, Any]] = []
    for entity_type in sorted(entities_by_type):
        items = entities_by_type[entity_type]
        if not has_known_rule_schema(entity_type):
            return None
        plans.append(build_database_spec_by_rule(entity_type, items))

    plans.extend(_system_database_specs(state))
    domain = " / ".join(sorted(entities_by_type.keys())[:3])
    return {
        "domain": domain or "Structured Knowledge",
        "databases": plans,
        "relations": [],
        "built_by": "rule",
    }


def has_known_rule_schema(entity_type: str) -> bool:
    raw = str(entity_type or "").strip()
    normalized = normalize_entity_type(raw)
    return raw in KNOWN_ENTITY_SCHEMAS or normalized in KNOWN_ENTITY_SCHEMAS


def build_database_spec_by_rule(entity_type: str, entities: List[Dict[str, Any]]) -> Dict[str, Any]:
    schema = KNOWN_ENTITY_SCHEMAS.get(str(entity_type).strip()) or KNOWN_ENTITY_SCHEMAS.get(normalize_entity_type(entity_type), {})
    suggested_fields = _merge_fields(
        GENERIC_ENTITY_FIELDS,
        schema.get("fields", []),
        _infer_entity_fields(entities),
    )
    table_name = schema.get("table") or default_table_name_for_entity_type(entity_type)
    return {
        "name": table_name,
        "title": str(entity_type or table_name),
        "description": f"Structured {entity_type} entities generated by rule.",
        "entity_types": [entity_type],
        "suggested_fields": suggested_fields,
        "visibility": "business",
        "row_source": "entities",
        "record_granularity": "entity",
        "primary_key": "id",
        "built_by": "rule",
    }


def build_fallback_database_spec(entity_type: str, entities: List[Dict[str, Any]]) -> Dict[str, Any]:
    suggested_fields = _merge_fields(
        GENERIC_ENTITY_FIELDS,
        _infer_entity_fields(entities),
        ["content", "source_id"],
    )
    table_name = default_table_name_for_entity_type(entity_type)
    return {
        "name": table_name,
        "title": str(entity_type or table_name),
        "description": f"Fallback structured {entity_type} entities generated without ontology planning.",
        "entity_types": [entity_type] if entity_type else [],
        "suggested_fields": suggested_fields,
        "visibility": "business",
        "row_source": "entities",
        "record_granularity": "entity",
        "primary_key": "id",
        "built_by": "fallback",
    }


def build_table_by_rule(
    entity_type: str,
    entities: List[Dict[str, Any]],
    relations: List[Dict[str, Any]],
    *,
    database_spec: Dict[str, Any] | None = None,
    entity_index: Dict[str, Dict[str, Any]] | None = None,
) -> Dict[str, Any]:
    spec = database_spec or build_database_spec_by_rule(entity_type, entities)
    rows = [_build_entity_row(entity) for entity in entities]
    if entity_index:
        _apply_relation_fields(rows, relations, entity_index)

    columns = _merge_fields(
        spec.get("suggested_fields", []),
        _collect_columns(rows),
    )
    return {
        "name": spec.get("name") or default_table_name_for_entity_type(entity_type),
        "title": spec.get("title") or str(entity_type),
        "description": spec.get("description", ""),
        "primary_key": spec.get("primary_key", "id"),
        "columns": columns or ["id"],
        "rows": rows,
        "visibility": spec.get("visibility", "business"),
        "built_by": "rule",
        "row_count": len(rows),
    }


def _infer_type(field_name: str) -> str:
    normalized = normalize_entity_type(field_name)
    if normalized in {"id", "meeting_id", "task_id", "project_id", "person_id", "document_id", "event_id"}:
        return "UUID"
    if normalized.endswith("_ids"):
        return "TEXT[]"
    if "date" in normalized or "time" in normalized:
        return "TIMESTAMP"
    if normalized in {"participants", "tags"}:
        return "TEXT[]"
    if normalized == "status":
        return "VARCHAR(32)"
    return "TEXT"


def _system_database_specs(state: Dict[str, Any], *, built_by: str = "rule") -> List[Dict[str, Any]]:
    specs = [
        {
            "name": "claims",
            "title": "claims",
            "description": "Atomic statements",
            "entity_types": [],
            "suggested_fields": ["id", "subject", "predicate", "object", "claim_type", "confidence", "source_ref", "source_refs"],
            "visibility": "system",
            "row_source": "claims",
            "record_granularity": "claim",
            "primary_key": "id",
            "built_by": built_by,
        },
        {
            "name": "relations",
            "title": "relations",
            "description": "Cross-record links",
            "entity_types": [],
            "suggested_fields": ["id", "source", "relation", "target", "confidence", "source_refs"],
            "visibility": "system",
            "row_source": "relations",
            "record_granularity": "relation",
            "primary_key": "id",
            "built_by": built_by,
        },
        {
            "name": "sources",
            "title": "sources",
            "description": "Source references",
            "entity_types": [],
            "suggested_fields": ["id", "name", "mentions", "kinds"],
            "visibility": "system",
            "row_source": "sources",
            "record_granularity": "source",
            "primary_key": "id",
            "built_by": built_by,
        },
    ]
    if state.get("events"):
        specs.append(
            {
                "name": "events",
                "title": "events",
                "description": "Structured events",
                "entity_types": [],
                "suggested_fields": ["id", "type", "description", "timestamp", "participants", "confidence", "source_refs"],
                "visibility": "business",
                "row_source": "events",
                "record_granularity": "event",
                "primary_key": "id",
                "built_by": built_by,
            }
        )
    return specs


def _infer_entity_fields(entities: List[Dict[str, Any]]) -> List[str]:
    inferred: List[str] = []
    seen = set()
    for entity in entities:
        row = _build_entity_row(entity)
        for key in row.keys():
            if key not in seen:
                seen.add(key)
                inferred.append(key)
    return inferred


def _build_entity_row(entity: Dict[str, Any]) -> Dict[str, Any]:
    row = {
        "id": entity.get("id", entity.get("entity_id", "")),
        "name": entity.get("name", ""),
        "type": entity.get("type", ""),
        "confidence": entity.get("confidence"),
        "source_refs": entity.get("source_refs", []),
        "updated_at": entity.get("updated_at", ""),
        "status": entity.get("status", ""),
    }
    attributes = entity.get("attributes", {})
    if isinstance(attributes, dict):
        row.update(attributes)
    for key, value in entity.items():
        if key in {"id", "entity_id", "name", "type", "confidence", "source_refs", "updated_at", "status", "attributes"}:
            continue
        if key.startswith("_"):
            continue
        row.setdefault(key, value)
    return row


def _apply_relation_fields(
    rows: List[Dict[str, Any]],
    relations: List[Dict[str, Any]],
    entity_index: Dict[str, Dict[str, Any]],
) -> None:
    rows_by_id = {
        str(row.get("id", "")).strip(): row
        for row in rows
        if str(row.get("id", "")).strip()
    }
    linked_values: Dict[str, Dict[str, List[str]]] = defaultdict(lambda: defaultdict(list))
    for relation in relations:
        source_id = str(relation.get("source", relation.get("source_entity", "")) or "").strip()
        target_id = str(relation.get("target", relation.get("target_entity", "")) or "").strip()
        if source_id not in rows_by_id or not target_id:
            continue
        target_entity = entity_index.get(target_id)
        if not target_entity:
            continue
        target_type = str(target_entity.get("type", "") or "entity").strip()
        base_name = normalize_entity_type(target_type).rstrip("s") or "entity"
        linked_values[source_id][f"{base_name}_id"].append(target_id)

    for row_id, field_map in linked_values.items():
        row = rows_by_id.get(row_id)
        if not row:
            continue
        for field_name, values in field_map.items():
            unique_values = list(dict.fromkeys(values))
            if len(unique_values) == 1:
                row[field_name] = unique_values[0]
            elif unique_values:
                row[f"{field_name}s"] = unique_values


def _collect_columns(rows: List[Dict[str, Any]]) -> List[str]:
    columns: List[str] = []
    seen = set()
    for row in rows:
        for key in row.keys():
            if key not in seen:
                seen.add(key)
                columns.append(key)
    return columns


def _merge_fields(*field_groups: List[str]) -> List[str]:
    merged: List[str] = []
    seen = set()
    for group in field_groups:
        for field in group or []:
            normalized = str(field).strip()
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)
            merged.append(normalized)
    return merged
