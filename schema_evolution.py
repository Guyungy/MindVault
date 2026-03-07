"""Schema evolution manager for candidate promotion and taxonomy growth."""
from __future__ import annotations

from collections import defaultdict
from pathlib import Path
from typing import Any, Dict
import json


class SchemaEvolutionAgent:
    def __init__(self, canonical_schema_path: Path, candidates_path: Path, taxonomy_path: Path) -> None:
        self.schema_path = canonical_schema_path
        self.candidates_path = candidates_path
        self.taxonomy_path = taxonomy_path
        for p in (self.schema_path, self.candidates_path, self.taxonomy_path):
            p.parent.mkdir(parents=True, exist_ok=True)

    def evolve(self, fragment: Dict[str, Any]) -> Dict[str, Any]:
        schema = self._load(self.schema_path, {"entity_fields": ["id", "type", "name"], "relation_types": []})
        candidates = self._load(self.candidates_path, {"attribute_candidates": {}, "entity_type_candidates": {}, "relation_type_candidates": {}})
        taxonomy = self._load(self.taxonomy_path, {"entity_types": [], "relation_types": []})

        attr_counter = defaultdict(lambda: {"count": 0, "sources": set(), "types": set()})
        for ent in fragment.get("entity_candidates", fragment.get("entities", [])):
            etype = ent.get("type", "unknown")
            candidates["entity_type_candidates"].setdefault(etype, 0)
            candidates["entity_type_candidates"][etype] += 1
            for k, v in ent.get("attributes", {}).items():
                attr_counter[k]["count"] += 1
                attr_counter[k]["sources"].update(ent.get("source_refs", []))
                attr_counter[k]["types"].add(type(v).__name__)

        for rel in fragment.get("relation_candidates", fragment.get("relations", [])):
            rtype = rel.get("relation", "related_to")
            candidates["relation_type_candidates"].setdefault(rtype, 0)
            candidates["relation_type_candidates"][rtype] += 1

        promoted = {"fields": [], "entity_types": [], "relation_types": []}
        for field, stats in attr_counter.items():
            cand = candidates["attribute_candidates"].setdefault(field, {"count": 0, "sources": [], "types": []})
            cand["count"] += stats["count"]
            cand["sources"] = sorted(set(cand["sources"]) | stats["sources"])
            cand["types"] = sorted(set(cand["types"]) | stats["types"])
            if cand["count"] >= 3 and len(cand["sources"]) >= 2 and len(cand["types"]) <= 1 and field not in schema["entity_fields"]:
                schema["entity_fields"].append(field)
                promoted["fields"].append(field)

        for etype, count in list(candidates["entity_type_candidates"].items()):
            if count >= 3 and etype not in taxonomy["entity_types"]:
                taxonomy["entity_types"].append(etype)
                promoted["entity_types"].append(etype)

        for rtype, count in list(candidates["relation_type_candidates"].items()):
            if count >= 2 and rtype not in taxonomy["relation_types"]:
                taxonomy["relation_types"].append(rtype)
                schema["relation_types"].append(rtype)
                promoted["relation_types"].append(rtype)

        self._save(self.schema_path, schema)
        candidates["recent_promotions"] = promoted
        self._save(self.candidates_path, candidates)
        self._save(self.taxonomy_path, taxonomy)
        return {"schema": schema, "schema_candidates": candidates, "taxonomy": taxonomy}

    @staticmethod
    def _load(path: Path, default: Dict[str, Any]) -> Dict[str, Any]:
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8"))
        return default

    @staticmethod
    def _save(path: Path, payload: Dict[str, Any]) -> None:
        path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
