"""Knowledge store: three-layer knowledge persistence (raw → extracted → canonical)."""
from __future__ import annotations

from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List
import json


class KnowledgeStore:
    """
    Manages the three-layer knowledge lifecycle:
      - Raw: unmodified source material
      - Extracted: AI intermediate results (claims, candidates)
      - Canonical: formally accepted knowledge (entities, relations, events)
    """

    def __init__(self, canonical_path: str | Path) -> None:
        self.canonical_path = Path(canonical_path)
        self.canonical_path.parent.mkdir(parents=True, exist_ok=True)
        self.state = self._load_or_init()

    # ── Load / Save ──────────────────────────────────────────────────────────

    def _load_or_init(self) -> Dict[str, Any]:
        if self.canonical_path.exists():
            return json.loads(self.canonical_path.read_text(encoding="utf-8"))
        return {
            "entities": [],
            "events": [],
            "relations": [],
            "claims": [],
            "insights": [],
            "placeholders": [],
            "schema": {},
            "version_history": [],
            "metadata": {
                "created_at": datetime.utcnow().isoformat(),
                "last_updated_at": datetime.utcnow().isoformat(),
            },
        }

    def save(self) -> None:
        self.state["metadata"]["last_updated_at"] = datetime.utcnow().isoformat()
        self.canonical_path.write_text(
            json.dumps(self.state, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )

    # ── Merge: extracted → canonical ─────────────────────────────────────────

    def merge(self, fragment: Dict[str, Any]) -> Dict[str, Any]:
        """Merge extracted candidates into canonical knowledge."""
        self._merge_entities(fragment.get("entity_candidates", []), fragment.get("claims", []))
        self._merge_events(fragment.get("event_candidates", []))
        self._merge_relations(fragment.get("relation_candidates", []))
        self._merge_claims(fragment.get("claims", []))

        if "schema" in fragment:
            self.state["schema"] = fragment["schema"]

        self.save()
        return self.state

    def _merge_entities(self, candidates: List[Dict[str, Any]], claims: List[Dict[str, Any]]) -> None:
        existing = {e.get("id", e.get("entity_id", "")): e for e in self.state["entities"]}
        claim_map = self._build_claim_map(claims)

        for cand in candidates:
            eid = cand.get("id", cand.get("entity_id", ""))
            if not eid:
                continue

            supporting = [c.get("id", c.get("claim_id", "")) for c in claim_map.get(eid, []) if c.get("id", c.get("claim_id", ""))]

            if eid in existing:
                ent = existing[eid]
                ent["attributes"].update(cand.get("attributes", {}))
                ent["source_refs"] = sorted(set(ent.get("source_refs", []) + cand.get("source_refs", [])))
                ent["supporting_claim_ids"] = sorted(set(ent.get("supporting_claim_ids", []) + supporting))
                ent["updated_at"] = datetime.utcnow().isoformat()
                self._update_field_claims(ent, cand, claims)
            else:
                cand.setdefault("supporting_claim_ids", supporting)
                cand.setdefault("field_claims", {})
                cand["updated_at"] = datetime.utcnow().isoformat()
                self._update_field_claims(cand, cand, claims)
                existing[eid] = cand

        self.state["entities"] = list(existing.values())

    def _merge_events(self, candidates: List[Dict[str, Any]]) -> None:
        existing = {e.get("id", e.get("event_id", "")): e for e in self.state["events"]}
        for cand in candidates:
            eid = cand.get("id", cand.get("event_id", ""))
            if eid and eid not in existing:
                cand["updated_at"] = datetime.utcnow().isoformat()
                existing[eid] = cand
        self.state["events"] = list(existing.values())

    def _merge_relations(self, candidates: List[Dict[str, Any]]) -> None:
        seen = set()
        merged: List[Dict[str, Any]] = []
        for rel in self.state["relations"] + candidates:
            src = rel.get("source", rel.get("source_entity", ""))
            tgt = rel.get("target", rel.get("target_entity", ""))
            rtype = rel.get("relation", rel.get("relation_type", ""))
            key = (src, tgt, rtype)
            if key not in seen:
                seen.add(key)
                merged.append(rel)
        self.state["relations"] = merged

    def _merge_claims(self, claims: List[Dict[str, Any]]) -> None:
        existing_ids = {c.get("id", c.get("claim_id", "")) for c in self.state["claims"]}
        for claim in claims:
            cid = claim.get("id", claim.get("claim_id", ""))
            if cid and cid not in existing_ids:
                self.state["claims"].append(claim)
                existing_ids.add(cid)

    # ── Insights ─────────────────────────────────────────────────────────────

    def append_insights(self, insights: List[Dict[str, Any]]) -> None:
        self.state.setdefault("insights", []).extend(insights)
        self.save()

    # ── Version tracking ─────────────────────────────────────────────────────

    def add_version_record(self, meta: Dict[str, Any]) -> None:
        self.state.setdefault("version_history", []).append(meta)
        self.save()

    # ── Helpers ──────────────────────────────────────────────────────────────

    @staticmethod
    def _build_claim_map(claims: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
        mapping: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        for claim in claims:
            subject = claim.get("subject", "")
            if subject:
                mapping[subject].append(claim)
        return mapping

    @staticmethod
    def _update_field_claims(entity: Dict[str, Any], candidate: Dict[str, Any], claims: List[Dict[str, Any]]) -> None:
        fc = entity.setdefault("field_claims", {})
        subject_id = candidate.get("id", candidate.get("entity_id", ""))
        for claim in claims:
            if claim.get("subject") == subject_id:
                predicate = claim.get("predicate", "")
                if predicate:
                    fc.setdefault(predicate, []).append({
                        "claim_id": claim.get("id", claim.get("claim_id", "")),
                        "value": claim.get("object"),
                        "source_ref": claim.get("source_ref", ""),
                        "confidence": claim.get("confidence", 0.5),
                    })
