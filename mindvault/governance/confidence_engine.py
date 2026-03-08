"""Confidence engine: scores claims, entities, and relations."""
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List
import json


# Default confidence policy
DEFAULT_POLICY = {
    "source_trust": {
        "official_doc": 0.9,
        "ocr_document": 0.7,
        "chat_log": 0.5,
        "forum_post": 0.4,
        "unknown": 0.3,
    },
    "claim_type_weight": {
        "fact": 1.0,
        "opinion": 0.6,
        "rumor": 0.3,
        "ad": 0.2,
        "uncertain": 0.4,
        "historical": 0.5,
    },
    "multi_source_bonus": 0.1,
    "recency_decay_days": 90,
}


class ConfidenceEngine:
    """Assigns confidence scores based on configurable policy."""

    def __init__(self, policy_path: str | Path | None = None) -> None:
        if policy_path and Path(policy_path).exists():
            self.policy = json.loads(Path(policy_path).read_text(encoding="utf-8"))
        else:
            self.policy = DEFAULT_POLICY

    def score_claim(self, claim: Dict[str, Any]) -> float:
        source_trust = self.policy["source_trust"]
        claim_weights = self.policy["claim_type_weight"]

        source = claim.get("source_ref", "unknown")
        base = source_trust.get(source, source_trust.get("unknown", 0.3))
        claim_type = claim.get("claim_type", "fact")
        weight = claim_weights.get(claim_type, 0.5)

        score = round(base * weight, 3)

        # Multi-source bonus
        refs = claim.get("source_refs", [])
        if len(refs) > 1:
            score = min(1.0, score + self.policy.get("multi_source_bonus", 0.1))

        return score

    def annotate_items(self, items: List[Dict[str, Any]]) -> None:
        """Update confidence on a list of entity/event/relation dicts in-place."""
        for item in items:
            source_trust = self.policy["source_trust"]
            refs = item.get("source_refs", [])
            if refs:
                scores = [source_trust.get(r, 0.3) for r in refs]
                item["confidence"] = round(sum(scores) / len(scores), 3)
