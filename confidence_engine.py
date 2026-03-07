"""Confidence scoring utilities for extracted claims and KB objects."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Iterable


class ConfidenceEngine:
    SOURCE_WEIGHTS = {
        "official_doc": 0.9,
        "report": 0.8,
        "note": 0.7,
        "chat": 0.5,
        "ad": 0.3,
    }
    UNCERTAINTY_MARKERS = ["好像", "听说", "不太记得", "据说", "maybe", "rumor", "possibly"]
    AD_MARKERS = ["限时", "优惠", "立即", "best", "promotion", "广告"]

    def score_claim(self, claim: Dict[str, Any], support_count: int = 1) -> float:
        text = str(claim.get("claim_text", "")).lower()
        source_type = claim.get("source_type", "chat")
        base = self.SOURCE_WEIGHTS.get(source_type, 0.55)

        if any(marker in text for marker in self.UNCERTAINTY_MARKERS):
            base -= 0.2
        if any(marker in text for marker in self.AD_MARKERS):
            base -= 0.25

        if support_count > 1:
            base += min(0.2, 0.05 * (support_count - 1))

        claim_time = claim.get("claim_time")
        if claim_time:
            base -= self._temporal_decay(claim_time)

        return round(min(0.99, max(0.05, base)), 3)

    def annotate_items(self, items: Iterable[Dict[str, Any]], default_source_ref: str = "") -> None:
        now = datetime.utcnow().isoformat()
        for item in items:
            item.setdefault("source_refs", [default_source_ref] if default_source_ref else [])
            item.setdefault("status", "active")
            item.setdefault("updated_at", now)
            item.setdefault("confidence", 0.6)

    def _temporal_decay(self, iso_ts: str) -> float:
        try:
            ts = datetime.fromisoformat(iso_ts.replace("Z", "+00:00"))
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            age_days = max(0, (datetime.now(timezone.utc) - ts).days)
            return min(0.2, age_days / 3650)
        except ValueError:
            return 0.0
