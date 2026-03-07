"""Claim model definitions for extracted intermediate knowledge assertions."""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import Any, Dict, List


@dataclass
class Claim:
    id: str
    workspace_id: str
    subject: str
    predicate: str
    object: Any
    claim_text: str
    claim_type: str
    source_ref: str
    speaker: str
    claim_time: str
    confidence: float = 0.5
    verdict: str = "unreviewed"
    status: str = "active"
    source_refs: List[str] = field(default_factory=list)
    updated_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())

    def to_dict(self) -> Dict[str, Any]:
        payload = asdict(self)
        if not payload["source_refs"]:
            payload["source_refs"] = [self.source_ref]
        return payload
