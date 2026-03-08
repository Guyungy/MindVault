"""Core data models for MindVault knowledge system."""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional


# ─── Source ───────────────────────────────────────────────────────────────────
@dataclass
class Source:
    """Any knowledge must be traceable to its origin."""
    source_id: str
    source_type: str  # chat | web | doc | table | ocr | pdf_text | api
    ingested_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())
    author: str = "unknown"
    content_hash: str = ""
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


# ─── Claim ────────────────────────────────────────────────────────────────────
@dataclass
class Claim:
    """An assertion extracted from source material. NOT a fact — a 'statement'."""
    claim_id: str
    workspace_id: str
    subject: str
    predicate: str
    object: Any
    claim_text: str
    claim_type: str  # fact | opinion | rumor | ad | historical | uncertain
    source_ref: str
    speaker: str = "unknown"
    claim_time: str = ""
    confidence: float = 0.5
    verdict: str = "unreviewed"  # unreviewed | accepted | rejected | conflicting
    status: str = "active"
    source_refs: List[str] = field(default_factory=list)
    updated_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())

    def to_dict(self) -> Dict[str, Any]:
        payload = asdict(self)
        if not payload["source_refs"]:
            payload["source_refs"] = [self.source_ref]
        return payload


# ─── Entity ───────────────────────────────────────────────────────────────────
@dataclass
class Entity:
    """A long-lived, referenceable object in the knowledge graph."""
    entity_id: str
    type: str
    name: str
    attributes: Dict[str, Any] = field(default_factory=dict)
    placeholders: Dict[str, str] = field(default_factory=dict)
    confidence: float = 0.6
    source_refs: List[str] = field(default_factory=list)
    supporting_claim_ids: List[str] = field(default_factory=list)
    updated_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())
    status: str = "active"

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


# ─── Relation ─────────────────────────────────────────────────────────────────
@dataclass
class Relation:
    """A stable connection between two entities."""
    source_entity: str
    target_entity: str
    relation_type: str
    evidence: str = ""
    confidence: float = 0.6
    source_refs: List[str] = field(default_factory=list)
    supporting_claim_ids: List[str] = field(default_factory=list)
    updated_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())
    status: str = "active"

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


# ─── Event ────────────────────────────────────────────────────────────────────
@dataclass
class Event:
    """A time-bound occurrence involving entities."""
    event_id: str
    type: str
    description: str
    timestamp: str = ""
    participants: List[str] = field(default_factory=list)
    attributes: Dict[str, Any] = field(default_factory=dict)
    confidence: float = 0.6
    source_refs: List[str] = field(default_factory=list)
    updated_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())
    status: str = "active"

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


# ─── Insight ──────────────────────────────────────────────────────────────────
@dataclass
class Insight:
    """A generated summary or recommendation — not raw fact."""
    insight_id: str
    title: str
    summary: str
    metrics: Dict[str, Any] = field(default_factory=dict)
    generated_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


# ─── Placeholder ──────────────────────────────────────────────────────────────
@dataclass
class Placeholder:
    """Missing but important information — not just null."""
    target_id: str
    target_type: str  # entity | event
    field: str
    status: str = "missing"  # missing | inferred | pending_verification | filled
    fill_confidence: float = 0.0
    supporting_claims: List[str] = field(default_factory=list)
    first_detected_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())
    last_updated_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


# ─── Normalized Chunk (adapter → agent 的标准通信格式) ─────────────────────────
@dataclass
class NormalizedChunk:
    """Standard unit passed from adapter to agent runtime."""
    chunk_id: str
    source_id: str
    chunk_type: str  # message_batch | paragraph | table_row | section
    text: str
    context_hints: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)
