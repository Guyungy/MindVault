"""Ingestor agent: normalize raw inputs into a common document format."""
from __future__ import annotations

from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from typing import Iterable, List, Dict, Any
import json


@dataclass
class NormalizedDocument:
    """Canonical representation of any raw input payload."""

    source: str
    timestamp: str
    author: str
    text: str
    metadata: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class IngestorAgent:
    """Normalizes chat/web/document/OCR payloads into structured text docs."""

    def ingest(self, raw_items: Iterable[Dict[str, Any]]) -> List[NormalizedDocument]:
        docs: List[NormalizedDocument] = []
        for item in raw_items:
            docs.append(
                NormalizedDocument(
                    source=item.get("source", "unknown"),
                    timestamp=item.get("timestamp", datetime.utcnow().isoformat()),
                    author=item.get("author", "unknown"),
                    text=self._normalize_text(item.get("text", "")),
                    metadata=item.get("metadata", {}),
                )
            )
        return docs

    def load_json(self, path: str | Path) -> List[Dict[str, Any]]:
        path = Path(path)
        return json.loads(path.read_text(encoding="utf-8"))

    @staticmethod
    def _normalize_text(text: str) -> str:
        return " ".join(text.replace("\n", " ").split())
