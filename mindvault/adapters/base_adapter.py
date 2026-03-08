"""Base adapter: abstract interface for all source adapters."""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Dict, List

from mindvault.runtime.models import NormalizedChunk


class BaseAdapter(ABC):
    """
    All source adapters inherit from this base.
    Each adapter converts raw source material into a list of NormalizedChunk.
    """

    @abstractmethod
    def adapt(self, source: Dict[str, Any]) -> List[NormalizedChunk]:
        """
        Convert a single raw source record into normalized chunks.

        Args:
            source: a dict with at least:
                - source_id
                - source_type
                - content (raw text or structured data)
                - metadata (optional)

        Returns:
            List of NormalizedChunk ready for agent consumption.
        """
        ...

    @staticmethod
    def _make_chunk_id(source_id: str, index: int) -> str:
        return f"{source_id}_chunk_{index:04d}"
