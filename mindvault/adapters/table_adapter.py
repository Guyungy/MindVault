"""Table adapter: handles CSV / structured tabular inputs."""
from __future__ import annotations

import csv
import io
from typing import Any, Dict, List

from mindvault.adapters.base_adapter import BaseAdapter
from mindvault.runtime.models import NormalizedChunk


class TableAdapter(BaseAdapter):
    """Converts CSV/tabular data into row-group chunks."""

    ROWS_PER_CHUNK = 50

    def adapt(self, source: Dict[str, Any]) -> List[NormalizedChunk]:
        source_id = source.get("source_id", "unknown")
        content = source.get("content", "")
        metadata = source.get("metadata", {})

        reader = csv.DictReader(io.StringIO(content))
        rows = list(reader)
        headers = reader.fieldnames or []
        chunks: List[NormalizedChunk] = []

        for batch_idx in range(0, max(len(rows), 1), self.ROWS_PER_CHUNK):
            batch = rows[batch_idx:batch_idx + self.ROWS_PER_CHUNK]
            lines = [", ".join(f"{k}={v}" for k, v in row.items()) for row in batch]
            text = "\n".join(lines)

            chunks.append(NormalizedChunk(
                chunk_id=self._make_chunk_id(source_id, batch_idx // self.ROWS_PER_CHUNK),
                source_id=source_id,
                chunk_type="table_rows",
                text=text,
                context_hints={
                    "source_type": "table",
                    "headers": headers,
                    "row_count": len(batch),
                    **metadata,
                },
            ))

        return chunks
