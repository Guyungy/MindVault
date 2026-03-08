"""Chat adapter: handles chat log / conversation inputs."""
from __future__ import annotations

import re
from typing import Any, Dict, List

from mindvault.adapters.base_adapter import BaseAdapter
from mindvault.runtime.models import NormalizedChunk


class ChatAdapter(BaseAdapter):
    """Converts chat logs into batched message chunks."""

    BATCH_SIZE = 20  # messages per chunk

    def adapt(self, source: Dict[str, Any]) -> List[NormalizedChunk]:
        source_id = source.get("source_id", "unknown")
        content = source.get("content", "")
        metadata = source.get("metadata", {})

        messages = self._parse_messages(content)
        chunks: List[NormalizedChunk] = []

        for batch_idx in range(0, len(messages), self.BATCH_SIZE):
            batch = messages[batch_idx:batch_idx + self.BATCH_SIZE]
            text = "\n".join(f"[{m.get('author', '?')}] {m.get('text', '')}" for m in batch)
            speakers = list({m.get("author", "unknown") for m in batch})
            time_range = [
                batch[0].get("time", ""),
                batch[-1].get("time", ""),
            ]

            chunks.append(NormalizedChunk(
                chunk_id=self._make_chunk_id(source_id, batch_idx // self.BATCH_SIZE),
                source_id=source_id,
                chunk_type="message_batch",
                text=text,
                context_hints={
                    "source_type": "chat",
                    "speakers": speakers,
                    "message_count": len(batch),
                    "time_range": time_range,
                    **metadata,
                },
            ))

        return chunks

    @staticmethod
    def _parse_messages(content: str) -> List[Dict[str, str]]:
        """Simple line-based message parser. Each line = one message."""
        messages: List[Dict[str, str]] = []
        for line in content.strip().split("\n"):
            line = line.strip()
            if not line:
                continue
            # Try pattern: [speaker] text  or  speaker: text
            m = re.match(r"\[(.+?)\]\s*(.*)", line)
            if m:
                messages.append({"author": m.group(1), "text": m.group(2)})
                continue
            m = re.match(r"(.+?)[:：]\s*(.*)", line)
            if m and len(m.group(1)) < 30:
                messages.append({"author": m.group(1), "text": m.group(2)})
                continue
            messages.append({"author": "unknown", "text": line})
        return messages
