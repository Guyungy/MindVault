"""Document adapter: handles Markdown, plain text, and general document inputs."""
from __future__ import annotations

import hashlib
import re
from typing import Any, Dict, List

from mindvault.adapters.base_adapter import BaseAdapter
from mindvault.runtime.models import NormalizedChunk


class DocAdapter(BaseAdapter):
    """Splits markdown/text documents into meaningful chunks by sections."""

    MAX_CHUNK_CHARS = 3000  # approximate upper bound per chunk

    def adapt(self, source: Dict[str, Any]) -> List[NormalizedChunk]:
        source_id = source.get("source_id", "unknown")
        content = source.get("content", "")
        metadata = source.get("metadata", {})

        sections = self._split_sections(content)
        chunks: List[NormalizedChunk] = []

        for idx, section in enumerate(sections):
            text = section["text"].strip()
            if not text:
                continue
            chunks.append(NormalizedChunk(
                chunk_id=self._make_chunk_id(source_id, idx),
                source_id=source_id,
                chunk_type="section",
                text=text,
                context_hints={
                    "source_type": "doc",
                    "heading": section.get("heading", ""),
                    "language": self._detect_language(text),
                    "char_count": len(text),
                    **metadata,
                },
            ))

        return chunks

    def _split_sections(self, content: str) -> List[Dict[str, str]]:
        """Split markdown by headings (##, ###, etc.)."""
        lines = content.split("\n")
        sections: List[Dict[str, str]] = []
        current_heading = ""
        current_lines: List[str] = []

        for line in lines:
            if re.match(r"^#{1,4}\s+", line):
                # Save previous section
                if current_lines:
                    sections.append({"heading": current_heading, "text": "\n".join(current_lines)})
                current_heading = re.sub(r"^#+\s*", "", line).strip()
                current_lines = [line]
            else:
                current_lines.append(line)

        if current_lines:
            sections.append({"heading": current_heading, "text": "\n".join(current_lines)})

        # If no headings found, return as single chunk (may need sub-splitting)
        if len(sections) == 1 and len(sections[0]["text"]) > self.MAX_CHUNK_CHARS:
            return self._split_by_paragraphs(sections[0]["text"])

        return sections

    def _split_by_paragraphs(self, text: str) -> List[Dict[str, str]]:
        """Fallback: split by double-newline paragraphs."""
        paragraphs = re.split(r"\n\s*\n", text)
        result: List[Dict[str, str]] = []
        buffer: List[str] = []
        buf_len = 0

        for para in paragraphs:
            para = para.strip()
            if not para:
                continue
            if buf_len + len(para) > self.MAX_CHUNK_CHARS and buffer:
                result.append({"heading": "", "text": "\n\n".join(buffer)})
                buffer = []
                buf_len = 0
            buffer.append(para)
            buf_len += len(para)

        if buffer:
            result.append({"heading": "", "text": "\n\n".join(buffer)})

        return result

    @staticmethod
    def _detect_language(text: str) -> str:
        cjk = sum(1 for c in text if '\u4e00' <= c <= '\u9fff')
        return "zh" if cjk > len(text) * 0.1 else "en"
