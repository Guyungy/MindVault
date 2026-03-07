"""Insight generator agent: derives textual and structured insights from KB."""
from __future__ import annotations

from collections import Counter
from typing import Dict, Any, List

try:
    import pandas as pd  # type: ignore
except Exception:  # pragma: no cover - graceful fallback when pandas unavailable
    pd = None


class InsightGeneratorAgent:
    """Builds simple trend/recommendation insights and a human-readable report."""

    def generate(self, kb: Dict[str, Any]) -> List[Dict[str, Any]]:
        entities = kb.get("entities", [])
        events = kb.get("events", [])
        relations = kb.get("relations", [])

        type_counts = Counter(e.get("type", "unknown") for e in entities)
        placeholder_missing = sum(
            1
            for e in entities
            for _, v in e.get("placeholders", {}).items()
            if v == "missing"
        )

        most_active = None
        if pd is not None and entities:
            df_entities = pd.DataFrame(entities)
            most_active = df_entities["type"].value_counts().idxmax()
        elif entities:
            most_active = max(type_counts.items(), key=lambda x: x[1])[0]

        return [
            {
                "title": "Knowledge Base Growth",
                "summary": f"KB currently stores {len(entities)} entities, {len(events)} events, and {len(relations)} relations.",
                "metrics": {
                    "entity_type_distribution": dict(type_counts),
                    "missing_placeholder_fields": placeholder_missing,
                },
            },
            {
                "title": "Operational Recommendation",
                "summary": f"Prioritize enriching '{most_active}' records with missing contact details.",
                "metrics": {"dominant_entity_type": most_active},
            },
        ]

    def generate_report_text(self, kb: Dict[str, Any], insights: List[Dict[str, Any]]) -> str:
        lines = ["# Self-Growing Knowledge Base Report", ""]
        lines.append(f"- Entities: {len(kb.get('entities', []))}")
        lines.append(f"- Events: {len(kb.get('events', []))}")
        lines.append(f"- Relations: {len(kb.get('relations', []))}")
        lines.append("")
        lines.append("## Insights")
        for idx, insight in enumerate(insights, start=1):
            lines.append(f"{idx}. **{insight['title']}**: {insight['summary']}")
        lines.append("")
        lines.append("## Placeholder Focus")
        unresolved = []
        for ent in kb.get("entities", []):
            missing = [k for k, v in ent.get("placeholders", {}).items() if v == "missing"]
            if missing:
                unresolved.append(f"- {ent['name']} ({ent['type']}): {', '.join(missing)}")
        lines.extend(unresolved or ["- No unresolved placeholders."])
        return "\n".join(lines)
