"""Insight generator agent: derives textual and structured insights from KB."""
from __future__ import annotations

from collections import Counter
from typing import Dict, Any, List


class InsightGeneratorAgent:
    """Build trend/recommendation insights and human-readable report/dashboard text."""

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
        most_active = max(type_counts.items(), key=lambda x: x[1])[0] if type_counts else None

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
        entities = kb.get("entities", [])
        events = kb.get("events", [])
        relations = kb.get("relations", [])

        lines = ["# Self-Growing Knowledge Base Report", ""]
        lines.extend([
            "## Overview",
            "",
            "| Metric | Count |",
            "| --- | ---: |",
            f"| Entities | {len(entities)} |",
            f"| Events | {len(events)} |",
            f"| Relations | {len(relations)} |",
            "",
        ])

        type_counts = Counter(entity.get("type", "unknown") for entity in entities)
        if type_counts:
            lines.extend([
                "## Entity Type Distribution",
                "",
                "| Type | Count |",
                "| --- | ---: |",
            ])
            for entity_type, count in sorted(type_counts.items(), key=lambda item: (-item[1], item[0])):
                lines.append(f"| {entity_type} | {count} |")
            lines.append("")

        lines.append("## Insights")
        if insights:
            for idx, insight in enumerate(insights, start=1):
                title = insight.get("title", f"Insight {idx}")
                summary = insight.get("summary", "")
                lines.append(f"{idx}. **{title}**")
                if summary:
                    lines.append(f"   - {summary}")
        else:
            lines.append("- No insights generated yet.")
        lines.append("")

        lines.append("## Placeholder Focus")
        unresolved: List[tuple[str, str, List[str]]] = []
        for ent in entities:
            missing = sorted(k for k, v in ent.get("placeholders", {}).items() if v == "missing")
            if missing:
                unresolved.append((ent.get("name", "Unknown"), ent.get("type", "unknown"), missing))

        if unresolved:
            lines.extend([
                "",
                "| Entity | Type | Missing Fields |",
                "| --- | --- | --- |",
            ])
            for name, entity_type, missing in sorted(unresolved, key=lambda item: (-len(item[2]), item[0])):
                lines.append(f"| {name} | {entity_type} | {', '.join(missing)} |")
        else:
            lines.append("- No unresolved placeholders.")

        return "\n".join(lines)
