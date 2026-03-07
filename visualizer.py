"""Visualizer agent: generate no-dependency HTML dashboard and graph data JSON."""
from __future__ import annotations

from html import escape
from pathlib import Path
from typing import Dict, Any, List
import json


class VisualizerAgent:
    """Writes governance dashboard HTML + graph JSON so users can inspect KB quality."""

    def __init__(self, out_dir: str = "output"):
        self.out_dir = Path(out_dir)
        self.out_dir.mkdir(parents=True, exist_ok=True)

    def visualize(self, kb: Dict[str, Any], governance: Dict[str, Any] | None = None, trace: List[Dict[str, Any]] | None = None, version_meta: Dict[str, Any] | None = None) -> Dict[str, str]:
        governance = governance or {}
        graph_path = self._export_graph_data(kb, governance)
        dashboard_path = self._export_dashboard(kb, governance, trace or [], version_meta or {})
        return {"graph": graph_path, "dashboard": dashboard_path}

    def _export_graph_data(self, kb: Dict[str, Any], governance: Dict[str, Any]) -> str:
        conflict_entity_ids = {c.get("entity_id") for c in governance.get("conflicts", {}).get("conflicts", [])}
        graph_data = {
            "nodes": [
                {
                    "id": e["id"],
                    "label": e.get("name", e["id"]),
                    "kind": e.get("type", "entity"),
                    "placeholder": any(v == "missing" for v in e.get("placeholders", {}).values()),
                    "confidence": e.get("confidence", 0.0),
                    "has_conflict": e.get("id") in conflict_entity_ids,
                }
                for e in kb.get("entities", [])
            ]
            + [
                {
                    "id": ev["id"],
                    "label": ev["id"],
                    "kind": "event",
                    "placeholder": False,
                    "confidence": ev.get("confidence", 0.0),
                    "has_conflict": False,
                }
                for ev in kb.get("events", [])
            ],
            "edges": kb.get("relations", []),
        }
        path = self.out_dir / "graph_data.json"
        path.write_text(json.dumps(graph_data, indent=2, ensure_ascii=False), encoding="utf-8")
        return str(path)

    def _export_dashboard(self, kb: Dict[str, Any], governance: Dict[str, Any], trace: List[Dict[str, Any]], version_meta: Dict[str, Any]) -> str:
        entities_table = self._to_table(kb.get("entities", []), ["id", "type", "name", "confidence", "source_refs", "attributes", "supporting_claim_ids"])
        claims_table = self._to_table(kb.get("claims", []), ["id", "subject", "predicate", "object", "claim_type", "confidence", "source_ref"])
        conflict_table = self._to_table(governance.get("conflicts", {}).get("conflicts", []), ["entity_id", "field", "values", "supporting_claims", "selected_value", "resolution_status"])
        placeholder_rows = governance.get("placeholders", kb.get("placeholders", []))
        placeholder_table = self._to_table(placeholder_rows, ["target_id", "field", "status", "fill_confidence", "last_updated_at"])
        schema_candidates = governance.get("schema_candidates", {})
        schema_table = self._to_table(
            [{"kind": k, "content": v} for k, v in schema_candidates.items() if k != "recent_promotions"],
            ["kind", "content"],
        )
        trace_table = self._to_table(trace, ["agent", "task_type", "source", "timestamp"])
        version_table = self._to_table([version_meta], ["version", "snapshot_path", "changelog_path", "diff"])

        confidence_values = [x.get("confidence", 0) for section in [kb.get("entities", []), kb.get("events", []), kb.get("relations", []), kb.get("claims", [])] for x in section]
        avg_conf = round(sum(confidence_values) / len(confidence_values), 3) if confidence_values else 0.0
        low_conf_count = len([c for c in kb.get("claims", []) if c.get("confidence", 1) < 0.45])
        unresolved_conflicts = governance.get("conflicts", {}).get("unresolved_count", 0)

        html = f"""<!doctype html>
<html lang='en'>
<head>
  <meta charset='utf-8'>
  <title>MindVault Governance Dashboard</title>
  <style>
    body {{ font-family: Arial, sans-serif; margin: 24px; }}
    h1, h2 {{ margin: 0.5rem 0; }}
    .kpi {{ display: flex; gap: 16px; margin: 12px 0 20px; flex-wrap: wrap; }}
    .card {{ padding: 12px; border: 1px solid #ddd; border-radius: 8px; min-width: 180px; }}
    table {{ border-collapse: collapse; width: 100%; margin-bottom: 24px; }}
    th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; vertical-align: top; }}
    th {{ background: #f6f6f6; }}
    code {{ white-space: pre-wrap; }}
  </style>
</head>
<body>
  <h1>MindVault Workspace Governance Console</h1>
  <div class='kpi'>
    <div class='card'><strong>Entities</strong><div>{len(kb.get('entities', []))}</div></div>
    <div class='card'><strong>Claims</strong><div>{len(kb.get('claims', []))}</div></div>
    <div class='card'><strong>Avg Confidence</strong><div>{avg_conf}</div></div>
    <div class='card'><strong>Low Confidence Claims</strong><div>{low_conf_count}</div></div>
    <div class='card'><strong>Unresolved Conflicts</strong><div>{unresolved_conflicts}</div></div>
  </div>
  <h2>KPI Summary</h2>
  <p>Quality-first KPI with confidence and conflict posture.</p>
  <h2>Conflict Panel</h2>
  {conflict_table}
  <h2>Placeholder Panel</h2>
  {placeholder_table}
  <h2>Schema Candidates</h2>
  {schema_table}
  <h2>Version Diff Summary</h2>
  {version_table}
  <h2>Agent Trace Timeline</h2>
  {trace_table}
  <h2>Entities</h2>
  {entities_table}
  <h2>Claims</h2>
  {claims_table}
</body>
</html>"""

        path = self.out_dir / "dashboard.html"
        path.write_text(html, encoding="utf-8")
        return str(path)

    @staticmethod
    def _to_table(rows: List[Dict[str, Any]], columns: List[str]) -> str:
        head = "".join(f"<th>{escape(col)}</th>" for col in columns)
        body_parts: List[str] = []
        for row in rows:
            cols = []
            for col in columns:
                val = row.get(col, "")
                if isinstance(val, (dict, list)):
                    val = json.dumps(val, ensure_ascii=False)
                cols.append(f"<td><code>{escape(str(val))}</code></td>")
            body_parts.append(f"<tr>{''.join(cols)}</tr>")
        body = "".join(body_parts) if body_parts else f"<tr><td colspan='{len(columns)}'>No data</td></tr>"
        return f"<table><thead><tr>{head}</tr></thead><tbody>{body}</tbody></table>"
