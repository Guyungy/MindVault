"""Visualizer agent: generate no-dependency HTML dashboard and graph data JSON."""
from __future__ import annotations

from html import escape
from pathlib import Path
from typing import Dict, Any, List
import json


class VisualizerAgent:
    """Writes dashboard HTML + graph JSON so users can directly inspect KB results."""

    def __init__(self, out_dir: str = "output"):
        self.out_dir = Path(out_dir)
        self.out_dir.mkdir(parents=True, exist_ok=True)

    def visualize(self, kb: Dict[str, Any]) -> Dict[str, str]:
        graph_path = self._export_graph_data(kb)
        dashboard_path = self._export_dashboard(kb)
        return {"graph": graph_path, "dashboard": dashboard_path}

    def _export_graph_data(self, kb: Dict[str, Any]) -> str:
        graph_data = {
            "nodes": [
                {
                    "id": e["id"],
                    "label": e.get("name", e["id"]),
                    "kind": e.get("type", "entity"),
                    "placeholder": any(v == "missing" for v in e.get("placeholders", {}).values()),
                }
                for e in kb.get("entities", [])
            ]
            + [
                {
                    "id": ev["id"],
                    "label": ev["id"],
                    "kind": "event",
                    "placeholder": False,
                }
                for ev in kb.get("events", [])
            ],
            "edges": kb.get("relations", []),
        }
        path = self.out_dir / "graph_data.json"
        path.write_text(json.dumps(graph_data, indent=2, ensure_ascii=False), encoding="utf-8")
        return str(path)

    def _export_dashboard(self, kb: Dict[str, Any]) -> str:
        entities_table = self._to_table(kb.get("entities", []), ["id", "type", "name", "attributes", "placeholders"])
        events_table = self._to_table(kb.get("events", []), ["id", "type", "timestamp", "entities", "attributes"])
        relations_table = self._to_table(kb.get("relations", []), ["source", "target", "relation", "evidence"])

        html = f"""<!doctype html>
<html lang='en'>
<head>
  <meta charset='utf-8'>
  <title>Knowledge Base Dashboard</title>
  <style>
    body {{ font-family: Arial, sans-serif; margin: 24px; }}
    h1, h2 {{ margin: 0.5rem 0; }}
    .kpi {{ display: flex; gap: 16px; margin: 12px 0 20px; }}
    .card {{ padding: 12px; border: 1px solid #ddd; border-radius: 8px; min-width: 160px; }}
    table {{ border-collapse: collapse; width: 100%; margin-bottom: 24px; }}
    th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; vertical-align: top; }}
    th {{ background: #f6f6f6; }}
    code {{ white-space: pre-wrap; }}
  </style>
</head>
<body>
  <h1>Workspace Knowledge Base Dashboard</h1>
  <div class='kpi'>
    <div class='card'><strong>Entities</strong><div>{len(kb.get('entities', []))}</div></div>
    <div class='card'><strong>Events</strong><div>{len(kb.get('events', []))}</div></div>
    <div class='card'><strong>Relations</strong><div>{len(kb.get('relations', []))}</div></div>
    <div class='card'><strong>Insights</strong><div>{len(kb.get('insights', []))}</div></div>
  </div>
  <h2>Entities</h2>
  {entities_table}
  <h2>Events</h2>
  {events_table}
  <h2>Relations</h2>
  {relations_table}
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
