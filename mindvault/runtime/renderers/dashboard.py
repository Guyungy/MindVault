"""Dashboard renderer: generates a three-panel HTML governance dashboard."""
from __future__ import annotations

from html import escape
from pathlib import Path
from typing import Any, Dict, List
import json


class DashboardRenderer:
    """Renders Control / Knowledge / Governance HTML dashboard."""

    def __init__(self, out_dir: str) -> None:
        self.out_dir = Path(out_dir)
        self.out_dir.mkdir(parents=True, exist_ok=True)

    def render(self, state: Dict[str, Any], governance: Dict[str, Any],
               trace: List[Dict[str, Any]], version_meta: Dict[str, Any]) -> str:
        """Generate dashboard.html and graph_data.json, return dashboard path."""
        governance = governance or {}

        # Graph data
        self._export_graph(state, governance)

        # Dashboard HTML
        html = self._build_html(state, governance, trace, version_meta)
        path = self.out_dir / "dashboard.html"
        path.write_text(html, encoding="utf-8")
        return str(path)

    def _export_graph(self, state: Dict[str, Any], governance: Dict[str, Any]) -> None:
        conflict_ids = {c.get("entity_id") for c in governance.get("conflicts", {}).get("conflicts", [])}
        nodes = [
            {
                "id": e.get("id", e.get("entity_id", "")),
                "label": e.get("name", ""),
                "kind": e.get("type", "entity"),
                "confidence": e.get("confidence", 0),
                "has_conflict": e.get("id", "") in conflict_ids,
            }
            for e in state.get("entities", [])
        ]
        edges = state.get("relations", [])
        path = self.out_dir / "graph_data.json"
        path.write_text(json.dumps({"nodes": nodes, "edges": edges}, indent=2, ensure_ascii=False), encoding="utf-8")

    def _build_html(self, state, governance, trace, version_meta) -> str:
        entities = state.get("entities", [])
        claims = state.get("claims", [])
        relations = state.get("relations", [])
        conflicts = governance.get("conflicts", {})
        placeholders = governance.get("placeholders", state.get("placeholders", []))

        conf_vals = [x.get("confidence", 0) for section in [entities, claims, relations] for x in section]
        avg_conf = round(sum(conf_vals) / len(conf_vals), 3) if conf_vals else 0
        low_conf = sum(1 for c in claims if c.get("confidence", 1) < 0.45)
        unresolved = conflicts.get("unresolved_count", 0)
        missing_ph = sum(1 for p in (placeholders if isinstance(placeholders, list) else []) if p.get("status") == "missing")

        # Build tables
        entity_tbl = self._table(entities[:50])
        claim_tbl = self._table(claims[:50])
        conflict_tbl = self._table(conflicts.get("conflicts", [])[:30])
        ph_tbl = self._table(
            [p for p in (placeholders if isinstance(placeholders, list) else []) if p.get("status") == "missing"][:40]
        )
        trace_tbl = self._table(trace[:30])

        return f"""<!doctype html>
<html lang='zh'>
<head>
  <meta charset='utf-8'>
  <title>MindVault v2 Governance Dashboard</title>
  <style>
    :root {{ --bg: #1a1b26; --card: #24283b; --border: #414868; --text: #c0caf5; --accent: #7aa2f7; --green: #9ece6a; --red: #f7768e; --yellow: #e0af68; --purple: #bb9af7; }}
    * {{ box-sizing: border-box; margin: 0; padding: 0; }}
    body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', system-ui, sans-serif; background: var(--bg); color: var(--text); padding: 24px; }}
    h1 {{ color: var(--purple); margin-bottom: 8px; font-size: 28px; }}
    h2 {{ color: var(--accent); margin: 28px 0 12px; font-size: 20px; border-bottom: 1px solid var(--border); padding-bottom: 6px; }}
    .tabs {{ display: flex; gap: 0; margin: 16px 0 24px; }}
    .tab {{ padding: 10px 24px; cursor: pointer; border: 1px solid var(--border); background: var(--card); color: var(--text); font-size: 14px; }}
    .tab:first-child {{ border-radius: 8px 0 0 8px; }}
    .tab:last-child {{ border-radius: 0 8px 8px 0; }}
    .tab.active {{ background: var(--accent); color: var(--bg); font-weight: bold; }}
    .panel {{ display: none; }}
    .panel.active {{ display: block; }}
    .kpi {{ display: flex; gap: 16px; flex-wrap: wrap; margin: 16px 0; }}
    .kpi-card {{ background: var(--card); border: 1px solid var(--border); border-radius: 12px; padding: 16px 24px; min-width: 160px; }}
    .kpi-card .label {{ font-size: 12px; color: #565f89; text-transform: uppercase; }}
    .kpi-card .value {{ font-size: 32px; font-weight: bold; margin-top: 4px; }}
    .kpi-card .value.green {{ color: var(--green); }}
    .kpi-card .value.red {{ color: var(--red); }}
    .kpi-card .value.yellow {{ color: var(--yellow); }}
    .kpi-card .value.purple {{ color: var(--purple); }}
    table {{ width: 100%; border-collapse: collapse; margin-bottom: 24px; font-size: 14px; background: var(--card); border-radius: 8px; overflow: hidden; }}
    th {{ background: #1a1b26; color: var(--accent); padding: 14px 12px; text-align: left; border-bottom: 2px solid var(--border); font-weight: 600; text-transform: uppercase; font-size: 12px; letter-spacing: 0.5px; }}
    td {{ padding: 12px; border-bottom: 1px solid #292e42; max-width: 400px; color: #a9b1d6; line-height: 1.5; vertical-align: top; word-wrap: break-word; }}
    tr:last-child td {{ border-bottom: none; }}
    tr:hover td {{ background: #2f354d; }}
    td strong {{ color: var(--purple); font-size: 13px; }}
    .version {{ color: #565f89; font-size: 13px; margin-top: 4px; }}
  </style>
</head>
<body>
  <h1>🧠 MindVault Governance Dashboard</h1>
  <p class='version'>Version {version_meta.get('version', '?')} · {state.get('metadata', dict()).get('last_updated_at', '')}</p>

  <div class='tabs'>
    <div class='tab active' onclick='showPanel("control")'>⚙️ Control Console</div>
    <div class='tab' onclick='showPanel("knowledge")'>📚 Knowledge Console</div>
    <div class='tab' onclick='showPanel("governance")'>🛡️ Governance Console</div>
  </div>

  <!-- ═══ Control Console ═══ -->
  <div id='panel-control' class='panel active'>
    <div class='kpi'>
      <div class='kpi-card'><div class='label'>Entities</div><div class='value green'>{len(entities)}</div></div>
      <div class='kpi-card'><div class='label'>Claims</div><div class='value purple'>{len(claims)}</div></div>
      <div class='kpi-card'><div class='label'>Relations</div><div class='value'>{len(relations)}</div></div>
      <div class='kpi-card'><div class='label'>Avg Confidence</div><div class='value yellow'>{avg_conf}</div></div>
      <div class='kpi-card'><div class='label'>Conflicts</div><div class='value red'>{unresolved}</div></div>
      <div class='kpi-card'><div class='label'>Missing Fields</div><div class='value red'>{missing_ph}</div></div>
    </div>
    <h2>Agent Trace Timeline</h2>
    {trace_tbl}
  </div>

  <!-- ═══ Knowledge Console ═══ -->
  <div id='panel-knowledge' class='panel'>
    <h2>Entities</h2>
    {entity_tbl}
    <h2>Claims</h2>
    {claim_tbl}
  </div>

  <!-- ═══ Governance Console ═══ -->
  <div id='panel-governance' class='panel'>
    <div class='kpi'>
      <div class='kpi-card'><div class='label'>Unresolved Conflicts</div><div class='value red'>{unresolved}</div></div>
      <div class='kpi-card'><div class='label'>Low Confidence Claims</div><div class='value yellow'>{low_conf}</div></div>
      <div class='kpi-card'><div class='label'>Missing Placeholders</div><div class='value red'>{missing_ph}</div></div>
    </div>
    <h2>Conflicts</h2>
    {conflict_tbl}
    <h2>Missing Placeholders</h2>
    {ph_tbl}
  </div>

  <script>
    function showPanel(name) {{
      document.querySelectorAll('.panel').forEach(p => p.classList.remove('active'));
      document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
      document.getElementById('panel-' + name).classList.add('active');
      event.target.classList.add('active');
    }}
  </script>
</body>
</html>"""

    @staticmethod
    def _table(rows: List[Dict[str, Any]], columns: List[str] = None) -> str:
        if not rows:
            cols = columns or []
            head = "<th>#</th>" + "".join(f"<th>{escape(c.upper())}</th>" for c in cols)
            return f"<table><thead><tr>{head}</tr></thead><tbody><tr><td colspan='{len(cols)+1}' style='color:#565f89; text-align: center; padding: 20px;'>No data available</td></tr></tbody></table>"
        
        if not columns:
            # Dynamically extract columns from rows
            col_set = set()
            columns = []
            for row in rows:
                for k in row.keys():
                    if k not in col_set:
                        col_set.add(k)
                        columns.append(k)
                        
        # Generate headers with a # column first
        head = "<th>#</th>" + "".join(f"<th>{escape(c.upper())}</th>" for c in columns)
        body_parts = []
        for i, row in enumerate(rows, 1):
            cells = [f"<td><strong>{i}</strong></td>"]
            for c in columns:
                val = row.get(c, "")
                if isinstance(val, (dict, list)):
                    val = json.dumps(val, ensure_ascii=False)
                # Remove code tags and let text wrap for readability
                cells.append(f"<td>{escape(str(val)[:200])}</td>")
            body_parts.append(f"<tr>{''.join(cells)}</tr>")
        body = "".join(body_parts)
        return f"<table><thead><tr>{head}</tr></thead><tbody>{body}</tbody></table>"
