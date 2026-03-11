"""Multi-DB renderer: writes AI-first database outputs and a thin HTML viewer."""
from __future__ import annotations

from html import escape
from pathlib import Path
from typing import Any, Dict, List
import json


class MultiDBRenderer:
    """Persist multi-database JSON artifacts and optionally render a thin HTML viewer."""

    def __init__(self, out_dir: str | Path) -> None:
        self.out_dir = Path(out_dir)
        self.out_dir.mkdir(parents=True, exist_ok=True)

    def render(self, database_plan: Dict[str, Any], multi_db: Dict[str, Any], include_html: bool = False) -> Dict[str, str]:
        plan_path = self.out_dir / "database_plan.json"
        db_path = self.out_dir / "multi_db.json"

        plan_path.write_text(json.dumps(database_plan, indent=2, ensure_ascii=False), encoding="utf-8")
        db_path.write_text(json.dumps(multi_db, indent=2, ensure_ascii=False), encoding="utf-8")
        result = {
            "plan": str(plan_path),
            "data": str(db_path),
        }
        if include_html:
            html_path = self.out_dir / "multi_db.html"
            html_path.write_text(self._build_html(database_plan, multi_db), encoding="utf-8")
            result["html"] = str(html_path)
        return result

    def _build_html(self, database_plan: Dict[str, Any], multi_db: Dict[str, Any]) -> str:
        databases = multi_db.get("databases", [])
        relations = multi_db.get("relations", [])
        tabs = []
        panels = []
        stat_cards = []

        for index, database in enumerate(databases):
            name = database.get("name", f"db_{index}")
            title = database.get("title", name)
            active = " active" if index == 0 else ""
            row_count = len(database.get("rows", []))
            col_count = len(database.get("columns", []))
            tabs.append(f"<button class='tab{active}' onclick=\"showDb('{escape(name)}', event)\">{escape(title)}</button>")
            stat_cards.append(
                f"<div class='card'><div class='card-label'>{escape(title)}</div>"
                f"<div class='card-value'>{row_count}</div><div class='card-meta'>{col_count} fields</div></div>"
            )
            panels.append(
                f"<section id='panel-{escape(name)}' class='panel{active}'>"
                f"<div class='panel-head'><div><h2>{escape(title)}</h2><p class='panel-desc'>{escape(database.get('description', ''))}</p></div>"
                f"<div class='pill-row'><span class='pill'>{row_count} rows</span><span class='pill'>{col_count} fields</span></div></div>"
                f"{self._table(database.get('columns', []), database.get('rows', []))}"
                f"</section>"
            )

        relation_table = self._table(
            ["from_db", "from_field", "to_db", "to_field", "relation_type"],
            relations,
        )
        plan_table = self._table(
            ["name", "title", "description", "entity_types", "suggested_fields"],
            database_plan.get("databases", []),
        )

        return f"""<!doctype html>
<html lang='zh'>
<head>
  <meta charset='utf-8'>
  <title>Knowledge Databases</title>
  <style>
    :root {{ --bg: #f4efe6; --ink: #1f2937; --muted: #6b7280; --panel: #fffdf8; --line: #d6d3d1; --accent: #0f766e; --accent-soft: #d9f3ef; --warm: #7c2d12; }}
    * {{ box-sizing: border-box; }}
    body {{ font-family: Georgia, 'Iowan Old Style', 'Times New Roman', serif; margin: 0; color: var(--ink); background:
      radial-gradient(circle at top left, rgba(15,118,110,0.08), transparent 28%),
      linear-gradient(180deg, #f8f4ec 0%, var(--bg) 100%); }}
    .shell {{ max-width: 1440px; margin: 0 auto; padding: 28px; }}
    h1, h2 {{ margin: 0 0 12px; }}
    .hero {{ display: grid; grid-template-columns: 1.4fr 1fr; gap: 20px; align-items: end; margin-bottom: 22px; }}
    .hero-block {{ background: rgba(255,253,248,0.86); border: 1px solid rgba(124,45,18,0.12); border-radius: 24px; padding: 24px; box-shadow: 0 18px 40px rgba(15,23,42,0.06); backdrop-filter: blur(8px); }}
    .eyebrow {{ color: var(--accent); text-transform: uppercase; letter-spacing: 0.16em; font-size: 12px; margin-bottom: 10px; }}
    .hero h1 {{ font-size: 40px; line-height: 1.05; }}
    .hero p {{ color: var(--muted); margin: 0; line-height: 1.6; }}
    .stats {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 14px; margin: 20px 0 28px; }}
    .card {{ background: var(--panel); border: 1px solid var(--line); border-radius: 18px; padding: 16px 18px; box-shadow: 0 12px 30px rgba(15,23,42,0.04); }}
    .card-label {{ font-size: 12px; color: var(--muted); text-transform: uppercase; letter-spacing: 0.08em; }}
    .card-value {{ font-size: 34px; margin-top: 8px; color: var(--warm); }}
    .card-meta {{ color: var(--muted); font-size: 13px; }}
    .section {{ background: rgba(255,253,248,0.86); border: 1px solid rgba(214,211,209,0.9); border-radius: 24px; padding: 22px; margin-bottom: 22px; box-shadow: 0 18px 40px rgba(15,23,42,0.04); }}
    .tabs {{ display: flex; gap: 8px; flex-wrap: wrap; margin: 16px 0; }}
    .tab {{ border: 1px solid var(--line); background: rgba(255,255,255,0.75); padding: 9px 14px; border-radius: 999px; cursor: pointer; color: var(--ink); }}
    .tab.active {{ background: var(--accent); color: #fff; border-color: var(--accent); }}
    .panel {{ display: none; }}
    .panel.active {{ display: block; }}
    .panel-head {{ display: flex; justify-content: space-between; gap: 16px; align-items: start; margin-bottom: 12px; }}
    .panel-desc {{ color: var(--muted); margin: 0; line-height: 1.5; }}
    .pill-row {{ display: flex; gap: 8px; flex-wrap: wrap; }}
    .pill {{ background: var(--accent-soft); color: var(--accent); border: 1px solid rgba(15,118,110,0.14); border-radius: 999px; padding: 6px 10px; font-size: 12px; white-space: nowrap; }}
    table {{ width: 100%; border-collapse: collapse; margin: 16px 0 0; background: rgba(255,255,255,0.7); }}
    th, td {{ border: 1px solid #ece7df; padding: 10px 12px; text-align: left; vertical-align: top; }}
    th {{ background: #f8f5ef; color: var(--warm); position: sticky; top: 0; }}
    tr:nth-child(even) td {{ background: rgba(249,247,243,0.82); }}
    code {{ white-space: pre-wrap; font-family: ui-monospace, SFMono-Regular, Menlo, monospace; font-size: 12px; }}
    .meta {{ color: var(--muted); margin: 0; }}
    .relation-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(240px, 1fr)); gap: 12px; margin-top: 16px; }}
    .relation-card {{ background: rgba(255,255,255,0.74); border: 1px solid var(--line); border-radius: 16px; padding: 14px; }}
    .relation-card strong {{ color: var(--warm); }}
    @media (max-width: 900px) {{
      .hero {{ grid-template-columns: 1fr; }}
      .panel-head {{ flex-direction: column; }}
    }}
  </style>
</head>
<body>
  <main class='shell'>
    <section class='hero'>
      <div class='hero-block'>
        <div class='eyebrow'>Knowledge Workspace</div>
        <h1>{escape(str(multi_db.get("domain", database_plan.get("domain", "Knowledge Databases"))))}</h1>
        <p>输出以多数据库为中心。每张表的字段按真实数据动态展开，表间关联单独建模，适合继续接前端视图或增量更新链路。</p>
      </div>
      <div class='hero-block'>
        <div class='eyebrow'>Overview</div>
        <p class='meta'>Databases: {len(databases)}</p>
        <p class='meta'>Relations: {len(relations)}</p>
        <p class='meta'>Rows: {sum(len(database.get("rows", [])) for database in databases)}</p>
      </div>
    </section>

    <section class='stats'>
      {''.join(stat_cards) if stat_cards else "<div class='card'><div class='card-label'>Databases</div><div class='card-value'>0</div><div class='card-meta'>No output</div></div>"}
    </section>

    <section class='section'>
      <h2>Database Plan</h2>
      <p class='meta'>这是结构设计层给出的数据库规划，后续多数据库实例应尽量贴近这份规划。</p>
      {plan_table}
    </section>

    <section class='section'>
      <h2>Databases</h2>
      <div class='tabs'>{''.join(tabs) if tabs else '<span>No databases</span>'}</div>
      {''.join(panels)}
    </section>

    <section class='section'>
      <h2>Relations</h2>
      <div class='relation-grid'>
        {''.join(self._relation_cards(relations))}
      </div>
      {relation_table}
    </section>
  </main>
  <script>
    function showDb(name, event) {{
      document.querySelectorAll('.panel').forEach(p => p.classList.remove('active'));
      document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
      var panel = document.getElementById('panel-' + name);
      if (panel) panel.classList.add('active');
      if (event && event.target) event.target.classList.add('active');
    }}
  </script>
</body>
</html>"""

    def _table(self, columns: List[str], rows: List[Dict[str, Any]]) -> str:
        if not columns:
            return "<p>No structured data.</p>"
        head = "".join(f"<th>{escape(column)}</th>" for column in columns)
        body_rows = []
        if rows:
            for row in rows:
                body_rows.append(
                    "<tr>" + "".join(
                        f"<td><code>{escape(self._stringify(row.get(column, '')))}</code></td>"
                        for column in columns
                    ) + "</tr>"
                )
        else:
            body_rows.append(f"<tr><td colspan='{len(columns)}'>No rows</td></tr>")
        return f"<table><thead><tr>{head}</tr></thead><tbody>{''.join(body_rows)}</tbody></table>"

    def _relation_cards(self, relations: List[Dict[str, Any]]) -> List[str]:
        cards = []
        for relation in relations:
            cards.append(
                "<div class='relation-card'>"
                f"<strong>{escape(str(relation.get('from_db', '')))}</strong>"
                f" <span class='meta'>.{escape(str(relation.get('from_field', '')))}</span>"
                f" <span class='meta'>→</span> "
                f"<strong>{escape(str(relation.get('to_db', '')))}</strong>"
                f" <span class='meta'>.{escape(str(relation.get('to_field', '')))}</span>"
                f"<div class='meta' style='margin-top:8px'>{escape(str(relation.get('relation_type', '')))}</div>"
                "</div>"
            )
        return cards or ["<div class='relation-card'><div class='meta'>No relations</div></div>"]

    @staticmethod
    def _stringify(value: Any) -> str:
        if isinstance(value, (dict, list)):
            return json.dumps(value, ensure_ascii=False)
        return str(value)
