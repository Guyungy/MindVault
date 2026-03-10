"""Wiki exporter: turns canonical KB state into incrementally growing wiki artifacts."""
from __future__ import annotations

from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List
import json


class WikiExporter:
    """Exports machine-friendly tables and human-readable wiki pages."""

    def __init__(self, out_dir: str | Path) -> None:
        self.out_dir = Path(out_dir)
        self.entity_dir = self.out_dir / "entities"
        self.type_dir = self.out_dir / "by_type"
        self.area_dir = self.out_dir / "areas"
        for path in [self.out_dir, self.entity_dir, self.type_dir]:
            path.mkdir(parents=True, exist_ok=True)
        self.area_dir.mkdir(parents=True, exist_ok=True)

    def export(
        self,
        state: Dict[str, Any],
        governance: Dict[str, Any],
        version_meta: Dict[str, Any],
        wiki_payload: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        tables = self._build_tables(state, governance, version_meta)
        tables_path = self.out_dir / "tables.json"
        tables_path.write_text(json.dumps(tables, indent=2, ensure_ascii=False), encoding="utf-8")

        if wiki_payload and isinstance(wiki_payload.get("pages"), list):
            pages_path = self.out_dir / "pages.json"
            pages_path.write_text(json.dumps(wiki_payload, indent=2, ensure_ascii=False), encoding="utf-8")
            rendered = self._render_ai_pages(wiki_payload)
            rendered["tables"] = str(tables_path)
            rendered["pages_json"] = str(pages_path)
            return rendered

        entity_paths = self._write_entity_pages(state)
        type_paths = self._write_type_pages(state)
        area_paths = self._write_area_pages(state)
        governance_path = self._write_governance_page(governance)
        claims_path = self._write_claims_page(state.get("claims", []))
        relations_path = self._write_relations_page(state.get("relations", []))
        timeline_path = self._write_timeline_page(state)
        source_path = self._write_sources_page(state)
        overview_path = self._write_overview_page(state)
        index_path = self._write_index(state, governance, version_meta, entity_paths, type_paths, area_paths)
        pages_path = self.out_dir / "pages.json"
        pages_path.write_text(json.dumps(self._build_fallback_pages(state, governance, version_meta), indent=2, ensure_ascii=False), encoding="utf-8")

        return {
            "index": str(index_path),
            "overview": str(overview_path),
            "tables": str(tables_path),
            "pages_json": str(pages_path),
            "governance": str(governance_path),
            "claims": str(claims_path),
            "relations": str(relations_path),
            "timeline": str(timeline_path),
            "sources": str(source_path),
            "entity_pages": entity_paths,
            "type_pages": type_paths,
            "area_pages": area_paths,
        }

    def _render_ai_pages(self, wiki_payload: Dict[str, Any]) -> Dict[str, Any]:
        entity_pages: Dict[str, str] = {}
        type_pages: Dict[str, str] = {}
        area_pages: Dict[str, str] = {}
        root_paths: Dict[str, str] = {}

        for page in wiki_payload.get("pages", []):
            title = page.get("title", "Untitled")
            page_type = page.get("page_type", "root")
            slug = page.get("slug") or self._slug_for_filename(title)
            page_id = page.get("id", slug)
            path = self._page_path(page_type, slug)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(self._markdown_from_page(page), encoding="utf-8")

            if page_type == "entity":
                entity_pages[page_id] = str(path)
            elif page_type == "type":
                type_pages[page.get("name", title)] = str(path)
            elif page_type == "area":
                area_pages[page.get("name", title)] = str(path)
            else:
                root_paths[slug] = str(path)

        return {
            "index": root_paths.get("index", str(self.out_dir / "index.md")),
            "overview": root_paths.get("overview", str(self.out_dir / "overview.md")),
            "governance": root_paths.get("governance", str(self.out_dir / "governance.md")),
            "claims": root_paths.get("claims", str(self.out_dir / "claims.md")),
            "relations": root_paths.get("relations", str(self.out_dir / "relations.md")),
            "timeline": root_paths.get("timeline", str(self.out_dir / "timeline.md")),
            "sources": root_paths.get("sources", str(self.out_dir / "sources.md")),
            "entity_pages": entity_pages,
            "type_pages": type_pages,
            "area_pages": area_pages,
        }

    def _build_fallback_pages(
        self,
        state: Dict[str, Any],
        governance: Dict[str, Any],
        version_meta: Dict[str, Any],
    ) -> Dict[str, Any]:
        pages: List[Dict[str, Any]] = []
        entities = state.get("entities", [])
        claims = state.get("claims", [])
        relations = state.get("relations", [])

        pages.append({
            "id": "index",
            "slug": "index",
            "title": "MindVault Wiki",
            "page_type": "root",
            "summary": "知识库首页，汇总核心页面入口与统计。",
            "sections": [
                {
                    "heading": "Snapshot",
                    "table": {
                        "columns": ["metric", "value"],
                        "rows": [
                            {"metric": "entities", "value": len(entities)},
                            {"metric": "claims", "value": len(claims)},
                            {"metric": "relations", "value": len(relations)},
                            {"metric": "version", "value": version_meta.get("version", "")},
                        ],
                    },
                },
                {
                    "heading": "Core Pages",
                    "list": [
                        "overview",
                        "timeline",
                        "sources",
                        "governance",
                        "claims",
                        "relations",
                    ],
                },
            ],
        })

        pages.append({
            "id": "overview",
            "slug": "overview",
            "title": "Overview",
            "page_type": "root",
            "summary": "知识库整体概览页。",
            "sections": [
                {
                    "heading": "Entity Types",
                    "table": {
                        "columns": ["type", "count"],
                        "rows": self._type_rows(entities),
                    },
                },
                {
                    "heading": "Top Entities",
                    "table": {
                        "columns": ["id", "name", "type", "confidence"],
                        "rows": [
                            {
                                "id": entity.get("id", ""),
                                "name": entity.get("name", ""),
                                "type": entity.get("type", ""),
                                "confidence": entity.get("confidence", 0),
                            }
                            for entity in sorted(entities, key=lambda item: item.get("confidence", 0), reverse=True)[:12]
                        ],
                    },
                },
            ],
        })

        pages.append(self._simple_root_page("claims", "Claims", claims))
        pages.append(self._simple_root_page("relations", "Relations", relations))
        pages.append(self._simple_root_page("governance", "Governance", governance.get("conflicts", {}).get("conflicts", [])))
        pages.append(self._timeline_page(state))
        pages.append(self._sources_page(state))

        claims_by_subject = defaultdict(list)
        for claim in claims:
            claims_by_subject[claim.get("subject", "")].append(claim)
        relation_index = defaultdict(list)
        for relation in relations:
            relation_index[relation.get("source", "")].append(relation)
            relation_index[relation.get("target", "")].append({**relation, "_direction": "in"})

        grouped_by_type = defaultdict(list)
        grouped_by_area = defaultdict(list)
        for entity in entities:
            grouped_by_type[entity.get("type", "unknown")].append(entity)
            location = self._best_location(entity)
            if location:
                grouped_by_area[location].append(entity)
            entity_claims = claims_by_subject.get(entity.get("id", ""), [])
            pages.append({
                "id": entity.get("id", ""),
                "slug": entity.get("id", ""),
                "name": entity.get("name", ""),
                "title": entity.get("name", entity.get("id", "")),
                "page_type": "entity",
                "summary": self._entity_summary(entity, entity_claims),
                "sections": [
                    {
                        "heading": "Metadata",
                        "table": {
                            "columns": ["field", "value"],
                            "rows": [
                                {"field": "id", "value": entity.get("id", "")},
                                {"field": "type", "value": entity.get("type", "")},
                                {"field": "confidence", "value": entity.get("confidence", 0)},
                                {"field": "updated_at", "value": entity.get("updated_at", "")},
                            ],
                        },
                    },
                    {
                        "heading": "Attributes",
                        "table": {
                            "columns": ["field", "value"],
                            "rows": [{"field": key, "value": self._normalize_value(value)} for key, value in entity.get("attributes", {}).items()],
                        },
                    },
                    {
                        "heading": "Key Claims",
                        "table": {
                            "columns": ["predicate", "object", "claim_type", "confidence", "evidence"],
                            "rows": [
                                {
                                    "predicate": claim.get("predicate", ""),
                                    "object": self._normalize_value(claim.get("object")),
                                    "claim_type": claim.get("claim_type", ""),
                                    "confidence": claim.get("confidence", 0),
                                    "evidence": claim.get("claim_text", ""),
                                }
                                for claim in entity_claims
                            ],
                        },
                    },
                    {
                        "heading": "Relations",
                        "table": {
                            "columns": ["direction", "relation", "peer", "confidence"],
                            "rows": [
                                {
                                    "direction": relation.get("_direction", "out" if relation.get("source") == entity.get("id", "") else "in"),
                                    "relation": relation.get("relation", ""),
                                    "peer": relation.get("target", "") if relation.get("source") == entity.get("id", "") else relation.get("source", ""),
                                    "confidence": relation.get("confidence", 0),
                                }
                                for relation in relation_index.get(entity.get("id", ""), [])
                            ],
                        },
                    },
                ],
            })

        for entity_type, type_entities in grouped_by_type.items():
            pages.append({
                "id": f"type_{entity_type}",
                "slug": entity_type,
                "name": entity_type,
                "title": f"Entity Type: {entity_type}",
                "page_type": "type",
                "summary": f"{entity_type} 类型实体汇总。",
                "sections": [
                    {
                        "heading": "Entities",
                        "table": {
                            "columns": ["id", "name", "confidence", "updated_at"],
                            "rows": [
                                {
                                    "id": entity.get("id", ""),
                                    "name": entity.get("name", ""),
                                    "confidence": entity.get("confidence", 0),
                                    "updated_at": entity.get("updated_at", ""),
                                }
                                for entity in type_entities
                            ],
                        },
                    }
                ],
            })

        for area_name, area_entities in grouped_by_area.items():
            pages.append({
                "id": f"area_{area_name}",
                "slug": self._slug_for_filename(area_name),
                "name": area_name,
                "title": f"Area: {area_name}",
                "page_type": "area",
                "summary": f"{area_name} 相关实体汇总。",
                "sections": [
                    {
                        "heading": "Entities",
                        "table": {
                            "columns": ["id", "name", "type", "confidence"],
                            "rows": [
                                {
                                    "id": entity.get("id", ""),
                                    "name": entity.get("name", ""),
                                    "type": entity.get("type", ""),
                                    "confidence": entity.get("confidence", 0),
                                }
                                for entity in area_entities
                            ],
                        },
                    }
                ],
            })

        return {
            "domain": "MindVault Knowledge Wiki",
            "generated_at": datetime.utcnow().isoformat(),
            "pages": pages,
        }

    def _simple_root_page(self, slug: str, title: str, rows: List[Dict[str, Any]]) -> Dict[str, Any]:
        return {
            "id": slug,
            "slug": slug,
            "title": title,
            "page_type": "root",
            "summary": f"{title} 汇总页。",
            "sections": [
                {
                    "heading": title,
                    "table": self._table_payload(title.lower(), rows),
                }
            ],
        }

    def _timeline_page(self, state: Dict[str, Any]) -> Dict[str, Any]:
        events = state.get("events", [])
        return {
            "id": "timeline",
            "slug": "timeline",
            "title": "Timeline",
            "page_type": "root",
            "summary": "时间线视图。",
            "sections": [
                {
                    "heading": "Events",
                    "table": {
                        "columns": ["timestamp", "type", "description"],
                        "rows": [
                            {
                                "timestamp": event.get("timestamp", "") or event.get("updated_at", ""),
                                "type": event.get("type", ""),
                                "description": event.get("description", ""),
                            }
                            for event in events
                        ],
                    },
                }
            ],
        }

    def _sources_page(self, state: Dict[str, Any]) -> Dict[str, Any]:
        counts: Dict[str, int] = defaultdict(int)
        for entity in state.get("entities", []):
            for source_ref in entity.get("source_refs", []):
                counts[source_ref] += 1
        for claim in state.get("claims", []):
            source_ref = claim.get("source_ref", "")
            if source_ref:
                counts[source_ref] += 1
        return {
            "id": "sources",
            "slug": "sources",
            "title": "Sources",
            "page_type": "root",
            "summary": "来源引用统计。",
            "sections": [
                {
                    "heading": "Source Stats",
                    "table": {
                        "columns": ["source", "mentions"],
                        "rows": [{"source": key, "mentions": value} for key, value in sorted(counts.items())],
                    },
                }
            ],
        }

    def _type_rows(self, entities: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        counts: Dict[str, int] = defaultdict(int)
        for entity in entities:
            counts[entity.get("type", "unknown")] += 1
        return [{"type": entity_type, "count": count} for entity_type, count in sorted(counts.items())]

    def _page_path(self, page_type: str, slug: str) -> Path:
        if page_type == "entity":
            return self.entity_dir / f"{slug}.md"
        if page_type == "type":
            return self.type_dir / f"{slug}.md"
        if page_type == "area":
            return self.area_dir / f"{slug}.md"
        return self.out_dir / f"{slug}.md"

    def _markdown_from_page(self, page: Dict[str, Any]) -> str:
        lines = [f"# {page.get('title', 'Untitled')}", ""]
        summary = page.get("summary", "")
        if summary:
            lines.extend([summary, ""])
        for section in page.get("sections", []):
            heading = section.get("heading", "")
            if heading:
                lines.extend([f"## {heading}", ""])
            body = section.get("body", "")
            if body:
                lines.extend([body, ""])
            items = section.get("list", [])
            if items:
                lines.extend([f"- {item}" for item in items])
                lines.append("")
            table = section.get("table")
            if table:
                lines.extend(self._markdown_table(table))
                lines.append("")
        return "\n".join(lines).strip() + "\n"

    def _markdown_table(self, table: Dict[str, Any]) -> List[str]:
        columns = table.get("columns", [])
        rows = table.get("rows", [])
        if not columns:
            return ["- No structured rows"]
        lines = [
            "| " + " | ".join(columns) + " |",
            "| " + " | ".join("---" for _ in columns) + " |",
        ]
        if rows:
            for row in rows:
                lines.append("| " + " | ".join(self._stringify(row.get(column, "")) for column in columns) + " |")
        else:
            lines.append("| " + " | ".join("" for _ in columns) + " |")
        return lines

    def _build_tables(
        self,
        state: Dict[str, Any],
        governance: Dict[str, Any],
        version_meta: Dict[str, Any],
    ) -> Dict[str, Any]:
        entities = state.get("entities", [])
        claims = state.get("claims", [])
        relations = state.get("relations", [])
        events = state.get("events", [])
        placeholders = governance.get("placeholders", state.get("placeholders", []))
        conflicts = governance.get("conflicts", {}).get("conflicts", [])

        return {
            "generated_at": datetime.utcnow().isoformat(),
            "workspace_version": version_meta.get("version"),
            "tables": [
                {
                    "table_name": "overview",
                    "columns": ["metric", "value"],
                    "rows": [
                        {"metric": "entities", "value": len(entities)},
                        {"metric": "claims", "value": len(claims)},
                        {"metric": "relations", "value": len(relations)},
                        {"metric": "events", "value": len(events)},
                        {"metric": "conflicts", "value": len(conflicts)},
                        {"metric": "missing_placeholders", "value": sum(1 for row in placeholders if row.get("status") == "missing")},
                    ],
                },
                self._table_payload("entities", entities),
                self._table_payload("claims", claims),
                self._table_payload("relations", relations),
                self._table_payload("events", events),
                self._table_payload("conflicts", conflicts),
                self._table_payload("placeholders", placeholders),
            ],
        }

    def _write_index(
        self,
        state: Dict[str, Any],
        governance: Dict[str, Any],
        version_meta: Dict[str, Any],
        entity_paths: Dict[str, str],
        type_paths: Dict[str, str],
        area_paths: Dict[str, str],
    ) -> Path:
        entities = state.get("entities", [])
        typed = defaultdict(int)
        for entity in entities:
            typed[entity.get("type", "unknown")] += 1

        lines = [
            "# MindVault Wiki",
            "",
            f"- Generated at: {datetime.utcnow().isoformat()}",
            f"- Version: {version_meta.get('version', 'unknown')}",
            f"- Entities: {len(entities)}",
            f"- Claims: {len(state.get('claims', []))}",
            f"- Relations: {len(state.get('relations', []))}",
            f"- Unresolved conflicts: {governance.get('conflicts', {}).get('unresolved_count', 0)}",
            "",
            "## Core Pages",
            "",
            "- [Overview](overview.md)",
            "- [Timeline](timeline.md)",
            "- [Sources](sources.md)",
            "- [Governance](governance.md)",
            "- [Claims](claims.md)",
            "- [Relations](relations.md)",
            "",
            "## Type Pages",
            "",
        ]

        if type_paths:
            for entity_type, path in sorted(type_paths.items()):
                lines.append(f"- [{entity_type}](by_type/{Path(path).name})")
        else:
            lines.append("- No type pages generated.")

        lines.extend(["", "## Area Pages", ""])
        if area_paths:
            for area_name, path in sorted(area_paths.items()):
                lines.append(f"- [{area_name}](areas/{Path(path).name})")
        else:
            lines.append("- No area pages generated.")

        lines.extend(["", "## Entity Pages", ""])
        if entity_paths:
            for entity_id, path in sorted(entity_paths.items()):
                lines.append(f"- [{entity_id}](entities/{Path(path).name})")
        else:
            lines.append("- No entity pages generated.")

        lines.extend(["", "## Type Summary", "", "| Type | Count |", "| --- | ---: |"])
        for entity_type, count in sorted(typed.items(), key=lambda item: (-item[1], item[0])):
            lines.append(f"| {entity_type} | {count} |")

        index_path = self.out_dir / "index.md"
        index_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        return index_path

    def _write_entity_pages(self, state: Dict[str, Any]) -> Dict[str, str]:
        claims_by_subject = defaultdict(list)
        for claim in state.get("claims", []):
            claims_by_subject[claim.get("subject", "")].append(claim)

        outgoing = defaultdict(list)
        incoming = defaultdict(list)
        for relation in state.get("relations", []):
            source = relation.get("source", "")
            target = relation.get("target", "")
            if source:
                outgoing[source].append(relation)
            if target:
                incoming[target].append(relation)

        paths: Dict[str, str] = {}
        for entity in state.get("entities", []):
            entity_id = entity.get("id", "")
            if not entity_id:
                continue
            entity_claims = claims_by_subject.get(entity_id, [])
            summary = self._entity_summary(entity, entity_claims=entity_claims)

            lines = [
                f"# {entity.get('name', entity_id)}",
                "",
                f"- ID: {entity_id}",
                f"- Type: {entity.get('type', 'unknown')}",
                f"- Confidence: {entity.get('confidence', 0)}",
                f"- Updated: {entity.get('updated_at', '')}",
                "",
                "## Summary",
                "",
                summary,
                "",
                "## Attributes",
                "",
                "| Field | Value |",
                "| --- | --- |",
            ]

            attributes = entity.get("attributes", {})
            if attributes:
                for key, value in sorted(attributes.items()):
                    lines.append(f"| {key} | {self._stringify(value)} |")
            else:
                lines.append("| - | - |")

            lines.extend(["", "## Source Refs", ""])
            source_refs = entity.get("source_refs", [])
            if source_refs:
                lines.extend(f"- {ref}" for ref in source_refs)
            else:
                lines.append("- None")

            lines.extend(["", "## Key Facts", ""])
            if entity_claims:
                for claim in entity_claims[:8]:
                    lines.append(
                        f"- `{claim.get('predicate', '')}`: {self._stringify(claim.get('object'))} "
                        f"(type={claim.get('claim_type', '')}, confidence={claim.get('confidence', 0)})"
                    )
            else:
                lines.append("- No key facts extracted.")

            lines.extend(["", "## Claims", ""])
            if entity_claims:
                lines.extend([
                    "| Predicate | Object | Type | Confidence | Evidence |",
                    "| --- | --- | --- | ---: | --- |",
                ])
                for claim in entity_claims:
                    lines.append(
                        f"| {claim.get('predicate', '')} | {self._stringify(claim.get('object'))} | "
                        f"{claim.get('claim_type', '')} | {claim.get('confidence', 0)} | {self._stringify(claim.get('claim_text', ''))[:120]} |"
                    )
            else:
                lines.append("- No claims linked.")

            lines.extend(["", "## Relations", ""])
            if outgoing.get(entity_id) or incoming.get(entity_id):
                lines.extend([
                    "| Direction | Relation | Peer | Confidence |",
                    "| --- | --- | --- | ---: |",
                ])
                for relation in outgoing.get(entity_id, []):
                    lines.append(
                        f"| out | {relation.get('relation', '')} | {relation.get('target', '')} | {relation.get('confidence', 0)} |"
                    )
                for relation in incoming.get(entity_id, []):
                    lines.append(
                        f"| in | {relation.get('relation', '')} | {relation.get('source', '')} | {relation.get('confidence', 0)} |"
                    )
            else:
                lines.append("- No relations linked.")

            path = self.entity_dir / f"{entity_id}.md"
            path.write_text("\n".join(lines) + "\n", encoding="utf-8")
            paths[entity_id] = str(path)

        return paths

    def _write_type_pages(self, state: Dict[str, Any]) -> Dict[str, str]:
        grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        for entity in state.get("entities", []):
            grouped[entity.get("type", "unknown")].append(entity)

        paths: Dict[str, str] = {}
        for entity_type, entities in grouped.items():
            lines = [
                f"# Entity Type: {entity_type}",
                "",
                "| ID | Name | Confidence | Updated |",
                "| --- | --- | ---: | --- |",
            ]
            for entity in sorted(entities, key=lambda item: item.get("name", item.get("id", ""))):
                entity_id = entity.get("id", "")
                lines.append(
                    f"| [{entity_id}](../entities/{entity_id}.md) | {entity.get('name', '')} | "
                    f"{entity.get('confidence', 0)} | {entity.get('updated_at', '')} |"
                )

            path = self.type_dir / f"{entity_type}.md"
            path.write_text("\n".join(lines) + "\n", encoding="utf-8")
            paths[entity_type] = str(path)

        return paths

    def _write_area_pages(self, state: Dict[str, Any]) -> Dict[str, str]:
        grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        for entity in state.get("entities", []):
            location = self._best_location(entity)
            if location:
                grouped[location].append(entity)

        paths: Dict[str, str] = {}
        for area_name, entities in grouped.items():
            lines = [
                f"# Area: {area_name}",
                "",
                f"该页面汇总与 `{area_name}` 相关的实体和资料片段。",
                "",
                "| Entity | Type | Confidence |",
                "| --- | --- | ---: |",
            ]
            for entity in sorted(entities, key=lambda item: item.get("name", item.get("id", ""))):
                entity_id = entity.get("id", "")
                lines.append(
                    f"| [{entity.get('name', entity_id)}](../entities/{entity_id}.md) | {entity.get('type', '')} | {entity.get('confidence', 0)} |"
                )

            filename = self._slug_for_filename(area_name) + ".md"
            path = self.area_dir / filename
            path.write_text("\n".join(lines) + "\n", encoding="utf-8")
            paths[area_name] = str(path)

        return paths

    def _write_governance_page(self, governance: Dict[str, Any]) -> Path:
        conflicts = governance.get("conflicts", {}).get("conflicts", [])
        placeholders = governance.get("placeholders", [])
        lines = [
            "# Governance",
            "",
            "## Conflicts",
            "",
            "| Entity | Field | Resolution |",
            "| --- | --- | --- |",
        ]
        if conflicts:
            for row in conflicts:
                lines.append(
                    f"| {row.get('entity_id', '')} | {row.get('field', '')} | {row.get('resolution_status', '')} |"
                )
        else:
            lines.append("| - | - | - |")

        lines.extend(["", "## Placeholders", "", "| Target | Field | Status |", "| --- | --- | --- |"])
        if placeholders:
            for row in placeholders:
                lines.append(
                    f"| {row.get('target_id', '')} | {row.get('field', '')} | {row.get('status', '')} |"
                )
        else:
            lines.append("| - | - | - |")

        path = self.out_dir / "governance.md"
        path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        return path

    def _write_overview_page(self, state: Dict[str, Any]) -> Path:
        entities = state.get("entities", [])
        claims = state.get("claims", [])
        relations = state.get("relations", [])
        type_counts: Dict[str, int] = defaultdict(int)
        for entity in entities:
            type_counts[entity.get("type", "unknown")] += 1

        lines = [
            "# Overview",
            "",
            "## Snapshot",
            "",
            "| Metric | Value |",
            "| --- | ---: |",
            f"| Entities | {len(entities)} |",
            f"| Claims | {len(claims)} |",
            f"| Relations | {len(relations)} |",
            "",
            "## Entity Types",
            "",
            "| Type | Count |",
            "| --- | ---: |",
        ]
        for entity_type, count in sorted(type_counts.items(), key=lambda item: (-item[1], item[0])):
            lines.append(f"| {entity_type} | {count} |")

        lines.extend(["", "## High Confidence Entities", ""])
        ranked = sorted(entities, key=lambda item: item.get("confidence", 0), reverse=True)[:10]
        if ranked:
            for entity in ranked:
                entity_id = entity.get("id", "")
                lines.append(
                    f"- [{entity.get('name', entity_id)}](entities/{entity_id}.md) "
                    f"({entity.get('type', '')}, confidence={entity.get('confidence', 0)})"
                )
        else:
            lines.append("- No entities available.")

        path = self.out_dir / "overview.md"
        path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        return path

    def _write_claims_page(self, claims: List[Dict[str, Any]]) -> Path:
        lines = [
            "# Claims",
            "",
            "| ID | Subject | Predicate | Object | Type | Confidence |",
            "| --- | --- | --- | --- | --- | ---: |",
        ]
        if claims:
            for claim in claims:
                lines.append(
                    f"| {claim.get('id', '')} | {claim.get('subject', '')} | {claim.get('predicate', '')} | "
                    f"{self._stringify(claim.get('object'))} | {claim.get('claim_type', '')} | {claim.get('confidence', 0)} |"
                )
        else:
            lines.append("| - | - | - | - | - | - |")

        path = self.out_dir / "claims.md"
        path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        return path

    def _write_relations_page(self, relations: List[Dict[str, Any]]) -> Path:
        lines = [
            "# Relations",
            "",
            "| Source | Relation | Target | Confidence |",
            "| --- | --- | --- | ---: |",
        ]
        if relations:
            for relation in relations:
                lines.append(
                    f"| {relation.get('source', '')} | {relation.get('relation', '')} | {relation.get('target', '')} | {relation.get('confidence', 0)} |"
                )
        else:
            lines.append("| - | - | - | - |")

        path = self.out_dir / "relations.md"
        path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        return path

    def _write_timeline_page(self, state: Dict[str, Any]) -> Path:
        events = sorted(
            state.get("events", []),
            key=lambda item: item.get("timestamp", "") or item.get("updated_at", ""),
            reverse=True,
        )
        lines = [
            "# Timeline",
            "",
            "| Time | Type | Description |",
            "| --- | --- | --- |",
        ]
        if events:
            for event in events:
                lines.append(
                    f"| {event.get('timestamp', '') or event.get('updated_at', '')} | {event.get('type', '')} | {self._stringify(event.get('description', ''))[:160]} |"
                )
        else:
            lines.append("| - | - | - |")
        path = self.out_dir / "timeline.md"
        path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        return path

    def _write_sources_page(self, state: Dict[str, Any]) -> Path:
        counts: Dict[str, int] = defaultdict(int)
        for entity in state.get("entities", []):
            for source_ref in entity.get("source_refs", []):
                counts[source_ref] += 1
        for claim in state.get("claims", []):
            source_ref = claim.get("source_ref", "")
            if source_ref:
                counts[source_ref] += 1

        lines = [
            "# Sources",
            "",
            "| Source | Mentions |",
            "| --- | ---: |",
        ]
        if counts:
            for source_ref, count in sorted(counts.items(), key=lambda item: (-item[1], item[0])):
                lines.append(f"| {source_ref} | {count} |")
        else:
            lines.append("| - | - |")

        path = self.out_dir / "sources.md"
        path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        return path

    def _table_payload(self, table_name: str, rows: List[Dict[str, Any]]) -> Dict[str, Any]:
        columns: List[str] = []
        seen = set()
        for row in rows:
            for key in row.keys():
                if key not in seen:
                    seen.add(key)
                    columns.append(key)

        normalized_rows = []
        for row in rows:
            normalized_rows.append({column: self._normalize_value(row.get(column)) for column in columns})

        return {
            "table_name": table_name,
            "columns": columns,
            "rows": normalized_rows,
        }

    @staticmethod
    def _normalize_value(value: Any) -> Any:
        if isinstance(value, dict):
            return {key: WikiExporter._normalize_value(val) for key, val in value.items()}
        if isinstance(value, list):
            return [WikiExporter._normalize_value(item) for item in value]
        return value

    @staticmethod
    def _stringify(value: Any) -> str:
        if isinstance(value, (dict, list)):
            return json.dumps(value, ensure_ascii=False)
        return str(value if value is not None else "")

    def _entity_summary(self, entity: Dict[str, Any], entity_claims: List[Dict[str, Any]]) -> str:
        parts = [f"{entity.get('name', entity.get('id', ''))} 是一个 `{entity.get('type', 'unknown')}` 实体。"]
        location = self._best_location(entity)
        if location:
            parts.append(f"当前归档位置相关信息指向 `{location}`。")
        if entity_claims:
            predicates = [claim.get("predicate", "") for claim in entity_claims if claim.get("predicate")]
            if predicates:
                parts.append(f"已抽取到 {len(entity_claims)} 条声明，主要覆盖：{', '.join(sorted(set(predicates))[:6])}。")
        return " ".join(parts)

    @staticmethod
    def _best_location(entity: Dict[str, Any]) -> str:
        attributes = entity.get("attributes", {})
        location = attributes.get("location")
        if location:
            return str(location)
        for ref in entity.get("source_refs", []):
            if any(token in ref for token in ["佛山", "广州", "南海", "天河", "白云", "顺德", "禅城"]):
                return ref
        return ""

    @staticmethod
    def _slug_for_filename(text: str) -> str:
        slug = "".join(ch.lower() if ch.isascii() and ch.isalnum() else "_" for ch in text).strip("_")
        if slug:
            return slug
        return "u_" + "_".join(format(ord(ch), "x") for ch in text[:12])
