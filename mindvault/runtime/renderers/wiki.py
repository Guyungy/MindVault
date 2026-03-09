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
        for path in [self.out_dir, self.entity_dir, self.type_dir]:
            path.mkdir(parents=True, exist_ok=True)

    def export(
        self,
        state: Dict[str, Any],
        governance: Dict[str, Any],
        version_meta: Dict[str, Any],
    ) -> Dict[str, Any]:
        tables = self._build_tables(state, governance, version_meta)
        tables_path = self.out_dir / "tables.json"
        tables_path.write_text(json.dumps(tables, indent=2, ensure_ascii=False), encoding="utf-8")

        entity_paths = self._write_entity_pages(state)
        type_paths = self._write_type_pages(state)
        governance_path = self._write_governance_page(governance)
        claims_path = self._write_claims_page(state.get("claims", []))
        relations_path = self._write_relations_page(state.get("relations", []))
        index_path = self._write_index(state, governance, version_meta, entity_paths, type_paths)

        return {
            "index": str(index_path),
            "tables": str(tables_path),
            "governance": str(governance_path),
            "claims": str(claims_path),
            "relations": str(relations_path),
            "entity_pages": entity_paths,
            "type_pages": type_paths,
        }

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
            "## Type Pages",
            "",
        ]

        if type_paths:
            for entity_type, path in sorted(type_paths.items()):
                lines.append(f"- [{entity_type}](by_type/{Path(path).name})")
        else:
            lines.append("- No type pages generated.")

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

            lines = [
                f"# {entity.get('name', entity_id)}",
                "",
                f"- ID: {entity_id}",
                f"- Type: {entity.get('type', 'unknown')}",
                f"- Confidence: {entity.get('confidence', 0)}",
                f"- Updated: {entity.get('updated_at', '')}",
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

            lines.extend(["", "## Claims", ""])
            entity_claims = claims_by_subject.get(entity_id, [])
            if entity_claims:
                lines.extend([
                    "| Predicate | Object | Type | Confidence |",
                    "| --- | --- | --- | ---: |",
                ])
                for claim in entity_claims:
                    lines.append(
                        f"| {claim.get('predicate', '')} | {self._stringify(claim.get('object'))} | "
                        f"{claim.get('claim_type', '')} | {claim.get('confidence', 0)} |"
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
