"""Workspace state management with layered directory isolation."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict


@dataclass
class WorkspaceContext:
    """Resolved workspace paths used by all layers in a pipeline run."""

    workspace_id: str
    root_dir: Path
    # ── Layered storage ──
    raw_dir: Path           # Raw Layer: unmodified source material
    extracted_dir: Path     # Extracted Layer: AI intermediate results
    canonical_dir: Path     # Canonical Layer: formal accepted knowledge
    # ── Operational dirs ──
    snapshot_dir: Path
    report_dir: Path
    visualization_dir: Path
    governance_dir: Path
    config_dir: Path
    # ── Convenience paths ──
    kb_path: Path
    report_path: Path


class WorkspaceStore:
    """Creates, resolves, and lists per-workspace directories."""

    def __init__(self, base_dir: str = "output/workspaces") -> None:
        self.base_dir = Path(base_dir)
        self.base_dir.mkdir(parents=True, exist_ok=True)

    def resolve(self, workspace_id: str) -> WorkspaceContext:
        safe_id = self._sanitize(workspace_id)
        root = self.base_dir / safe_id

        dirs = {
            "raw": root / "raw",
            "extracted": root / "extracted",
            "canonical": root / "canonical",
            "snapshots": root / "snapshots",
            "reports": root / "reports",
            "visuals": root / "visuals",
            "governance": root / "governance",
            "config": root / "config",
        }
        for d in [root, *dirs.values()]:
            d.mkdir(parents=True, exist_ok=True)

        return WorkspaceContext(
            workspace_id=safe_id,
            root_dir=root,
            raw_dir=dirs["raw"],
            extracted_dir=dirs["extracted"],
            canonical_dir=dirs["canonical"],
            snapshot_dir=dirs["snapshots"],
            report_dir=dirs["reports"],
            visualization_dir=dirs["visuals"],
            governance_dir=dirs["governance"],
            config_dir=dirs["config"],
            kb_path=dirs["canonical"] / "knowledge_base.json",
            report_path=dirs["reports"] / "report.md",
        )

    def list_workspaces(self) -> Dict[str, str]:
        return {p.name: str(p) for p in sorted(self.base_dir.glob("*")) if p.is_dir()}

    @staticmethod
    def _sanitize(workspace_id: str) -> str:
        candidate = "".join(ch if ch.isalnum() or ch in ("-", "_") else "_" for ch in workspace_id.strip())
        return candidate or "default"
