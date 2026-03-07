"""Workspace management for isolating KB state, versions, reports, and visual artifacts."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict


@dataclass
class WorkspaceContext:
    """Resolved workspace paths used by all agents in a pipeline run."""

    workspace_id: str
    root_dir: Path
    kb_path: Path
    snapshot_dir: Path
    report_path: Path
    visualization_dir: Path


class WorkspaceManager:
    """Creates and resolves per-workspace directories."""

    def __init__(self, base_dir: str = "output/workspaces") -> None:
        self.base_dir = Path(base_dir)
        self.base_dir.mkdir(parents=True, exist_ok=True)

    def resolve(self, workspace_id: str) -> WorkspaceContext:
        safe_id = self._sanitize_workspace_id(workspace_id)
        root = self.base_dir / safe_id
        snapshots = root / "snapshots"
        visuals = root / "visuals"

        root.mkdir(parents=True, exist_ok=True)
        snapshots.mkdir(parents=True, exist_ok=True)
        visuals.mkdir(parents=True, exist_ok=True)

        return WorkspaceContext(
            workspace_id=safe_id,
            root_dir=root,
            kb_path=root / "knowledge_base.json",
            snapshot_dir=snapshots,
            report_path=root / "report.md",
            visualization_dir=visuals,
        )

    def list_workspaces(self) -> Dict[str, str]:
        out: Dict[str, str] = {}
        for p in sorted(self.base_dir.glob("*")):
            if p.is_dir():
                out[p.name] = str(p)
        return out

    @staticmethod
    def _sanitize_workspace_id(workspace_id: str) -> str:
        candidate = "".join(ch if ch.isalnum() or ch in ("-", "_") else "_" for ch in workspace_id.strip())
        return candidate or "default"
