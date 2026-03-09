"""Workspace management for isolating KB state, versions, reports, and governance artifacts."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict


@dataclass
class WorkspaceContext:
    """Resolved workspace paths used by all agents in a pipeline run."""

    workspace_id: str
    root_dir: Path
    raw_dir: Path
    extracted_dir: Path
    canonical_dir: Path
    snapshot_dir: Path
    report_dir: Path
    visualization_dir: Path
    governance_dir: Path
    config_dir: Path
    wiki_dir: Path
    kb_path: Path
    report_path: Path


class WorkspaceManager:
    """Creates and resolves per-workspace directories."""

    def __init__(self, base_dir: str = "output/workspaces") -> None:
        self.base_dir = Path(base_dir)
        self.base_dir.mkdir(parents=True, exist_ok=True)

    def resolve(self, workspace_id: str) -> WorkspaceContext:
        safe_id = self._sanitize_workspace_id(workspace_id)
        root = self.base_dir / safe_id
        raw_dir = root / "raw"
        extracted_dir = root / "extracted"
        canonical_dir = root / "canonical"
        snapshots = root / "snapshots"
        reports = root / "reports"
        visuals = root / "visuals"
        governance = root / "governance"
        config = root / "config"
        wiki = root / "wiki"

        for path in [root, raw_dir, extracted_dir, canonical_dir, snapshots, reports, visuals, governance, config, wiki]:
            path.mkdir(parents=True, exist_ok=True)

        return WorkspaceContext(
            workspace_id=safe_id,
            root_dir=root,
            raw_dir=raw_dir,
            extracted_dir=extracted_dir,
            canonical_dir=canonical_dir,
            snapshot_dir=snapshots,
            report_dir=reports,
            visualization_dir=visuals,
            governance_dir=governance,
            config_dir=config,
            wiki_dir=wiki,
            kb_path=canonical_dir / "knowledge_base.json",
            report_path=reports / "report.md",
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
