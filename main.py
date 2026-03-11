"""Compatibility entrypoint that forwards to the current runtime."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict

from mindvault.runtime.app import VaultRuntime, load_sources_from_path, main


def run_pipeline(workspace: str = "default", sample_path: str = "sample_data/raw_inputs.json", workflow: str = "") -> Dict[str, Any]:
    """Compatibility wrapper for legacy callers and tests."""
    sources = load_sources_from_path(Path(sample_path))
    runtime = VaultRuntime(workspace)
    result = runtime.ingest(sources, profile="fast")

    workspace_root = Path("output/workspaces") / workspace
    extracted_dir = workspace_root / "extracted"
    extracted_files = sorted(extracted_dir.glob("extracted_v*.json"))
    if extracted_files:
        latest = json.loads(extracted_files[-1].read_text(encoding="utf-8"))
        claims_path = extracted_dir / "claims_v1.json"
        claims_path.write_text(
            json.dumps(latest.get("claims", []), indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
    return result


if __name__ == "__main__":
    main()
