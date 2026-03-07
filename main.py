"""Main entry point: workspace-aware multi-agent orchestration."""
from __future__ import annotations

import argparse
import json

from agent_runtime import MultiAgentRuntime
from ingestor import IngestorAgent
from workspace_manager import WorkspaceManager


def build_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run self-growing KB pipeline with workspace isolation.")
    parser.add_argument("--workspace", default="default", help="Workspace id for isolated KB/version/report state.")
    parser.add_argument("--input", default="sample_data/raw_inputs.json", help="Path to raw ingestion JSON.")
    parser.add_argument("--workflow", default="workflow/default_workflow.json", help="Editable task-routing workflow file.")
    return parser.parse_args()


def run_pipeline(workspace: str = "default", sample_path: str = "sample_data/raw_inputs.json", workflow: str = "workflow/default_workflow.json") -> dict:
    workspace_ctx = WorkspaceManager().resolve(workspace)
    raw_items = IngestorAgent().load_json(sample_path)
    result = MultiAgentRuntime(workspace_ctx, workflow_path=workflow).run(raw_items)
    return result


if __name__ == "__main__":
    args = build_args()
    output = run_pipeline(workspace=args.workspace, sample_path=args.input, workflow=args.workflow)
    print("Pipeline completed.")
    print(json.dumps(output, indent=2, ensure_ascii=False))
