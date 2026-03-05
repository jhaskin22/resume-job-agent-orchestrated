from __future__ import annotations

from pathlib import Path

from app.core.config import settings
from app.core.config_loader import load_yaml
from app.models.api import RunWorkflowResponse, WorkflowDiagnostics
from app.models.tile import JobMatchTile
from app.workflow.graph import build_workflow
from app.workflow.nodes import WorkflowNodes
from app.workflow.state import WorkflowState


class ResumeJobOrchestrator:
    def __init__(self) -> None:
        workflow_config = load_yaml(settings.workflow_config_path)
        prompts_config = load_yaml(settings.prompts_config_path)

        generated_resume_dir = settings.generated_resume_dir
        generated_resume_dir.mkdir(parents=True, exist_ok=True)

        self._nodes = WorkflowNodes(workflow_config, prompts_config, generated_resume_dir)
        self._graph = build_workflow(self._nodes, workflow_config)

    def run(self, resume_filename: str, resume_file_bytes: bytes) -> RunWorkflowResponse:
        initial_state: WorkflowState = {
            "resume_filename": resume_filename,
            "resume_file_bytes": resume_file_bytes,
            "verification": {},
            "repair_counts": {},
            "errors": [],
            "failed": False,
        }
        final_state = self._graph.invoke(initial_state)

        raw_tiles = final_state.get("tiles", [])
        tiles = [JobMatchTile(**tile) for tile in raw_tiles]

        diagnostics = WorkflowDiagnostics(
            failed=bool(final_state.get("failed", False)),
            verification=dict(final_state.get("verification", {})),
            errors=list(final_state.get("errors", [])),
        )
        return RunWorkflowResponse(tiles=tiles, diagnostics=diagnostics)


def generated_resume_path(filename: str) -> Path:
    return settings.generated_resume_dir / filename
