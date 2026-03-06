from __future__ import annotations

from pathlib import Path
from uuid import uuid4

from app.core.config import settings
from app.core.config_loader import load_yaml
from app.models.schemas import JobMatchTile, RunWorkflowResponse, WorkflowDiagnostics
from app.workflow.graph import build_workflow
from app.workflow.io import persist_uploaded_resume
from app.workflow.nodes import WorkflowNodes
from app.workflow.state import WorkflowState


class ResumeJobOrchestrator:
    def __init__(self) -> None:
        workflow_config = load_yaml(settings.workflow_config_path)
        prompts_config = load_yaml(settings.prompts_config_path)

        generated_resume_dir = settings.generated_resume_dir
        generated_resume_dir.mkdir(parents=True, exist_ok=True)
        settings.uploaded_resume_dir.mkdir(parents=True, exist_ok=True)

        self._nodes = WorkflowNodes(workflow_config, prompts_config, generated_resume_dir)
        self._graph = build_workflow(self._nodes, workflow_config)

    def run(
        self,
        resume_filename: str,
        resume_file_bytes: bytes,
        *,
        run_id: str | None = None,
    ) -> RunWorkflowResponse:
        effective_run_id = run_id or str(uuid4())
        uploaded_path = persist_uploaded_resume(
            payload=resume_file_bytes,
            filename=resume_filename,
            upload_dir=settings.uploaded_resume_dir,
        )
        initial_state: WorkflowState = {
            "run_id": effective_run_id,
            "resume_filename": resume_filename,
            "uploaded_resume_path": str(uploaded_path),
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
        return RunWorkflowResponse(run_id=effective_run_id, tiles=tiles, diagnostics=diagnostics)


def generated_resume_path(filename: str) -> Path:
    return settings.generated_resume_dir / filename
