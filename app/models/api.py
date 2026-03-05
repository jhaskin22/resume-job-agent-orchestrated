from pydantic import BaseModel

from app.models.tile import JobMatchTile


class WorkflowDiagnostics(BaseModel):
    failed: bool
    verification: dict[str, dict[str, object]]
    errors: list[str]


class RunWorkflowResponse(BaseModel):
    tiles: list[JobMatchTile]
    diagnostics: WorkflowDiagnostics
