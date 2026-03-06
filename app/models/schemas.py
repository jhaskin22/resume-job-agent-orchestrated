from pydantic import BaseModel, Field, HttpUrl


class JobMatchTile(BaseModel):
    run_id: str
    company: str
    title: str
    location: str
    salary: str | None = None
    work_type: str
    match_score: float = Field(ge=0, le=100)
    resume_alignment: float = Field(ge=0, le=100)
    ats_score: float = Field(ge=0, le=100)
    job_link: HttpUrl
    generated_resume_link: str
    summary: str


class WorkflowDiagnostics(BaseModel):
    failed: bool
    verification: dict[str, dict[str, object]]
    errors: list[str]


class RunWorkflowResponse(BaseModel):
    run_id: str
    tiles: list[JobMatchTile]
    diagnostics: WorkflowDiagnostics


class StartWorkflowRunResponse(BaseModel):
    run_id: str
    status: str


class WorkflowRunStatusResponse(BaseModel):
    run_id: str
    status: str
    progress_current: int = 0
    progress_total: int = 0
    progress_company: str = ""
    tiles: list[JobMatchTile]
    diagnostics: WorkflowDiagnostics | None = None
