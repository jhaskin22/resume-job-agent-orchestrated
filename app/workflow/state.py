from typing import Any, TypedDict


class WorkflowState(TypedDict, total=False):
    run_id: int
    resume_filename: str
    uploaded_resume_path: str
    resume_file_bytes: bytes
    resume_text: str
    parsed_resume: dict[str, Any]
    discovered_jobs: list[dict[str, Any]]
    parsed_jobs: list[dict[str, Any]]
    scored_jobs: list[dict[str, Any]]
    generated_resumes: dict[str, str]
    resume_generation_meta: dict[str, dict[str, Any]]
    tiles: list[dict[str, Any]]
    verification: dict[str, dict[str, Any]]
    repair_counts: dict[str, int]
    errors: list[str]
    failed: bool
