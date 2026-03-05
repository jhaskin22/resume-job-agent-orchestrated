from typing import Any, TypedDict


class WorkflowState(TypedDict, total=False):
    resume_filename: str
    resume_file_bytes: bytes
    resume_text: str
    discovered_jobs: list[dict[str, Any]]
    scored_jobs: list[dict[str, Any]]
    generated_resumes: dict[str, str]
    tiles: list[dict[str, Any]]
    verification: dict[str, dict[str, Any]]
    repair_counts: dict[str, int]
    errors: list[str]
    failed: bool
