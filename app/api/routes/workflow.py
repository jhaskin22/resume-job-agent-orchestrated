from pathlib import Path
from typing import Annotated

from fastapi import APIRouter, File, HTTPException, UploadFile
from fastapi.responses import FileResponse

from app.models.schemas import (
    GenerateResumeRequest,
    GenerateResumeResponse,
    RunWorkflowResponse,
    StartWorkflowRunResponse,
    WorkflowRunStatusResponse,
)
from app.services.orchestrator import ResumeJobOrchestrator, generated_resume_path
from app.services.run_manager import WorkflowRunManager

router = APIRouter()
orchestrator = ResumeJobOrchestrator()
run_manager = WorkflowRunManager(orchestrator)


@router.post("/workflow/run", response_model=RunWorkflowResponse)
async def run_workflow(resume: Annotated[UploadFile, File(...)]) -> RunWorkflowResponse:
    extension = Path(resume.filename or "").suffix.lower()
    if extension not in {".pdf", ".docx"}:
        raise HTTPException(status_code=400, detail="Only PDF or DOCX resumes are supported.")

    payload = await resume.read()
    if not payload:
        raise HTTPException(status_code=400, detail="Uploaded resume is empty.")

    return orchestrator.run(resume.filename or "resume.docx", payload)


@router.post("/workflow/runs", response_model=StartWorkflowRunResponse)
async def start_workflow_run(resume: Annotated[UploadFile, File(...)]) -> StartWorkflowRunResponse:
    extension = Path(resume.filename or "").suffix.lower()
    if extension not in {".pdf", ".docx"}:
        raise HTTPException(status_code=400, detail="Only PDF or DOCX resumes are supported.")

    payload = await resume.read()
    if not payload:
        raise HTTPException(status_code=400, detail="Uploaded resume is empty.")

    run_id = run_manager.start_run(resume.filename or "resume.docx", payload)
    return StartWorkflowRunResponse(run_id=run_id, status="queued")


@router.get("/workflow/runs/{run_id}", response_model=WorkflowRunStatusResponse)
def get_workflow_run(run_id: int) -> WorkflowRunStatusResponse:
    status = run_manager.get_status(run_id)
    if status is None:
        raise HTTPException(status_code=404, detail="Run not found.")
    return status


@router.post("/workflow/runs/{run_id}/resume", response_model=GenerateResumeResponse)
def generate_resume_for_job(run_id: int, request: GenerateResumeRequest) -> GenerateResumeResponse:
    status = run_manager.get_status(run_id)
    if status is None:
        raise HTTPException(status_code=404, detail="Run not found.")
    try:
        generated_link = run_manager.generate_resume_for_job(run_id, str(request.job_link))
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"Resume generation failed: {exc}") from exc
    return GenerateResumeResponse(
        run_id=run_id,
        job_link=request.job_link,
        generated_resume_link=generated_link,
    )


@router.get("/resumes/{filename}")
def download_generated_resume(filename: str) -> FileResponse:
    path = generated_resume_path(filename)
    if not path.exists() or not path.is_file():
        raise HTTPException(status_code=404, detail="Generated resume not found.")

    return FileResponse(path=path, filename=filename)
