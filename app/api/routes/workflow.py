from pathlib import Path
from typing import Annotated

from fastapi import APIRouter, File, HTTPException, UploadFile
from fastapi.responses import FileResponse

from app.models.api import RunWorkflowResponse
from app.services.orchestrator import ResumeJobOrchestrator, generated_resume_path

router = APIRouter()
orchestrator = ResumeJobOrchestrator()


@router.post("/workflow/run", response_model=RunWorkflowResponse)
async def run_workflow(resume: Annotated[UploadFile, File(...)]) -> RunWorkflowResponse:
    extension = Path(resume.filename or "").suffix.lower()
    if extension not in {".pdf", ".docx"}:
        raise HTTPException(status_code=400, detail="Only PDF or DOCX resumes are supported.")

    payload = await resume.read()
    if not payload:
        raise HTTPException(status_code=400, detail="Uploaded resume is empty.")

    return orchestrator.run(resume.filename or "resume.docx", payload)


@router.get("/resumes/{filename}")
def download_generated_resume(filename: str) -> FileResponse:
    path = generated_resume_path(filename)
    if not path.exists() or not path.is_file():
        raise HTTPException(status_code=404, detail="Generated resume not found.")

    return FileResponse(path=path, filename=filename)
