from io import BytesIO
from pathlib import Path

import pytest
from docx import Document
from httpx import ASGITransport, AsyncClient

from app.core.config import settings
from app.main import app


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


def _build_docx_payload() -> bytes:
    buffer = BytesIO()
    doc = Document()
    doc.add_heading("Summary", level=1)
    doc.add_paragraph("Senior Python engineer with AI workflow and API development experience.")
    doc.add_heading("Experience", level=1)
    doc.add_paragraph(
        "• Design and operate high-reliability, low-latency software systems for telemetry."
    )
    doc.add_paragraph(
        "• Build API services and automation tooling to improve testing and deployment confidence."
    )
    doc.add_heading("Skills", level=1)
    doc.add_paragraph("Python, FastAPI, Docker, Kubernetes, AWS, LangGraph")
    doc.save(buffer)
    return buffer.getvalue()


@pytest.mark.anyio
async def test_run_workflow_with_docx() -> None:
    upload_dir = Path(settings.uploaded_resume_dir)
    before_uploads = {path.name for path in upload_dir.glob("*")} if upload_dir.exists() else set()

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/api/workflow/run",
            files={
                "resume": (
                    "candidate.docx",
                    _build_docx_payload(),
                    "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                )
            },
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["tiles"]
    assert payload["diagnostics"]["verification"]["job_parsing"]["ok"] is True
    assert payload["diagnostics"]["verification"]["ats_evaluation"]["ok"] is True
    first = payload["tiles"][0]
    assert first["company"]
    assert first["generated_resume_link"].startswith("/api/resumes/")
    assert "verification" in payload["diagnostics"]

    after_uploads = {path.name for path in upload_dir.glob("*")} if upload_dir.exists() else set()
    assert len(after_uploads - before_uploads) >= 1


@pytest.mark.anyio
async def test_generated_resume_download() -> None:
    original_payload = _build_docx_payload()
    original_doc = Document(BytesIO(original_payload))
    original_paragraph_count = len(original_doc.paragraphs)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        run_response = await client.post(
            "/api/workflow/run",
            files={
                "resume": (
                    "candidate.docx",
                    original_payload,
                    "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                )
            },
        )
        assert run_response.status_code == 200
        payload = run_response.json()
        assert len(payload["tiles"]) >= 2
        link = payload["tiles"][0]["generated_resume_link"]
        second_link = payload["tiles"][1]["generated_resume_link"]

        download_response = await client.get(link)
        second_download_response = await client.get(second_link)

    assert download_response.status_code == 200
    assert download_response.headers["content-type"].startswith(
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
    )
    assert len(download_response.content) > 100

    generated_doc = Document(BytesIO(download_response.content))
    second_doc = Document(BytesIO(second_download_response.content))
    assert len(generated_doc.paragraphs) == original_paragraph_count
    assert len(second_doc.paragraphs) == original_paragraph_count
    first_text = "\n".join(paragraph.text for paragraph in generated_doc.paragraphs)
    second_text = "\n".join(paragraph.text for paragraph in second_doc.paragraphs)
    assert first_text != second_text
