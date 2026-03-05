from io import BytesIO

import pytest
from docx import Document
from httpx import ASGITransport, AsyncClient

from app.main import app


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


def _build_docx_payload() -> bytes:
    buffer = BytesIO()
    doc = Document()
    doc.add_paragraph("Senior Python engineer with AI workflow and API development experience.")
    doc.add_paragraph("Built LLM orchestration tools with verification loops and ATS optimization.")
    doc.save(buffer)
    return buffer.getvalue()


@pytest.mark.anyio
async def test_run_workflow_with_docx() -> None:
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
    first = payload["tiles"][0]
    assert first["company"]
    assert first["generated_resume_link"].startswith("/api/resumes/")
    assert "verification" in payload["diagnostics"]
