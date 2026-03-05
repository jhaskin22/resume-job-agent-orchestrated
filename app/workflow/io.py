import re
from io import BytesIO
from pathlib import Path

from docx import Document
from pypdf import PdfReader


class ResumeParsingError(RuntimeError):
    pass


def parse_resume_content(filename: str, payload: bytes) -> str:
    extension = Path(filename).suffix.lower()

    if extension == ".pdf":
        return _parse_pdf(payload)
    if extension == ".docx":
        return _parse_docx(payload)

    raise ResumeParsingError("Unsupported resume format. Accepted types: PDF, DOCX")


def _parse_pdf(payload: bytes) -> str:
    reader = PdfReader(BytesIO(payload))
    pages = [page.extract_text() or "" for page in reader.pages]
    return "\n".join(part.strip() for part in pages if part.strip())


def _parse_docx(payload: bytes) -> str:
    doc = Document(BytesIO(payload))
    paragraphs = [paragraph.text.strip() for paragraph in doc.paragraphs if paragraph.text.strip()]
    return "\n".join(paragraphs)


def safe_stem(raw_filename: str) -> str:
    base = Path(raw_filename).stem.lower()
    base = re.sub(r"[^a-z0-9]+", "-", base).strip("-")
    return base or "resume"


def write_resume_docx(content: str, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    doc = Document()
    for block in content.split("\n"):
        clean = block.strip()
        if clean:
            doc.add_paragraph(clean)
    doc.save(str(output_path))
