from __future__ import annotations

import os
from pathlib import Path
from typing import Any
from urllib.parse import urlparse
from urllib.request import Request, urlopen

from docx import Document

from app.models.tile import JobMatchTile


def verify_parsed_resume(parsed_resume: dict[str, Any], min_items: int) -> tuple[bool, str]:
    required = ("skills", "experience", "technologies", "roles")
    for key in required:
        value = parsed_resume.get(key)
        if not isinstance(value, list):
            return (False, f"Parsed resume missing list field: {key}")
    non_empty_count = sum(1 for key in required if parsed_resume.get(key))
    if non_empty_count < min_items:
        return (
            False,
            f"Parsed resume has too few populated fields: {non_empty_count} < {min_items}",
        )
    return (True, "")


def verify_discovered_jobs(
    jobs: list[dict[str, Any]],
    min_count: int,
    required_keys: set[str],
) -> tuple[bool, str]:
    if len(jobs) < min_count:
        return (False, f"Not enough jobs discovered: {len(jobs)} < {min_count}")
    for index, item in enumerate(jobs):
        if not required_keys.issubset(item):
            return (False, f"Job {index} missing required keys.")
        if not str(item.get("title", "")).strip():
            return (False, f"Job {index} missing title.")
        url = str(item.get("job_url", item.get("job_link", "")))
        if not _is_valid_http_url(url):
            return (False, f"Job {index} has invalid URL.")
        if not _looks_like_posting_url(url):
            return (False, f"Job {index} URL does not look like a posting.")
        if len(str(item.get("description", "")).split()) < 20:
            return (False, f"Job {index} description too short.")
        if not os.getenv("PYTEST_CURRENT_TEST"):
            reachable, page_text = _fetch_page_text(url)
            if not reachable:
                return (False, f"Job {index} URL is not reachable.")
            if not _looks_like_job_page_text(page_text):
                return (False, f"Job {index} page does not look like a job posting.")
    return (True, "")


def verify_job_parsing(
    discovered_jobs: list[dict[str, Any]],
    parsed_jobs: list[dict[str, Any]],
) -> tuple[bool, str]:
    if len(parsed_jobs) < len(discovered_jobs):
        return (False, "Parsed job count does not match discovered jobs.")
    by_link = {str(item.get("job_link", "")): item for item in parsed_jobs}
    for index, job in enumerate(discovered_jobs):
        link = str(job.get("job_link", ""))
        parsed = by_link.get(link)
        if not parsed:
            return (False, f"Missing parsed job for index {index}.")
        for field in ("required_skills", "technologies", "ats_keywords"):
            if not isinstance(parsed.get(field), list):
                return (False, f"Parsed job {index} missing list field {field}.")
        if not isinstance(parsed.get("experience_level"), str):
            return (False, f"Parsed job {index} missing experience_level.")
    return (True, "")


def verify_scored_jobs(jobs: list[dict[str, Any]]) -> tuple[bool, str]:
    if not jobs:
        return (False, "No scored jobs.")
    for index, item in enumerate(jobs):
        if "match_score" not in item or "resume_alignment" not in item:
            return (False, f"Job {index} missing score fields.")
        for field in ("match_score", "resume_alignment"):
            value = float(item[field])
            if value < 0 or value > 100:
                return (False, f"Job {index} has out-of-range {field}.")
    return (True, "")


def verify_generated_resumes(
    generated_links: dict[str, str],
    scored_jobs: list[dict[str, Any]],
    generated_resume_dir: Path,
    min_size_bytes: int,
    generation_meta: dict[str, dict[str, Any]] | None = None,
) -> tuple[bool, str]:
    if not generated_links:
        return (False, "No generated resume links.")
    generation_meta = generation_meta or {}
    rendered_texts: list[str] = []
    for job in scored_jobs:
        job_link = str(job.get("job_link", ""))
        output_link = generated_links.get(job_link, "")
        if not output_link:
            return (False, "Generated resume missing for one or more jobs.")
        filename = output_link.rsplit("/", maxsplit=1)[-1]
        path = generated_resume_dir / filename
        if not path.exists() or path.stat().st_size < min_size_bytes:
            return (False, f"Generated resume missing or too small: {filename}")
        try:
            doc = Document(str(path))
        except Exception:
            return (False, f"Generated resume is not a valid DOCX: {filename}")
        text = " ".join(
            paragraph.text.strip() for paragraph in doc.paragraphs if paragraph.text.strip()
        )
        if "aligned with " in text.lower():
            return (False, f"Unnatural keyword injection detected in {filename}")
        rendered_texts.append(text)
        meta = generation_meta.get(job_link, {})
        if meta:
            if int(meta.get("original_paragraphs", 0)) != int(meta.get("output_paragraphs", -1)):
                return (False, f"Resume structure mismatch for {filename}")
            if int(meta.get("modified_bullets", 0)) <= 0:
                return (False, f"No bullet modifications detected for {filename}")
    if len(rendered_texts) >= 2 and len(set(rendered_texts)) < 2:
        return (False, "Generated resumes are not distinct across jobs.")
    return (True, "")


def verify_ats_scores(jobs: list[dict[str, Any]]) -> tuple[bool, str]:
    if not jobs:
        return (False, "No jobs for ATS verification.")
    for index, job in enumerate(jobs):
        ats = job.get("ats_score")
        if ats is None:
            return (False, f"Job {index} missing ats_score.")
        value = float(ats)
        if value < 0 or value > 100:
            return (False, f"Job {index} ats_score out of range.")
    return (True, "")


def verify_tiles(tiles: list[dict[str, Any]], min_summary_chars: int) -> tuple[bool, str]:
    if not tiles:
        return (False, "No job tiles built.")
    for tile in tiles:
        if len(str(tile.get("summary", ""))) < min_summary_chars:
            return (False, "Tile summary below minimum threshold.")
        JobMatchTile(**tile)
    return (True, "")


def _is_valid_http_url(url: str) -> bool:
    parsed = urlparse(url)
    blocked_hosts = (
        "example.com",
        "localhost",
        "127.0.0.1",
        "test",
        ".test",
        "greenhouse",
        "lever",
        "ashby",
        "linkedin",
        "indeed",
        "wellfound",
    )
    host = (parsed.netloc or "").lower()
    if any(blocked in host for blocked in blocked_hosts):
        return False
    return parsed.scheme in {"http", "https"} and bool(parsed.netloc)


def _looks_like_posting_url(url: str) -> bool:
    parsed = urlparse(url)
    lowered = url.lower()
    if any(token in lowered for token in ("about", "team", "culture", "benefits", "company")):
        return False
    path = parsed.path.lower().strip("/")
    segments = [part for part in path.split("/") if part]
    if segments in (["jobs"], ["job"], ["positions"], ["careers", "jobs"]):
        return False
    if parsed.path.lower().rstrip("/").endswith("/careers/jobs"):
        return False
    return any(
        token in lowered for token in ("/jobs/", "/job/", "/positions/", "/opening", "/careers/")
    )


def _fetch_page_text(url: str) -> tuple[bool, str]:
    try:
        request = Request(url, headers={"User-Agent": "resume-agent/1.0"})
        with urlopen(request, timeout=1.0) as response:
            status = getattr(response, "status", 200)
            if status == 404:
                return (False, "")
            payload = response.read().decode("utf-8", errors="ignore")
            return (status < 400, payload.lower())
    except Exception:
        return (False, "")


def _looks_like_job_page_text(page_text: str) -> bool:
    if not page_text:
        return False
    if any(phrase in page_text for phrase in ("our culture", "about us", "employee benefits")):
        return False
    hints = ("responsibilities", "requirements", "qualifications", "job description", "apply")
    return any(hint in page_text for hint in hints)
