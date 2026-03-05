from __future__ import annotations

from pathlib import Path
from typing import Any
from urllib.parse import urlparse

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
        if not _is_valid_http_url(str(item.get("job_url", item.get("job_link", "")))):
            return (False, f"Job {index} has invalid URL.")
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
) -> tuple[bool, str]:
    if not generated_links:
        return (False, "No generated resume links.")
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
            Document(str(path))
        except Exception:
            return (False, f"Generated resume is not a valid DOCX: {filename}")
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
    return parsed.scheme in {"http", "https"} and bool(parsed.netloc)
