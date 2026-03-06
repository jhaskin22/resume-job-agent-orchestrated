from __future__ import annotations

import re
from statistics import mean
from typing import Any

from app.tools.resume import tokenize


def evaluate_ats_score(
    job: dict[str, Any],
    resume_text: str,
    parsed_job: dict[str, Any] | None = None,
) -> tuple[float, dict[str, float]]:
    parsed_job = parsed_job or {}
    job_tokens = tokenize(f"{job.get('title', '')} {job.get('description', '')}")
    resume_tokens = tokenize(resume_text)

    if job_tokens:
        keyword_alignment = len(job_tokens.intersection(resume_tokens)) / len(job_tokens)
    else:
        keyword_alignment = 0.0

    required_tech = {str(item).lower() for item in parsed_job.get("technologies", [])}
    if required_tech:
        tech_coverage = len(required_tech.intersection(resume_tokens)) / len(required_tech)
    else:
        tech_coverage = 0.5

    required_keywords = {str(item).lower() for item in parsed_job.get("ats_keywords", [])}
    if required_keywords:
        keyword_coverage = len(required_keywords.intersection(resume_tokens)) / len(
            required_keywords
        )
    else:
        keyword_coverage = keyword_alignment

    readability = _readability_score(resume_text)
    clarity = _clarity_score(resume_text)
    structure = _section_structure_score(resume_text)

    total = (
        (keyword_alignment * 30)
        + (keyword_coverage * 20)
        + (tech_coverage * 15)
        + (readability * 15)
        + (clarity * 10)
        + (structure * 10)
    )
    score = round(max(0.0, min(100.0, total)), 2)
    factors = {
        "keyword_alignment": round(keyword_alignment * 100, 2),
        "keyword_coverage": round(keyword_coverage * 100, 2),
        "tech_coverage": round(tech_coverage * 100, 2),
        "readability": round(readability * 100, 2),
        "clarity": round(clarity * 100, 2),
        "structure": round(structure * 100, 2),
    }
    return (score, factors)


def _readability_score(text: str) -> float:
    words = re.findall(r"[A-Za-z0-9+#.-]+", text)
    sentences = re.split(r"[.!?]\s+", text)
    clean_sentences = [segment for segment in sentences if segment.strip()]

    if not words or not clean_sentences:
        return 0.2

    avg_sentence_length = len(words) / max(1, len(clean_sentences))

    if 8 <= avg_sentence_length <= 22:
        return 1.0
    if 5 <= avg_sentence_length <= 30:
        return 0.75
    if 3 <= avg_sentence_length <= 40:
        return 0.5
    return 0.25


def _section_structure_score(text: str) -> float:
    lowered = text.lower()
    section_hits = [
        "summary" in lowered,
        "skills" in lowered or "technologies" in lowered,
        "experience" in lowered,
    ]
    return mean(1.0 if item else 0.0 for item in section_hits)


def _clarity_score(text: str) -> float:
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    bullet_like = [line for line in lines if line.startswith(("-", "•", "*"))]
    if not lines:
        return 0.0
    if not bullet_like:
        return 0.5

    concise = 0
    for line in bullet_like:
        words = len(re.findall(r"[A-Za-z0-9+#.-]+", line))
        if 8 <= words <= 32:
            concise += 1
    return concise / max(1, len(bullet_like))
