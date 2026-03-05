from __future__ import annotations

import re
from statistics import mean
from typing import Any

from app.tools.resume import tokenize


def evaluate_ats_score(job: dict[str, Any], resume_text: str) -> float:
    job_tokens = tokenize(f"{job.get('title', '')} {job.get('description', '')}")
    resume_tokens = tokenize(resume_text)

    if job_tokens:
        keyword_alignment = len(job_tokens.intersection(resume_tokens)) / len(job_tokens)
    else:
        keyword_alignment = 0.0

    readability = _readability_score(resume_text)
    structure = _section_structure_score(resume_text)

    total = (keyword_alignment * 55) + (readability * 25) + (structure * 20)
    return round(max(0.0, min(100.0, total)), 2)


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

