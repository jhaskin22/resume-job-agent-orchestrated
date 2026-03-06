from __future__ import annotations

import re
from typing import Any

from app.tools.resume import tokenize


def score_jobs(
    parsed_resume: dict[str, Any],
    discovered_jobs: list[dict[str, Any]],
    parsed_jobs: dict[str, dict[str, Any]],
    top_k: int,
) -> list[dict[str, Any]]:
    resume_tokens = tokenize(str(parsed_resume.get("raw_text", "")))
    resume_skills = {str(item).lower() for item in parsed_resume.get("skills", [])}

    scored: list[dict[str, Any]] = []
    for job in discovered_jobs:
        parsed_job = parsed_jobs.get(str(job.get("job_link", "")), {})
        title = str(job.get("title", ""))
        description = str(job.get("description", ""))
        job_tokens = tokenize(f"{title} {description}")

        overlap_count = len(resume_tokens.intersection(job_tokens))
        overlap_ratio = overlap_count / max(len(job_tokens), 1)

        parsed_required = [str(item).lower() for item in parsed_job.get("required_skills", [])]
        matched_skills = sorted(
            skill
            for skill in resume_skills
            if skill in description.lower() or skill in parsed_required
        )
        skill_ratio = len(matched_skills) / max(len(resume_skills), 1)
        technology_ratio = len(parsed_job.get("technologies", [])) / 10

        seniority_bonus = _seniority_bonus(title)

        match_score = _clamp(
            25 + overlap_ratio * 45 + skill_ratio * 22 + technology_ratio * 8 + seniority_bonus
        )
        alignment_score = _clamp(20 + overlap_ratio * 40 + skill_ratio * 30 + technology_ratio * 10)

        scored.append(
            {
                **job,
                "required_skills": matched_skills,
                "technologies": parsed_job.get("technologies", []),
                "experience_level": parsed_job.get("experience_level", "mid"),
                "ats_keywords": parsed_job.get("ats_keywords", []),
                "match_score": round(match_score, 2),
                "resume_alignment": round(alignment_score, 2),
            }
        )

    scored.sort(key=lambda item: float(item["match_score"]), reverse=True)
    if top_k <= 0:
        return scored
    return scored[:top_k]


def _seniority_bonus(title: str) -> float:
    lowered = title.lower()
    if re.search(r"\b(senior|staff|lead|principal)\b", lowered):
        return 5.0
    if re.search(r"\b(junior|entry)\b", lowered):
        return -5.0
    return 0.0


def _clamp(value: float) -> float:
    return max(0.0, min(100.0, value))
