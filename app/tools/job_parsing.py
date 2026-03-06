from __future__ import annotations

import re
from typing import Any

from app.tools.resume import tokenize

COMMON_SKILLS = {
    "python",
    "java",
    "javascript",
    "typescript",
    "sql",
    "aws",
    "gcp",
    "azure",
    "docker",
    "kubernetes",
    "fastapi",
    "django",
    "flask",
    "react",
    "node",
    "postgres",
    "redis",
    "kafka",
    "spark",
    "langgraph",
    "langchain",
    "llm",
    "pytorch",
    "terraform",
}


def parse_job_details(job: dict[str, Any]) -> dict[str, Any]:
    title = str(job.get("title", ""))
    description = str(job.get("description", ""))
    text = f"{title}\n{description}"
    tokens = tokenize(text)

    required_skills = sorted(skill for skill in COMMON_SKILLS if skill in tokens)
    technologies = sorted(
        tech
        for tech in required_skills
        if tech
        in {
            "aws",
            "gcp",
            "azure",
            "docker",
            "kubernetes",
            "terraform",
            "langgraph",
            "langchain",
            "llm",
            "pytorch",
        }
    )
    experience_level = _experience_level(text)
    ats_keywords = _ats_keywords(title, description, required_skills, technologies)

    return {
        "job_link": str(job.get("job_link", "")),
        "required_skills": required_skills,
        "technologies": technologies,
        "experience_level": experience_level,
        "ats_keywords": ats_keywords,
    }


def _experience_level(text: str) -> str:
    lowered = text.lower()
    if re.search(r"\b(staff|principal|lead)\b", lowered):
        return "lead"
    if re.search(r"\b(senior|sr\.?)\b", lowered):
        return "senior"
    if re.search(r"\b(junior|entry|associate)\b", lowered):
        return "junior"
    if re.search(r"\b([5-9]|1\d)\+?\s+years?\b", lowered):
        return "senior"
    if re.search(r"\b([2-4])\+?\s+years?\b", lowered):
        return "mid"
    return "mid"


def _ats_keywords(
    title: str,
    description: str,
    required_skills: list[str],
    technologies: list[str],
) -> list[str]:
    title_tokens = [token.lower() for token in re.findall(r"[A-Za-z][A-Za-z0-9+#.-]{1,20}", title)]
    description_tokens = [
        token.lower() for token in re.findall(r"[A-Za-z][A-Za-z0-9+#.-]{2,20}", description)
    ]
    key_phrases = [
        phrase
        for phrase in (
            "scalable systems",
            "distributed systems",
            "api development",
            "observability",
            "testing",
            "automation",
        )
        if phrase in description.lower()
    ]

    keywords: list[str] = []
    merged = required_skills + technologies + title_tokens + key_phrases + description_tokens[:50]
    for word in merged:
        if word not in keywords and len(word) > 2:
            keywords.append(word)
        if len(keywords) >= 18:
            break
    return keywords
