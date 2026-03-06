from __future__ import annotations

import re
from typing import Any

SOFTWARE_SKILLS = {
    "c",
    "c++",
    "python",
    "java",
    "go",
    "rust",
    "javascript",
    "typescript",
    "sql",
    "linux",
    "embedded",
    "firmware",
    "networking",
    "distributed systems",
    "aws",
    "docker",
    "kubernetes",
    "postgres",
    "redis",
    "kafka",
    "terraform",
}

TECHNOLOGIES = {
    "aws",
    "docker",
    "kubernetes",
    "postgres",
    "redis",
    "kafka",
    "terraform",
    "linux",
    "networking",
    "embedded",
    "firmware",
    "distributed systems",
}

ROLE_INCLUDE = {
    "software engineer",
    "backend engineer",
    "platform engineer",
    "systems engineer",
    "embedded engineer",
    "firmware engineer",
    "full stack engineer",
    "site reliability engineer",
    "devops engineer",
}

ROLE_EXCLUDE = {
    "data scientist",
    "machine learning researcher",
    "sales",
    "marketing",
    "hr",
    "human resources",
    "finance",
    "product manager",
}


def parse_job_details(job: dict[str, Any]) -> dict[str, Any]:
    title = str(job.get("title", ""))
    description = str(job.get("description", ""))
    text = f"{title}\n{description}"

    required_skills = _extract_skills(text)
    technologies = [skill for skill in required_skills if skill in TECHNOLOGIES]
    experience_level = _experience_level(text)
    role_relevance = _role_relevance_score(title, description, required_skills)
    ats_keywords = _ats_keywords(
        title=title,
        description=description,
        required_skills=required_skills,
        technologies=technologies,
    )

    if role_relevance < 10 and required_skills:
        # Keep parsing interface stable while deprioritizing unrelated jobs downstream.
        ats_keywords = ["low-relevance", *ats_keywords]

    return {
        "job_link": str(job.get("job_link", "")),
        "required_skills": required_skills,
        "technologies": technologies,
        "experience_level": experience_level,
        "ats_keywords": ats_keywords,
    }


def _extract_skills(text: str) -> list[str]:
    lowered = text.lower()
    found: list[str] = []

    for skill in sorted(SOFTWARE_SKILLS):
        if " " in skill:
            if skill in lowered:
                found.append(skill)
            continue

        if re.search(rf"\b{re.escape(skill)}\b", lowered):
            found.append(skill)

    return found


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


def _role_relevance_score(title: str, description: str, skills: list[str]) -> int:
    text = f"{title} {description}".lower()
    score = 0

    if any(role in text for role in ROLE_INCLUDE):
        score += 16
    if "engineer" in text:
        score += 6
    if any(role in text for role in ROLE_EXCLUDE):
        score -= 18

    programming = {"c", "c++", "python", "java", "go", "rust", "javascript", "typescript"}
    infra = {"aws", "docker", "kubernetes", "postgres", "redis", "kafka", "terraform"}
    systems = {"linux", "embedded", "firmware", "networking", "distributed systems"}

    skill_set = set(skills)
    score += len(programming.intersection(skill_set)) * 2
    score += len(infra.intersection(skill_set)) * 2
    score += len(systems.intersection(skill_set)) * 3
    return score


def _ats_keywords(
    title: str,
    description: str,
    required_skills: list[str],
    technologies: list[str],
) -> list[str]:
    title_tokens = [token.lower() for token in re.findall(r"[A-Za-z][A-Za-z0-9+#.-]{1,20}", title)]
    description_tokens = [
        token.lower()
        for token in re.findall(r"[A-Za-z][A-Za-z0-9+#.-]{2,20}", description)
    ]

    key_phrases = [
        phrase
        for phrase in (
            "distributed systems",
            "microservices",
            "api development",
            "network protocols",
            "linux systems",
            "production reliability",
            "embedded systems",
            "firmware development",
        )
        if phrase in description.lower()
    ]

    keywords: list[str] = []
    merged = required_skills + technologies + title_tokens + key_phrases + description_tokens[:60]
    for term in merged:
        if term not in keywords and len(term) > 1:
            keywords.append(term)
        if len(keywords) >= 24:
            break
    return keywords
