from __future__ import annotations

import re
from collections.abc import Iterable


def tokenize(text: str) -> set[str]:
    return set(re.findall(r"[a-zA-Z][a-zA-Z0-9+#.-]{1,24}", text.lower()))


def parse_resume_sections(
    text: str,
    skill_keywords: Iterable[str],
    role_keywords: Iterable[str],
    technology_keywords: Iterable[str],
) -> dict[str, object]:
    tokens = tokenize(text)
    lowered_lines = [line.strip().lower() for line in text.splitlines() if line.strip()]

    skills = sorted({item for item in skill_keywords if item.lower() in tokens})
    technologies = sorted({item for item in technology_keywords if item.lower() in tokens})
    roles = sorted(
        {item for item in role_keywords if _contains_phrase(tokens, lowered_lines, item)}
    )

    experience_blocks: list[str] = []
    for line in text.splitlines():
        clean = line.strip()
        if not clean:
            continue
        if re.search(r"\b(20\d{2}|19\d{2}|present)\b", clean.lower()):
            experience_blocks.append(clean)
        elif re.search(
            r"\b(led|built|developed|shipped|managed|improved|launched)\b",
            clean.lower(),
        ):
            experience_blocks.append(clean)
        if len(experience_blocks) >= 8:
            break

    return {
        "skills": skills,
        "technologies": technologies,
        "roles": roles,
        "experience": experience_blocks,
        "raw_text": text,
    }


def _contains_phrase(tokens: set[str], lines: list[str], phrase: str) -> bool:
    lowered = phrase.lower().strip()
    if " " in lowered:
        return any(lowered in line for line in lines)
    return lowered in tokens
