from __future__ import annotations

import re
from typing import Any

from app.tools.resume import tokenize


def score_jobs(
    parsed_resume: dict[str, Any],
    discovered_jobs: list[dict[str, Any]],
    parsed_jobs: dict[str, dict[str, Any]],
    top_k: int,
    scoring_config: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    cfg = dict(scoring_config or {})
    career_cfg = dict(cfg.get("career_path", {}))
    weights = _weights(career_cfg.get("weights", {}))

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

        resume_fit = _clamp(
            100 * (overlap_ratio * 0.62 + skill_ratio * 0.28 + technology_ratio * 0.10)
        )
        career_fit = _career_fit_score(
            title=title,
            description=description,
            career_cfg=career_cfg,
        )
        must_have_fit = _keyword_fit_ratio(
            text=f"{title}\n{description}",
            keywords=[str(item) for item in career_cfg.get("must_have_keywords", [])],
        )
        nice_to_have_fit = _keyword_fit_ratio(
            text=f"{title}\n{description}",
            keywords=[str(item) for item in career_cfg.get("nice_to_have_keywords", [])],
        )

        level = _job_level(title, parsed_job)
        management_role = _is_management_role(title, description)
        base_match = _clamp(
            resume_fit * weights["resume_fit"]
            + career_fit * weights["career_fit"]
            + must_have_fit * weights["must_have_fit"]
            + nice_to_have_fit * weights["nice_to_have_fit"]
        )

        salary_score, salary_signal = _salary_score(
            str(job.get("salary", "")),
            career_cfg.get("salary", {}),
        )
        salary_bonus = (salary_score - 50.0) * 0.10

        match_score = _clamp(base_match + salary_bonus)
        alignment_score = _clamp((resume_fit * 0.65) + (career_fit * 0.35))

        if not _passes_career_gates(
            level=level,
            management_role=management_role,
            base_match=base_match,
            career_fit=career_fit,
            career_cfg=career_cfg,
        ):
            continue

        scored.append(
            {
                **job,
                "required_skills": matched_skills,
                "technologies": parsed_job.get("technologies", []),
                "experience_level": parsed_job.get("experience_level", "mid"),
                "ats_keywords": parsed_job.get("ats_keywords", []),
                "match_score": round(match_score, 2),
                "resume_alignment": round(alignment_score, 2),
                "ranking_debug": {
                    "resume_fit": round(resume_fit, 2),
                    "career_fit": round(career_fit, 2),
                    "must_have_fit": round(must_have_fit, 2),
                    "nice_to_have_fit": round(nice_to_have_fit, 2),
                    "job_level": level,
                    "management_role": management_role,
                    "salary_score": round(salary_score, 2),
                    "salary_signal": salary_signal,
                },
            }
        )

    prefer_disclosed = bool(
        dict(career_cfg.get("salary", {})).get("prefer_disclosed_tiebreaker", False)
    )
    if prefer_disclosed:
        scored.sort(
            key=lambda item: (
                float(item["match_score"]),
                1 if _salary_disclosed(item.get("salary")) else 0,
                float(item["resume_alignment"]),
            ),
            reverse=True,
        )
    else:
        scored.sort(key=lambda item: float(item["match_score"]), reverse=True)
    if top_k <= 0:
        return scored
    return scored[:top_k]


def _weights(weight_cfg: dict[str, Any]) -> dict[str, float]:
    base = {
        "resume_fit": float(weight_cfg.get("resume_fit", 0.45)),
        "career_fit": float(weight_cfg.get("career_fit", 0.30)),
        "must_have_fit": float(weight_cfg.get("must_have_fit", 0.15)),
        "nice_to_have_fit": float(weight_cfg.get("nice_to_have_fit", 0.10)),
    }
    total = sum(max(v, 0.0) for v in base.values())
    if total <= 0:
        return {
            "resume_fit": 0.45,
            "career_fit": 0.30,
            "must_have_fit": 0.15,
            "nice_to_have_fit": 0.10,
        }
    return {k: max(v, 0.0) / total for k, v in base.items()}


def _career_fit_score(*, title: str, description: str, career_cfg: dict[str, Any]) -> float:
    text = f"{title}\n{description}"
    title_score = _title_family_fit(
        title=title,
        target_titles=[str(item) for item in career_cfg.get("target_titles", [])],
    )
    focus_score = _keyword_fit_ratio(
        text=text,
        keywords=[str(item) for item in career_cfg.get("responsibility_focus", [])],
    )
    return _clamp((title_score * 0.6) + (focus_score * 0.4))


def _title_family_fit(*, title: str, target_titles: list[str]) -> float:
    lowered = title.lower()
    role_keywords = {
        "backend",
        "platform",
        "infrastructure",
        "distributed",
        "cloud",
        "systems",
        "system",
        "services",
        "software",
        "engineer",
    }
    for phrase in target_titles:
        for token in re.findall(r"[a-zA-Z][a-zA-Z0-9+#.-]+", phrase.lower()):
            if token in {"senior", "software", "engineer"}:
                continue
            role_keywords.add(token)
    matched = 0
    total = 0
    if re.search(r"\bsoftware engineer\b", lowered):
        matched += 2
        total += 2
    if re.search(r"\bbackend engineer\b", lowered):
        matched += 2
        total += 2
    for keyword in sorted(role_keywords):
        if keyword in {"software", "engineer"}:
            continue
        total += 1
        if re.search(rf"\b{re.escape(keyword)}\b", lowered):
            matched += 1
    if total == 0:
        return 0.0
    return 100.0 * (matched / total)


def _keyword_fit_ratio(*, text: str, keywords: list[str]) -> float:
    if not keywords:
        return 0.0
    lowered = text.lower()
    hits = 0
    total = 0
    for keyword in keywords:
        key = keyword.strip().lower()
        if not key:
            continue
        total += 1
        if " " in key or "/" in key or "-" in key:
            if key in lowered:
                hits += 1
            continue
        if re.search(rf"\b{re.escape(key)}\b", lowered):
            hits += 1
    if total == 0:
        return 0.0
    return 100.0 * (hits / total)


def _job_level(title: str, parsed_job: dict[str, Any]) -> str:
    text = f"{title} {str(parsed_job.get('experience_level', ''))}".lower()
    if re.search(r"\b(senior manager|director|head|vp)\b", text):
        return "senior_manager"
    if re.search(r"\b(staff|principal)\b", text):
        return "staff"
    if re.search(r"\b(senior|sr\.?)\b", text):
        return "senior"
    if re.search(r"\b(junior|entry|associate)\b", text):
        return "junior"
    return "mid"


def _is_management_role(title: str, description: str) -> bool:
    lowered = f"{title}\n{description}".lower()
    return bool(re.search(r"\b(manager|director|head|vp|vice president)\b", lowered))


def _passes_career_gates(
    *,
    level: str,
    management_role: bool,
    base_match: float,
    career_fit: float,
    career_cfg: dict[str, Any],
) -> bool:
    allow_management = bool(career_cfg.get("allow_management_roles", True))
    allowed_levels = {str(item).strip().lower() for item in career_cfg.get("allowed_levels", [])}
    discouraged_levels = {
        str(item).strip().lower() for item in career_cfg.get("discouraged_levels", [])
    }
    strong_override = float(career_cfg.get("strong_match_override", 85))
    min_career_fit = float(career_cfg.get("min_career_fit", 0))

    if management_role and not allow_management and base_match < strong_override:
        return False
    if level in discouraged_levels and base_match < strong_override:
        return False
    if allowed_levels and level not in allowed_levels and level not in discouraged_levels:
        # If explicit allowed levels were set, reject levels outside that set unless a strong match.
        if base_match < strong_override:
            return False
    if min_career_fit > 0:
        if career_fit < min_career_fit and base_match < strong_override:
            return False
    return True


def _salary_disclosed(salary_raw: Any) -> bool:
    return bool(_extract_salary_numbers(str(salary_raw or "")))


def _salary_score(salary_raw: str, salary_cfg: Any) -> tuple[float, str]:
    cfg = dict(salary_cfg or {})
    minimum = float(cfg.get("minimum", 0))
    target_min = float(cfg.get("target_min", minimum))
    target_max = float(cfg.get("target_max", target_min))
    low, high = _extract_salary_numbers(salary_raw)

    # Neutral when undisclosed; no penalty.
    if low is None and high is None:
        return (50.0, "undisclosed")

    # Normalize to annual figure if hourly was provided.
    if "hour" in salary_raw.lower() and low is not None:
        low *= 2080
        if high is not None:
            high *= 2080

    observed_low = low if low is not None else high
    observed_high = high if high is not None else low
    if observed_low is None or observed_high is None:
        return (50.0, "undisclosed")

    if observed_high < minimum:
        return (20.0, "below_minimum")
    if observed_low >= target_min and observed_high <= target_max:
        return (80.0, "in_target_band")
    if observed_low >= target_max:
        return (72.0, "above_target_band")
    if observed_low >= minimum:
        return (62.0, "meets_minimum")
    return (45.0, "partial_range")


def _extract_salary_numbers(raw: str) -> tuple[float | None, float | None]:
    if not raw.strip():
        return (None, None)
    numbers: list[float] = []
    for token in re.findall(r"\$?\s*\d[\d,]*(?:\.\d+)?\s*[kK]?", raw):
        normalized = token.lower().replace("$", "").replace(",", "").strip()
        if not normalized:
            continue
        multiplier = 1000.0 if normalized.endswith("k") else 1.0
        normalized = normalized[:-1] if normalized.endswith("k") else normalized
        try:
            numbers.append(float(normalized) * multiplier)
        except ValueError:
            continue
    if not numbers:
        return (None, None)
    if len(numbers) == 1:
        return (numbers[0], numbers[0])
    return (min(numbers), max(numbers))


def _clamp(value: float) -> float:
    return max(0.0, min(100.0, value))
