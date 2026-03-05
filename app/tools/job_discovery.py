from __future__ import annotations

import re
from dataclasses import dataclass
from html import unescape
from urllib.parse import urljoin, urlparse
from urllib.request import Request, urlopen


@dataclass(slots=True)
class DiscoveryConfig:
    companies: list[dict[str, object]]
    max_jobs: int
    timeout_seconds: float
    fallback_jobs: list[dict[str, object]]


def discover_jobs(config: DiscoveryConfig) -> list[dict[str, object]]:
    jobs: list[dict[str, object]] = []

    for company in config.companies:
        if len(jobs) >= config.max_jobs:
            break
        jobs.extend(_discover_company_jobs(company, config.timeout_seconds))

    if len(jobs) < config.max_jobs:
        jobs.extend(config.fallback_jobs)

    unique: list[dict[str, object]] = []
    seen_links: set[str] = set()
    for job in jobs:
        link = str(job.get("job_link", ""))
        if link in seen_links:
            continue
        seen_links.add(link)
        unique.append(job)
        if len(unique) >= config.max_jobs:
            break

    return unique


def _discover_company_jobs(
    company_cfg: dict[str, object],
    timeout_seconds: float,
) -> list[dict[str, object]]:
    company_name = str(company_cfg.get("name", "Unknown Company"))
    career_url = str(company_cfg.get("careers_url", "")).strip()
    default_location = str(company_cfg.get("default_location", "Remote - US"))
    default_work_type = str(company_cfg.get("default_work_type", "remote"))
    keywords = [str(item).lower() for item in company_cfg.get("job_link_keywords", [])]

    if not career_url:
        return []

    html = _fetch_html(career_url, timeout_seconds)
    if not html:
        return []

    found: list[dict[str, object]] = []
    for href, label in _extract_links(html):
        absolute = urljoin(career_url, href)
        if not _looks_like_job_url(absolute, keywords):
            continue
        title = _clean_title(label) or "Software Engineer"
        work_type = _guess_work_type(title, default_work_type)
        found.append(
            {
                "company": company_name,
                "title": title,
                "job_url": absolute,
                "job_link": absolute,
                "location": default_location,
                "salary": "",
                "work_type": work_type,
                "description": f"{title} role from {company_name} careers page.",
            }
        )
        if len(found) >= 8:
            break
    return found


def _fetch_html(url: str, timeout_seconds: float) -> str:
    try:
        request = Request(url, headers={"User-Agent": "resume-agent/1.0"})
        with urlopen(request, timeout=timeout_seconds) as response:
            payload = response.read()
    except Exception:
        return ""
    return payload.decode("utf-8", errors="ignore")


def _extract_links(html: str) -> list[tuple[str, str]]:
    matches = re.findall(
        r"<a[^>]+href=[\"']([^\"']+)[\"'][^>]*>(.*?)</a>",
        html,
        flags=re.IGNORECASE | re.DOTALL,
    )
    links: list[tuple[str, str]] = []
    for href, text in matches:
        plain_text = re.sub(r"<[^>]+>", " ", text)
        clean_text = re.sub(r"\s+", " ", unescape(plain_text)).strip()
        if not href.strip():
            continue
        links.append((href.strip(), clean_text))
    return links


def _looks_like_job_url(url: str, keywords: list[str]) -> bool:
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"}:
        return False

    lowered = url.lower()
    if any(keyword in lowered for keyword in keywords):
        return True
    return any(token in lowered for token in ("job", "career", "position", "opening", "role"))


def _clean_title(label: str) -> str:
    if not label:
        return ""
    normalized = re.sub(r"\s+", " ", label).strip()
    normalized = re.sub(r"(apply now|learn more|view job)$", "", normalized, flags=re.IGNORECASE)
    return normalized.strip(" -|")


def _guess_work_type(title: str, default_work_type: str) -> str:
    lowered = title.lower()
    if "remote" in lowered:
        return "remote"
    if "hybrid" in lowered:
        return "hybrid"
    if "onsite" in lowered or "on-site" in lowered:
        return "onsite"
    return default_work_type
