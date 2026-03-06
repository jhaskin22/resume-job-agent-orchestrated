from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass
from html import unescape
from pathlib import Path
from urllib.parse import urljoin, urlparse
from urllib.request import Request, urlopen

logger = logging.getLogger(__name__)

JOB_PATH_HINTS = ("/jobs/", "/job/", "/positions/", "/opening", "/careers/")
REJECT_PATH_HINTS = ("about", "team", "culture", "benefits", "company")
BLOCKED_JOB_SOURCES = (
    "greenhouse",
    "lever",
    "ashby",
    "linkedin",
    "indeed",
    "wellfound",
)


@dataclass(slots=True)
class DiscoveryConfig:
    companies: list[dict[str, object]]
    max_jobs: int
    timeout_seconds: float
    fallback_jobs: list[dict[str, object]]
    use_fallback: bool = True
    cache_path: str | None = None


def discover_jobs(config: DiscoveryConfig) -> list[dict[str, object]]:
    jobs: list[dict[str, object]] = []

    for company in config.companies:
        if len(jobs) >= config.max_jobs:
            break
        discovered = _discover_company_jobs(company, config.timeout_seconds)
        logger.info(
            "job_discovery company=%s found=%s",
            company.get("name", "unknown"),
            len(discovered),
        )
        jobs.extend(discovered)

    if jobs and config.cache_path:
        _write_cache(config.cache_path, jobs)

    if config.use_fallback and len(jobs) < config.max_jobs:
        cached = _read_cache(config.cache_path) if config.cache_path else []
        logger.info("job_discovery using cached jobs count=%s", len(cached))
        jobs.extend(cached)

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

    if not career_url:
        return []

    html = _fetch_html(career_url, timeout_seconds)
    if not html:
        return []
    logger.info("career_page_fetched url=%s", career_url)

    found: list[dict[str, object]] = []
    extracted = _extract_links(html)
    logger.info("job_links_extracted url=%s count=%s", career_url, len(extracted))
    for href, label in extracted:
        absolute = urljoin(career_url, href)
        if not _is_official_company_url(absolute, career_url):
            continue
        if not _looks_like_job_url(absolute, label):
            continue
        posting_html = _fetch_html(absolute, timeout_seconds)
        posting_text = _extract_posting_text(posting_html)
        if not _is_real_job_posting(posting_text):
            continue
        logger.info("job_page_parsed url=%s", absolute)
        title = _clean_title(label) or _extract_title(posting_html) or "Software Engineer"
        location = _extract_location(posting_text, default_location)
        work_type = _guess_work_type(f"{title} {posting_text}", default_work_type)
        salary = _extract_salary(posting_text)
        technologies = _extract_technologies(posting_text)
        description = _build_description(posting_text)
        found.append(
            {
                "company": company_name,
                "title": title,
                "role": title,
                "job_url": absolute,
                "verified_url": absolute,
                "job_link": absolute,
                "location": location,
                "salary": salary,
                "work_type": work_type,
                "required_technologies": technologies,
                "description": description,
            }
        )
        if len(found) >= 8:
            break
    return found


def _fetch_html(url: str, timeout_seconds: float) -> str:
    try:
        request = Request(url, headers={"User-Agent": "resume-agent/1.0"})
        with urlopen(request, timeout=timeout_seconds) as response:
            status = getattr(response, "status", 200)
            if status != 200:
                return ""
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


def _looks_like_job_url(url: str, label: str) -> bool:
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"}:
        return False

    lowered = url.lower()
    path = parsed.path.lower()
    lowered_label = label.lower()
    if any(blocked in lowered for blocked in BLOCKED_JOB_SOURCES):
        return False
    segments = [part for part in path.strip("/").split("/") if part]
    if segments in (["jobs"], ["job"], ["positions"], ["careers", "jobs"]):
        return False
    if path.rstrip("/").endswith("/careers/jobs"):
        return False
    if any(token in lowered for token in REJECT_PATH_HINTS) or any(
        token in lowered_label for token in REJECT_PATH_HINTS
    ):
        return False
    if any(hint in path for hint in ("/jobs/", "/job/", "/positions/", "/opening")):
        return True
    if "/careers/" in path and "job" in path:
        return True
    return any(
        token in lowered_label for token in ("job", "position", "opening")
    )


def _clean_title(label: str) -> str:
    if not label:
        return ""
    normalized = re.sub(r"\s+", " ", label).strip()
    normalized = re.sub(r"(apply now|learn more|view job)$", "", normalized, flags=re.IGNORECASE)
    return normalized.strip(" -|")


def _extract_title(html: str) -> str:
    if not html:
        return ""
    h1 = re.search(r"<h1[^>]*>(.*?)</h1>", html, flags=re.IGNORECASE | re.DOTALL)
    if h1:
        plain = re.sub(r"<[^>]+>", " ", h1.group(1))
        return re.sub(r"\s+", " ", unescape(plain)).strip()
    title = re.search(r"<title[^>]*>(.*?)</title>", html, flags=re.IGNORECASE | re.DOTALL)
    if title:
        plain = re.sub(r"<[^>]+>", " ", title.group(1))
        return re.sub(r"\s+", " ", unescape(plain)).strip()
    return ""


def _extract_posting_text(html: str) -> str:
    if not html:
        return ""
    body = re.sub(r"(?is)<script.*?>.*?</script>", " ", html)
    body = re.sub(r"(?is)<style.*?>.*?</style>", " ", body)
    body = re.sub(r"(?is)<[^>]+>", " ", body)
    body = re.sub(r"\s+", " ", unescape(body)).strip()
    return body[:3000]


def _is_real_job_posting(text: str) -> bool:
    lowered = text.lower()
    if len(lowered.split()) < 80:
        return False
    positive_hints = ("responsibilities", "requirements", "qualifications", "experience", "apply")
    if not any(hint in lowered for hint in positive_hints):
        return False
    if sum(1 for term in REJECT_PATH_HINTS if term in lowered) > 3:
        return False
    return True


def _extract_location(text: str, default_location: str) -> str:
    lowered = text.lower()
    for marker in ("location:", "locations:", "based in"):
        idx = lowered.find(marker)
        if idx >= 0:
            snippet = text[idx : idx + 120]
            snippet = re.sub(r"\s+", " ", snippet)
            return snippet.split(":")[-1].strip(" .,-")[:60] or default_location
    if "remote" in lowered:
        return "Remote - US"
    return default_location


def _extract_salary(text: str) -> str:
    salary_match = re.search(
        r"(\$[\d,]{2,7}\s*(?:-|to)\s*\$[\d,]{2,7})|(\$[\d,]{2,7}\s*(?:per year|/year|annually))",
        text,
        flags=re.IGNORECASE,
    )
    return salary_match.group(0) if salary_match else ""


def _extract_technologies(text: str) -> list[str]:
    tech_terms = (
        "python",
        "java",
        "go",
        "typescript",
        "aws",
        "gcp",
        "azure",
        "docker",
        "kubernetes",
        "postgres",
        "redis",
        "kafka",
        "terraform",
    )
    lowered = text.lower()
    return [term for term in tech_terms if term in lowered]


def _build_description(text: str) -> str:
    snippets = re.split(r"(?<=[.!?])\s+", text)
    selected = []
    for sentence in snippets:
        lowered = sentence.lower()
        if any(
            keyword in lowered
            for keyword in ("responsibil", "require", "qualification", "experience")
        ):
            selected.append(sentence.strip())
        if len(selected) >= 4:
            break
    candidate = " ".join(selected) or text
    return re.sub(r"\s+", " ", candidate).strip()[:2500]


def _is_official_company_url(candidate_url: str, careers_url: str) -> bool:
    parsed_candidate = urlparse(candidate_url)
    parsed_careers = urlparse(careers_url)
    host = parsed_candidate.netloc.lower()
    careers_host = parsed_careers.netloc.lower()
    if not host or not careers_host:
        return False
    if any(blocked in host for blocked in BLOCKED_JOB_SOURCES):
        return False

    def _base_domain(value: str) -> str:
        parts = [part for part in value.split(".") if part]
        if len(parts) >= 2:
            return ".".join(parts[-2:])
        return value

    return _base_domain(host) == _base_domain(careers_host)


def _write_cache(cache_path: str, jobs: list[dict[str, object]]) -> None:
    try:
        path = Path(cache_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = {"jobs": jobs[:50]}
        path.write_text(json.dumps(payload), encoding="utf-8")
    except Exception:
        return


def _read_cache(cache_path: str | None) -> list[dict[str, object]]:
    if not cache_path:
        return []
    try:
        path = Path(cache_path)
        if not path.exists():
            return []
        payload = json.loads(path.read_text(encoding="utf-8"))
        items = payload.get("jobs", [])
        if isinstance(items, list):
            return [item for item in items if isinstance(item, dict)]
    except Exception:
        return []
    return []


def _guess_work_type(title: str, default_work_type: str) -> str:
    lowered = title.lower()
    if "remote" in lowered:
        return "remote"
    if "hybrid" in lowered:
        return "hybrid"
    if "onsite" in lowered or "on-site" in lowered:
        return "onsite"
    return default_work_type
