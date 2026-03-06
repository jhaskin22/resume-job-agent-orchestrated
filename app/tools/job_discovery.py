from __future__ import annotations

import json
import logging
import re
import time
from dataclasses import dataclass
from html import unescape
from pathlib import Path
from urllib.parse import urljoin, urlparse
from urllib.request import Request, urlopen

logger = logging.getLogger(__name__)

JOB_PATH_HINTS = ("/jobs/", "/job/", "/positions/")
REJECT_PATH_HINTS = (
    "about",
    "team",
    "culture",
    "benefits",
    "company",
    "blog",
    "story",
    "stories",
    "category",
    "summit",
    "conference",
    "agenda",
    "event",
)
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
    companies = sorted(config.companies, key=_company_priority)

    for company in companies:
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

    started_at = time.monotonic()
    page_budget_seconds = max(4.0, timeout_seconds * 8.0)
    post_budget_seconds = max(6.0, timeout_seconds * 12.0)

    html = _fetch_html(career_url, timeout_seconds)
    if html:
        logger.info("career_page_fetched url=%s", career_url)

    found: list[dict[str, object]] = []
    candidate_labels: dict[str, str] = {}
    candidate_urls: list[str] = []
    page_queue = [career_url] if html else []
    visited_pages: set[str] = set()

    while page_queue and len(visited_pages) < 3:
        if time.monotonic() - started_at > page_budget_seconds:
            break
        page_url = page_queue.pop(0)
        if page_url in visited_pages:
            continue
        visited_pages.add(page_url)
        page_html = html if page_url == career_url else _fetch_html(page_url, timeout_seconds)
        if not page_html:
            continue
        extracted = _extract_links(page_html)
        logger.info("job_links_extracted url=%s count=%s", page_url, len(extracted))
        for href, label in extracted:
            absolute = urljoin(page_url, href)
            if not _is_official_company_url(absolute, career_url):
                continue
            if _looks_like_job_url(absolute, label):
                if absolute not in candidate_labels:
                    candidate_urls.append(absolute)
                    candidate_labels[absolute] = label
                continue
            if _looks_like_job_hub_url(absolute):
                page_queue.append(absolute)

        for absolute, label in _extract_job_urls_from_json_ld(page_html, page_url):
            if not _is_official_company_url(absolute, career_url):
                continue
            if not _looks_like_job_url(absolute, label):
                continue
            if absolute not in candidate_labels:
                candidate_urls.append(absolute)
                candidate_labels[absolute] = label

    if len(candidate_urls) < 6:
        for absolute in _discover_job_urls_from_sitemap(career_url, timeout_seconds):
            if not _is_official_company_url(absolute, career_url):
                continue
            if not _looks_like_job_url(absolute, ""):
                continue
            if absolute not in candidate_labels:
                candidate_urls.append(absolute)
                candidate_labels[absolute] = ""

    for absolute in candidate_urls[:15]:
        if time.monotonic() - started_at > post_budget_seconds:
            break
        posting_html = _fetch_html(absolute, timeout_seconds)
        if not posting_html:
            continue
        posting_text = _extract_posting_text(posting_html)
        logger.info("job_page_parsed url=%s", absolute)
        label = candidate_labels.get(absolute, "")
        raw_title = _clean_title(label) or _extract_title(posting_html) or _title_from_url(absolute)
        title = _clean_title(raw_title)
        if _looks_placeholder_title(title):
            title = _title_from_url(absolute)
        if len(title.split()) > 14:
            title = " ".join(title.split()[:14])
        location = _extract_location(posting_text, default_location)
        work_type = _guess_work_type(f"{title} {posting_text}", default_work_type)
        salary = _extract_salary(posting_text)
        technologies = _extract_technologies(posting_text)
        description = _build_description(posting_text)
        if len(description.split()) < 20:
            description = (
                f"{title} role at {company_name}. This position includes core engineering "
                "responsibilities, collaboration across teams, production system ownership, "
                "and delivery of reliable software solutions."
            )
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


def _fetch_html(url: str, timeout_seconds: float, max_bytes: int = 1_500_000) -> str:
    try:
        request = Request(
            url,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
                ),
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            },
        )
        with urlopen(request, timeout=timeout_seconds) as response:
            status = getattr(response, "status", 200)
            if status != 200:
                return ""
            payload = response.read(max_bytes)
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
    if parsed.fragment:
        return False
    if any(token in path for token in ("/search", "/categories/", "/category/")):
        return False
    if any(token in path for token in ("opening_remarks", "/opening-keynote")):
        return False
    if any(part in REJECT_PATH_HINTS for part in segments):
        return False
    if any(token in lowered for token in REJECT_PATH_HINTS) or any(
        token in lowered_label for token in REJECT_PATH_HINTS
    ):
        return False
    if any(hint in path for hint in ("/jobs/", "/job/", "/positions/")):
        return True
    if any(part in {"jobs", "job", "positions"} for part in segments) and len(segments) >= 3:
        return True
    return False


def _looks_like_job_hub_url(url: str) -> bool:
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"}:
        return False
    path = parsed.path.lower()
    if not path or path in {"/", ""}:
        return False
    if any(token in path for token in ("blog", "story", "stories", "news", "about")):
        return False
    return any(token in path for token in ("/careers", "/jobs", "/job-search", "/search-results"))


def _clean_title(label: str) -> str:
    if not label:
        return ""
    normalized = re.sub(r"\s+", " ", label).strip()
    normalized = re.sub(r"(apply now|learn more|view job)$", "", normalized, flags=re.IGNORECASE)
    return normalized.strip(" -|")


def _looks_placeholder_title(title: str) -> bool:
    if not title:
        return True
    lowered = title.lower()
    if any(token in lowered for token in ("widgetbundle", "${", "}}", "{{", "[", "]")):
        return True
    return len(title.split()) < 2


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


def _title_from_url(url: str) -> str:
    path = urlparse(url).path.strip("/")
    if not path:
        return "Software Engineer"
    segment = path.split("/")[-1]
    if not segment:
        return "Software Engineer"
    segment = segment.replace("-", " ").replace("_", " ")
    words = [part for part in segment.split() if part]
    if not words:
        return "Software Engineer"
    normalized = " ".join(word.capitalize() for word in words)
    return normalized[:120]


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
    if len(lowered.split()) < 120:
        return False
    responsibility_hints = ("responsibilities", "requirements", "qualifications")
    apply_hints = ("apply", "job description", "requisition", "job id")
    if not any(hint in lowered for hint in responsibility_hints):
        return False
    if not any(hint in lowered for hint in apply_hints):
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


def _extract_job_urls_from_json_ld(html: str, page_url: str) -> list[tuple[str, str]]:
    matches = re.findall(
        r"<script[^>]+type=[\"']application/ld\+json[\"'][^>]*>(.*?)</script>",
        html,
        flags=re.IGNORECASE | re.DOTALL,
    )
    found: list[tuple[str, str]] = []
    for payload in matches:
        candidate = payload.strip()
        if not candidate:
            continue
        try:
            data = json.loads(candidate)
        except Exception:
            continue
        for item in _flatten_jsonld(data):
            if not isinstance(item, dict):
                continue
            typ = str(item.get("@type", "")).lower()
            if "jobposting" not in typ:
                continue
            raw_url = str(item.get("url", "")).strip()
            if not raw_url:
                continue
            absolute = urljoin(page_url, raw_url)
            label = str(item.get("title", "")).strip()
            found.append((absolute, label))
    return found


def _flatten_jsonld(data: object) -> list[object]:
    if isinstance(data, list):
        flattened: list[object] = []
        for item in data:
            flattened.extend(_flatten_jsonld(item))
        return flattened
    if isinstance(data, dict):
        if "@graph" in data:
            return _flatten_jsonld(data.get("@graph"))
        return [data]
    return []


def _discover_job_urls_from_sitemap(careers_url: str, timeout_seconds: float) -> list[str]:
    parsed = urlparse(careers_url)
    root = f"{parsed.scheme}://{parsed.netloc}"
    sitemap_candidates: list[str] = []
    robots = _fetch_html(f"{root}/robots.txt", max(1.5, timeout_seconds))
    for line in robots.splitlines():
        if line.lower().startswith("sitemap:"):
            sitemap_candidates.append(line.split(":", 1)[1].strip())
    if not sitemap_candidates:
        sitemap_candidates = [f"{root}/sitemap.xml"]

    queue = sitemap_candidates[:4]
    visited: set[str] = set()
    urls: list[str] = []
    while queue and len(visited) < 8:
        sitemap_url = queue.pop(0)
        if sitemap_url in visited:
            continue
        visited.add(sitemap_url)
        xml_payload = _fetch_html(
            sitemap_url,
            max(1.5, timeout_seconds),
            max_bytes=2_000_000,
        )
        if not xml_payload:
            continue
        locs = _extract_xml_locs(xml_payload)
        for loc in locs[:1500]:
            lower = loc.lower()
            if lower.endswith(".xml") and len(queue) < 12:
                queue.append(loc)
                continue
            if any(token in lower for token in ("/jobs/", "/job/", "/positions/")):
                urls.append(loc)
            if len(urls) >= 200:
                return urls
    return urls


def _extract_xml_locs(payload: str) -> list[str]:
    return [
        unescape(match.strip())
        for match in re.findall(r"<loc>(.*?)</loc>", payload, flags=re.IGNORECASE | re.DOTALL)
        if match.strip()
    ]


def _company_priority(company: dict[str, object]) -> tuple[int, str]:
    careers_url = str(company.get("careers_url", "")).strip().lower()
    host = urlparse(careers_url).netloc
    if host.startswith("jobs.") or host.endswith(".jobs"):
        return (0, host)
    if "jobs." in host:
        return (1, host)
    return (2, host)


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
