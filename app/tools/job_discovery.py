from __future__ import annotations

import json
import logging
import re
import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from html import unescape
from pathlib import Path
from typing import Any
from urllib.parse import quote, urlencode, urljoin, urlparse
from urllib.request import Request, urlopen

logger = logging.getLogger(__name__)

SOFTWARE_ROLE_INCLUDE = (
    "software engineer",
    "backend engineer",
    "platform engineer",
    "systems engineer",
    "embedded engineer",
    "firmware engineer",
    "full stack engineer",
    "site reliability engineer",
    "devops engineer",
)

SOFTWARE_ROLE_HINTS = (
    "software",
    "backend",
    "platform",
    "systems",
    "embedded",
    "firmware",
    "full-stack",
    "fullstack",
    "devops",
    "sre",
    "infrastructure",
)

SOFTWARE_ROLE_EXCLUDE = (
    "data scientist",
    "machine learning researcher",
    "ml researcher",
    "sales",
    "marketing",
    "human resources",
    "hr ",
    "finance",
    "product manager",
    "recruiter",
)

SKILL_VOCABULARY = {
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

TECH_VOCABULARY = {
    "aws",
    "docker",
    "kubernetes",
    "postgres",
    "redis",
    "kafka",
    "terraform",
    "linux",
    "networking",
    "distributed systems",
    "embedded",
    "firmware",
}

JOB_PATH_HINTS = ("/jobs/", "/job/", "/positions/")
ATS_DOMAIN_HINTS = (
    "boards.greenhouse.io",
    "api.lever.co",
    "jobs.ashbyhq.com",
    "ashbyhq.com",
    "myworkdayjobs.com",
    "wd1.myworkdayjobs.com",
    "wd3.myworkdayjobs.com",
    "smartrecruiters.com",
    "icims.com",
    "taleo.net",
)
PROVIDER_SCAN_LIMIT = 300
PROVIDER_KEEP_LIMIT = 80
GREENHOUSE_DETAIL_LOOKUP_LIMIT = 40
FALLBACK_CANDIDATE_LIMIT = 120
FALLBACK_ATTEMPT_LIMIT = 40
REJECT_PATH_HINTS = (
    "about",
    "team",
    "culture",
    "benefits",
    "blog",
    "stories",
    "story",
    "category",
    "news",
    "event",
)

CAREERS_URL_OVERRIDES = {
    "Stripe": "https://stripe.com/jobs/search",
    "Cloudflare": "https://www.cloudflare.com/careers/jobs/",
    "Datadog": "https://careers.datadoghq.com/",
    "MongoDB": "https://www.mongodb.com/careers",
    "Twilio": "https://jobs.twilio.com/careers",
    "Toyota Connected": "https://toyotaconnected.com/careers",
    "Bell": "https://textron.taleo.net/careersection/bell/jobsearch.ftl?lang=en",
    "NVIDIA": "https://nvidia.wd5.myworkdayjobs.com/NVIDIAExternalCareerSite",
}

_HTTP_CACHE_LOCK = threading.Lock()
_HTTP_CACHE: dict[str, tuple[float, Any]] = {}
_HTTP_CACHE_ENABLED = True
_HTTP_CACHE_TTL_SECONDS = 180.0


@dataclass(slots=True)
class DiscoveryConfig:
    companies: list[dict[str, object]]
    max_jobs: int | None
    timeout_seconds: float
    fallback_jobs: list[dict[str, object]]
    use_fallback: bool = True
    cache_path: str | None = None
    max_jobs_per_company: int = 0
    global_budget_seconds: float = 0.0
    company_workers: int = 1


def configure_http_cache(*, enabled: bool, ttl_seconds: float) -> None:
    global _HTTP_CACHE_ENABLED, _HTTP_CACHE_TTL_SECONDS
    _HTTP_CACHE_ENABLED = bool(enabled)
    _HTTP_CACHE_TTL_SECONDS = max(0.0, float(ttl_seconds))


def clear_http_cache() -> None:
    with _HTTP_CACHE_LOCK:
        _HTTP_CACHE.clear()


@dataclass(slots=True)
class AtsDetection:
    ats_type: str
    company_key: str


@dataclass(slots=True)
class CompanyDiscoveryStats:
    company: str
    provider: str
    fetched: int = 0
    deduped: int = 0
    normalized: int = 0
    kept: int = 0
    dropped_normalization: int = 0
    dropped_relevance: int = 0
    dropped_reasons: dict[str, int] | None = None

    def bump_reason(self, reason: str) -> None:
        if self.dropped_reasons is None:
            self.dropped_reasons = {}
        self.dropped_reasons[reason] = self.dropped_reasons.get(reason, 0) + 1


def discover_jobs(config: DiscoveryConfig) -> list[dict[str, object]]:
    jobs: list[dict[str, object]] = []
    started = time.monotonic()
    global_budget_seconds = float(config.global_budget_seconds)
    companies = _prioritized_companies(config.companies)
    workers = max(1, int(config.company_workers))

    if workers == 1 or global_budget_seconds > 0:
        for company in companies:
            if global_budget_seconds > 0 and time.monotonic() - started > global_budget_seconds:
                break
            discovered = _discover_company_jobs(company, config.timeout_seconds)
            if int(config.max_jobs_per_company) > 0:
                discovered = discovered[: int(config.max_jobs_per_company)]
            logger.info(
                "job_discovery company=%s found=%s",
                company.get("name", "unknown"),
                len(discovered),
            )
            jobs.extend(discovered)
    else:
        ordered_results: dict[int, list[dict[str, object]]] = {}
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures: dict[Future[list[dict[str, object]]], tuple[int, str]] = {}
            for idx, company in enumerate(companies):
                future = executor.submit(_discover_company_jobs, company, config.timeout_seconds)
                futures[future] = (idx, str(company.get("name", "unknown")))
            for future in as_completed(futures):
                idx, company_name = futures[future]
                try:
                    discovered = future.result()
                except Exception as exc:  # noqa: BLE001
                    logger.exception(
                        "job_discovery parallel_failure company=%s error=%s",
                        company_name,
                        exc,
                    )
                    discovered = []
                if int(config.max_jobs_per_company) > 0:
                    discovered = discovered[: int(config.max_jobs_per_company)]
                logger.info(
                    "job_discovery company=%s found=%s",
                    company_name,
                    len(discovered),
                )
                ordered_results[idx] = discovered
        for idx in sorted(ordered_results):
            jobs.extend(ordered_results[idx])

    if jobs and config.cache_path:
        _write_cache(config.cache_path, jobs)

    if config.use_fallback:
        cached = _read_cache(config.cache_path)
        logger.info("job_discovery using cached jobs count=%s", len(cached))
        jobs.extend(cached)

    unique = _dedupe_jobs(jobs)
    return unique


def enrich_missing_salaries(
    jobs: list[dict[str, object]],
    *,
    timeout_seconds: float,
    max_lookups: int = 60,
    workers: int = 6,
) -> list[dict[str, object]]:
    if max_lookups <= 0:
        return jobs
    enriched: list[dict[str, object]] = []
    missing_urls: list[str] = []
    for job in jobs:
        salary = str(job.get("salary", "")).strip()
        job_url = str(job.get("job_url", job.get("job_link", ""))).strip()
        if not salary and job_url:
            missing_urls.append(job_url)
    ordered_unique_urls = list(dict.fromkeys(missing_urls))[: max(0, int(max_lookups))]
    page_cache: dict[str, tuple[int, str]] = {}
    if ordered_unique_urls:
        with ThreadPoolExecutor(max_workers=max(1, int(workers))) as executor:
            future_to_url = {
                executor.submit(_http_get_text, url, timeout_seconds, 600_000): url
                for url in ordered_unique_urls
            }
            for future in as_completed(future_to_url):
                url = future_to_url[future]
                try:
                    page_cache[url] = future.result()
                except Exception as exc:  # noqa: BLE001
                    logger.exception("salary_backfill fetch_failure url=%s error=%s", url, exc)
                    page_cache[url] = (0, "")

    for job in jobs:
        updated = dict(job)
        salary = str(updated.get("salary", "")).strip()
        job_url = str(updated.get("job_url", updated.get("job_link", ""))).strip()
        if not salary and job_url in page_cache:
            status, html = page_cache[job_url]
            if status == 200 and html:
                posting_text = _extract_posting_text(html)
                salary = _extract_salary(posting_text, html, updated)
                if salary:
                    updated["salary"] = salary
        enriched.append(updated)
    return enriched


def _discover_company_jobs(
    company_cfg: dict[str, object],
    timeout_seconds: float,
) -> list[dict[str, object]]:
    started = time.monotonic()
    company_budget_seconds = max(10.0, timeout_seconds * 6.0)
    company_name = str(company_cfg.get("name", "Unknown Company"))
    raw_careers_url = str(company_cfg.get("careers_url", "")).strip()
    careers_url = _effective_careers_url(company_name, raw_careers_url)
    default_location = str(company_cfg.get("default_location", "United States"))
    default_work_type = str(company_cfg.get("default_work_type", "hybrid"))
    if not careers_url:
        return []
    page_ctx = _fetch_page_context(careers_url, timeout_seconds)
    html = page_ctx["html"]
    status = int(page_ctx["status"])
    final_url = str(page_ctx["final_url"])
    detection = _detect_ats(careers_url, html, final_url, company_name=company_name)
    jobs: list[dict[str, object]] = []
    provider = detection.ats_type if detection else "direct"
    stats = CompanyDiscoveryStats(company=company_name, provider=provider)
    ingest_path = "provider" if detection else "fallback"

    if detection is not None:
        logger.info(
            "job_discovery provider_detected company=%s provider=%s",
            company_name,
            provider,
        )
        jobs.extend(
            _discover_jobs_via_ats(
                company_name=company_name,
                careers_url=careers_url,
                default_location=default_location,
                default_work_type=default_work_type,
                detection=detection,
                timeout_seconds=timeout_seconds,
            )
        )
    pre_filter_count = len(jobs)
    stats.fetched = pre_filter_count

    if len(jobs) < 4:
        if time.monotonic() - started > company_budget_seconds:
            kept = _finalize_company_jobs(jobs, stats=stats)
            logger.info(
                (
                    "job_discovery company=%s provider=%s path=%s status=%s "
                    "fetched=%s deduped=%s normalized=%s kept=%s "
                    "drop_normalization=%s drop_relevance=%s drop_reasons=%s reason=%s"
                ),
                company_name,
                provider,
                ingest_path,
                status,
                stats.fetched,
                stats.deduped,
                stats.normalized,
                stats.kept,
                stats.dropped_normalization,
                stats.dropped_relevance,
                stats.dropped_reasons or {},
                "company-time-budget-exceeded",
            )
            return kept
        ingest_path = "provider+fallback" if detection else "fallback"
        crawl_root = final_url or careers_url
        jobs.extend(
            _discover_jobs_via_fallback_crawler(
                company_name=company_name,
                careers_url=crawl_root,
                default_location=default_location,
                default_work_type=default_work_type,
                initial_html=html,
                timeout_seconds=timeout_seconds,
            )
        )
    stats.fetched = len(jobs)
    kept = _finalize_company_jobs(jobs, stats=stats)
    reason = "ok" if kept else "no-matching-software-jobs"
    logger.info(
        (
            "job_discovery company=%s provider=%s path=%s status=%s "
            "fetched=%s deduped=%s normalized=%s kept=%s "
            "drop_normalization=%s drop_relevance=%s drop_reasons=%s reason=%s"
        ),
        company_name,
        provider,
        ingest_path,
        status,
        stats.fetched,
        stats.deduped,
        stats.normalized,
        stats.kept,
        stats.dropped_normalization,
        stats.dropped_relevance,
        stats.dropped_reasons or {},
        reason,
    )
    return kept


def _finalize_company_jobs(
    jobs: list[dict[str, object]],
    *,
    stats: CompanyDiscoveryStats | None = None,
) -> list[dict[str, object]]:
    deduped = _dedupe_jobs(jobs)
    if stats is not None:
        stats.deduped = len(deduped)

    normalized: list[dict[str, object]] = []
    for job in deduped:
        clean, reason = _normalize_discovered_job(job)
        if clean is None:
            if stats is not None:
                stats.dropped_normalization += 1
                stats.bump_reason(f"normalize:{reason or 'unknown'}")
            continue
        normalized.append(clean)
    if stats is not None:
        stats.normalized = len(normalized)

    filtered: list[dict[str, object]] = []
    for job in normalized:
        if not _is_relevant_software_job(job):
            if stats is not None:
                stats.dropped_relevance += 1
                stats.bump_reason("relevance:not_software_role")
            continue
        filtered.append(job)
    if stats is not None:
        stats.kept = len(filtered)

    filtered.sort(key=_job_rank, reverse=True)
    return filtered


def _normalize_discovered_job(
    job: dict[str, object],
) -> tuple[dict[str, object] | None, str | None]:
    company = str(job.get("company", "")).strip()
    title = str(job.get("title", "")).strip()
    url = str(job.get("job_url", job.get("job_link", ""))).strip()
    description = str(job.get("description", "")).strip()
    location = str(job.get("location", "")).strip()
    work_type = str(job.get("work_type", "")).strip().lower()
    provider = str(job.get("source_provider", "direct")).strip().lower() or "direct"
    salary = str(job.get("salary", "")).strip()

    if not company:
        return (None, "missing_company")
    if not title:
        return (None, "missing_title")
    if not url:
        return (None, "missing_url")
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return (None, "invalid_url")
    if len(description.split()) < 20:
        description = (
            f"{title} role at {company}. Responsibilities include software development, "
            "system reliability, and collaboration across engineering teams."
        )

    normalized_work_type = work_type if work_type in {"remote", "hybrid", "onsite"} else "hybrid"
    normalized = {
        **job,
        "company": company,
        "title": title,
        "role": str(job.get("role") or title).strip(),
        "location": location or "United States",
        "salary": salary,
        "description": re.sub(r"\s+", " ", description).strip(),
        "work_type": normalized_work_type,
        "source_provider": provider,
        "url": url,
        "job_url": url,
        "job_link": str(job.get("job_link", url)).strip() or url,
        "verified_url": str(job.get("verified_url", url)).strip() or url,
    }
    return (normalized, None)


def _discover_jobs_via_ats(
    *,
    company_name: str,
    careers_url: str,
    default_location: str,
    default_work_type: str,
    detection: AtsDetection,
    timeout_seconds: float,
) -> list[dict[str, object]]:
    try:
        if detection.ats_type == "greenhouse":
            return _greenhouse_jobs(
                company_name,
                detection.company_key,
                careers_url,
                default_location,
                default_work_type,
                timeout_seconds,
            )
        if detection.ats_type == "lever":
            return _lever_jobs(
                company_name,
                detection.company_key,
                careers_url,
                default_location,
                default_work_type,
                timeout_seconds,
            )
        if detection.ats_type == "ashby":
            return _ashby_jobs(
                company_name,
                detection.company_key,
                careers_url,
                default_location,
                default_work_type,
                timeout_seconds,
            )
        if detection.ats_type == "workday":
            return _workday_jobs(
                company_name,
                detection.company_key,
                careers_url,
                default_location,
                default_work_type,
                timeout_seconds,
            )
        if detection.ats_type == "smartrecruiters":
            return _smartrecruiters_jobs(
                company_name,
                detection.company_key,
                careers_url,
                default_location,
                default_work_type,
                timeout_seconds,
            )
        if detection.ats_type == "icims":
            return _icims_jobs(
                company_name,
                detection.company_key,
                careers_url,
                default_location,
                default_work_type,
                timeout_seconds,
            )
        if detection.ats_type == "jibe":
            return _jibe_jobs(
                company_name,
                detection.company_key,
                careers_url,
                default_location,
                default_work_type,
                timeout_seconds,
            )
        if detection.ats_type == "taleo":
            return _taleo_jobs(
                company_name,
                detection.company_key,
                careers_url,
                default_location,
                default_work_type,
                timeout_seconds,
            )
    except Exception:  # noqa: BLE001
        logger.exception("ats_discovery_failed company=%s ats=%s", company_name, detection.ats_type)
    return []


def _detect_ats(
    careers_url: str,
    html: str,
    final_url: str = "",
    *,
    company_name: str = "",
) -> AtsDetection | None:
    signal_text = _provider_signal_text(careers_url, final_url, html)
    board = _extract_greenhouse_board(careers_url, html)
    if board:
        return AtsDetection("greenhouse", board)

    lever = _extract_lever_company(careers_url, html)
    if lever:
        return AtsDetection("lever", lever)

    ashby = _extract_ashby_company(careers_url, html)
    if ashby:
        return AtsDetection("ashby", ashby)

    workday = _extract_workday_tenant_site(careers_url, html)
    if workday:
        return AtsDetection("workday", workday)

    smart = _extract_smartrecruiters_company(careers_url, html)
    if smart:
        return AtsDetection("smartrecruiters", smart)

    jibe = _extract_jibe_site(careers_url, html, final_url)
    if jibe:
        return AtsDetection("jibe", jibe)

    icims = _extract_icims_host(careers_url, html)
    if icims:
        return AtsDetection("icims", icims)
    taleo = _extract_taleo_site(careers_url, html)
    if taleo:
        return AtsDetection("taleo", taleo)
    if "myworkdayjobs.com" in signal_text:
        workday = _extract_workday_tenant_site(final_url or careers_url, html)
        if workday:
            return AtsDetection("workday", workday)
    if "greenhouse" in signal_text:
        board_guess = _guess_company_slug(careers_url)
        if board_guess:
            return AtsDetection("greenhouse", board_guess)
    if "lever.co" in signal_text:
        lever_guess = _guess_company_slug(careers_url)
        if lever_guess:
            return AtsDetection("lever", lever_guess)
    if "ashbyhq.com" in signal_text:
        ashby_guess = _guess_company_slug(careers_url)
        if ashby_guess:
            return AtsDetection("ashby", ashby_guess)
    if "smartrecruiters.com" in signal_text:
        smart_guess = _guess_company_slug(careers_url)
        if smart_guess:
            return AtsDetection("smartrecruiters", smart_guess)
    if "taleo.net" in signal_text:
        taleo_guess = _extract_taleo_site(final_url or careers_url, html)
        if taleo_guess:
            return AtsDetection("taleo", taleo_guess)
    probed = _probe_ats_from_company_name(company_name, careers_url)
    if probed is not None:
        return probed
    return None


def _provider_signal_text(careers_url: str, final_url: str, html: str) -> str:
    snippets = [careers_url, final_url, html]
    return "\n".join(snippet for snippet in snippets if snippet).lower()


def _greenhouse_jobs(
    company_name: str,
    board: str,
    careers_url: str,
    default_location: str,
    default_work_type: str,
    timeout_seconds: float,
) -> list[dict[str, object]]:
    api_url = f"https://boards-api.greenhouse.io/v1/boards/{quote(board)}/jobs"
    payload = _http_get_json(api_url, timeout_seconds)
    if not isinstance(payload, dict):
        return []
    jobs = payload.get("jobs", [])
    if not isinstance(jobs, list):
        return []

    found: list[dict[str, object]] = []
    detail_cache: dict[str, dict[str, Any] | None] = {}
    detail_lookups = 0
    for item in jobs[:PROVIDER_SCAN_LIMIT]:
        if len(found) >= PROVIDER_KEEP_LIMIT:
            break
        if not isinstance(item, dict):
            continue
        url = str(item.get("absolute_url", "")).strip()
        title = str(item.get("title", "")).strip()
        if not url or not title:
            continue
        content = str(item.get("content", ""))
        description = _clean_text(content)[:2600]
        candidate = {
            "company": company_name,
            "title": title,
            "description": description or f"{title} role at {company_name}.",
        }
        if not _is_relevant_software_job(candidate):
            continue
        location = _extract_greenhouse_location(item, default_location)
        salary = _extract_salary(description, item)
        detail = None
        job_id = _extract_greenhouse_job_id(url)
        if (
            not salary
            and job_id
            and detail_lookups < GREENHOUSE_DETAIL_LOOKUP_LIMIT
        ):
            if job_id not in detail_cache:
                detail_cache[job_id] = _greenhouse_job_detail(board, job_id, timeout_seconds)
                detail_lookups += 1
            detail = detail_cache[job_id]
            if isinstance(detail, dict):
                detail_description_full = _clean_text(str(detail.get("content", "")))
                detail_description = detail_description_full[:2600]
                if detail_description and (
                    not description or len(detail_description.split()) > len(description.split())
                ):
                    description = detail_description
                salary = _extract_salary(detail_description_full, detail, item)
        work_type = _guess_work_type(f"{title} {description}", default_work_type)
        found.append(
            _job_record(
                company=company_name,
                title=title,
                location=location,
                salary=salary,
                description=description,
                url=url,
                work_type=work_type,
                source_provider="greenhouse",
            )
        )
    return found


def _extract_greenhouse_job_id(job_url: str) -> str:
    patterns = (
        r"[?&]gh_jid=(\d+)",
        r"/jobs/(\d+)(?:[/?#]|$)",
    )
    for pattern in patterns:
        match = re.search(pattern, job_url)
        if match:
            return match.group(1)
    return ""


def _greenhouse_job_detail(
    board: str,
    job_id: str,
    timeout_seconds: float,
) -> dict[str, Any] | None:
    detail_url = f"https://boards-api.greenhouse.io/v1/boards/{quote(board)}/jobs/{quote(job_id)}"
    payload = _http_get_json(detail_url, timeout_seconds)
    if isinstance(payload, dict):
        return payload
    return None


def _lever_jobs(
    company_name: str,
    company_key: str,
    careers_url: str,
    default_location: str,
    default_work_type: str,
    timeout_seconds: float,
) -> list[dict[str, object]]:
    api_url = f"https://api.lever.co/v0/postings/{quote(company_key)}?mode=json"
    payload = _http_get_json(api_url, timeout_seconds)
    if not isinstance(payload, list):
        return []

    found: list[dict[str, object]] = []
    for item in payload[:PROVIDER_SCAN_LIMIT]:
        if len(found) >= PROVIDER_KEEP_LIMIT:
            break
        if not isinstance(item, dict):
            continue
        url = str(item.get("hostedUrl", "")).strip() or str(item.get("applyUrl", "")).strip()
        title = str(item.get("text", "")).strip()
        if not url or not title:
            continue
        description = _clean_text(str(item.get("descriptionPlain", "")))
        if not description:
            description = _clean_text(str(item.get("description", "")))
        location = _extract_lever_location(item, default_location)
        salary = _extract_salary(description, item)
        work_type = _guess_work_type(f"{title} {description}", default_work_type)
        found.append(
            _job_record(
                company=company_name,
                title=title,
                location=location,
                salary=salary,
                description=description,
                url=url,
                work_type=work_type,
                source_provider="lever",
            )
        )
    return found


def _ashby_jobs(
    company_name: str,
    company_key: str,
    careers_url: str,
    default_location: str,
    default_work_type: str,
    timeout_seconds: float,
) -> list[dict[str, object]]:
    api_url = f"https://api.ashbyhq.com/posting-api/job-board/{quote(company_key)}"
    payload = _http_get_json(api_url, timeout_seconds)
    if not isinstance(payload, dict):
        return []

    jobs = payload.get("jobs")
    if not isinstance(jobs, list):
        return []

    found: list[dict[str, object]] = []
    for item in jobs[:PROVIDER_SCAN_LIMIT]:
        if len(found) >= PROVIDER_KEEP_LIMIT:
            break
        if not isinstance(item, dict):
            continue
        url = str(item.get("jobUrl", "")).strip() or str(item.get("jobPostUrl", "")).strip()
        title = str(item.get("title", "")).strip()
        if not url or not title:
            continue
        description = _clean_text(str(item.get("descriptionHtml", "")))
        location = _extract_ashby_location(item, default_location)
        salary = _extract_salary(description, item)
        work_type = _guess_work_type(f"{title} {description}", default_work_type)
        found.append(
            _job_record(
                company=company_name,
                title=title,
                location=location,
                salary=salary,
                description=description,
                url=url,
                work_type=work_type,
                source_provider="ashby",
            )
        )
    return found


def _workday_jobs(
    company_name: str,
    tenant_site: str,
    careers_url: str,
    default_location: str,
    default_work_type: str,
    timeout_seconds: float,
) -> list[dict[str, object]]:
    parts = tenant_site.split("|")
    if len(parts) == 3:
        host, tenant, site = parts
        base = f"https://{host}"
    else:
        tenant, site = tenant_site.split("|", maxsplit=1)
        base = f"https://{tenant}.myworkdayjobs.com"
    api_url = f"{base}/wday/cxs/{tenant}/{site}/jobs"
    payloads: list[dict[str, Any]] = []
    first = _http_post_json(api_url, {"limit": 50, "offset": 0}, timeout_seconds)
    if isinstance(first, dict):
        payloads.append(first)
    for keyword in ("engineer", "software", "platform", "systems", "developer", "firmware"):
        match_payload = _http_post_json(
            api_url,
            {"limit": 25, "offset": 0, "searchText": keyword},
            timeout_seconds,
        )
        if isinstance(match_payload, dict):
            payloads.append(match_payload)

    jobs: list[dict[str, Any]] = []
    for payload in payloads:
        batch = payload.get("jobPostings", [])
        if isinstance(batch, list):
            jobs.extend(item for item in batch if isinstance(item, dict))
    if not jobs:
        return []

    found: list[dict[str, object]] = []
    seen_urls: set[str] = set()
    for item in jobs[:PROVIDER_SCAN_LIMIT]:
        if len(found) >= PROVIDER_KEEP_LIMIT:
            break
        if not isinstance(item, dict):
            continue
        title = str(item.get("title", "")).strip()
        path = str(item.get("externalPath", "")).strip()
        if not title or not path:
            continue
        url = urljoin(careers_url.rstrip("/") + "/", path.lstrip("/"))
        if url in seen_urls:
            continue
        seen_urls.add(url)
        description = _clean_text(str(item.get("bulletFields", "")))
        location = _extract_workday_location(item, default_location)
        salary = _extract_salary(description, item)
        work_type = _guess_work_type(f"{title} {description}", default_work_type)
        found.append(
            _job_record(
                company=company_name,
                title=title,
                location=location,
                salary=salary,
                description=description,
                url=url,
                work_type=work_type,
                source_provider="workday",
            )
        )
    return found


def _smartrecruiters_jobs(
    company_name: str,
    company_key: str,
    careers_url: str,
    default_location: str,
    default_work_type: str,
    timeout_seconds: float,
) -> list[dict[str, object]]:
    params = urlencode({"limit": 100, "offset": 0})
    api_url = f"https://api.smartrecruiters.com/v1/companies/{quote(company_key)}/postings?{params}"
    payload = _http_get_json(api_url, timeout_seconds)
    if not isinstance(payload, dict):
        return []

    jobs = payload.get("content", [])
    if not isinstance(jobs, list):
        return []

    found: list[dict[str, object]] = []
    for item in jobs[:PROVIDER_SCAN_LIMIT]:
        if len(found) >= PROVIDER_KEEP_LIMIT:
            break
        if not isinstance(item, dict):
            continue
        title = str(item.get("name", "")).strip()
        url = str(item.get("ref", "")).strip() or str(item.get("applyUrl", "")).strip()
        if not title or not url:
            continue
        location = _extract_smart_location(item, default_location)
        description = _clean_text(str(item.get("jobAd", "")))
        salary = _extract_salary(description, item)
        work_type = _guess_work_type(f"{title} {description}", default_work_type)
        found.append(
            _job_record(
                company=company_name,
                title=title,
                location=location,
                salary=salary,
                description=description,
                url=url,
                work_type=work_type,
                source_provider="smartrecruiters",
            )
        )
    return found


def _icims_jobs(
    company_name: str,
    icims_host: str,
    careers_url: str,
    default_location: str,
    default_work_type: str,
    timeout_seconds: float,
) -> list[dict[str, object]]:
    search_urls = [
        f"https://{icims_host}/jobs/search?ss=1",
        f"https://{icims_host}/jobs/search?pr=1&searchRelation=keyword_all",
    ]
    candidate_urls: dict[str, str] = {}
    for search_url in search_urls:
        status, html = _http_get_text(search_url, timeout_seconds)
        if status != 200 or not html:
            continue
        for href, label in _extract_links(html):
            absolute = urljoin(search_url, href)
            if _looks_like_job_url(absolute, label):
                candidate_urls.setdefault(absolute, label)

    found: list[dict[str, object]] = []
    for url, label in list(candidate_urls.items())[:FALLBACK_CANDIDATE_LIMIT]:
        status, posting_html = _http_get_text(url, timeout_seconds)
        if status != 200 or not posting_html:
            continue
        description = _extract_posting_text(posting_html)
        title = _clean_title(label) or _extract_title(posting_html) or _title_from_url(url)
        if not title:
            continue
        location = _extract_location_from_posting(posting_html, description, default_location)
        salary = _extract_salary(description, posting_html)
        work_type = _guess_work_type(f"{title} {description}", default_work_type)
        found.append(
            _job_record(
                company=company_name,
                title=title,
                location=location,
                salary=salary,
                description=description,
                url=url,
                work_type=work_type,
                source_provider="icims",
            )
        )
        if len(found) >= PROVIDER_KEEP_LIMIT:
            break
    return found


def _taleo_jobs(
    company_name: str,
    taleo_site: str,
    careers_url: str,
    default_location: str,
    default_work_type: str,
    timeout_seconds: float,
) -> list[dict[str, object]]:
    parts = taleo_site.split("|")
    if len(parts) != 4:
        return []
    host, section, lang, portal = parts
    search_url = f"https://{host}/careersection/rest/jobboard/searchjobs?lang={quote(lang)}&portal={quote(portal)}"
    headers = {"tz": "UTC", "tzname": "UTC"}
    payload = _http_post_json(search_url, {"pageNo": 1}, timeout_seconds, headers=headers)
    if not isinstance(payload, dict):
        return []
    paging = payload.get("pagingData", {})
    total_count = int(paging.get("totalCount", 0)) if isinstance(paging, dict) else 0
    page_size = int(paging.get("pageSize", 25)) if isinstance(paging, dict) else 25
    max_pages = max(1, min(8, (total_count // max(1, page_size)) + 1))

    pages = [payload]
    for page_no in range(2, max_pages + 1):
        page_payload = _http_post_json(
            search_url,
            {"pageNo": page_no},
            timeout_seconds,
            headers=headers,
        )
        if isinstance(page_payload, dict):
            pages.append(page_payload)

    found: list[dict[str, object]] = []
    seen_urls: set[str] = set()
    for page_payload in pages:
        req_list = page_payload.get("requisitionList", [])
        if not isinstance(req_list, list):
            continue
        for item in req_list:
            if not isinstance(item, dict):
                continue
            contest_no = str(item.get("contestNo", "")).strip()
            columns = item.get("column", [])
            title = str(columns[0]).strip() if isinstance(columns, list) and columns else ""
            if not contest_no or not title:
                continue
            title_lower = title.lower()
            if any(token in title_lower for token in SOFTWARE_ROLE_EXCLUDE):
                continue
            software_title_tokens = SOFTWARE_ROLE_HINTS + ("engineer", "developer")
            if not any(token in title_lower for token in software_title_tokens):
                continue
            url = (
                f"https://{host}/careersection/{section}/jobdetail.ftl"
                f"?job={quote(contest_no)}&lang={quote(lang)}"
            )
            if url in seen_urls:
                continue
            seen_urls.add(url)
            location = _extract_taleo_location(item, default_location)
            description = _extract_taleo_description(url, timeout_seconds)
            if not description:
                description = f"{title} role at {company_name} in {location}."
            salary = _extract_salary(description, item)
            work_type = _guess_work_type(f"{title} {description}", default_work_type)
            found.append(
                _job_record(
                    company=company_name,
                    title=title,
                    location=location,
                    salary=salary,
                    description=description,
                    url=url,
                    work_type=work_type,
                    source_provider="taleo",
                )
            )
    return found


def _jibe_jobs(
    company_name: str,
    jibe_key: str,
    careers_url: str,
    default_location: str,
    default_work_type: str,
    timeout_seconds: float,
) -> list[dict[str, object]]:
    parts = jibe_key.split("|")
    if len(parts) < 2:
        return []
    base_url = parts[0].rstrip("/")
    client = parts[1].strip() or _guess_company_slug(careers_url)
    if not client:
        return []
    ref_url = parts[2] if len(parts) >= 3 else careers_url
    found: list[dict[str, object]] = []

    for page in range(1, 5):
        query = urlencode({"domain": f"{client}.jibeapply.com", "page": page, "limit": 50})
        payload = _http_get_json(
            f"{base_url}/api/jobs?{query}",
            timeout_seconds=max(timeout_seconds, 2.0),
            max_bytes=5_000_000,
        )
        if not isinstance(payload, dict):
            break
        jobs = payload.get("jobs", [])
        if not isinstance(jobs, list) or not jobs:
            break
        for item in jobs:
            if len(found) >= 20:
                break
            if not isinstance(item, dict):
                continue
            data = item.get("data", {})
            if not isinstance(data, dict):
                continue
            title = str(data.get("title", "")).strip()
            if not title:
                continue
            description = _clean_text(str(data.get("description", "")))
            candidate = {"company": company_name, "title": title, "description": description}
            if not _is_relevant_software_job(candidate):
                continue
            location = (
                str(data.get("location_name", "")).strip()
                or ", ".join(
                    part
                    for part in (
                        str(data.get("city", "")).strip(),
                        str(data.get("state", "")).strip(),
                        str(data.get("country", "")).strip(),
                    )
                    if part
                )
                or default_location
            )
            raw_url = str(data.get("apply_url", "")).strip()
            if not raw_url:
                slug = str(data.get("slug", "")).strip()
                if slug:
                    raw_url = f"{base_url}/jobs/{slug}/login"
            if not raw_url:
                continue
            url = raw_url if raw_url.startswith("http") else urljoin(ref_url, raw_url)
            salary = _extract_salary(description, data, item)
            work_type = _guess_work_type(f"{title} {description} {location}", default_work_type)
            found.append(
                _job_record(
                    company=company_name,
                    title=title,
                    location=location,
                    salary=salary,
                    description=description,
                    url=url,
                    work_type=work_type,
                    source_provider="jibe",
                )
            )
        if len(found) >= 20:
            break
    return found


def _discover_jobs_via_fallback_crawler(
    *,
    company_name: str,
    careers_url: str,
    default_location: str,
    default_work_type: str,
    initial_html: str,
    timeout_seconds: float,
) -> list[dict[str, object]]:
    queue = [careers_url]
    visited_pages: set[str] = set()
    candidate_urls: dict[str, str] = {}

    while queue and len(visited_pages) < 3:
        page_url = queue.pop(0)
        if page_url in visited_pages:
            continue
        visited_pages.add(page_url)

        status, html = (
            (200, initial_html)
            if page_url == careers_url and initial_html
            else _http_get_text(page_url, timeout_seconds)
        )
        if status != 200 or not html:
            continue

        for href, label in _extract_links(html):
            absolute = urljoin(page_url, href)
            if not _is_same_or_linked_domain(absolute, careers_url):
                continue
            if _looks_like_job_url(absolute, label):
                candidate_urls.setdefault(absolute, label)
            elif _looks_like_job_hub_url(absolute):
                queue.append(absolute)

        for absolute, label in _extract_job_urls_from_json_ld(html, page_url):
            if _is_same_or_linked_domain(absolute, careers_url):
                candidate_urls.setdefault(absolute, label)

    if len(candidate_urls) < 8:
        for url in _discover_job_urls_from_sitemap(careers_url, timeout_seconds):
            if _is_same_or_linked_domain(url, careers_url):
                candidate_urls.setdefault(url, "")

    found: list[dict[str, object]] = []
    prioritized_candidates = sorted(
        candidate_urls.items(),
        key=lambda pair: _candidate_priority(pair[0], pair[1]),
    )
    attempts = 0
    for url, label in prioritized_candidates[:60]:
        attempts += 1
        if attempts > FALLBACK_ATTEMPT_LIMIT:
            break
        status, posting_html = _http_get_text(url, timeout_seconds)
        if status != 200 or not posting_html:
            continue

        description = _extract_posting_text(posting_html)
        title = _clean_title(label) or _extract_title(posting_html) or _title_from_url(url)
        if not title:
            continue

        location = _extract_location_from_posting(posting_html, description, default_location)
        salary = _extract_salary(description, posting_html)
        work_type = _guess_work_type(f"{title} {description}", default_work_type)
        found.append(
            _job_record(
                company=company_name,
                title=title,
                location=location,
                salary=salary,
                description=description,
                url=url,
                work_type=work_type,
                source_provider="direct",
            )
        )
    return found


def _job_record(
    *,
    company: str,
    title: str,
    location: str,
    salary: str,
    description: str,
    url: str,
    work_type: str,
    source_provider: str = "direct",
) -> dict[str, object]:
    if _looks_template_text(title):
        title = _title_from_url(url)
    clean_description = re.sub(r"\s+", " ", description).strip()[:2800]
    if len(clean_description.split()) < 20:
        clean_description = (
            f"{title} role at {company}. Responsibilities include software development, "
            "system reliability, and collaboration across engineering teams. "
            "Candidates should have relevant technical skills and production experience."
        )
    technologies = _extract_technologies(clean_description)
    return {
        "company": company,
        "title": title,
        "role": title,
        "location": location,
        "salary": salary,
        "description": clean_description,
        "required_technologies": technologies,
        "work_type": work_type,
        "source_provider": source_provider,
        "url": url,
        "job_url": url,
        "job_link": url,
        "verified_url": url,
    }


def _is_relevant_software_job(job: dict[str, object]) -> bool:
    title = str(job.get("title", ""))
    description = str(job.get("description", ""))
    title_lower = title.lower()
    description_head = description[:900].lower()

    if any(term in title_lower for term in SOFTWARE_ROLE_EXCLUDE):
        return False

    if any(term in title_lower for term in SOFTWARE_ROLE_INCLUDE):
        return True
    if any(token in title_lower for token in ("engineer", "developer", "devops", "sre")):
        if not any(term in title_lower for term in ("sales", "marketing", "finance", "hr")):
            return True
    if any(term in description_head for term in SOFTWARE_ROLE_EXCLUDE) and not any(
        token in title_lower for token in ("engineer", "developer", "firmware", "embedded")
    ):
        return False

    score = _job_relevance_score(title, description_head)
    return score >= 18


def _looks_template_text(value: str) -> bool:
    lowered = value.lower()
    return any(token in lowered for token in ("${", "{{", "}}", "widgetbundle", "jobdetail"))


def _candidate_priority(url: str, label: str) -> tuple[int, int]:
    haystack = f"{url} {label}".lower()
    include_hits = sum(1 for hint in SOFTWARE_ROLE_HINTS if hint in haystack)
    exclude_hits = sum(1 for term in SOFTWARE_ROLE_EXCLUDE if term in haystack)
    # lower tuple sorts first: more include hits, fewer exclude hits.
    return (-include_hits, exclude_hits)


def _job_rank(job: dict[str, object]) -> float:
    title = str(job.get("title", ""))
    description = str(job.get("description", ""))
    return float(_job_relevance_score(title, description))


def _job_relevance_score(title: str, description: str) -> int:
    title_text = title.lower()
    text = f"{title} {description}".lower()
    skills = _extract_technologies(text)

    score = 0
    if any(role in title_text for role in SOFTWARE_ROLE_INCLUDE):
        score += 24
    elif any(role in text for role in SOFTWARE_ROLE_INCLUDE):
        score += 20
    if "engineer" in title_text or "developer" in title_text:
        score += 12
    elif "engineer" in text:
        score += 8

    programming = {"c", "c++", "python", "java", "go", "rust", "javascript", "typescript"}
    infra = {"aws", "docker", "kubernetes", "postgres", "redis", "kafka", "terraform"}
    systems = {"linux", "embedded", "firmware", "networking", "distributed systems"}

    score += len(programming.intersection(skills)) * 3
    score += len(infra.intersection(skills)) * 2
    score += len(systems.intersection(skills)) * 4
    return score


def _guess_company_slug(careers_url: str) -> str:
    host = urlparse(careers_url).netloc.lower()
    parts = [part for part in host.split(".") if part and part not in {"www", "careers", "jobs"}]
    if not parts:
        return ""
    return re.sub(r"[^a-z0-9_-]+", "", parts[0])


def _fetch_page_context(url: str, timeout_seconds: float) -> dict[str, object]:
    try:
        request = Request(url, headers=_default_headers())
        with urlopen(request, timeout=max(timeout_seconds, 1.0)) as response:
            status = int(getattr(response, "status", 200))
            html = response.read(2_000_000).decode("utf-8", errors="ignore")
            final_url = str(getattr(response, "url", url))
            return {"status": status, "html": html, "final_url": final_url}
    except Exception:
        return {"status": 0, "html": "", "final_url": url}


def _cache_get(cache_key: str) -> Any | None:
    if not _HTTP_CACHE_ENABLED or _HTTP_CACHE_TTL_SECONDS <= 0:
        return None
    now = time.time()
    with _HTTP_CACHE_LOCK:
        entry = _HTTP_CACHE.get(cache_key)
        if entry is None:
            return None
        expires_at, value = entry
        if expires_at < now:
            _HTTP_CACHE.pop(cache_key, None)
            return None
        return value


def _cache_set(cache_key: str, value: Any) -> None:
    if not _HTTP_CACHE_ENABLED or _HTTP_CACHE_TTL_SECONDS <= 0:
        return
    expires_at = time.time() + _HTTP_CACHE_TTL_SECONDS
    with _HTTP_CACHE_LOCK:
        _HTTP_CACHE[cache_key] = (expires_at, value)


def _http_get_text(url: str, timeout_seconds: float, max_bytes: int = 2_000_000) -> tuple[int, str]:
    cache_key = f"GET_TEXT|{max_bytes}|{url}"
    cached = _cache_get(cache_key)
    if isinstance(cached, tuple) and len(cached) == 2:
        return cached
    try:
        request = Request(url, headers=_default_headers())
        with urlopen(request, timeout=max(timeout_seconds, 1.0)) as response:
            status = int(getattr(response, "status", 200))
            payload = response.read(max_bytes).decode("utf-8", errors="ignore")
            result = (status, payload)
            _cache_set(cache_key, result)
            return result
    except Exception:
        result = (0, "")
        _cache_set(cache_key, result)
        return result


def _http_get_json(url: str, timeout_seconds: float, max_bytes: int = 6_000_000) -> Any:
    cache_key = f"GET_JSON|{max_bytes}|{url}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached
    status, payload = _http_get_text(url, timeout_seconds, max_bytes=max_bytes)
    if status != 200 or not payload:
        _cache_set(cache_key, None)
        return None
    try:
        parsed = json.loads(payload)
        _cache_set(cache_key, parsed)
        return parsed
    except Exception:
        _cache_set(cache_key, None)
        return None


def _probe_ats_from_company_name(company_name: str, careers_url: str) -> AtsDetection | None:
    candidates = _slug_candidates(company_name, careers_url)
    for slug in candidates:
        payload = _http_get_json(
            f"https://boards-api.greenhouse.io/v1/boards/{quote(slug)}/jobs",
            timeout_seconds=1.5,
            max_bytes=3_500_000,
        )
        if isinstance(payload, dict) and isinstance(payload.get("jobs"), list):
            return AtsDetection("greenhouse", slug)
    for slug in candidates:
        payload = _http_get_json(
            f"https://api.lever.co/v0/postings/{quote(slug)}?mode=json",
            timeout_seconds=1.5,
            max_bytes=900_000,
        )
        if isinstance(payload, list):
            return AtsDetection("lever", slug)
    for slug in candidates:
        payload = _http_get_json(
            f"https://api.ashbyhq.com/posting-api/job-board/{quote(slug)}",
            timeout_seconds=1.5,
            max_bytes=900_000,
        )
        if isinstance(payload, dict) and isinstance(payload.get("jobs"), list):
            return AtsDetection("ashby", slug)
    return None


def _slug_candidates(company_name: str, careers_url: str) -> list[str]:
    out: list[str] = []
    parsed = urlparse(careers_url)
    host_parts = [part for part in parsed.netloc.lower().split(".") if part]
    for part in host_parts:
        if part in {"www", "careers", "jobs", "job", "com", "io", "net", "org"}:
            continue
        normalized = re.sub(r"[^a-z0-9_-]+", "", part)
        if normalized:
            out.append(normalized)
    name_slug = re.sub(r"[^a-z0-9]+", "", company_name.lower())
    if name_slug:
        out.append(name_slug)
    dashed = re.sub(r"[^a-z0-9]+", "-", company_name.lower()).strip("-")
    if dashed:
        out.append(dashed)
    return list(dict.fromkeys(out))


def _http_post_json(
    url: str,
    body: dict[str, object],
    timeout_seconds: float,
    headers: dict[str, str] | None = None,
) -> Any:
    body_encoded = json.dumps(body, sort_keys=True)
    header_key = json.dumps(headers or {}, sort_keys=True)
    cache_key = f"POST_JSON|{url}|{body_encoded}|{header_key}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached
    try:
        payload = body_encoded.encode("utf-8")
        request = Request(
            url,
            data=payload,
            headers={**_default_headers(), "Content-Type": "application/json", **(headers or {})},
            method="POST",
        )
        with urlopen(request, timeout=max(timeout_seconds, 1.0)) as response:
            status = int(getattr(response, "status", 200))
            if status != 200:
                _cache_set(cache_key, None)
                return None
            text = response.read(2_000_000).decode("utf-8", errors="ignore")
            parsed = json.loads(text)
            _cache_set(cache_key, parsed)
            return parsed
    except Exception:
        _cache_set(cache_key, None)
        return None


def _verify_url_200(url: str, timeout_seconds: float) -> bool:
    status, _ = _http_get_text(url, timeout_seconds, max_bytes=128_000)
    return status == 200


def _default_headers() -> dict[str, str]:
    return {
        "User-Agent": (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }


def _extract_greenhouse_board(careers_url: str, html: str) -> str:
    query_match = re.search(
        r"boards\.greenhouse\.io/embed/job_board/js\?for=([a-z0-9_-]+)",
        f"{careers_url}\n{html}".lower(),
    )
    if query_match:
        return query_match.group(1)
    candidates = re.findall(
        r"(?:boards\.greenhouse\.io|boards-api\.greenhouse\.io/v1/boards)/([a-z0-9_-]+)",
        f"{careers_url}\n{html}".lower(),
    )
    if candidates:
        return candidates[0]
    parsed = urlparse(careers_url)
    if parsed.netloc.endswith("greenhouse.io"):
        parts = [part for part in parsed.path.split("/") if part]
        if parts:
            return parts[0]
    return ""


def _extract_lever_company(careers_url: str, html: str) -> str:
    candidates = re.findall(r"api\.lever\.co/v0/postings/([a-z0-9_-]+)", html.lower())
    if candidates:
        return candidates[0]
    parsed = urlparse(careers_url)
    if parsed.netloc.endswith("jobs.lever.co"):
        parts = [part for part in parsed.path.split("/") if part]
        if parts:
            return parts[0]
    return ""


def _extract_ashby_company(careers_url: str, html: str) -> str:
    candidates = re.findall(
        r"api\.ashbyhq\.com/posting-api/job-board/([a-z0-9_-]+)",
        html.lower(),
    )
    if candidates:
        return candidates[0]
    parsed = urlparse(careers_url)
    if parsed.netloc.endswith("ashbyhq.com"):
        parts = [part for part in parsed.path.split("/") if part]
        if parts:
            return parts[-1]
    return ""


def _extract_workday_tenant_site(careers_url: str, html: str) -> str:
    url_matches = re.findall(
        r"https://([a-z0-9.-]*myworkdayjobs\.com)/([a-z0-9-/_-]+)",
        f"{careers_url}\n{html}".lower(),
    )
    locale_tokens = {"en-us", "en-gb", "fr-fr", "de-de", "es-es", "ja-jp"}
    for host, raw_path in url_matches:
        tenant = host.split(".")[0]
        segments = [part for part in raw_path.split("/") if part]
        candidates = [part for part in segments if part not in locale_tokens and part != "login"]
        if candidates:
            return f"{host}|{tenant}|{candidates[0]}"

    parsed = urlparse(careers_url)
    host = parsed.netloc.lower()
    if host.endswith("myworkdayjobs.com"):
        tenant = host.split(".")[0]
        parts = [part for part in parsed.path.split("/") if part]
        if parts:
            return f"{tenant}|{parts[0]}"
    return ""


def _extract_smartrecruiters_company(careers_url: str, html: str) -> str:
    candidates = re.findall(r"smartrecruiters\.com/([a-z0-9_-]+)", f"{careers_url}\n{html}".lower())
    if candidates:
        return candidates[0]
    parsed = urlparse(careers_url)
    if parsed.netloc.endswith("smartrecruiters.com"):
        parts = [part for part in parsed.path.split("/") if part]
        if parts:
            return parts[0]
    return ""


def _extract_icims_host(careers_url: str, html: str) -> str:
    candidates = re.findall(
        r"https?://([a-z0-9.-]*icims\.com)",
        f"{careers_url}\n{html}".lower(),
    )
    if candidates:
        ranked = sorted(
            set(candidates),
            key=lambda host: (
                0 if ("careers-" in host or host.startswith("jobs.")) else 1,
                1 if host.startswith("www.icims.com") else 0,
                host,
            ),
        )
        for host in ranked:
            if host != "www.icims.com":
                return host
        return ranked[0]
    host = urlparse(careers_url).netloc.lower()
    if "icims.com" in host:
        return host
    return ""


def _extract_taleo_site(careers_url: str, html: str) -> str:
    signal = f"{careers_url}\n{html}"
    match = re.search(
        r"https?://([a-z0-9.-]*taleo\.net)/careersection/([a-z0-9_-]+)/jobsearch\.ftl\?[^\"'\s>]*lang=([a-z-]+)(?:[^\"'\s>]*portal=(\d+))?",
        signal,
        flags=re.IGNORECASE,
    )
    if match:
        host = match.group(1).lower()
        section = match.group(2).strip()
        lang = match.group(3).strip().lower()
        portal = (match.group(4) or "").strip()
        if not portal:
            portal_match = re.search(r"portal=(\d+)", signal, flags=re.IGNORECASE)
            if portal_match:
                portal = portal_match.group(1)
        if portal:
            return f"{host}|{section}|{lang}|{portal}"

    parsed = urlparse(careers_url)
    if parsed.netloc.lower().endswith("taleo.net"):
        section_match = re.search(
            r"/careersection/([a-z0-9_-]+)/",
            parsed.path,
            flags=re.IGNORECASE,
        )
        lang_match = re.search(r"(?:^|&)lang=([a-z-]+)", parsed.query, flags=re.IGNORECASE)
        portal_match = re.search(r"(?:^|&)portal=(\d+)", parsed.query, flags=re.IGNORECASE)
        if section_match and lang_match and portal_match:
            return (
                f"{parsed.netloc.lower()}|{section_match.group(1)}|"
                f"{lang_match.group(1).lower()}|{portal_match.group(1)}"
            )
        if section_match and lang_match:
            html_portal = re.search(r"portal=(\d+)", html, flags=re.IGNORECASE)
            if html_portal:
                return (
                    f"{parsed.netloc.lower()}|{section_match.group(1)}|"
                    f"{lang_match.group(1).lower()}|{html_portal.group(1)}"
                )
    return ""


def _extract_jibe_site(careers_url: str, html: str, final_url: str) -> str:
    signal = f"{careers_url}\n{final_url}\n{html}"
    base_match = re.search(r"<base href=\"([^\"]+)\"", html, flags=re.IGNORECASE)
    has_jibe_marker = "data-jibe-search-version" in signal.lower()
    if not base_match and not has_jibe_marker:
        return ""
    base_url = base_match.group(1).strip().rstrip("/") if base_match else ""
    if base_url.startswith("/"):
        root = final_url or careers_url
        parsed = urlparse(root)
        if parsed.scheme and parsed.netloc:
            base_url = f"{parsed.scheme}://{parsed.netloc}"
    if base_url and "jibeapply" not in base_url and "icims.com" not in base_url:
        if not has_jibe_marker and "github.careers" not in (final_url or careers_url):
            return ""
    client_match = re.search(r'currentClient\":\"([a-z0-9_-]+)\"', html, flags=re.IGNORECASE)
    client = client_match.group(1).strip() if client_match else _guess_company_slug(careers_url)
    if not client:
        return ""
    ref = final_url or careers_url
    return f"{base_url}|{client}|{ref}"


def _extract_greenhouse_location(item: dict[str, Any], default_location: str) -> str:
    location = item.get("location", {})
    if isinstance(location, dict):
        name = str(location.get("name", "")).strip()
        if name:
            return name
    return default_location


def _extract_lever_location(item: dict[str, Any], default_location: str) -> str:
    categories = item.get("categories", {})
    if isinstance(categories, dict):
        location = str(categories.get("location", "")).strip()
        if location:
            return location
    return default_location


def _extract_ashby_location(item: dict[str, Any], default_location: str) -> str:
    location = item.get("location")
    if isinstance(location, str) and location.strip():
        return location.strip()
    location_obj = item.get("locationName")
    if isinstance(location_obj, str) and location_obj.strip():
        return location_obj.strip()
    return default_location


def _extract_workday_location(item: dict[str, Any], default_location: str) -> str:
    location = item.get("locationsText")
    if isinstance(location, str) and location.strip():
        return location.strip()
    locations = item.get("locations")
    if isinstance(locations, list) and locations:
        first = locations[0]
        if isinstance(first, dict):
            name = str(first.get("displayText", "")).strip()
            if name:
                return name
    return default_location


def _extract_smart_location(item: dict[str, Any], default_location: str) -> str:
    location = item.get("location")
    if isinstance(location, dict):
        city = str(location.get("city", "")).strip()
        region = str(location.get("region", "")).strip()
        country = str(location.get("country", "")).strip()
        parts = [part for part in (city, region, country) if part]
        if parts:
            return ", ".join(parts)
    return default_location


def _extract_taleo_location(item: dict[str, Any], default_location: str) -> str:
    columns = item.get("column", [])
    if isinstance(columns, list) and len(columns) > 1:
        raw = str(columns[1]).strip()
        cleaned = _clean_text(raw).strip("[]\" ")
        if cleaned:
            return cleaned
    return default_location


def _extract_taleo_description(url: str, timeout_seconds: float) -> str:
    status, html = _http_get_text(url, timeout_seconds)
    if status != 200 or not html:
        return ""
    text = _extract_posting_text(html)
    return text[:2600]


def _extract_links(html: str) -> list[tuple[str, str]]:
    matches = re.findall(
        r"<a[^>]+href=[\"']([^\"']+)[\"'][^>]*>(.*?)</a>",
        html,
        flags=re.IGNORECASE | re.DOTALL,
    )
    links: list[tuple[str, str]] = []
    for href, text in matches:
        plain = re.sub(r"<[^>]+>", " ", text)
        clean = re.sub(r"\s+", " ", unescape(plain)).strip()
        if href.strip():
            links.append((href.strip(), clean))
    return links


def _extract_job_urls_from_json_ld(html: str, page_url: str) -> list[tuple[str, str]]:
    matches = re.findall(
        r"<script[^>]+type=[\"']application/ld\+json[\"'][^>]*>(.*?)</script>",
        html,
        flags=re.IGNORECASE | re.DOTALL,
    )
    found: list[tuple[str, str]] = []
    for payload in matches:
        try:
            data = json.loads(payload.strip())
        except Exception:
            continue
        for item in _flatten_jsonld(data):
            if not isinstance(item, dict):
                continue
            if "jobposting" not in str(item.get("@type", "")).lower():
                continue
            raw_url = str(item.get("url", "")).strip()
            if not raw_url:
                continue
            title = str(item.get("title", "")).strip()
            found.append((urljoin(page_url, raw_url), title))
    return found


def _flatten_jsonld(data: object) -> list[object]:
    if isinstance(data, list):
        out: list[object] = []
        for item in data:
            out.extend(_flatten_jsonld(item))
        return out
    if isinstance(data, dict):
        if "@graph" in data:
            return _flatten_jsonld(data.get("@graph"))
        return [data]
    return []


def _discover_job_urls_from_sitemap(careers_url: str, timeout_seconds: float) -> list[str]:
    parsed = urlparse(careers_url)
    root = f"{parsed.scheme}://{parsed.netloc}"
    robots_status, robots = _http_get_text(f"{root}/robots.txt", max(timeout_seconds, 1.0))
    candidates: list[str] = []

    if robots_status == 200:
        for line in robots.splitlines():
            if line.lower().startswith("sitemap:"):
                candidates.append(line.split(":", 1)[1].strip())
    if not candidates:
        candidates = [f"{root}/sitemap.xml"]

    queue = candidates[:4]
    visited: set[str] = set()
    urls: list[str] = []

    while queue and len(visited) < 10:
        sitemap_url = queue.pop(0)
        if sitemap_url in visited:
            continue
        visited.add(sitemap_url)

        status, xml = _http_get_text(sitemap_url, max(timeout_seconds, 1.0))
        if status != 200 or not xml:
            continue

        for loc in _extract_xml_locs(xml)[:2500]:
            lower = loc.lower()
            if lower.endswith(".xml") and len(queue) < 20:
                queue.append(loc)
                continue
            if any(token in lower for token in JOB_PATH_HINTS):
                urls.append(loc)
            if len(urls) >= 250:
                return urls
    return urls


def _extract_xml_locs(payload: str) -> list[str]:
    return [
        unescape(value.strip())
        for value in re.findall(r"<loc>(.*?)</loc>", payload, flags=re.IGNORECASE | re.DOTALL)
        if value.strip()
    ]


def _looks_like_job_url(url: str, label: str) -> bool:
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"}:
        return False

    lowered = url.lower()
    path = parsed.path.lower()
    if parsed.fragment:
        return False
    if any(token in lowered for token in REJECT_PATH_HINTS):
        return False
    if any(token in path for token in ("/search", "/category", "/categories")):
        return False

    segments = [part for part in path.strip("/").split("/") if part]
    if segments in (["jobs"], ["job"], ["positions"], ["careers", "jobs"]):
        return False
    if any(hint in path for hint in JOB_PATH_HINTS):
        return True
    if any(part in {"jobs", "job", "positions"} for part in segments) and len(segments) >= 3:
        return True
    if len(segments) >= 2 and segments[0] == "careers":
        slug = segments[-1]
        if re.search(r"[a-f0-9]{8,}", slug):
            return True
        if any(hint in slug for hint in SOFTWARE_ROLE_HINTS):
            return True
        if re.search(
            r"[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}",
            slug,
        ):
            return True

    lowered_label = label.lower()
    return any(token in lowered_label for token in ("engineer", "developer", "firmware"))


def _looks_like_job_hub_url(url: str) -> bool:
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"}:
        return False
    path = parsed.path.lower()
    if any(token in path for token in REJECT_PATH_HINTS):
        return False
    return any(token in path for token in ("/careers", "/jobs", "/job-search", "/search-results"))


def _extract_title(html: str) -> str:
    h1 = re.search(r"<h1[^>]*>(.*?)</h1>", html, flags=re.IGNORECASE | re.DOTALL)
    if h1:
        return _clean_text(h1.group(1))
    title = re.search(r"<title[^>]*>(.*?)</title>", html, flags=re.IGNORECASE | re.DOTALL)
    if title:
        return _clean_text(title.group(1))
    return ""


def _title_from_url(url: str) -> str:
    path = urlparse(url).path.strip("/")
    if not path:
        return "Software Engineer"
    slug = path.split("/")[-1].replace("-", " ").replace("_", " ")
    words = [part for part in slug.split() if part]
    if not words:
        return "Software Engineer"
    return " ".join(word.capitalize() for word in words)[:120]


def _clean_title(text: str) -> str:
    clean = _clean_text(text)
    clean = re.sub(r"(apply now|learn more|view job)$", "", clean, flags=re.IGNORECASE)
    return clean.strip(" -|")


def _extract_posting_text(html: str) -> str:
    body = re.sub(r"(?is)<script.*?>.*?</script>", " ", html)
    body = re.sub(r"(?is)<style.*?>.*?</style>", " ", body)
    body = re.sub(r"(?is)<[^>]+>", " ", body)
    return re.sub(r"\s+", " ", unescape(body)).strip()[:3500]


def _clean_text(text: str) -> str:
    plain = re.sub(r"(?is)<[^>]+>", " ", text)
    return re.sub(r"\s+", " ", unescape(plain)).strip()


def _extract_location(text: str, default_location: str) -> str:
    lowered = text.lower()
    for marker in ("location:", "locations:", "based in"):
        idx = lowered.find(marker)
        if idx >= 0:
            snippet = re.sub(r"\s+", " ", text[idx : idx + 120])
            candidate = snippet.split(":")[-1].strip(" .,-")[:80]
            if candidate:
                return candidate
    if "remote" in lowered:
        return "Remote"
    return default_location


def _extract_location_from_posting(html: str, text: str, default_location: str) -> str:
    candidates: list[str] = []
    candidates.extend(_extract_locations_from_json_ld(html))
    candidates.extend(_extract_locations_from_inline_json(html))
    candidates.extend(_extract_locations_from_meta(html))
    text_location = _extract_location(text, "")
    if text_location:
        candidates.append(text_location)

    for candidate in candidates:
        normalized = _normalize_location_string(candidate)
        if normalized:
            return normalized
    return default_location


def _extract_locations_from_json_ld(html: str) -> list[str]:
    matches = re.findall(
        r"<script[^>]+type=[\"']application/ld\+json[\"'][^>]*>(.*?)</script>",
        html,
        flags=re.IGNORECASE | re.DOTALL,
    )
    out: list[str] = []
    for payload in matches:
        try:
            parsed = json.loads(payload.strip())
        except Exception:
            continue
        for item in _flatten_jsonld(parsed):
            if not isinstance(item, dict):
                continue
            out.extend(_collect_location_candidates(item))
    return out


def _extract_locations_from_inline_json(html: str) -> list[str]:
    blobs = re.findall(
        r"(?is)(?:window\.[A-Za-z0-9_.$]+|var\s+[A-Za-z0-9_.$]+)\s*=\s*(\{.*?\});",
        html,
    )
    out: list[str] = []
    for blob in blobs[:12]:
        try:
            parsed = json.loads(blob)
        except Exception:
            continue
        out.extend(_collect_location_candidates(parsed))
    return out


def _extract_locations_from_meta(html: str) -> list[str]:
    out: list[str] = []
    meta_values = re.findall(
        r"<meta[^>]+content=[\"']([^\"']+)[\"'][^>]*>",
        html,
        flags=re.IGNORECASE,
    )
    for value in meta_values:
        # Common SEO format: "Role | Warsaw, Poland | Engineering"
        parts = [part.strip() for part in value.split("|")]
        for part in parts:
            if _looks_like_location_phrase(part):
                out.append(part)
    return out


def _collect_location_candidates(node: Any) -> list[str]:
    out: list[str] = []

    if isinstance(node, dict):
        lower_keys = {str(key).lower(): key for key in node.keys()}
        for key_lower in (
            "joblocation",
            "location",
            "workplacelocation",
            "primarylocation",
            "multilocation",
            "standardised_multi_location",
            "standardized_multi_location",
        ):
            if key_lower in lower_keys:
                out.extend(_collect_location_candidates(node[lower_keys[key_lower]]))

        if any(key in lower_keys for key in ("addresslocality", "addressregion", "addresscountry")):
            locality = str(node.get(lower_keys.get("addresslocality", ""), "")).strip()
            region = str(node.get(lower_keys.get("addressregion", ""), "")).strip()
            country = str(node.get(lower_keys.get("addresscountry", ""), "")).strip()
            merged = ", ".join(part for part in (locality, region, country) if part)
            if merged:
                out.append(merged)

        for key_lower in (
            "standardisedmapquerylocation",
            "standardizedmapquerylocation",
            "city",
            "country",
            "countryname",
            "state",
            "statecode",
            "displaytext",
            "locationname",
            "formattedaddress",
            "name",
        ):
            if key_lower in lower_keys:
                value = node[lower_keys[key_lower]]
                if isinstance(value, str) and _looks_like_location_phrase(value):
                    out.append(value)

        for value in node.values():
            if isinstance(value, dict | list):
                out.extend(_collect_location_candidates(value))

    elif isinstance(node, list):
        for item in node:
            out.extend(_collect_location_candidates(item))
    elif isinstance(node, str):
        if _looks_like_location_phrase(node):
            out.append(node)
    return out


def _looks_like_location_phrase(value: str) -> bool:
    text = _normalize_location_string(value)
    if not text:
        return False
    lowered = text.lower()
    if len(text) > 120:
        return False
    if any(token in lowered for token in ("job ", "engineer", "apply", "experience", "salary")):
        return False
    # Keep broad enough for global cities while avoiding arbitrary text.
    return bool(re.search(r"[a-z]{2,}", lowered))


def _normalize_location_string(value: str) -> str:
    text = unescape(value or "")
    text = text.strip().strip("[]{}()\"'")
    text = re.sub(r"\s+", " ", text)
    text = re.sub(r"\s*,\s*", ", ", text)
    text = re.sub(r"^(us|usa|u\.s\.)[-\s]+", "US-", text, flags=re.IGNORECASE)
    text = re.sub(r"^(ca|canada)[-\s]+", "CA-", text, flags=re.IGNORECASE)
    text = text.strip(" .,-")
    if not text:
        return ""
    if len(text) > 120:
        return ""
    return text


SALARY_CONTEXT_TOKENS = (
    "salary",
    "compensation",
    "pay",
    "wage",
    "basepay",
    "payrange",
    "salaryrange",
    "remuneration",
)
SALARY_MIN_TOKENS = ("min", "minimum", "from", "low", "start", "starting")
SALARY_MAX_TOKENS = ("max", "maximum", "to", "high", "ceiling", "target")
SALARY_CURRENCY_TOKENS = ("currency", "curr")
SALARY_CADENCE_TOKENS = ("period", "interval", "unit", "frequency", "timeunit", "cadence")
SALARY_CURRENCY_SYMBOL = {
    "USD": "$",
    "US$": "$",
    "CAD": "C$",
    "AUD": "A$",
    "GBP": "GBP ",
    "EUR": "EUR ",
}


def _extract_salary(text: str, *sources: Any) -> str:
    direct = _extract_salary_from_text(text)
    if direct:
        return direct
    structured = _extract_salary_from_sources(sources)
    if structured:
        return structured
    return ""


def _extract_salary_from_text(text: str) -> str:
    if not text:
        return ""
    compact = re.sub(r"\s+", " ", text).strip()
    patterns = (
        (
            r"(?:[A-Z]{1,3}\$|[$€£])\s?\d{1,3}(?:[,\s]\d{3})*(?:\.\d+)?\s*[kK]?"
            r"\s*(?:-|to|–|—)\s*(?:[A-Z]{1,3}\$|[$€£])?\s?\d{1,3}(?:[,\s]\d{3})*(?:\.\d+)?\s*[kK]?"
            r"\s*(?:/year|per year|annually|/hr|per hour)?"
        ),
        (
            r"(?:USD|US\$|CAD|AUD|EUR|GBP|CA\$|C\$|A\$)\s*\d{1,3}(?:[,\s]\d{3})*(?:\.\d+)?\s*[kK]?"
            r"\s*(?:-|to|–|—)\s*(?:USD|US\$|CAD|AUD|EUR|GBP|CA\$|C\$|A\$)?\s*"
            r"\d{1,3}(?:[,\s]\d{3})*(?:\.\d+)?\s*[kK]?"
            r"\s*(?:/year|per year|annually|/hr|per hour)?"
        ),
        (
            r"(?:[A-Z]{1,3}\$|[$€£])\s?\d{1,3}(?:[,\s]\d{3})*(?:\.\d+)?\s*[kK]?"
            r"\s*(?:/year|per year|annually|/hr|per hour)"
        ),
    )
    for pattern in patterns:
        match = re.search(pattern, compact, flags=re.IGNORECASE)
        if match:
            return re.sub(r"\s+", " ", match.group(0)).strip()
    return ""


def _extract_salary_from_sources(sources: tuple[Any, ...]) -> str:
    for source in sources:
        salary = _extract_salary_from_node(source, in_salary_context=False)
        if salary:
            return salary
    return ""


def _extract_salary_from_node(node: Any, *, in_salary_context: bool) -> str:
    if isinstance(node, dict):
        normalized_keys = {_normalize_salary_key(str(key)): key for key in node}
        has_salary_key = any(
            any(token in key for token in SALARY_CONTEXT_TOKENS) for key in normalized_keys
        )
        context = in_salary_context or has_salary_key

        # Direct salary strings often appear as a single field.
        for key, value in node.items():
            if isinstance(value, str):
                key_norm = _normalize_salary_key(str(key))
                if context or any(token in key_norm for token in SALARY_CONTEXT_TOKENS):
                    salary = _extract_salary_from_text(value)
                    if salary:
                        return salary

        if context:
            min_value, max_value = _find_salary_bounds(node)
            currency = _find_salary_currency(node)
            cadence = _find_salary_cadence(node)
            if min_value is not None and max_value is not None:
                return _format_salary_range(min_value, max_value, currency, cadence)
            if min_value is not None:
                return _format_salary_single(min_value, currency, cadence)
            if max_value is not None:
                return _format_salary_single(max_value, currency, cadence)

        for key, value in node.items():
            key_norm = _normalize_salary_key(str(key))
            child_context = context or any(token in key_norm for token in SALARY_CONTEXT_TOKENS)
            salary = _extract_salary_from_node(value, in_salary_context=child_context)
            if salary:
                return salary
        return ""

    if isinstance(node, list):
        for item in node:
            salary = _extract_salary_from_node(item, in_salary_context=in_salary_context)
            if salary:
                return salary
        return ""

    if isinstance(node, str):
        return _extract_salary_from_text(node) if (in_salary_context or node) else ""
    return ""


def _find_salary_bounds(mapping: dict[Any, Any]) -> tuple[float | None, float | None]:
    min_value: float | None = None
    max_value: float | None = None
    for key, value in mapping.items():
        key_norm = _normalize_salary_key(str(key))
        if not isinstance(value, str | int | float):
            continue
        number = _parse_salary_number(value)
        if number is None:
            continue
        if any(token in key_norm for token in SALARY_MIN_TOKENS):
            min_value = number if min_value is None else min(min_value, number)
            continue
        if any(token in key_norm for token in SALARY_MAX_TOKENS):
            max_value = number if max_value is None else max(max_value, number)
            continue
        if any(token in key_norm for token in SALARY_CONTEXT_TOKENS):
            if min_value is None:
                min_value = number
            elif max_value is None and abs(number - min_value) > 1:
                max_value = max(number, min_value)
                min_value = min(number, min_value)
    return (min_value, max_value)


def _find_salary_currency(mapping: dict[Any, Any]) -> str:
    for key, value in mapping.items():
        key_norm = _normalize_salary_key(str(key))
        if any(token in key_norm for token in SALARY_CURRENCY_TOKENS):
            text = str(value).strip().upper()
            if text in SALARY_CURRENCY_SYMBOL:
                return SALARY_CURRENCY_SYMBOL[text]
            if text in {"$", "€", "£"}:
                return text
    return "$"


def _find_salary_cadence(mapping: dict[Any, Any]) -> str:
    for key, value in mapping.items():
        key_norm = _normalize_salary_key(str(key))
        if not any(token in key_norm for token in SALARY_CADENCE_TOKENS):
            continue
        cadence = str(value).strip().lower()
        if any(token in cadence for token in ("year", "annual", "annually", "yr")):
            return "/year"
        if any(token in cadence for token in ("hour", "hourly", "hr")):
            return "/hour"
    return ""


def _normalize_salary_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]", "", value.lower())


def _parse_salary_number(value: str | int | float) -> float | None:
    if isinstance(value, int | float):
        amount = float(value)
    else:
        text = value.strip().replace(",", "")
        text = text.replace("$", "").replace("€", "").replace("£", "")
        match = re.search(r"(\d+(?:\.\d+)?)\s*([kK]?)", text)
        if not match:
            return None
        amount = float(match.group(1))
        if match.group(2):
            amount *= 1000
    if amount < 10:
        return None
    return amount


def _format_salary_number(value: float) -> str:
    return f"{int(round(value)):,}"


def _format_salary_single(value: float, currency_symbol: str, cadence: str) -> str:
    return f"{currency_symbol}{_format_salary_number(value)}{cadence}"


def _format_salary_range(
    min_value: float,
    max_value: float,
    currency_symbol: str,
    cadence: str,
) -> str:
    low = min(min_value, max_value)
    high = max(min_value, max_value)
    return (
        f"{currency_symbol}{_format_salary_number(low)} - "
        f"{currency_symbol}{_format_salary_number(high)}{cadence}"
    )


def _extract_technologies(text: str) -> list[str]:
    lowered = text.lower()
    found: list[str] = []
    for skill in sorted(SKILL_VOCABULARY):
        if " " in skill:
            if skill in lowered:
                found.append(skill)
            continue
        if re.search(rf"\\b{re.escape(skill)}\\b", lowered):
            found.append(skill)
    return found


def _guess_work_type(text: str, default_work_type: str) -> str:
    lowered = text.lower()
    if "remote" in lowered:
        return "remote"
    if "hybrid" in lowered:
        return "hybrid"
    if "onsite" in lowered or "on-site" in lowered:
        return "onsite"
    return default_work_type


def _is_same_or_linked_domain(candidate_url: str, careers_url: str) -> bool:
    candidate_host = urlparse(candidate_url).netloc.lower()
    careers_host = urlparse(careers_url).netloc.lower()
    if not candidate_host or not careers_host:
        return False

    if any(hint in candidate_host for hint in ATS_DOMAIN_HINTS):
        return True

    return _base_domain(candidate_host) == _base_domain(careers_host)


def _base_domain(host: str) -> str:
    parts = [part for part in host.split(".") if part]
    if len(parts) >= 2:
        return ".".join(parts[-2:])
    return host


def _prioritized_companies(companies: list[dict[str, object]]) -> list[dict[str, object]]:
    return list(companies)


def _effective_careers_url(company_name: str, configured_url: str) -> str:
    override = CAREERS_URL_OVERRIDES.get(company_name)
    return override or configured_url


def _dedupe_jobs(jobs: list[dict[str, object]]) -> list[dict[str, object]]:
    out: list[dict[str, object]] = []
    seen: set[str] = set()
    for job in jobs:
        url = str(job.get("job_url", job.get("job_link", ""))).strip()
        if not url or url in seen:
            continue
        seen.add(url)
        out.append(job)
    return out


def _write_cache(cache_path: str, jobs: list[dict[str, object]]) -> None:
    try:
        path = Path(cache_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps({"jobs": jobs[:80]}), encoding="utf-8")
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
