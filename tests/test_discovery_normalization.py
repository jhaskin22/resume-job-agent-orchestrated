from __future__ import annotations

from app.tools.job_discovery import (
    CompanyDiscoveryStats,
    _extract_salary,
    _finalize_company_jobs,
    _normalize_discovered_job,
    enrich_missing_salaries,
)


def test_normalize_discovered_job_rejects_missing_url() -> None:
    normalized, reason = _normalize_discovered_job(
        {
            "company": "Acme",
            "title": "Software Engineer",
            "description": "Build APIs and distributed systems with python and go.",
            "work_type": "remote",
        }
    )
    assert normalized is None
    assert reason == "missing_url"


def test_normalize_discovered_job_pads_short_description() -> None:
    normalized, reason = _normalize_discovered_job(
        {
            "company": "Acme",
            "title": "Backend Engineer",
            "description": "Great role.",
            "work_type": "remote",
            "job_url": "https://jobs.acme.com/1",
        }
    )
    assert reason is None
    assert normalized is not None
    assert len(str(normalized["description"]).split()) >= 10


def test_finalize_company_jobs_updates_drop_stats() -> None:
    stats = CompanyDiscoveryStats(company="Acme", provider="direct")
    jobs = [
        {
            "company": "Acme",
            "title": "Software Engineer",
            "description": "Build backend services and distributed systems using python and sql.",
            "work_type": "remote",
            "job_url": "https://jobs.acme.com/1",
        },
        {
            "company": "Acme",
            "title": "",
            "description": "Invalid because title missing.",
            "work_type": "remote",
            "job_url": "https://jobs.acme.com/2",
        },
        {
            "company": "Acme",
            "title": "Product Marketing Manager",
            "description": "Marketing strategy and campaigns for product launches.",
            "work_type": "hybrid",
            "job_url": "https://jobs.acme.com/3",
        },
    ]
    kept = _finalize_company_jobs(jobs, stats=stats)
    assert len(kept) == 1
    assert stats.deduped == 3
    assert stats.normalized == 2
    assert stats.dropped_normalization == 1
    assert stats.dropped_relevance == 1


def test_extract_salary_from_structured_fields() -> None:
    salary = _extract_salary(
        "",
        {
            "compensationRange": {
                "minimum": 120000,
                "maximum": 155000,
                "currency": "USD",
                "period": "year",
            }
        },
    )
    assert salary == "$120,000 - $155,000/year"


def test_extract_salary_supports_k_notation() -> None:
    salary = _extract_salary("Compensation: $140k - $185k per year")
    assert "$140k - $185k per year" == salary


def test_extract_salary_supports_currency_prefix_symbol() -> None:
    salary = _extract_salary(
        "The salary range for this role in the primary location is CA$135,200 - CA$202,800."
    )
    assert salary == "CA$135,200 - CA$202,800"


def test_enrich_missing_salaries_backfills_from_posting(monkeypatch) -> None:
    def fake_get_text(url: str, timeout_seconds: float, max_bytes: int = 2_000_000):
        return (
            200,
            "<html><body><p>Salary: $150,000 - $190,000 per year.</p></body></html>",
        )

    monkeypatch.setattr("app.tools.job_discovery._http_get_text", fake_get_text)
    jobs = [
        {
            "company": "Acme",
            "title": "Platform Engineer",
            "job_url": "https://jobs.acme.com/123",
            "salary": "",
        }
    ]
    enriched = enrich_missing_salaries(jobs, timeout_seconds=1.0, max_lookups=5)
    assert enriched[0]["salary"] == "$150,000 - $190,000 per year"
