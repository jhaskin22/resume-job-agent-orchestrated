from __future__ import annotations

from app.tools import job_discovery as jd
from app.tools.job_discovery import DiscoveryConfig, discover_jobs


def test_provider_detection_signals() -> None:
    greenhouse_html = '<script src="https://boards.greenhouse.io/embed/job_board/js?for=acme"></script>'
    workday_html = '<iframe src="https://att.wd1.myworkdayjobs.com/en-US/ATTGeneral/login"></iframe>'
    taleo_html = (
        '<a href="https://textron.taleo.net/careersection/bell/'
        'jobsearch.ftl?lang=en&portal=20140753014">Jobs</a>'
    )

    det_greenhouse = jd._detect_ats("https://careers.acme.com", greenhouse_html, "")
    det_workday = jd._detect_ats("https://www.att.jobs/", workday_html, "")
    det_taleo = jd._detect_ats("https://www.bellflight.com/company/careers", taleo_html, "")

    assert det_greenhouse is not None
    assert det_greenhouse.ats_type == "greenhouse"
    assert det_greenhouse.company_key == "acme"

    assert det_workday is not None
    assert det_workday.ats_type == "workday"
    assert "att" in det_workday.company_key

    assert det_taleo is not None
    assert det_taleo.ats_type == "taleo"
    assert det_taleo.company_key.startswith("textron.taleo.net|bell|en|")


def test_discover_jobs_multi_provider_ingestion(monkeypatch) -> None:
    careers_pages = {
        "https://careers.acme.com": (
            200,
            '<script src="https://boards.greenhouse.io/embed/job_board/js?for=acme"></script>',
        ),
        "https://www.att.jobs/": (
            200,
            '<iframe src="https://att.wd1.myworkdayjobs.com/en-US/ATTGeneral/login"></iframe>',
        ),
        "https://www.bellflight.com/company/careers": (
            200,
            '<a href="https://textron.taleo.net/careersection/bell/'
            'jobsearch.ftl?lang=en&portal=20140753014">Jobs</a>',
        ),
        "https://textron.taleo.net/careersection/bell/jobsearch.ftl?lang=en": (
            200,
            "queryString: 'lang=en&amp;portal=20140753014'",
        ),
    }

    def fake_fetch_page_context(url: str, timeout_seconds: float) -> dict[str, object]:
        status, html = careers_pages.get(url, (0, ""))
        return {"status": status, "html": html, "final_url": url}

    def fake_get_json(url: str, timeout_seconds: float):
        if "boards-api.greenhouse.io" in url:
            return {
                "jobs": [
                    {
                        "title": "Software Engineer",
                        "absolute_url": "https://boards.greenhouse.io/acme/jobs/123",
                        "content": "Build backend services with python and aws.",
                        "salaryRange": {
                            "minimum": 130000,
                            "maximum": 170000,
                            "currency": "USD",
                            "period": "year",
                        },
                        "location": {"name": "Remote"},
                    }
                ]
            }
        return None

    def fake_post_json(
        url: str,
        body: dict[str, object],
        timeout_seconds: float,
        headers: dict[str, str] | None = None,
    ):
        if "myworkdayjobs.com" in url:
            return {
                "jobPostings": [
                    {
                        "title": "Principal Network Data Modernization Engineer",
                        "externalPath": "/job/plano/engineer/1",
                        "locationsText": "Plano, TX",
                        "bulletFields": "Design distributed systems with kubernetes and terraform.",
                        "compensation": {"minSalary": 145000, "maxSalary": 190000},
                    }
                ]
            }
        if "careersection/rest/jobboard/searchjobs" in url:
            return {
                "requisitionList": [
                    {
                        "contestNo": "339826",
                        "column": [
                            "Avionics Embedded Software Engineer",
                            '["US-Texas-Fort Worth"]',
                            "03/06/2026",
                        ],
                    }
                ],
                "pagingData": {"currentPageNo": 1, "pageSize": 25, "totalCount": 1},
            }
        return None

    def fake_get_text(url: str, timeout_seconds: float, max_bytes: int = 2_000_000):
        if "careersection/bell/jobdetail.ftl" in url:
            return (
                200,
                "<html><body><h1>Avionics Embedded Software Engineer</h1>"
                "<p>Responsibilities include firmware and embedded development in C++.</p>"
                "<p>Salary: $125,000 - $165,000 per year.</p>"
                "</body></html>",
            )
        return (0, "")

    monkeypatch.setattr(jd, "_fetch_page_context", fake_fetch_page_context)
    monkeypatch.setattr(jd, "_http_get_json", fake_get_json)
    monkeypatch.setattr(jd, "_http_post_json", fake_post_json)
    monkeypatch.setattr(jd, "_http_get_text", fake_get_text)

    jobs = discover_jobs(
        DiscoveryConfig(
            companies=[
                {
                    "name": "Acme",
                    "careers_url": "https://careers.acme.com",
                    "default_location": "United States",
                    "default_work_type": "remote",
                },
                {
                    "name": "Bell",
                    "careers_url": "https://www.bellflight.com/company/careers",
                    "default_location": "United States",
                    "default_work_type": "onsite",
                },
                {
                    "name": "AT&T",
                    "careers_url": "https://www.att.jobs/",
                    "default_location": "United States",
                    "default_work_type": "hybrid",
                },
            ],
            max_jobs=10,
            timeout_seconds=2.0,
            fallback_jobs=[],
            use_fallback=False,
            cache_path=None,
        )
    )

    assert len(jobs) == 3
    providers = {str(item.get("source_provider")) for item in jobs}
    assert providers == {"greenhouse", "workday", "taleo"}
    assert all(item.get("url") for item in jobs)
    assert all(item.get("description") for item in jobs)
    assert all(str(item.get("salary", "")).strip() for item in jobs)


def test_greenhouse_enriches_salary_from_detail_when_missing_in_list(monkeypatch) -> None:
    def fake_get_json(url: str, timeout_seconds: float):
        if "boards-api.greenhouse.io/v1/boards/fastly/jobs/123" in url:
            return {
                "id": 123,
                "content": (
                    "<p>Staff Engineer role.</p>"
                    "<p><strong>Salary:</strong> $211,370 to $253,644 per year.</p>"
                ),
                "location": {"name": "Remote - US"},
                "metadata": [],
            }
        if "boards-api.greenhouse.io/v1/boards/fastly/jobs" in url:
            return {
                "jobs": [
                    {
                        "title": "Staff Engineer - API Services",
                        "absolute_url": "https://www.fastly.com/about/jobs/apply?gh_jid=123",
                        "content": "",
                        "location": {"name": "Remote - US"},
                    }
                ]
            }
        return None

    monkeypatch.setattr(jd, "_http_get_json", fake_get_json)
    jobs = jd._greenhouse_jobs(
        company_name="Fastly",
        board="fastly",
        careers_url="https://www.fastly.com/about/careers",
        default_location="United States",
        default_work_type="remote",
        timeout_seconds=2.0,
    )

    assert len(jobs) == 1
    assert jobs[0]["salary"] == "$211,370 to $253,644 per year"
