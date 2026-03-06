from __future__ import annotations

from app.services.run_manager import WorkflowRunManager
from app.tools import scoring
from app.tools.job_discovery import DiscoveryConfig, discover_jobs


def test_score_jobs_top_k_zero_returns_all() -> None:
    parsed_resume = {"raw_text": "python api docker kubernetes", "skills": ["python", "docker"]}
    discovered = [
        {
            "job_link": "https://example.com/jobs/1",
            "title": "Software Engineer",
            "description": "python",
        },
        {
            "job_link": "https://example.com/jobs/2",
            "title": "Platform Engineer",
            "description": "docker",
        },
        {
            "job_link": "https://example.com/jobs/3",
            "title": "Backend Engineer",
            "description": "api",
        },
    ]
    parsed_jobs = {
        item["job_link"]: {"required_skills": [], "technologies": [], "experience_level": "mid"}
        for item in discovered
    }
    scored = scoring.score_jobs(parsed_resume, discovered, parsed_jobs=parsed_jobs, top_k=0)
    assert len(scored) == len(discovered)


def test_discovery_zero_budget_and_cap_means_unlimited(monkeypatch) -> None:
    called: list[str] = []

    def fake_discover_company_jobs(company_cfg: dict[str, object], timeout_seconds: float):
        called.append(str(company_cfg.get("name")))
        base = f"https://{str(company_cfg.get('name', 'x')).lower()}.example/jobs"
        return [
            {
                "company": str(company_cfg.get("name", "")),
                "title": "Software Engineer",
                "role": "Software Engineer",
                "location": "Plano, Texas, United States",
                "salary": "",
                "work_type": "hybrid",
                "job_url": f"{base}/1",
                "verified_url": f"{base}/1",
                "job_link": f"{base}/1",
                "url": f"{base}/1",
                "description": "Build software systems and APIs with python and docker.",
                "source_provider": "workday",
            },
            {
                "company": str(company_cfg.get("name", "")),
                "title": "Platform Engineer",
                "role": "Platform Engineer",
                "location": "Plano, Texas, United States",
                "salary": "",
                "work_type": "hybrid",
                "job_url": f"{base}/2",
                "verified_url": f"{base}/2",
                "job_link": f"{base}/2",
                "url": f"{base}/2",
                "description": "Platform engineering with kubernetes and terraform.",
                "source_provider": "workday",
            },
        ]

    monkeypatch.setattr(
        "app.tools.job_discovery._discover_company_jobs",
        fake_discover_company_jobs,
    )

    jobs = discover_jobs(
        DiscoveryConfig(
            companies=[
                {"name": "A", "careers_url": "https://a.example/careers"},
                {"name": "B", "careers_url": "https://b.example/careers"},
                {"name": "C", "careers_url": "https://c.example/careers"},
            ],
            max_jobs=None,
            timeout_seconds=1.0,
            fallback_jobs=[],
            use_fallback=False,
            cache_path=None,
            max_jobs_per_company=0,
            global_budget_seconds=0,
        )
    )

    assert called == ["A", "B", "C"]
    assert len(jobs) == 6


def test_run_manager_merge_tiles_keeps_preview_and_final() -> None:
    manager = WorkflowRunManager(orchestrator=None)  # type: ignore[arg-type]
    preview = [
        {"job_link": "https://example.com/jobs/1", "match_score": 40.0, "company": "A"},
        {"job_link": "https://example.com/jobs/2", "match_score": 42.0, "company": "B"},
    ]
    final = [
        {"job_link": "https://example.com/jobs/2", "match_score": 55.0, "company": "B"},
        {"job_link": "https://example.com/jobs/3", "match_score": 50.0, "company": "C"},
    ]
    merged = manager._merge_tiles(preview, final)  # noqa: SLF001
    assert [item["job_link"] for item in merged] == [
        "https://example.com/jobs/2",
        "https://example.com/jobs/3",
        "https://example.com/jobs/1",
    ]
