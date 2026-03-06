from __future__ import annotations

from app.tools.scoring import score_jobs


def _base_resume() -> dict[str, object]:
    return {
        "raw_text": (
            "senior backend engineer building distributed systems, "
            "api services, sql, python, go"
        ),
        "skills": ["python", "go", "sql", "docker", "kubernetes", "aws", "git"],
    }


def _career_cfg() -> dict[str, object]:
    return {
        "career_path": {
            "target_titles": [
                "Senior Software Engineer",
                "Backend Engineer",
                "Platform Engineer",
                "Infrastructure Engineer",
                "Distributed Systems Engineer",
                "Cloud Engineer",
                "Systems Software Engineer",
            ],
            "responsibility_focus": [
                "backend systems",
                "backend services",
                "distributed systems",
                "cloud platforms",
                "api",
                "microservices",
            ],
            "must_have_keywords": [
                "java",
                "python",
                "go",
                "distributed systems",
                "api",
                "microservices",
                "sql",
                "git",
            ],
            "nice_to_have_keywords": [
                "aws",
                "docker",
                "kubernetes",
                "infrastructure",
                "devops",
                "ci/cd",
                "reliability engineering",
            ],
            "allow_management_roles": False,
            "allowed_levels": ["mid", "senior"],
            "discouraged_levels": ["staff", "principal", "director", "senior_manager"],
            "strong_match_override": 82,
            "salary": {
                "minimum": 110000,
                "target_min": 130000,
                "target_max": 180000,
                "prefer_disclosed_tiebreaker": True,
            },
            "weights": {
                "resume_fit": 0.42,
                "career_fit": 0.28,
                "must_have_fit": 0.18,
                "nice_to_have_fit": 0.12,
            },
        }
    }


def test_management_roles_filtered_when_not_allowed() -> None:
    jobs = [
        {
            "job_link": "https://example.com/jobs/1",
            "title": "Senior Manager, Software Engineering",
            "description": "Lead teams building distributed systems and cloud APIs.",
            "location": "Plano, Texas, United States",
            "work_type": "hybrid",
            "salary": "$160,000 - $190,000",
        },
        {
            "job_link": "https://example.com/jobs/2",
            "title": "Senior Backend Engineer",
            "description": (
                "Build backend services, APIs, and distributed systems "
                "using Python and Go."
            ),
            "location": "Plano, Texas, United States",
            "work_type": "hybrid",
            "salary": "$160,000 - $190,000",
        },
    ]
    parsed_jobs = {
        job["job_link"]: {
            "required_skills": ["python", "go", "sql"],
            "technologies": ["aws"],
        }
        for job in jobs
    }
    scored = score_jobs(
        _base_resume(),
        jobs,
        parsed_jobs=parsed_jobs,
        top_k=0,
        scoring_config=_career_cfg(),
    )
    assert len(scored) == 1
    assert scored[0]["title"] == "Senior Backend Engineer"


def test_missing_salary_is_neutral_not_penalized() -> None:
    jobs = [
        {
            "job_link": "https://example.com/jobs/3",
            "title": "Backend Engineer",
            "description": "Build distributed systems and APIs with Python and SQL.",
            "location": "Remote - US",
            "work_type": "remote",
            "salary": "",
        },
        {
            "job_link": "https://example.com/jobs/4",
            "title": "Backend Engineer",
            "description": "Build distributed systems and APIs with Python and SQL.",
            "location": "Remote - US",
            "work_type": "remote",
            "salary": "$135,000 - $170,000",
        },
    ]
    parsed_jobs = {
        job["job_link"]: {
            "required_skills": ["python", "sql"],
            "technologies": [],
        }
        for job in jobs
    }
    scored = score_jobs(
        _base_resume(),
        jobs,
        parsed_jobs=parsed_jobs,
        top_k=0,
        scoring_config=_career_cfg(),
    )
    by_link = {item["job_link"]: item for item in scored}
    undisclosed = by_link["https://example.com/jobs/3"]
    disclosed = by_link["https://example.com/jobs/4"]
    assert undisclosed["ranking_debug"]["salary_signal"] == "undisclosed"
    assert disclosed["match_score"] >= undisclosed["match_score"]


def test_target_roles_rank_above_unrelated_engineering_roles() -> None:
    jobs = [
        {
            "job_link": "https://example.com/jobs/5",
            "title": "Platform Engineer",
            "description": (
                "Own cloud platform reliability, APIs, microservices, "
                "and distributed systems."
            ),
            "location": "Plano, Texas, United States",
            "work_type": "hybrid",
            "salary": "",
        },
        {
            "job_link": "https://example.com/jobs/6",
            "title": "QA Automation Engineer",
            "description": "Build automated test suites and manual validation workflows.",
            "location": "Plano, Texas, United States",
            "work_type": "hybrid",
            "salary": "",
        },
    ]
    parsed_jobs = {
        "https://example.com/jobs/5": {
            "required_skills": ["python", "go", "sql"],
            "technologies": ["aws", "kubernetes"],
        },
        "https://example.com/jobs/6": {
            "required_skills": ["python"],
            "technologies": [],
        },
    }
    scored = score_jobs(
        _base_resume(),
        jobs,
        parsed_jobs=parsed_jobs,
        top_k=0,
        scoring_config=_career_cfg(),
    )
    assert scored[0]["job_link"] == "https://example.com/jobs/5"
