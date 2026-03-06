from __future__ import annotations

from app.tools.location_resolver import LocationResolver


def _resolver() -> LocationResolver:
    return LocationResolver(
        {
            "enabled": True,
            "allowed_work_types": ["remote", "hybrid"],
            "metro": "dfw",
            "dfw_center": [32.8998, -97.0403],
            "dfw_radius_miles": 45,
            "registry_path": "app/config/location_registry.json",
            "geocode_cache_path": "var/test_location_geocode_cache.json",
            "unknown_queue_path": "var/test_location_unknown_queue.json",
        }
    )


def test_dfw_alias_normalization_match() -> None:
    resolver = _resolver()
    job = {
        "work_type": "onsite",
        "location": "Grapevine, TX",
        "title": "Software Engineer",
        "job_url": "https://jobs.example.com/123",
    }
    assert resolver.matches_preference(job) is True


def test_remote_unknown_region_reject() -> None:
    resolver = _resolver()
    job = {
        "work_type": "remote",
        "location": "Anywhere",
        "title": "Backend Engineer",
        "job_url": "https://jobs.example.com/456",
    }
    assert resolver.matches_preference(job) is False


def test_non_dfw_onsite_reject() -> None:
    resolver = _resolver()
    job = {
        "work_type": "onsite",
        "location": "Boston, MA",
        "title": "Software Engineer",
        "job_url": "https://jobs.example.com/789",
    }
    assert resolver.matches_preference(job) is False


def test_non_dfw_hybrid_reject() -> None:
    resolver = _resolver()
    job = {
        "work_type": "hybrid",
        "location": "San Francisco, CA",
        "title": "Software Engineer",
        "job_url": "https://jobs.example.com/101",
    }
    assert resolver.matches_preference(job) is False


def test_dfw_hybrid_accept() -> None:
    resolver = _resolver()
    job = {
        "work_type": "hybrid",
        "location": "Arlington, TX",
        "title": "Software Engineer",
        "job_url": "https://jobs.example.com/102",
    }
    assert resolver.matches_preference(job) is True


def test_remote_north_america_accept() -> None:
    resolver = _resolver()
    job = {
        "work_type": "remote",
        "location": "United States",
        "title": "Backend Engineer",
        "job_url": "https://jobs.example.com/201",
    }
    assert resolver.matches_preference(job) is True


def test_remote_non_north_america_reject() -> None:
    resolver = _resolver()
    job = {
        "work_type": "remote",
        "location": "Warsaw, Poland",
        "title": "Backend Engineer",
        "job_url": "https://jobs.example.com/202",
    }
    assert resolver.matches_preference(job) is False
