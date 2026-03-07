from __future__ import annotations

import threading
import time

from app.tools import job_discovery as jd
from app.tools.job_discovery import DiscoveryConfig, discover_jobs


class _FakeResponse:
    def __init__(self, payload: str, status: int = 200, url: str = "https://example.com") -> None:
        self._payload = payload
        self.status = status
        self.url = url

    def __enter__(self) -> _FakeResponse:
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def read(self, max_bytes: int) -> bytes:
        return self._payload.encode("utf-8")[:max_bytes]


def test_http_get_text_uses_cache(monkeypatch) -> None:
    calls = {"count": 0}

    def fake_urlopen(request, timeout=1.0):  # noqa: ANN001
        calls["count"] += 1
        return _FakeResponse("ok")

    jd.clear_http_cache()
    jd.configure_http_cache(enabled=True, ttl_seconds=60)
    monkeypatch.setattr(jd, "urlopen", fake_urlopen)

    first = jd._http_get_text("https://example.com/jobs/1", timeout_seconds=1.0)
    second = jd._http_get_text("https://example.com/jobs/1", timeout_seconds=1.0)

    assert first == (200, "ok")
    assert second == (200, "ok")
    assert calls["count"] == 1


def test_http_get_text_without_cache_calls_twice(monkeypatch) -> None:
    calls = {"count": 0}

    def fake_urlopen(request, timeout=1.0):  # noqa: ANN001
        calls["count"] += 1
        return _FakeResponse("ok")

    jd.clear_http_cache()
    jd.configure_http_cache(enabled=False, ttl_seconds=0)
    monkeypatch.setattr(jd, "urlopen", fake_urlopen)

    jd._http_get_text("https://example.com/jobs/1", timeout_seconds=1.0)
    jd._http_get_text("https://example.com/jobs/1", timeout_seconds=1.0)

    assert calls["count"] == 2


def test_discover_jobs_parallel_company_workers(monkeypatch) -> None:
    inflight = {"count": 0, "max": 0}
    lock = threading.Lock()

    def fake_discover_company_jobs(company_cfg: dict[str, object], timeout_seconds: float):
        with lock:
            inflight["count"] += 1
            inflight["max"] = max(inflight["max"], inflight["count"])
        time.sleep(0.05)
        with lock:
            inflight["count"] -= 1
        name = str(company_cfg.get("name", "x"))
        return [{"job_url": f"https://{name}.example/jobs/1", "company": name}]

    monkeypatch.setattr(jd, "_discover_company_jobs", fake_discover_company_jobs)

    jobs = discover_jobs(
        DiscoveryConfig(
            companies=[
                {"name": "a", "careers_url": "https://a.example/careers"},
                {"name": "b", "careers_url": "https://b.example/careers"},
                {"name": "c", "careers_url": "https://c.example/careers"},
                {"name": "d", "careers_url": "https://d.example/careers"},
            ],
            max_jobs=None,
            timeout_seconds=1.0,
            fallback_jobs=[],
            use_fallback=False,
            cache_path=None,
            global_budget_seconds=0,
            company_workers=4,
        )
    )

    assert len(jobs) == 4
    assert inflight["max"] >= 2
