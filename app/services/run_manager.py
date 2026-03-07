from __future__ import annotations

import logging
import os
import sys
import threading
import time
import traceback
from collections import Counter
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from app.core.config import settings
from app.models.schemas import (
    JobMatchTile,
    RunWorkflowResponse,
    WorkflowDiagnostics,
    WorkflowRunStatusResponse,
)
from app.services.orchestrator import ResumeJobOrchestrator
from app.services.run_ids import next_run_id
from app.tools.job_discovery import _discover_company_jobs, configure_http_cache
from app.tools.job_parsing import parse_job_details
from app.tools.location_resolver import LocationResolver
from app.tools.scoring import score_jobs
from app.workflow.io import parse_resume_content, rewrite_resume_docx, safe_stem, write_resume_docx

logger = logging.getLogger(__name__)
FINAL_STAGE_TIMEOUT_SECONDS = 20 * 60


@dataclass(slots=True)
class _RunRecord:
    run_id: int
    status: str
    created_at: str
    updated_at: str
    started_monotonic: float = 0.0
    progress_current: int = 0
    progress_total: int = 0
    progress_company: str = ""
    current_stage: str = "queued"
    stage_started_at: float = 0.0
    tiles: list[dict[str, Any]] = field(default_factory=list)
    resume_filename: str = ""
    resume_file_bytes: bytes = b""
    parsed_resume: dict[str, Any] = field(default_factory=dict)
    discovered_jobs: list[dict[str, Any]] = field(default_factory=list)
    generated_resumes: dict[str, str] = field(default_factory=dict)
    diagnostics: dict[str, Any] | None = None
    watchdog_timed_out: bool = False


class WorkflowRunManager:
    def __init__(self, orchestrator: ResumeJobOrchestrator) -> None:
        self._orchestrator = orchestrator
        self._lock = threading.Lock()
        self._runs: dict[int, _RunRecord] = {}

    def start_run(self, resume_filename: str, resume_file_bytes: bytes) -> int:
        run_id = next_run_id()
        now = datetime.now(UTC).isoformat()
        started_monotonic = time.perf_counter()
        with self._lock:
            self._runs[run_id] = _RunRecord(
                run_id=run_id,
                status="queued",
                created_at=now,
                updated_at=now,
                started_monotonic=started_monotonic,
                current_stage="queued",
                stage_started_at=time.time(),
                diagnostics=None,
            )
        logger.info(
            "run_lifecycle event=queued run_id=%s created_at=%s resume=%s size_bytes=%s",
            run_id,
            now,
            resume_filename,
            len(resume_file_bytes),
        )
        thread = threading.Thread(
            target=self._execute_run,
            args=(run_id, resume_filename, resume_file_bytes),
            daemon=True,
        )
        thread.start()
        return run_id

    def get_status(self, run_id: int) -> WorkflowRunStatusResponse | None:
        with self._lock:
            record = self._runs.get(run_id)
            if record is None:
                return None
            tiles = [JobMatchTile(**tile) for tile in record.tiles]
            diagnostics_payload = record.diagnostics
            stage_elapsed_seconds = int(max(0.0, time.time() - float(record.stage_started_at)))
        diagnostics = (
            WorkflowDiagnostics(**diagnostics_payload) if diagnostics_payload is not None else None
        )
        return WorkflowRunStatusResponse(
            run_id=record.run_id,
            status=record.status,
            progress_current=record.progress_current,
            progress_total=record.progress_total,
            progress_company=record.progress_company,
            current_stage=record.current_stage,
            stage_elapsed_seconds=stage_elapsed_seconds,
            tiles=tiles,
            diagnostics=diagnostics,
        )

    def _update(self, run_id: int, **kwargs: Any) -> None:
        with self._lock:
            record = self._runs.get(run_id)
            if record is None:
                return
            if record.watchdog_timed_out and kwargs.get("status") in {"running", "completed"}:
                return
            for key, value in kwargs.items():
                setattr(record, key, value)
            if "current_stage" in kwargs:
                record.stage_started_at = time.time()
            record.updated_at = datetime.now(UTC).isoformat()

    def _set_stage(self, run_id: int, stage: str) -> None:
        self._update(run_id, current_stage=stage)
        logger.info("run_lifecycle event=stage run_id=%s stage=%s", run_id, stage)

    def _set_timeout_failure(self, run_id: int, reason: str, stack: str) -> None:
        self._update(
            run_id,
            status="failed",
            watchdog_timed_out=True,
            diagnostics=WorkflowDiagnostics(
                failed=True,
                verification={},
                errors=[reason, stack],
            ).model_dump(),
        )

    def _watchdog(self, run_id: int, target_thread_ident: int) -> None:
        while True:
            time.sleep(2)
            with self._lock:
                record = self._runs.get(run_id)
                if record is None:
                    return
                status = record.status
                stage = record.current_stage
                stage_started = float(record.stage_started_at)
                timed_out = record.watchdog_timed_out
            if status in {"failed", "completed"} or timed_out:
                return
            if stage != "final_workflow":
                continue
            if time.time() - stage_started < FINAL_STAGE_TIMEOUT_SECONDS:
                continue
            frame = sys._current_frames().get(target_thread_ident)
            stack = (
                "".join(traceback.format_stack(frame))
                if frame is not None
                else "No stack frame available for workflow thread."
            )
            self._set_timeout_failure(
                run_id,
                f"run_manager timeout: final_workflow exceeded {FINAL_STAGE_TIMEOUT_SECONDS}s",
                stack[:12000],
            )
            logger.error("run_manager watchdog timeout run_id=%s", run_id)
            return

    def _current_tiles(self, run_id: int) -> list[dict[str, Any]]:
        with self._lock:
            record = self._runs.get(run_id)
            if record is None:
                return []
            return [dict(tile) for tile in record.tiles]

    def _merge_tiles(
        self,
        preview_tiles: list[dict[str, Any]],
        final_tiles: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        by_key: dict[str, dict[str, Any]] = {}
        for tile in preview_tiles:
            key = str(tile.get("job_link", "")).strip() or f"preview:{len(by_key)}"
            by_key[key] = dict(tile)
        for tile in final_tiles:
            key = str(tile.get("job_link", "")).strip() or f"final:{len(by_key)}"
            existing = by_key.get(key, {})
            by_key[key] = {**existing, **dict(tile)}
        merged = list(by_key.values())
        merged.sort(key=lambda item: float(item.get("match_score", 0.0)), reverse=True)
        return merged

    def _execute_run(self, run_id: int, resume_filename: str, resume_file_bytes: bytes) -> None:
        started_at_utc = datetime.now(UTC).isoformat()
        logger.info(
            "run_lifecycle event=started run_id=%s started_at=%s",
            run_id,
            started_at_utc,
        )
        self._update(
            run_id,
            status="running",
            resume_filename=resume_filename,
            resume_file_bytes=resume_file_bytes,
        )
        self._set_stage(run_id, "preview_discovery")
        watcher = threading.Thread(
            target=self._watchdog,
            args=(run_id, threading.get_ident()),
            daemon=True,
        )
        watcher.start()
        try:
            preview = self._stream_preview(run_id, resume_filename, resume_file_bytes)
            self._update(
                run_id,
                parsed_resume=preview["parsed_resume"],
                discovered_jobs=preview["discovered_jobs"],
            )
            self._set_stage(run_id, "final_workflow")
            final = self._orchestrator.run(
                resume_filename,
                resume_file_bytes,
                run_id=run_id,
            )
            with self._lock:
                record = self._runs.get(run_id)
                if record is not None and record.watchdog_timed_out:
                    return
            merged_tiles = self._merge_tiles(
                self._current_tiles(run_id),
                [tile.model_dump() for tile in final.tiles],
            )
            self._set_stage(run_id, "completed")
            self._update(
                run_id,
                status="completed" if not final.diagnostics.failed else "failed",
                tiles=merged_tiles,
                diagnostics=final.diagnostics.model_dump(),
            )
            with self._lock:
                record = self._runs.get(run_id)
                elapsed = (
                    (time.perf_counter() - record.started_monotonic)
                    if record is not None and record.started_monotonic > 0
                    else 0.0
                )
            finished_at_utc = datetime.now(UTC).isoformat()
            logger.info(
                (
                    "run_lifecycle event=finished run_id=%s status=%s "
                    "finished_at=%s elapsed_seconds=%.2f tiles=%s diagnostics_failed=%s"
                ),
                run_id,
                "completed" if not final.diagnostics.failed else "failed",
                finished_at_utc,
                elapsed,
                len(merged_tiles),
                bool(final.diagnostics.failed),
            )
        except Exception as exc:  # noqa: BLE001
            diagnostics = RunWorkflowResponse(
                run_id=run_id,
                tiles=[],
                diagnostics=WorkflowDiagnostics(
                    failed=True,
                    verification={},
                    errors=[f"run_manager: {exc}"],
                ),
            ).diagnostics
            self._set_stage(run_id, "failed")
            self._update(run_id, status="failed", diagnostics=diagnostics.model_dump())
            with self._lock:
                record = self._runs.get(run_id)
                elapsed = (
                    (time.perf_counter() - record.started_monotonic)
                    if record is not None and record.started_monotonic > 0
                    else 0.0
                )
            logger.exception(
                "run_lifecycle event=failed run_id=%s elapsed_seconds=%.2f error=%s",
                run_id,
                elapsed,
                exc,
            )

    def _stream_preview(
        self, run_id: int, resume_filename: str, resume_file_bytes: bytes
    ) -> dict[str, Any]:
        workflow_config = self._orchestrator._nodes.workflow_config  # noqa: SLF001
        discovery_cfg = workflow_config.get("job_discovery", {})
        configure_http_cache(
            enabled=bool(discovery_cfg.get("http_cache_enabled", True)),
            ttl_seconds=float(discovery_cfg.get("http_cache_ttl_seconds", 180.0)),
        )
        companies = [dict(item) for item in discovery_cfg.get("companies", [])]
        self._update(run_id, progress_total=len(companies))

        try:
            resume_text = parse_resume_content(resume_filename, resume_file_bytes)
            parsed_resume = self._orchestrator._nodes._parse_resume_text(resume_text)  # noqa: SLF001
        except Exception:
            parsed_resume = {}

        location_cfg = discovery_cfg.get("location_preferences", {})
        resolver = LocationResolver(location_cfg)
        discovered: list[dict[str, Any]] = []

        if os.getenv("PYTEST_CURRENT_TEST"):
            seeded = self._orchestrator._nodes.job_discovery({}).get("discovered_jobs", {})  # noqa: SLF001
            if isinstance(seeded, list):
                discovered = [dict(item) for item in seeded if isinstance(item, dict)]
            preview_tiles = self._build_preview_tiles(run_id, parsed_resume, discovered, resolver)
            self._update(
                run_id,
                progress_total=1,
                progress_current=1,
                progress_company="pytest-seed",
                tiles=preview_tiles,
            )
            return {"parsed_resume": parsed_resume, "discovered_jobs": discovered}

        for idx, company in enumerate(companies, start=1):
            company_name = str(company.get("name", ""))
            timeout = float(discovery_cfg.get("fetch_timeout_seconds", 4.0))
            company_jobs = _discover_company_jobs(company, timeout)
            invalid_reasons = Counter[str]()
            location_reasons = Counter[str]()
            kept_count = 0
            review_bucket_count = 0

            for job in company_jobs:
                invalid_reason = self._orchestrator._nodes._discovered_job_invalid_reason(job)  # noqa: SLF001
                if invalid_reason is not None:
                    invalid_reasons[invalid_reason] += 1
                    continue
                matched, location_reason = resolver.match_decision(job)
                if not matched:
                    location_reasons[location_reason] += 1
                    if self._should_route_to_review_bucket(location_reason, job):
                        review_bucket_count += 1
                        discovered.append(
                            {
                                **job,
                                "title": str(job.get("title", "")).strip(),
                                "role": str(job.get("role") or job.get("title", "")).strip(),
                                "job_url": str(
                                    job.get("job_url", job.get("job_link", ""))
                                ).strip(),
                                "verified_url": str(
                                    job.get(
                                        "verified_url",
                                        job.get("job_url", job.get("job_link", "")),
                                    )
                                ).strip(),
                                "job_link": str(
                                    job.get("job_link", job.get("job_url", ""))
                                ).strip(),
                                "location_review_bucket": True,
                                "location_filter_reason": location_reason,
                            }
                        )
                    continue
                kept_count += 1
                discovered.append(
                    {
                        **job,
                        "title": str(job.get("title", "")).strip(),
                        "role": str(job.get("role") or job.get("title", "")).strip(),
                        "job_url": str(job.get("job_url", job.get("job_link", ""))).strip(),
                        "verified_url": str(
                            job.get("verified_url", job.get("job_url", job.get("job_link", "")))
                        ).strip(),
                        "job_link": str(job.get("job_link", job.get("job_url", ""))).strip(),
                    }
                )
            logger.info(
                (
                    "preview_discovery company=%s fetched=%s kept=%s review_bucket=%s "
                    "drop_validation=%s drop_location=%s cumulative=%s"
                ),
                company_name,
                len(company_jobs),
                kept_count,
                review_bucket_count,
                dict(invalid_reasons),
                dict(location_reasons),
                len(discovered),
            )

            preview_tiles = self._build_preview_tiles(run_id, parsed_resume, discovered, resolver)
            self._update(
                run_id,
                progress_current=idx,
                progress_company=company_name,
                tiles=preview_tiles,
            )
        return {"parsed_resume": parsed_resume, "discovered_jobs": discovered}

    def generate_resume_for_job(self, run_id: int, job_link: str) -> str:
        normalized_link = str(job_link).strip()
        if not normalized_link:
            raise ValueError("job_link is required.")

        with self._lock:
            record = self._runs.get(run_id)
            if record is None:
                raise KeyError("Run not found.")
            existing = record.generated_resumes.get(normalized_link, "")
            if existing:
                return existing
            resume_filename = record.resume_filename
            resume_file_bytes = record.resume_file_bytes
            parsed_resume = dict(record.parsed_resume)
            discovered_jobs = [dict(item) for item in record.discovered_jobs]
            tiles = [dict(item) for item in record.tiles]

        if not resume_file_bytes:
            raise ValueError("Resume payload is unavailable for this run.")

        tile = next(
            (
                item
                for item in tiles
                if str(item.get("job_link", "")).strip() == normalized_link
            ),
            None,
        )
        if tile is None:
            raise ValueError("Job link not found in run tiles.")

        job = next(
            (
                item
                for item in discovered_jobs
                if str(item.get("job_link", "")).strip() == normalized_link
            ),
            None,
        ) or {
            "company": tile.get("company", ""),
            "title": tile.get("title", ""),
            "job_link": normalized_link,
            "description": "",
            "location": tile.get("location", ""),
            "work_type": tile.get("work_type", ""),
            "salary": tile.get("salary", ""),
        }

        parsed_job = parse_job_details(job)
        base_keywords = [
            str(item)
            for item in (
                parsed_job.get("ats_keywords", [])[:6]
                or parsed_job.get("required_skills", [])[:6]
                or parsed_resume.get("skills", [])[:6]
                or ["python", "api", "distributed systems"]
            )
        ]
        emphasis_keywords = [
            str(tile.get("title", "")),
            str(tile.get("company", "")),
            *base_keywords,
        ]

        stem = safe_stem(resume_filename or "resume.docx")
        output_filename = f"{stem}-{run_id}-{abs(hash(normalized_link)) % 10_000_000:07d}.docx"
        output_path = settings.generated_resume_dir / output_filename

        is_docx_input = str(resume_filename).lower().endswith(".docx")
        if is_docx_input:
            rewrite_resume_docx(
                payload=resume_file_bytes,
                output_path=output_path,
                emphasis_keywords=emphasis_keywords,
                max_rewrites=int(
                    self._orchestrator._nodes.workflow_config["nodes"]["resume_generation"].get(  # noqa: SLF001
                        "max_bullet_rewrites",
                        2,
                    )
                ),
            )
        else:
            write_resume_docx(
                (
                    f"Target Company: {tile.get('company', '')}\n"
                    f"Target Role: {tile.get('title', '')}\n"
                    f"ATS Keywords: {', '.join(emphasis_keywords)}\n"
                ),
                output_path,
            )

        output_link = f"/api/resumes/{output_filename}"
        with self._lock:
            record = self._runs.get(run_id)
            if record is None:
                raise KeyError("Run not found.")
            record.generated_resumes[normalized_link] = output_link
            for item in record.tiles:
                if str(item.get("job_link", "")).strip() == normalized_link:
                    item["generated_resume_link"] = output_link
                    break
            record.updated_at = datetime.now(UTC).isoformat()
        return output_link

    def _build_preview_tiles(
        self,
        run_id: int,
        parsed_resume: dict[str, Any],
        discovered_jobs: list[dict[str, Any]],
        resolver: LocationResolver,
    ) -> list[dict[str, Any]]:
        if not discovered_jobs:
            return []
        workflow_config = self._orchestrator._nodes.workflow_config  # noqa: SLF001
        top_k = int(workflow_config["nodes"]["job_scoring"]["top_k"])
        min_score = float(workflow_config["nodes"]["job_scoring"]["min_match_score"])
        max_per_company = int(workflow_config["nodes"]["job_scoring"].get("max_per_company", 0))

        parsed_map = {
            str(job.get("job_link", "")): parse_job_details(job) for job in discovered_jobs
        }
        scored = score_jobs(
            parsed_resume,
            discovered_jobs,
            parsed_jobs=parsed_map,
            top_k=0 if top_k <= 0 else max(top_k, len(discovered_jobs)),
            scoring_config=workflow_config["nodes"]["job_scoring"],
        )
        scored = [job for job in scored if float(job.get("match_score", 0)) >= min_score]

        company_counts: dict[str, int] = {}
        output: list[dict[str, Any]] = []
        for job in scored:
            company_key = str(job.get("company", "")).lower().strip()
            used = company_counts.get(company_key, 0)
            if max_per_company > 0 and used >= max_per_company:
                continue
            company_counts[company_key] = used + 1
            summary = (
                f"{job.get('company', '')} aligns with your profile for "
                f"{job.get('title', '')} (preview while discovery is running)."
            )
            tile = JobMatchTile(
                run_id=run_id,
                company=str(job.get("company", "")),
                title=str(job.get("title", "")),
                location=self._preview_location_label(job, resolver),
                salary=str(job.get("salary", "")) or None,
                work_type=str(job.get("work_type", "remote")),
                match_score=float(job.get("match_score", 0.0)),
                resume_alignment=float(job.get("resume_alignment", 0.0)),
                ats_score=0.0,
                job_link=str(job.get("job_link", "https://example.com")),
                generated_resume_link="",
                summary=summary,
            )
            output.append(tile.model_dump())
            if top_k > 0 and len(output) >= top_k:
                break
        return output

    def _preview_location_label(
        self,
        job: dict[str, Any],
        resolver: LocationResolver,
    ) -> str:
        work_type = str(job.get("work_type", "")).strip().lower()
        location = str(job.get("location", "")).strip()
        if work_type == "remote":
            if resolver.is_dfw_applicable(job):
                city = resolver.dfw_city_label(location)
                return f"{city} Remote" if city else "DFW Remote"
            return "Remote"
        if work_type == "hybrid":
            if resolver.is_dfw_applicable(job):
                city = resolver.dfw_city_label(location)
                return f"{city} Hybrid" if city else "DFW Hybrid"
            return "Hybrid"
        return location or "Unknown"

    def _should_route_to_review_bucket(
        self,
        location_reason: str,
        job: dict[str, Any],
    ) -> bool:
        reviewable_reasons = {"location_missing", "location_unresolved", "metro_no_match"}
        if location_reason not in reviewable_reasons:
            return False
        if str(job.get("work_type", "")).strip().lower() == "remote":
            return False
        return True
