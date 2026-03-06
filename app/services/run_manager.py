from __future__ import annotations

import threading
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from app.models.schemas import (
    JobMatchTile,
    RunWorkflowResponse,
    WorkflowDiagnostics,
    WorkflowRunStatusResponse,
)
from app.services.orchestrator import ResumeJobOrchestrator
from app.tools.job_discovery import _discover_company_jobs
from app.tools.job_parsing import parse_job_details
from app.tools.location_resolver import LocationResolver
from app.tools.scoring import score_jobs
from app.workflow.io import parse_resume_content


@dataclass(slots=True)
class _RunRecord:
    run_id: str
    status: str
    created_at: str
    updated_at: str
    progress_current: int = 0
    progress_total: int = 0
    progress_company: str = ""
    tiles: list[dict[str, Any]] = field(default_factory=list)
    diagnostics: dict[str, Any] | None = None


class WorkflowRunManager:
    def __init__(self, orchestrator: ResumeJobOrchestrator) -> None:
        self._orchestrator = orchestrator
        self._lock = threading.Lock()
        self._runs: dict[str, _RunRecord] = {}

    def start_run(self, resume_filename: str, resume_file_bytes: bytes) -> str:
        run_id = str(uuid4())
        now = datetime.now(UTC).isoformat()
        with self._lock:
            self._runs[run_id] = _RunRecord(
                run_id=run_id,
                status="queued",
                created_at=now,
                updated_at=now,
                diagnostics=None,
            )
        thread = threading.Thread(
            target=self._execute_run,
            args=(run_id, resume_filename, resume_file_bytes),
            daemon=True,
        )
        thread.start()
        return run_id

    def get_status(self, run_id: str) -> WorkflowRunStatusResponse | None:
        with self._lock:
            record = self._runs.get(run_id)
            if record is None:
                return None
            tiles = [JobMatchTile(**tile) for tile in record.tiles]
            diagnostics_payload = record.diagnostics
        diagnostics = (
            WorkflowDiagnostics(**diagnostics_payload) if diagnostics_payload is not None else None
        )
        return WorkflowRunStatusResponse(
            run_id=record.run_id,
            status=record.status,
            progress_current=record.progress_current,
            progress_total=record.progress_total,
            progress_company=record.progress_company,
            tiles=tiles,
            diagnostics=diagnostics,
        )

    def _update(self, run_id: str, **kwargs: Any) -> None:
        with self._lock:
            record = self._runs.get(run_id)
            if record is None:
                return
            for key, value in kwargs.items():
                setattr(record, key, value)
            record.updated_at = datetime.now(UTC).isoformat()

    def _current_tiles(self, run_id: str) -> list[dict[str, Any]]:
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

    def _execute_run(self, run_id: str, resume_filename: str, resume_file_bytes: bytes) -> None:
        self._update(run_id, status="running")
        try:
            self._stream_preview(run_id, resume_filename, resume_file_bytes)
            final = self._orchestrator.run(
                resume_filename,
                resume_file_bytes,
                run_id=run_id,
            )
            merged_tiles = self._merge_tiles(
                self._current_tiles(run_id),
                [tile.model_dump() for tile in final.tiles],
            )
            self._update(
                run_id,
                status="completed" if not final.diagnostics.failed else "failed",
                tiles=merged_tiles,
                diagnostics=final.diagnostics.model_dump(),
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
            self._update(run_id, status="failed", diagnostics=diagnostics.model_dump())

    def _stream_preview(self, run_id: str, resume_filename: str, resume_file_bytes: bytes) -> None:
        workflow_config = self._orchestrator._nodes.workflow_config  # noqa: SLF001
        discovery_cfg = workflow_config.get("job_discovery", {})
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

        for idx, company in enumerate(companies, start=1):
            company_name = str(company.get("name", ""))
            timeout = float(discovery_cfg.get("fetch_timeout_seconds", 4.0))
            company_jobs = _discover_company_jobs(company, timeout)

            for job in company_jobs:
                if not self._orchestrator._nodes._is_valid_discovered_job(job):  # noqa: SLF001
                    continue
                if not resolver.matches_preference(job):
                    continue
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

            preview_tiles = self._build_preview_tiles(run_id, parsed_resume, discovered)
            self._update(
                run_id,
                progress_current=idx,
                progress_company=company_name,
                tiles=preview_tiles,
            )

    def _build_preview_tiles(
        self,
        run_id: str,
        parsed_resume: dict[str, Any],
        discovered_jobs: list[dict[str, Any]],
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
                location=str(job.get("location", "")),
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
