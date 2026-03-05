from __future__ import annotations

from pathlib import Path
from statistics import mean
from typing import Any

from app.models.tile import JobMatchTile
from app.workflow.io import ResumeParsingError, parse_resume_content, safe_stem, write_resume_docx
from app.workflow.state import WorkflowState

REQUIRED_DISCOVERY_KEYS = {
    "company",
    "title",
    "location",
    "salary",
    "work_type",
    "job_link",
    "description",
}


class WorkflowNodes:
    def __init__(
        self,
        workflow_config: dict[str, Any],
        prompts_config: dict[str, Any],
        generated_resume_dir: Path,
    ) -> None:
        self.workflow_config = workflow_config
        self.prompts_config = prompts_config
        self.generated_resume_dir = generated_resume_dir

    def _add_error(self, state: WorkflowState, message: str) -> None:
        errors = state.setdefault("errors", [])
        errors.append(message)

    def _repair_count(self, state: WorkflowState, stage: str) -> int:
        repair_counts = state.setdefault("repair_counts", {})
        return int(repair_counts.get(stage, 0))

    def _inc_repair_count(self, state: WorkflowState, stage: str) -> None:
        repair_counts = state.setdefault("repair_counts", {})
        repair_counts[stage] = self._repair_count(state, stage) + 1

    def _set_verification(
        self,
        state: WorkflowState,
        stage: str,
        ok: bool,
        reason: str = "",
    ) -> dict[str, Any]:
        verification = state.setdefault("verification", {})
        result = {
            "ok": ok,
            "reason": reason,
            "repair_count": self._repair_count(state, stage),
        }
        verification[stage] = result
        return {"verification": verification}

    def resume_parsing(self, state: WorkflowState) -> dict[str, Any]:
        filename = state.get("resume_filename", "resume.docx")
        payload = state.get("resume_file_bytes", b"")
        try:
            parsed = parse_resume_content(filename, payload)
            return {"resume_text": parsed}
        except ResumeParsingError as exc:
            self._add_error(state, f"resume_parsing: {exc}")
            return {"resume_text": ""}
        except Exception as exc:  # noqa: BLE001
            self._add_error(state, f"resume_parsing failure: {exc}")
            return {"resume_text": ""}

    def verify_resume_parsing(self, state: WorkflowState) -> dict[str, Any]:
        min_chars = int(self.workflow_config["nodes"]["resume_parsing"]["min_text_chars"])
        text = state.get("resume_text", "")
        if len(text.strip()) >= min_chars:
            return self._set_verification(state, "resume_parsing", True)
        return self._set_verification(
            state,
            "resume_parsing",
            False,
            f"Resume text too short. Need >= {min_chars} characters.",
        )

    def repair_resume_parsing(self, state: WorkflowState) -> dict[str, Any]:
        self._inc_repair_count(state, "resume_parsing")
        if state.get("resume_text"):
            return {}

        filename = state.get("resume_filename", "")
        payload = state.get("resume_file_bytes", b"")
        fallback = f"Recovered resume context from {filename}. "
        fallback += payload.decode("utf-8", errors="ignore")[:1200]
        return {"resume_text": fallback}

    def job_discovery(self, state: WorkflowState) -> dict[str, Any]:
        jobs = list(self.workflow_config.get("job_discovery", {}).get("sample_jobs", []))
        max_jobs = int(self.workflow_config["nodes"]["job_discovery"]["max_jobs"])
        return {"discovered_jobs": jobs[:max_jobs]}

    def verify_job_discovery(self, state: WorkflowState) -> dict[str, Any]:
        jobs = state.get("discovered_jobs", [])
        if not jobs:
            return self._set_verification(state, "job_discovery", False, "No jobs were discovered.")

        missing = [
            index for index, item in enumerate(jobs) if not REQUIRED_DISCOVERY_KEYS.issubset(item)
        ]
        if missing:
            return self._set_verification(
                state,
                "job_discovery",
                False,
                f"Jobs missing required fields at indexes: {missing}",
            )

        return self._set_verification(state, "job_discovery", True)

    def repair_job_discovery(self, state: WorkflowState) -> dict[str, Any]:
        self._inc_repair_count(state, "job_discovery")
        jobs = state.get("discovered_jobs", [])
        repaired_jobs: list[dict[str, Any]] = []
        for item in jobs:
            repaired_jobs.append(
                {
                    "company": item.get("company") or "Unknown Company",
                    "title": item.get("title") or "Unknown Title",
                    "location": item.get("location") or "Unknown",
                    "salary": item.get("salary") or "",
                    "work_type": item.get("work_type") or "remote",
                    "job_link": item.get("job_link") or "https://example.com/jobs/unknown",
                    "description": item.get("description") or "General software engineering role.",
                }
            )
        if not repaired_jobs:
            repaired_jobs.append(
                {
                    "company": "Fallback Labs",
                    "title": "AI Software Engineer",
                    "location": "Remote - US",
                    "salary": "",
                    "work_type": "remote",
                    "job_link": "https://example.com/jobs/fallback-role",
                    "description": "Build practical AI workflow systems in Python.",
                }
            )
        return {"discovered_jobs": repaired_jobs}

    def job_scoring(self, state: WorkflowState) -> dict[str, Any]:
        resume_text = state.get("resume_text", "").lower()
        resume_tokens = set(token for token in resume_text.replace("\n", " ").split(" ") if token)

        scored: list[dict[str, Any]] = []
        for job in state.get("discovered_jobs", []):
            job_text = f"{job.get('title', '')} {job.get('description', '')}".lower()
            job_tokens = set(token for token in job_text.split(" ") if token)
            overlap = len(resume_tokens.intersection(job_tokens))
            denominator = max(1, len(job_tokens))
            overlap_ratio = overlap / denominator

            match_score = round(min(100.0, 35 + overlap_ratio * 65), 2)
            alignment = round(min(100.0, 30 + overlap_ratio * 70), 2)
            ats_score = round(min(100.0, mean([match_score, alignment]) + 3), 2)

            scored.append(
                {
                    **job,
                    "match_score": match_score,
                    "resume_alignment": alignment,
                    "ats_score": ats_score,
                }
            )

        scored.sort(key=lambda item: item["match_score"], reverse=True)
        return {"scored_jobs": scored}

    def verify_job_scoring(self, state: WorkflowState) -> dict[str, Any]:
        min_score = float(self.workflow_config["nodes"]["job_scoring"]["min_match_score"])
        jobs = state.get("scored_jobs", [])
        if not jobs:
            return self._set_verification(state, "job_scoring", False, "No jobs were scored.")
        if any("match_score" not in item for item in jobs):
            return self._set_verification(state, "job_scoring", False, "Missing match_score.")
        if max(float(item["match_score"]) for item in jobs) < min_score:
            return self._set_verification(
                state,
                "job_scoring",
                False,
                f"Top match score below configured threshold {min_score}.",
            )
        return self._set_verification(state, "job_scoring", True)

    def repair_job_scoring(self, state: WorkflowState) -> dict[str, Any]:
        self._inc_repair_count(state, "job_scoring")
        repaired: list[dict[str, Any]] = []
        for item in state.get("scored_jobs", []) or state.get("discovered_jobs", []):
            repaired.append(
                {
                    **item,
                    "match_score": float(item.get("match_score", 55)),
                    "resume_alignment": float(item.get("resume_alignment", 52)),
                    "ats_score": float(item.get("ats_score", 54)),
                }
            )
        repaired.sort(key=lambda it: float(it["match_score"]), reverse=True)
        return {"scored_jobs": repaired}

    def resume_generation(self, state: WorkflowState) -> dict[str, Any]:
        filename = state.get("resume_filename", "resume.docx")
        stem = safe_stem(filename)
        generated_map: dict[str, str] = {}

        for index, job in enumerate(state.get("scored_jobs", []), start=1):
            company = str(job.get("company", "company"))
            role = str(job.get("title", "role"))
            tailored = (
                f"Target Company: {company}\n"
                f"Target Role: {role}\n"
                f"Match Score: {job.get('match_score', 0)}\n\n"
                "Tailored Highlights:\n"
                "- Delivered measurable outcomes in AI workflow systems.\n"
                "- Built resilient API integrations and automated validation loops.\n"
                "- Prioritized ATS-relevant terms and role-specific outcomes.\n"
            )

            output_filename = f"{stem}-{index:02d}.docx"
            output_path = self.generated_resume_dir / output_filename
            write_resume_docx(tailored, output_path)
            generated_map[job["job_link"]] = f"/api/resumes/{output_filename}"

        return {"generated_resumes": generated_map}

    def verify_resume_generation(self, state: WorkflowState) -> dict[str, Any]:
        min_chars = int(self.workflow_config["nodes"]["resume_generation"]["min_resume_chars"])
        generated = state.get("generated_resumes", {})
        if not generated:
            return self._set_verification(
                state,
                "resume_generation",
                False,
                "No generated resumes were produced.",
            )

        missing_links = [
            job for job in state.get("scored_jobs", []) if job.get("job_link") not in generated
        ]
        if missing_links:
            return self._set_verification(
                state,
                "resume_generation",
                False,
                "At least one scored job has no generated resume link.",
            )

        for link in generated.values():
            filename = link.rsplit("/", maxsplit=1)[-1]
            local_path = self.generated_resume_dir / filename
            if not local_path.exists() or local_path.stat().st_size < min_chars:
                return self._set_verification(
                    state,
                    "resume_generation",
                    False,
                    f"Generated resume missing or too small: {filename}",
                )

        return self._set_verification(state, "resume_generation", True)

    def repair_resume_generation(self, state: WorkflowState) -> dict[str, Any]:
        self._inc_repair_count(state, "resume_generation")
        return self.resume_generation(state)

    def tile_construction(self, state: WorkflowState) -> dict[str, Any]:
        tiles: list[dict[str, Any]] = []

        for job in state.get("scored_jobs", []):
            generated_link = state.get("generated_resumes", {}).get(job["job_link"], "")
            summary = (
                f"{job['company']} aligns with your profile for {job['title']} "
                f"with match score {job['match_score']:.1f} and ATS score {job['ats_score']:.1f}."
            )
            tile = JobMatchTile(
                company=str(job["company"]),
                title=str(job["title"]),
                location=str(job["location"]),
                salary=str(job.get("salary") or "") or None,
                work_type=str(job["work_type"]),
                match_score=float(job["match_score"]),
                resume_alignment=float(job["resume_alignment"]),
                ats_score=float(job["ats_score"]),
                job_link=str(job["job_link"]),
                generated_resume_link=generated_link,
                summary=summary,
            )
            tiles.append(tile.model_dump())

        return {"tiles": tiles}

    def verify_tile_construction(self, state: WorkflowState) -> dict[str, Any]:
        min_summary = int(self.workflow_config["nodes"]["tile_construction"]["min_summary_chars"])
        tiles = state.get("tiles", [])
        if not tiles:
            return self._set_verification(state, "tile_construction", False, "No job tiles built.")

        for tile in tiles:
            summary = str(tile.get("summary", ""))
            if len(summary) < min_summary:
                return self._set_verification(
                    state,
                    "tile_construction",
                    False,
                    "Tile summary below minimum threshold.",
                )
            JobMatchTile(**tile)

        return self._set_verification(state, "tile_construction", True)

    def repair_tile_construction(self, state: WorkflowState) -> dict[str, Any]:
        self._inc_repair_count(state, "tile_construction")
        repaired: list[dict[str, Any]] = []

        for tile in state.get("tiles", []):
            candidate = {
                "company": tile.get("company") or "Unknown Company",
                "title": tile.get("title") or "Unknown Title",
                "location": tile.get("location") or "Unknown",
                "salary": tile.get("salary") or None,
                "work_type": tile.get("work_type") or "remote",
                "match_score": float(tile.get("match_score", 50)),
                "resume_alignment": float(tile.get("resume_alignment", 50)),
                "ats_score": float(tile.get("ats_score", 50)),
                "job_link": tile.get("job_link") or "https://example.com/jobs/repaired",
                "generated_resume_link": tile.get("generated_resume_link") or "",
                "summary": tile.get("summary")
                or "Role appears moderately aligned with your profile after repair fallback.",
            }
            repaired.append(JobMatchTile(**candidate).model_dump())

        return {"tiles": repaired}

    def mark_failed(self, state: WorkflowState) -> dict[str, Any]:
        return {"failed": True}

    def mark_success(self, state: WorkflowState) -> dict[str, Any]:
        return {"failed": False}
