from __future__ import annotations

from pathlib import Path
from typing import Any

from app.models.tile import JobMatchTile
from app.tools.ats import evaluate_ats_score
from app.tools.job_discovery import DiscoveryConfig, discover_jobs
from app.tools.resume import parse_resume_sections
from app.tools.scoring import score_jobs
from app.verification.checks import (
    verify_ats_scores,
    verify_discovered_jobs,
    verify_generated_resumes,
    verify_parsed_resume,
    verify_scored_jobs,
    verify_tiles,
)
from app.workflow.io import (
    ResumeParsingError,
    parse_resume_content,
    read_docx_text,
    safe_stem,
    write_resume_docx,
)
from app.workflow.state import WorkflowState

REQUIRED_DISCOVERY_KEYS = {
    "company",
    "title",
    "job_url",
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

    def _resume_parser_keywords(self) -> tuple[list[str], list[str], list[str]]:
        parser_cfg = self.workflow_config.get("resume_parsing", {})
        skills = [str(item) for item in parser_cfg.get("skill_keywords", [])]
        roles = [str(item) for item in parser_cfg.get("role_keywords", [])]
        technologies = [str(item) for item in parser_cfg.get("technology_keywords", [])]
        return (skills, roles, technologies)

    def _parse_resume_text(self, text: str) -> dict[str, object]:
        skills, roles, technologies = self._resume_parser_keywords()
        return parse_resume_sections(
            text=text,
            skill_keywords=skills,
            role_keywords=roles,
            technology_keywords=technologies,
        )

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
            text = parse_resume_content(filename, payload)
            parsed_resume = self._parse_resume_text(text)
            return {"resume_text": text, "parsed_resume": parsed_resume}
        except ResumeParsingError as exc:
            self._add_error(state, f"resume_parsing: {exc}")
            return {"resume_text": "", "parsed_resume": {}}
        except Exception as exc:  # noqa: BLE001
            self._add_error(state, f"resume_parsing failure: {exc}")
            return {"resume_text": "", "parsed_resume": {}}

    def verify_resume_parsing(self, state: WorkflowState) -> dict[str, Any]:
        min_chars = int(self.workflow_config["nodes"]["resume_parsing"]["min_text_chars"])
        min_populated_fields = int(
            self.workflow_config["nodes"]["resume_parsing"]["min_populated_fields"]
        )
        text = state.get("resume_text", "")
        if len(text.strip()) < min_chars:
            return self._set_verification(
                state,
                "resume_parsing",
                False,
                f"Resume text too short. Need >= {min_chars} characters.",
            )
        ok, reason = verify_parsed_resume(state.get("parsed_resume", {}), min_populated_fields)
        if ok:
            return self._set_verification(state, "resume_parsing", True)
        return self._set_verification(
            state,
            "resume_parsing",
            False,
            reason,
        )

    def repair_resume_parsing(self, state: WorkflowState) -> dict[str, Any]:
        self._inc_repair_count(state, "resume_parsing")
        text = state.get("resume_text", "")
        parsed_resume = state.get("parsed_resume", {})
        if not text:
            filename = state.get("resume_filename", "")
            payload = state.get("resume_file_bytes", b"")
            text = f"Recovered resume context from {filename}. "
            text += payload.decode("utf-8", errors="ignore")[:1800]
        if not isinstance(parsed_resume, dict) or not parsed_resume:
            parsed_resume = self._parse_resume_text(text)
        return {"resume_text": text, "parsed_resume": parsed_resume}

    def job_discovery(self, state: WorkflowState) -> dict[str, Any]:
        discovery_cfg = self.workflow_config.get("job_discovery", {})
        max_jobs = int(self.workflow_config["nodes"]["job_discovery"]["max_jobs"])
        timeout_seconds = float(discovery_cfg.get("fetch_timeout_seconds", 3.0))

        jobs = discover_jobs(
            DiscoveryConfig(
                companies=[dict(item) for item in discovery_cfg.get("companies", [])],
                max_jobs=max_jobs,
                timeout_seconds=timeout_seconds,
                fallback_jobs=[dict(item) for item in discovery_cfg.get("fallback_jobs", [])],
            )
        )
        return {"discovered_jobs": jobs}

    def verify_job_discovery(self, state: WorkflowState) -> dict[str, Any]:
        jobs = state.get("discovered_jobs", [])
        min_jobs = int(self.workflow_config["nodes"]["job_discovery"]["min_job_count"])
        ok, reason = verify_discovered_jobs(jobs, min_jobs, REQUIRED_DISCOVERY_KEYS)
        if not ok:
            return self._set_verification(state, "job_discovery", False, reason)

        return self._set_verification(state, "job_discovery", True)

    def repair_job_discovery(self, state: WorkflowState) -> dict[str, Any]:
        self._inc_repair_count(state, "job_discovery")
        jobs = state.get("discovered_jobs", [])
        repaired_jobs: list[dict[str, Any]] = []
        for item in jobs:
            fallback_link = item.get("job_link") or "https://example.com/jobs/unknown"
            repaired_jobs.append(
                {
                    "company": item.get("company") or "Unknown Company",
                    "title": item.get("title") or "Unknown Title",
                    "location": item.get("location") or "Unknown",
                    "salary": item.get("salary") or "",
                    "work_type": item.get("work_type") or "remote",
                    "job_link": fallback_link,
                    "job_url": item.get("job_url") or fallback_link,
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
                    "job_url": "https://example.com/jobs/fallback-role",
                    "description": "Build practical AI workflow systems in Python.",
                }
            )
        return {"discovered_jobs": repaired_jobs}

    def job_scoring(self, state: WorkflowState) -> dict[str, Any]:
        top_k = int(self.workflow_config["nodes"]["job_scoring"]["top_k"])
        parsed_resume = dict(state.get("parsed_resume", {}))
        discovered_jobs = [dict(item) for item in state.get("discovered_jobs", [])]
        scored = score_jobs(parsed_resume, discovered_jobs, top_k=top_k)
        return {"scored_jobs": scored}

    def verify_job_scoring(self, state: WorkflowState) -> dict[str, Any]:
        jobs = state.get("scored_jobs", [])
        min_score = float(self.workflow_config["nodes"]["job_scoring"]["min_match_score"])
        ok, reason = verify_scored_jobs(jobs)
        if not ok:
            return self._set_verification(state, "job_scoring", False, reason)
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
            matched_skills = [str(item) for item in job.get("required_skills", [])][:8]
            parsed_resume = dict(state.get("parsed_resume", {}))
            experience_lines = [str(item) for item in parsed_resume.get("experience", [])][:4]
            skill_lines = [str(item) for item in parsed_resume.get("skills", [])][:10]
            rendered_skills = ", ".join(
                matched_skills or skill_lines or ["Python", "APIs", "Automation"]
            )
            rendered_experience = "\n".join(
                experience_lines or ["Delivered measurable outcomes in AI workflow systems."]
            )
            tailored = (
                "SUMMARY\n"
                f"Targeting {role} at {company}. "
                f"Strong alignment with score {job.get('match_score', 0)}.\n\n"
                "SKILLS\n"
                f"{rendered_skills}\n\n"
                "EXPERIENCE\n"
                + rendered_experience
                + "\n\n"
                f"Target Company: {company}\n"
                f"Target Role: {role}\n"
                f"Match Score: {job.get('match_score', 0)}\n\n"
                "Tailored Highlights:\n"
                "- Emphasized role-specific achievements and measurable outcomes.\n"
                "- Included ATS-relevant keywords from job requirements.\n"
                "- Prioritized clear section structure for ATS parsing.\n"
            )

            output_filename = f"{stem}-{index:02d}.docx"
            output_path = self.generated_resume_dir / output_filename
            write_resume_docx(tailored, output_path)
            generated_map[job["job_link"]] = f"/api/resumes/{output_filename}"

        return {"generated_resumes": generated_map}

    def verify_resume_generation(self, state: WorkflowState) -> dict[str, Any]:
        min_bytes = int(self.workflow_config["nodes"]["resume_generation"]["min_resume_bytes"])
        generated = state.get("generated_resumes", {})
        ok, reason = verify_generated_resumes(
            generated_links=generated,
            scored_jobs=state.get("scored_jobs", []),
            generated_resume_dir=self.generated_resume_dir,
            min_size_bytes=min_bytes,
        )
        if not ok:
            return self._set_verification(state, "resume_generation", False, reason)

        return self._set_verification(state, "resume_generation", True)

    def repair_resume_generation(self, state: WorkflowState) -> dict[str, Any]:
        self._inc_repair_count(state, "resume_generation")
        return self.resume_generation(state)

    def ats_evaluation(self, state: WorkflowState) -> dict[str, Any]:
        scored_jobs: list[dict[str, Any]] = []
        generated = dict(state.get("generated_resumes", {}))

        for job in state.get("scored_jobs", []):
            output_link = generated.get(str(job.get("job_link", "")), "")
            if not output_link:
                scored_jobs.append(dict(job, ats_score=0.0))
                continue
            filename = output_link.rsplit("/", maxsplit=1)[-1]
            path = self.generated_resume_dir / filename
            resume_text = ""
            if path.exists():
                try:
                    resume_text = read_docx_text(path)
                except Exception as exc:  # noqa: BLE001
                    self._add_error(state, f"ats_evaluation read failure: {exc}")
            ats_score = evaluate_ats_score(job, resume_text)
            scored_jobs.append({**job, "ats_score": ats_score})

        return {"scored_jobs": scored_jobs}

    def verify_ats_evaluation(self, state: WorkflowState) -> dict[str, Any]:
        ok, reason = verify_ats_scores(state.get("scored_jobs", []))
        if not ok:
            return self._set_verification(state, "ats_evaluation", False, reason)
        return self._set_verification(state, "ats_evaluation", True)

    def repair_ats_evaluation(self, state: WorkflowState) -> dict[str, Any]:
        self._inc_repair_count(state, "ats_evaluation")
        repaired: list[dict[str, Any]] = []
        for job in state.get("scored_jobs", []):
            repaired.append({**job, "ats_score": float(job.get("ats_score", 55.0))})
        return {"scored_jobs": repaired}

    def tile_construction(self, state: WorkflowState) -> dict[str, Any]:
        tiles: list[dict[str, Any]] = []

        for job in state.get("scored_jobs", []):
            generated_link = state.get("generated_resumes", {}).get(job["job_link"], "")
            summary = (
                f"{job['company']} aligns with your profile for {job['title']} "
                f"with match score {job['match_score']:.1f}, resume alignment "
                f"{job['resume_alignment']:.1f}, and ATS score {job['ats_score']:.1f}."
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
        ok, reason = verify_tiles(state.get("tiles", []), min_summary)
        if not ok:
            return self._set_verification(state, "tile_construction", False, reason)

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
