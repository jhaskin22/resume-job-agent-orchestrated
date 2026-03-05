from __future__ import annotations

from collections.abc import Callable
from typing import Any

from langgraph.graph import END, START, StateGraph

from app.workflow.nodes import WorkflowNodes
from app.workflow.state import WorkflowState


def _verification_gate(
    stage: str,
    max_repairs: int,
    next_stage: str,
) -> Callable[[WorkflowState], str]:
    def _route(state: WorkflowState) -> str:
        verification = state.get("verification", {}).get(stage, {})
        ok = bool(verification.get("ok", False))
        repairs = int(state.get("repair_counts", {}).get(stage, 0))

        if ok:
            return next_stage
        if repairs >= max_repairs:
            return "mark_failed"
        return f"repair_{stage}"

    return _route


def build_workflow(nodes: WorkflowNodes, workflow_config: dict[str, Any]):
    graph = StateGraph(WorkflowState)
    max_repairs = int(workflow_config["workflow"]["max_repair_attempts"])

    graph.add_node("resume_parsing", nodes.resume_parsing)
    graph.add_node("verify_resume_parsing", nodes.verify_resume_parsing)
    graph.add_node("repair_resume_parsing", nodes.repair_resume_parsing)

    graph.add_node("job_discovery", nodes.job_discovery)
    graph.add_node("verify_job_discovery", nodes.verify_job_discovery)
    graph.add_node("repair_job_discovery", nodes.repair_job_discovery)

    graph.add_node("job_scoring", nodes.job_scoring)
    graph.add_node("verify_job_scoring", nodes.verify_job_scoring)
    graph.add_node("repair_job_scoring", nodes.repair_job_scoring)

    graph.add_node("resume_generation", nodes.resume_generation)
    graph.add_node("verify_resume_generation", nodes.verify_resume_generation)
    graph.add_node("repair_resume_generation", nodes.repair_resume_generation)

    graph.add_node("ats_evaluation", nodes.ats_evaluation)
    graph.add_node("verify_ats_evaluation", nodes.verify_ats_evaluation)
    graph.add_node("repair_ats_evaluation", nodes.repair_ats_evaluation)

    graph.add_node("tile_construction", nodes.tile_construction)
    graph.add_node("verify_tile_construction", nodes.verify_tile_construction)
    graph.add_node("repair_tile_construction", nodes.repair_tile_construction)

    graph.add_node("mark_failed", nodes.mark_failed)
    graph.add_node("mark_success", nodes.mark_success)

    graph.add_edge(START, "resume_parsing")
    graph.add_edge("resume_parsing", "verify_resume_parsing")
    graph.add_conditional_edges(
        "verify_resume_parsing",
        _verification_gate("resume_parsing", max_repairs, "job_discovery"),
        {
            "job_discovery": "job_discovery",
            "repair_resume_parsing": "repair_resume_parsing",
            "mark_failed": "mark_failed",
        },
    )
    graph.add_edge("repair_resume_parsing", "verify_resume_parsing")

    graph.add_edge("job_discovery", "verify_job_discovery")
    graph.add_conditional_edges(
        "verify_job_discovery",
        _verification_gate("job_discovery", max_repairs, "job_scoring"),
        {
            "job_scoring": "job_scoring",
            "repair_job_discovery": "repair_job_discovery",
            "mark_failed": "mark_failed",
        },
    )
    graph.add_edge("repair_job_discovery", "verify_job_discovery")

    graph.add_edge("job_scoring", "verify_job_scoring")
    graph.add_conditional_edges(
        "verify_job_scoring",
        _verification_gate("job_scoring", max_repairs, "resume_generation"),
        {
            "resume_generation": "resume_generation",
            "repair_job_scoring": "repair_job_scoring",
            "mark_failed": "mark_failed",
        },
    )
    graph.add_edge("repair_job_scoring", "verify_job_scoring")

    graph.add_edge("resume_generation", "verify_resume_generation")
    graph.add_conditional_edges(
        "verify_resume_generation",
        _verification_gate("resume_generation", max_repairs, "ats_evaluation"),
        {
            "ats_evaluation": "ats_evaluation",
            "repair_resume_generation": "repair_resume_generation",
            "mark_failed": "mark_failed",
        },
    )
    graph.add_edge("repair_resume_generation", "verify_resume_generation")

    graph.add_edge("ats_evaluation", "verify_ats_evaluation")
    graph.add_conditional_edges(
        "verify_ats_evaluation",
        _verification_gate("ats_evaluation", max_repairs, "tile_construction"),
        {
            "tile_construction": "tile_construction",
            "repair_ats_evaluation": "repair_ats_evaluation",
            "mark_failed": "mark_failed",
        },
    )
    graph.add_edge("repair_ats_evaluation", "verify_ats_evaluation")

    graph.add_edge("tile_construction", "verify_tile_construction")
    graph.add_conditional_edges(
        "verify_tile_construction",
        _verification_gate("tile_construction", max_repairs, "mark_success"),
        {
            "mark_success": "mark_success",
            "repair_tile_construction": "repair_tile_construction",
            "mark_failed": "mark_failed",
        },
    )
    graph.add_edge("repair_tile_construction", "verify_tile_construction")

    graph.add_edge("mark_success", END)
    graph.add_edge("mark_failed", END)

    return graph.compile()
