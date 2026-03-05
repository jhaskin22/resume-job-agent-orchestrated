# resume-job-agent-orchestrated

LangGraph-first foundation for resume-to-job orchestration with verification and repair loops.

## What this base project includes

- Resume upload support (`PDF` and `DOCX`)
- LangGraph workflow with stage-level `generate -> verify -> repair` routing
- Structured job tile output model for UI rendering
- Tailored resume generation in `DOCX`
- FastAPI backend on `0.0.0.0:18000`
- Static frontend on `0.0.0.0:8090`
- Startup and shutdown scripts in `scripts/`
- Externalized prompt/workflow configuration for agent-driven iteration

## Architecture overview

Workflow stages:

1. `resume_parsing`
2. `job_discovery`
3. `job_scoring`
4. `resume_generation`
5. `tile_construction`

Each stage is paired with:

- `verify_<stage>`
- `repair_<stage>`

Verification failure routes to repair until `max_repair_attempts` is reached; then workflow fails explicitly.

## Run locally

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

./scripts/appup.sh
```

- Frontend: `http://localhost:8090`
- Backend docs: `http://localhost:18000/docs`

Stop services:

```bash
./scripts/appdown.sh
```

## Config for agent extensibility

- Workflow behavior and thresholds: `app/config/workflow.yaml`
- Prompt templates: `app/config/prompts.yaml`
- Node logic: `app/workflow/nodes.py`
- Graph topology: `app/workflow/graph.py`

Codex agents can evolve prompts, thresholds, stage logic, and discovery strategy without redesigning the app shell.
