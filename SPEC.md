# Specification

## Purpose

Create an extensible base system that:

- accepts a resume upload (`PDF`/`DOCX`)
- discovers jobs
- scores fit
- generates tailored resumes (`DOCX`)
- emits structured job-match tiles

## Backend contracts

### `POST /api/workflow/run`

Multipart form-data with `resume` file.

Returns:

- `tiles`: ordered list of structured job tiles
- `diagnostics`: workflow verification details and failure state

### `GET /api/resumes/{filename}`

Downloads generated DOCX resumes.

## Job tile schema

- `company`
- `title`
- `location`
- `salary`
- `work_type`
- `match_score`
- `resume_alignment`
- `ats_score`
- `job_link`
- `generated_resume_link`
- `summary`

## Workflow requirement

Every operational node has:

- verification node
- repair node

No stage is assumed successful without verification.
