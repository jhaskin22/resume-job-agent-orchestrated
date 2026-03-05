from pydantic import BaseModel, Field, HttpUrl


class JobMatchTile(BaseModel):
    company: str
    title: str
    location: str
    salary: str | None = None
    work_type: str
    match_score: float = Field(ge=0, le=100)
    resume_alignment: float = Field(ge=0, le=100)
    ats_score: float = Field(ge=0, le=100)
    job_link: HttpUrl
    generated_resume_link: str
    summary: str
