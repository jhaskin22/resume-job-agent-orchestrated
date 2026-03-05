from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    app_name: str = "Resume Job Agent Orchestrated"
    backend_host: str = "0.0.0.0"
    backend_port: int = 18000
    frontend_host: str = "0.0.0.0"
    frontend_port: int = 8090
    generated_resume_dir: Path = Field(default_factory=lambda: Path("var/generated_resumes"))
    workflow_config_path: Path = Field(default_factory=lambda: Path("app/config/workflow.yaml"))
    prompts_config_path: Path = Field(default_factory=lambda: Path("app/config/prompts.yaml"))

    model_config = SettingsConfigDict(env_file=".env", env_prefix="APP_")


settings = Settings()
