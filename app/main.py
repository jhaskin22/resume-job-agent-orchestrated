from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api.router import api_router
from app.core.config import settings
from app.core.logging_setup import configure_pipeline_logging


def create_app() -> FastAPI:
    configure_pipeline_logging()
    app = FastAPI(title=settings.app_name)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    app.include_router(api_router, prefix="/api")

    @app.get("/")
    def root() -> dict[str, str]:
        return {"service": settings.app_name, "status": "running"}

    return app


app = create_app()
