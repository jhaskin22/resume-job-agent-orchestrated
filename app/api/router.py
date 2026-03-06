from fastapi import APIRouter

from app.api.routes.frontend_log import router as frontend_log_router
from app.api.routes.health import router as health_router
from app.api.routes.workflow import router as workflow_router

api_router = APIRouter()
api_router.include_router(health_router, tags=["health"])
api_router.include_router(workflow_router, tags=["workflow"])
api_router.include_router(frontend_log_router, tags=["frontend-log"])
