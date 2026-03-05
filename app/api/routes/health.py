from fastapi import APIRouter

from app.services.health import health_payload

router = APIRouter()


@router.get("/health")
def health() -> dict[str, str]:
    return health_payload()
