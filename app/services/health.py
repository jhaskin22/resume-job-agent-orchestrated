from app.core.config import settings


def health_payload() -> dict[str, str]:
    return {"status": "ok", "app_name": settings.app_name}
