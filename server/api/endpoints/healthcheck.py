from fastapi import APIRouter

from server.api.schemas.healthcheck import HealthCheck

router = APIRouter()


@router.get("/", response_model=HealthCheck)
def healthcheck() -> HealthCheck:
    return HealthCheck(status="OK")
