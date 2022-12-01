import logging
from typing import Any, Dict

from fastapi import APIRouter, Depends
import httpx

from server.api.endpoints.token import get_cs_auth_headers
from server.config.settings import settings

router = APIRouter()
logger = logging.getLogger(__name__)


@router.get(
    "/attributes",
    tags=["exchange"],
)
async def get_all_attributes(
    headers: Dict[str, str] = Depends(get_cs_auth_headers),
) -> Any:
    url = f"{settings.CS_EXCHANGE_URL}/api/allAttrs"

    async with httpx.AsyncClient() as client:
        response = await client.get(url=url, headers=headers)
        response.raise_for_status()

    return response.json()
