from fastapi import APIRouter

from server.api.endpoints import exchange

api_router: APIRouter = APIRouter()
api_router.include_router(
    exchange.router,
    prefix="/exchange",
    tags=["exchange"],
)
