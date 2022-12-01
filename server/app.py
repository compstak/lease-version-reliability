from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from starlette_exporter import PrometheusMiddleware, handle_metrics
import structlog
import uvicorn

from server.api.endpoints import healthcheck, token
from server.api.router import api_router
from server.config.settings import settings
from server.data.database import cs_mysql_instance

app = FastAPI(
    title=settings.PROJECT_NAME,
    openapi_url=f"{settings.API_V1_STR}/openapi.json",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.add_middleware(PrometheusMiddleware)

app.add_route("/metrics", handle_metrics)
app.include_router(healthcheck.router, tags=["health"])
app.include_router(token.router, tags=["token"])
app.include_router(api_router, prefix=settings.API_V1_STR)

logger = structlog.get_logger()


@app.on_event("startup")
async def startup() -> None:
    await cs_mysql_instance.connect()
    app.state.db = cs_mysql_instance
    logger.info("CompStak MySQL database connected")


@app.on_event("shutdown")
async def shutdown() -> None:
    await cs_mysql_instance.disconnect()
    logger.info("CompStak MySQL database disconnected")


if __name__ == "__main__":
    uvicorn.run(
        "core.src.main.app:app",
        host="0.0.0.0",
        port=8080,
        reload=True,
    )
