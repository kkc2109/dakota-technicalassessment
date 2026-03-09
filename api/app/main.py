"""Dakota Analytics Enrichment API."""

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from app.config import settings
from app.routers import carbon, market, weather

logging.basicConfig(
    level=settings.log_level,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting %s v%s", settings.app_name, settings.app_version)
    yield
    logger.info("Shutting down %s", settings.app_name)


app = FastAPI(
    title=settings.app_name,
    version=settings.app_version,
    description=__doc__,
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

app.include_router(weather.router, prefix="/weather", tags=["Weather"])
app.include_router(carbon.router, prefix="/carbon", tags=["Carbon Intensity"])
app.include_router(market.router, prefix="/market", tags=["Market"])


@app.get("/health", tags=["Health"], summary="Service health check")
async def health_check() -> JSONResponse:
    """Returns 200 OK when the service is running and ready to serve requests."""
    return JSONResponse(content={"status": "healthy", "service": settings.app_name, "version": settings.app_version})


@app.get("/", tags=["Health"], include_in_schema=False)
async def root() -> JSONResponse:
    return JSONResponse(content={"message": f"Welcome to {settings.app_name}. See /docs for API reference."})
