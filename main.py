# main.py
import os
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from slowapi.errors import RateLimitExceeded
from slowapi.middleware import SlowAPIMiddleware
from slowapi import _rate_limit_exceeded_handler

from routers import health, stats, user_report
from common.limiter import limiter
from common.logger import setup_logger

def _get_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return v.strip().lower() in ("1", "true", "yes", "y", "on")

def _get_list(name: str, default_csv: str) -> list[str]:
    raw = os.getenv(name, default_csv)
    items = [x.strip() for x in raw.split(",")]
    return [x for x in items if x]

APP_ENV = os.getenv("APP_ENV", "dev")
SERVE_STATIC = _get_bool("SERVE_STATIC", default=(APP_ENV == "dev"))

CORS_ORIGINS = _get_list(
    "CORS_ORIGINS",
    "http://localhost:8000,http://localhost:5173"
)

app = FastAPI()
logger = setup_logger("api:main")

app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
app.add_middleware(SlowAPIMiddleware)

app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ORIGINS,
    allow_credentials=False,
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)

# Routers
app.include_router(health.router)
app.include_router(stats.router)
app.include_router(user_report.router)
logger.info("api started: env=%s serve_static=%s", APP_ENV, SERVE_STATIC)

# (선택) 개발에서만 정적 서빙: 지금 구조 유지하고 싶을 때 켜기
if SERVE_STATIC:
    from fastapi.staticfiles import StaticFiles

    BASE_DIR = Path(__file__).resolve().parent
    WEB_DIR = BASE_DIR / "web"
    STATIC_DIR = WEB_DIR / "static"

    if STATIC_DIR.exists():
        app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")
    if WEB_DIR.exists():
        app.mount("/", StaticFiles(directory=str(WEB_DIR), html=True), name="web")
