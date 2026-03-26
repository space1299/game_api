from fastapi import HTTPException, Request

from common.logger import setup_logger
from config import API_ACCESS_HEADER, API_ACCESS_KEY

logger = setup_logger("api:auth")


def require_api_key(request: Request) -> None:
    if not API_ACCESS_KEY:
        logger.error("API_ACCESS_KEY not configured")
        raise HTTPException(status_code=500, detail="API access key not configured")

    got = request.headers.get(API_ACCESS_HEADER, "").strip()
    if got != API_ACCESS_KEY:
        logger.warning("invalid api key")
        raise HTTPException(status_code=401, detail="Invalid API key")
