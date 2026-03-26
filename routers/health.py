from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from common.auth import require_api_key
from common.db_scheme import build_db_context
from common.logger import setup_logger

router = APIRouter()
logger = setup_logger("api:health")


@router.get("/health")
def get_health(request: Request):
    require_api_key(request)

    try:
        ctx = build_db_context()
        ctx.client["er_game_view"].command("ping")
        ctx.client["er_user_report"].command("ping")
    except Exception as exc:
        logger.exception("health db check failed")
        return JSONResponse(
            status_code=503,
            content={
                "ok": False,
                "status": "degraded",
                "databases": {
                    "view": "error",
                    "report": "error",
                },
                "detail": str(exc),
            },
        )

    return {
        "ok": True,
        "status": "ok",
        "databases": {
            "view": "ok",
            "report": "ok",
        },
    }
