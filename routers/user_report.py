import asyncio
import json
from datetime import datetime, timedelta, timezone
from typing import Any, Dict

from bson import ObjectId
from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import JSONResponse, StreamingResponse
from pymongo import ReturnDocument

from common.auth import require_api_key
from common.db_scheme import build_db_context
from common.er_version_api import get_current_season_id
from common.limiter import limiter
from common.logger import setup_logger
from config import (
    REPORT_MAX_VERSIONS_BACK,
    REPORT_N_MIN,
    REPORT_N_TARGET,
    REPORT_SEASON_CAP,
    REPORT_WINDOW_RULE_VERSION,
)

router = APIRouter()
logger = setup_logger("api:user_report")

REFRESH_COOLDOWN_SECONDS = 600  # 10 minutes
_SSE_POLL_INTERVAL = 3          # seconds between DB polls
_SSE_KEEPALIVE_INTERVAL = 20    # seconds between SSE keepalive comments


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def normalize_nickname(nickname: str) -> str:
    return (nickname or "").strip().lower()


def build_dedupe_key(
    nickname: str, season_id: int, matching_mode: int, window_rule_version: str
) -> str:
    norm = normalize_nickname(nickname)
    return f"{norm}|{season_id}|{matching_mode}|{window_rule_version}"


def _serialize_doc(doc: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(doc)
    if isinstance(out.get("_id"), ObjectId):
        out["_id"] = str(out["_id"])
    if isinstance(out.get("resultRef"), ObjectId):
        out["resultRef"] = str(out["resultRef"])
    return out


def _is_report_valid(doc: Dict[str, Any]) -> bool:
    expires_at = doc.get("expiresAt")
    if not isinstance(expires_at, datetime):
        return True
    return expires_at > utc_now()


def _upsert_job(nickname: str, season_id: int, dedupe_key: str) -> Dict[str, Any]:
    ctx = build_db_context()
    col_jobs = ctx.report.report_jobs
    now = utc_now()

    existing = col_jobs.find_one({"dedupeKey": dedupe_key})
    if existing and existing.get("status") in ("queued", "running"):
        return existing

    params = {
        "nTarget": REPORT_N_TARGET,
        "nMin": REPORT_N_MIN,
        "maxVersionsBack": REPORT_MAX_VERSIONS_BACK,
        "seasonCap": REPORT_SEASON_CAP,
    }

    update = {
        "$set": {
            "dedupeKey": dedupe_key,
            "nickname": nickname,
            "params": params,
            "status": "queued",
            "updatedAt": now,
            "lockedAt": None,
            "lockedBy": None,
        },
        "$setOnInsert": {
            "createdAt": now,
        },
    }

    job = col_jobs.find_one_and_update(
        {"dedupeKey": dedupe_key},
        update,
        upsert=True,
        return_document=ReturnDocument.AFTER,
    )

    if not job:
        raise RuntimeError("failed to upsert report job")

    return job


def _resolve_request_params(request: Request, nickname: str):
    """Validate auth and common inputs. Returns (normalized_nickname, season_id, dedupe_key)."""
    require_api_key(request)

    if not nickname.strip():
        raise HTTPException(status_code=400, detail="nickname is required")
    nickname = normalize_nickname(nickname)
    if not nickname:
        raise HTTPException(status_code=400, detail="nickname is required")

    if REPORT_SEASON_CAP != "current_season_only":
        logger.warning("unsupported season_cap: %s", REPORT_SEASON_CAP)
        raise HTTPException(status_code=400, detail="unsupported season_cap")

    season_id = get_current_season_id()
    if season_id is None:
        logger.warning("current season not available")
        raise HTTPException(status_code=503, detail="current season not available")

    matching_mode = 3
    dedupe_key = build_dedupe_key(nickname, season_id, matching_mode, REPORT_WINDOW_RULE_VERSION)
    return nickname, season_id, dedupe_key


# ---------------------------------------------------------------------------
# GET /api/user-report
# ---------------------------------------------------------------------------

@router.get("/api/user-report")
@limiter.limit("60/minute")
def get_user_report(request: Request, nickname: str = Query(...)):
    nickname, season_id, dedupe_key = _resolve_request_params(request, nickname)

    ctx = build_db_context()
    col_jobs = ctx.report.report_jobs
    col_reports = ctx.view.user_reports

    # Step 1: check view.user_reports by dedupeKey directly for an existing report
    existing_report = col_reports.find_one({"dedupeKey": dedupe_key})
    if existing_report and not _is_report_valid(existing_report):
        existing_report = None

    # Step 2: ensure a refresh job exists; preserve active (queued/running) jobs
    existing_job = col_jobs.find_one({"dedupeKey": dedupe_key})
    if existing_job and existing_job.get("status") in ("queued", "running"):
        job = existing_job
        logger.info(
            "active job preserved: nickname=%s season_id=%s job_id=%s status=%s",
            nickname, season_id, existing_job.get("_id"), existing_job.get("status"),
        )
    else:
        job = _upsert_job(nickname, season_id, dedupe_key)
        logger.info(
            "job upserted: nickname=%s season_id=%s job_id=%s",
            nickname, season_id, job.get("_id"),
        )

    job_id = str(job.get("_id", ""))
    job_status = job.get("status", "queued")
    job_error = str(job.get("error", "")) if job_status == "error" and job.get("error") else None

    if existing_report:
        logger.info(
            "report returned immediately from view: nickname=%s season_id=%s",
            nickname, season_id,
        )
        body: Dict[str, Any] = {
            "status": "done",
            "report": _serialize_doc(existing_report),
            "jobId": job_id,
            "jobStatus": job_status,
        }
        if job_error:
            body["jobError"] = job_error
        return body

    logger.info(
        "no report available: nickname=%s season_id=%s job_id=%s job_status=%s",
        nickname, season_id, job_id, job_status,
    )
    body = {
        "status": "pending",
        "report": None,
        "jobId": job_id,
        "jobStatus": job_status,
    }
    if job_error:
        body["jobError"] = job_error
    return JSONResponse(status_code=202, content=body)


# ---------------------------------------------------------------------------
# POST /api/user-report/refresh
# ---------------------------------------------------------------------------

@router.post("/api/user-report/refresh")
@limiter.limit("10/minute")
def post_user_report_refresh(request: Request, nickname: str = Query(...)):
    nickname, season_id, dedupe_key = _resolve_request_params(request, nickname)

    ctx = build_db_context()
    col_jobs = ctx.report.report_jobs
    now = utc_now()

    existing_job = col_jobs.find_one({"dedupeKey": dedupe_key})

    if existing_job:
        status = existing_job.get("status")
        job_id = str(existing_job.get("_id", ""))

        # Already refreshing — no duplicate
        if status in ("queued", "running"):
            logger.info(
                "refresh already in progress: nickname=%s season_id=%s job_id=%s status=%s",
                nickname, season_id, job_id, status,
            )
            return JSONResponse(
                status_code=200,
                content={
                    "refreshStatus": "in_progress",
                    "jobId": job_id,
                    "jobStatus": status,
                },
            )

        # Cooldown check for completed/failed jobs
        if status in ("done", "error"):
            updated_at = existing_job.get("updatedAt")
            if isinstance(updated_at, datetime):
                if updated_at.tzinfo is None:
                    updated_at = updated_at.replace(tzinfo=timezone.utc)
                age_seconds = (now - updated_at).total_seconds()
                if age_seconds < REFRESH_COOLDOWN_SECONDS:
                    cooldown_ends_at = updated_at + timedelta(seconds=REFRESH_COOLDOWN_SECONDS)
                    logger.info(
                        "refresh cooldown active: nickname=%s season_id=%s job_id=%s remaining=%.0fs",
                        nickname, season_id, job_id,
                        REFRESH_COOLDOWN_SECONDS - age_seconds,
                    )
                    return JSONResponse(
                        status_code=200,
                        content={
                            "refreshStatus": "cooldown",
                            "jobId": job_id,
                            "jobStatus": status,
                            "updatedAt": updated_at.isoformat(),
                            "cooldownEndsAt": cooldown_ends_at.isoformat(),
                        },
                    )

    # Eligible: create or requeue
    job = _upsert_job(nickname, season_id, dedupe_key)
    job_id = str(job.get("_id", ""))
    logger.info(
        "manual refresh accepted: nickname=%s season_id=%s job_id=%s",
        nickname, season_id, job_id,
    )
    return JSONResponse(
        status_code=200,
        content={
            "refreshStatus": "accepted",
            "jobId": job_id,
            "jobStatus": job.get("status", "queued"),
        },
    )


# ---------------------------------------------------------------------------
# GET /api/user-report/status
# ---------------------------------------------------------------------------

@router.get("/api/user-report/status")
@limiter.limit("120/minute")
def get_user_report_status(request: Request, jobId: str = Query(...)):
    require_api_key(request)

    if not jobId:
        raise HTTPException(status_code=400, detail="jobId is required")

    try:
        obj_id = ObjectId(jobId)
    except Exception:
        logger.warning("invalid jobId: %s", jobId)
        raise HTTPException(status_code=400, detail="invalid jobId")

    ctx = build_db_context()
    col_jobs = ctx.report.report_jobs
    job = col_jobs.find_one({"_id": obj_id})
    if not job:
        logger.warning("job not found: %s", jobId)
        raise HTTPException(status_code=404, detail="job not found")

    out = _serialize_doc(job)
    status = out.get("status")
    if status == "done":
        out["message"] = "report job completed"
    elif status == "error":
        out["message"] = "report job failed"
    elif status == "running":
        out["message"] = "report job is running"
    else:
        out["message"] = "report job is queued"
    return out


# ---------------------------------------------------------------------------
# GET /api/user-report/stream  (SSE)
# ---------------------------------------------------------------------------

@router.get("/api/user-report/stream")
@limiter.limit("10/minute")
async def get_user_report_stream(request: Request, nickname: str = Query(...)):
    nickname, season_id, dedupe_key = _resolve_request_params(request, nickname)

    async def event_generator():
        last_status = None
        keepalive_counter = 0

        while True:
            if await request.is_disconnected():
                logger.info("sse client disconnected: nickname=%s", nickname)
                break

            try:
                ctx = build_db_context()
                job = ctx.report.report_jobs.find_one({"dedupeKey": dedupe_key})
            except Exception as exc:
                logger.error("sse db error: nickname=%s error=%s", nickname, exc)
                yield f"data: {json.dumps({'type': 'error', 'error': 'internal error'})}\n\n"
                break

            if job:
                current_status = job.get("status")
                if current_status != last_status:
                    last_status = current_status
                    payload: Dict[str, Any] = {
                        "type": "status",
                        "jobId": str(job["_id"]),
                        "jobStatus": current_status,
                    }
                    if current_status == "error":
                        payload["error"] = str(job.get("error", "")) or None
                    elif current_status == "done":
                        payload["dedupeKey"] = dedupe_key
                    yield f"data: {json.dumps(payload)}\n\n"

                    if current_status in ("done", "error"):
                        logger.info(
                            "sse terminal state reached: nickname=%s status=%s",
                            nickname, current_status,
                        )
                        break

            await asyncio.sleep(_SSE_POLL_INTERVAL)
            keepalive_counter += _SSE_POLL_INTERVAL
            if keepalive_counter >= _SSE_KEEPALIVE_INTERVAL:
                yield ": keepalive\n\n"
                keepalive_counter = 0

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )
