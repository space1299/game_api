from datetime import datetime, timezone
from typing import Any, Dict

from bson import ObjectId
from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import JSONResponse
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
    if isinstance(out.get("result_ref"), ObjectId):
        out["result_ref"] = str(out["result_ref"])
    return out


def _is_report_valid(doc: Dict[str, Any]) -> bool:
    expires_at = doc.get("expiresAt")
    if not isinstance(expires_at, datetime):
        return True
    return expires_at > utc_now()


def _find_report_for_job(
    job: Dict[str, Any], col_reports
) -> Dict[str, Any] | None:
    result_ref = job.get("result_ref")
    if not result_ref:
        return None

    report = col_reports.find_one({"_id": str(result_ref)})
    if not report or not _is_report_valid(report):
        return None
    return report


def _upsert_job(nickname: str, season_id: int, dedupe_key: str) -> Dict[str, Any]:
    ctx = build_db_context()
    col_jobs = ctx.report.report_jobs
    now = utc_now()

    existing = col_jobs.find_one({"dedupe_key": dedupe_key})
    if existing and existing.get("status") in ("queued", "running"):
        return existing

    params = {
        "n_target": REPORT_N_TARGET,
        "n_min": REPORT_N_MIN,
        "max_versions_back": REPORT_MAX_VERSIONS_BACK,
        "season_cap": REPORT_SEASON_CAP,
    }

    update = {
        "$set": {
            "dedupe_key": dedupe_key,
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
        {"dedupe_key": dedupe_key},
        update,
        upsert=True,
        return_document=ReturnDocument.AFTER,
    )

    if not job:
        raise RuntimeError("failed to upsert report job")

    return job


@router.get("/api/user-report")
@limiter.limit("60/minute")
def get_user_report(request: Request, nickname: str = Query(...)):
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
    dedupe_key = build_dedupe_key(
        nickname, season_id, matching_mode, REPORT_WINDOW_RULE_VERSION
    )
    ctx = build_db_context()
    col_jobs = ctx.report.report_jobs
    col_reports = ctx.view.user_reports

    existing_job = col_jobs.find_one({"dedupe_key": dedupe_key})
    if existing_job:
        report = _find_report_for_job(existing_job, col_reports)
        if report:
            logger.info(
                "report resolved via job result_ref: nickname=%s season_id=%s job_id=%s",
                nickname,
                season_id,
                existing_job.get("_id"),
            )
            return {"status": "done", "report": _serialize_doc(report)}

        if existing_job.get("status") in ("queued", "running"):
            logger.info(
                "existing job reused: nickname=%s season_id=%s job_id=%s status=%s",
                nickname,
                season_id,
                existing_job.get("_id"),
                existing_job.get("status"),
            )
            return JSONResponse(
                status_code=202,
                content={
                    "status": existing_job.get("status"),
                    "jobId": str(existing_job.get("_id")),
                },
            )

    job = _upsert_job(nickname, season_id, dedupe_key)
    report = _find_report_for_job(job, col_reports)
    if report:
        logger.info("report cache hit: nickname=%s season_id=%s", nickname, season_id)
        return {"status": "done", "report": _serialize_doc(report)}

    logger.info(
        "job upserted: nickname=%s season_id=%s job_id=%s",
        nickname,
        season_id,
        job.get("_id"),
    )
    return JSONResponse(
        status_code=202,
        content={"status": job.get("status"), "jobId": str(job.get("_id"))},
    )


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
