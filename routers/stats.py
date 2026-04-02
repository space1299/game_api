# routers/stats.py
import time
from typing import Dict, List, Any

from fastapi import APIRouter, HTTPException, Request

from common.auth import require_api_key
from common.db_scheme import build_db_context
from common.limiter import limiter
from common.logger import setup_logger

router = APIRouter()
logger = setup_logger("api:stats")

_CACHE_TTL_SEC = int((__import__("os").getenv("STATS_CACHE_TTL", "20")).strip() or "20")
_stats_cache: dict[str, tuple[float, Dict[str, List[Any]]]] = {}
_versions_cache: tuple[float, List[str]] | None = None


def _parse_semver(v: str):
    parts = v.split(".")
    parts = (parts + ["0", "0", "0"])[:3]
    out = []
    for x in parts:
        try:
            out.append(int(x))
        except Exception:
            out.append(0)
    return tuple(out)


@router.get("/versions", response_model=List[str])
@limiter.limit("20/minute")
def get_versions(request: Request) -> List[str]:
    require_api_key(request)
    global _versions_cache

    now = time.time()
    if _versions_cache and (now - _versions_cache[0]) < _CACHE_TTL_SEC:
        logger.debug("versions cache hit")
        return _versions_cache[1]

    db = build_db_context()
    col = db.view.versions

    docs = list(col.find({}, {"_id": 0, "versionStr": 1}))
    versions: List[str] = []
    for d in docs:
        v = d.get("versionStr")
        if isinstance(v, str) and v.strip():
            versions.append(v.strip())

    versions = sorted(set(versions), key=_parse_semver, reverse=True)
    if not versions:
        logger.warning("versions empty")
        raise HTTPException(status_code=404, detail="versions not found")

    _versions_cache = (now, versions)
    logger.info("versions loaded: %d", len(versions))
    return versions


@router.get("/stats/character/{version}")
@limiter.limit("60/minute")
def get_character_statistics(request: Request, version: str) -> Dict[str, List[Any]]:
    require_api_key(request)
    version = version.strip().replace("_", ".")
    parts = version.split(".")
    if len(parts) != 3 or any(not p.isdigit() for p in parts):
        raise HTTPException(status_code=400, detail="invalid version format")

    now = time.time()

    hit = _stats_cache.get(version)
    if hit and (now - hit[0]) < _CACHE_TTL_SEC:
        logger.debug("character stats cache hit: %s", version)
        return hit[1]

    db = build_db_context()
    col = db.view.character_statistics

    docs = list(
        col.find(
            {"versionStr": version},
            {"_id": 0, "mmrRange": 1, "data": 1},
        )
    )

    if not docs:
        logger.warning("character stats not found: %s", version)
        raise HTTPException(status_code=404, detail="character stats not found")

    out: Dict[str, List[Any]] = {}
    for d in docs:
        mmr = d.get("mmrRange")
        data = d.get("data")
        if isinstance(mmr, str) and isinstance(data, list):
            out[mmr] = data

    if not out:
        logger.warning("character stats empty after parse: %s", version)
        raise HTTPException(status_code=404, detail="character stats empty after parse")

    _stats_cache[version] = (now, out)
    logger.info("character stats loaded: %s ranges=%d", version, len(out))
    return out
