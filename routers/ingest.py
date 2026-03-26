from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field
from typing import Any, Dict, List, Optional

from config import INGEST_API_KEY, INGEST_ALLOWED_COLLECTIONS

from common.db_scheme import build_db_context
from common.limiter import limiter
from common.logger import setup_logger

logger = setup_logger("api:ingest")

router = APIRouter(prefix="/ingest", tags=["ingest"])


def _require_api_key(request: Request) -> None:
    if not INGEST_API_KEY:
        logger.error("INGEST_API_KEY not configured")
        raise HTTPException(status_code=500, detail="INGEST_API_KEY not configured")
    got = request.headers.get("X-API-Key", "").strip()
    if got != INGEST_API_KEY:
        logger.warning("invalid ingest api key")
        raise HTTPException(status_code=401, detail="Invalid API key")


def _require_allowed_collection(name: str) -> None:
    if name not in INGEST_ALLOWED_COLLECTIONS:
        logger.warning("collection not allowed: %s", name)
        raise HTTPException(
            status_code=400,
            detail=f"collection not allowed: {name}",
        )


class UpsertDoc(BaseModel):
    key: Dict[str, Any] = Field(default_factory=dict)
    doc: Dict[str, Any] = Field(default_factory=dict)


class IngestRequest(BaseModel):
    collection: str
    docs: List[UpsertDoc]
    source: Optional[str] = None


@router.post("/upsert")
@limiter.limit("30/minute")
def upsert_view_docs(request: Request, body: IngestRequest):
    _require_api_key(request)
    _require_allowed_collection(body.collection)

    db = build_db_context()
    col = getattr(db.view, body.collection)

    if not body.docs:
        logger.warning("empty docs for collection: %s", body.collection)
        raise HTTPException(status_code=400, detail="docs is empty")

    upserted_count = 0
    modified_count = 0

    for item in body.docs:
        if not item.key:
            logger.warning("missing doc.key for collection: %s", body.collection)
            raise HTTPException(status_code=400, detail="doc.key is required for upsert")

        doc_to_set = dict(item.doc)
        upsert_filter = {}

        doc_id_from_key = item.key.get("_id")
        if "_id" in doc_to_set:
            del doc_to_set["_id"]

        if body.collection == "character_statistics":
            version_str = doc_to_set.get("versionStr")
            mmr_range = doc_to_set.get("mmrRange")

            if version_str is None or mmr_range is None:
                continue

            upsert_filter = {
                "versionStr": version_str,
                "mmrRange": mmr_range,
            }
        else:
            if doc_id_from_key is None:
                continue
            upsert_filter = {"_id": doc_id_from_key}

        res = col.update_one(
            filter=upsert_filter,
            update={"$set": doc_to_set},
            upsert=True,
        )

        if res.upserted_id is not None:
            upserted_count += 1
        modified_count += int(res.modified_count or 0)

    logger.info(
        "ingest upsert: collection=%s received=%d upserted=%d modified=%d",
        body.collection,
        len(body.docs),
        upserted_count,
        modified_count,
    )

    return {
        "ok": True,
        "collection": body.collection,
        "received": len(body.docs),
        "upserted": upserted_count,
        "modified": modified_count,
        "source": body.source,
    }
