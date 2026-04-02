from typing import Optional

from common.db_scheme import build_db_context
from common.logger import setup_logger

logger = setup_logger("er_version_api")


def get_current_season_id() -> Optional[int]:
    ctx = build_db_context()
    col = ctx.view.versions

    doc = col.find_one(sort=[("lastSeenAt", -1)])
    if not doc:
        logger.warning("view.versions empty")
        return None

    season_id = doc.get("seasonId")
    try:
        return int(season_id)
    except Exception:
        logger.warning("invalid seasonId in versions: %r", season_id)
        return None
