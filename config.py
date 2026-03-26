import os
from urllib.parse import quote_plus
from typing import Set

from dotenv import load_dotenv

load_dotenv()

# log 관련 설정
LOG_FILE = os.environ.get("LOG_FILE", "./log/api.log")

# MongoDB 관련 설정
DB_HOST = os.environ.get("DB_HOST", "")
DB_USER = os.environ.get("DB_USER", "")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "")

user_enc = quote_plus(DB_USER or "")
pass_enc = quote_plus(DB_PASSWORD or "")

DB_URL = os.environ.get("DB_URL", f"mongodb://{user_enc}:{pass_enc}@{DB_HOST}")

API_ACCESS_KEY = os.environ.get("API_ACCESS_KEY", "")
API_ACCESS_HEADER = os.environ.get("API_ACCESS_HEADER", "X-API-Key")

INGEST_API_KEY = os.environ.get("INGEST_API_KEY", "")

def _parse_csv_env(name: str, default: str = "") -> Set[str]:
    raw = os.getenv(name, default)
    # "a,b,c" 형태를 a/b/c로 파싱
    items = [x.strip() for x in raw.split(",")]
    return {x for x in items if x}

INGEST_ALLOWED_COLLECTIONS: Set[str] = _parse_csv_env(
    "INGEST_ALLOWED_COLLECTIONS",
    "versions,character_statistics"
)

# user report settings
REPORT_N_TARGET = int(os.environ.get("REPORT_N_TARGET", "50"))
REPORT_N_MIN = int(os.environ.get("REPORT_N_MIN", "30"))
REPORT_MAX_VERSIONS_BACK = int(os.environ.get("REPORT_MAX_VERSIONS_BACK", "2"))
REPORT_WINDOW_RULE_VERSION = os.environ.get("REPORT_WINDOW_RULE_VERSION", "v1")
REPORT_SEASON_CAP = os.environ.get("REPORT_SEASON_CAP", "current_season_only")
