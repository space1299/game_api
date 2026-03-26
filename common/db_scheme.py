from dataclasses import dataclass, field
from typing import Dict, Iterable, Set

from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.database import Database

class StrictNamespace:
    def __init__(self, db: Database, allowed: Iterable[str], ns_name: str):
        self._db = db
        self._allowed: Set[str] = set(allowed)
        self._ns_name = ns_name

    def col(self, name: str) -> Collection:
        if name not in self._allowed:
            raise AttributeError(
                f"[DBContext] {self._ns_name}.{name!r} 는 허용되지 않은 컬렉션입니다. "
                f"허용 목록: {sorted(self._allowed)}"
            )
        return self._db[name]

    def __getattr__(self, name: str) -> Collection:
        return self.col(name)

    def __dir__(self):
        return sorted(set(super().__dir__()) | self._allowed)

@dataclass
class ViewCollections:
    versions: Collection
    info_changes: Collection
    character_statistics: Collection
    user_reports: Collection

@dataclass
class InfoCollections:
    l10n_info: Collection
    current: Collection
    snapshots: Collection
    changes: Collection

@dataclass
class RawCollections:
    game_data_raw: Collection
    user_checks: Collection
    character_statistics: Collection

@dataclass
class ReportCollections:
    report_jobs: Collection

@dataclass
class DBContext:
    client: MongoClient

    # === 여기서 “명시적 스키마(허용 컬렉션)”를 선언 ===
    VIEW_COLS: tuple[str, ...] = (
        "versions",
        "info_changes",
        "character_statistics",
        "user_reports",
    )
    INFO_COLS: tuple[str, ...] = (
        "l10n_info",
        "current",
        "snapshots",
        "changes",
    )
    RAW_COLS:  tuple[str, ...] = (
        "game_data_raw",
        "user_checks",
        "character_statistics"
    )
    REPORT_COLS: tuple[str, ...] = (
        "report_jobs",
    )

    _view_ns: StrictNamespace | None = field(default=None, init=False)
    _info_ns: StrictNamespace | None = field(default=None, init=False)
    _raw_cache: Dict[str, StrictNamespace] = field(default_factory=dict, init=False)
    _report_ns: StrictNamespace | None = field(default=None, init=False)

    @property
    def view(self) -> ViewCollections:
        if self._view_ns is None:
            self._view_ns = StrictNamespace(
                self.client["er_game_view"], self.VIEW_COLS, "view"
            )
        ns = self._view_ns
        return ViewCollections(
            versions=ns.versions,
            info_changes=ns.info_changes,
            character_statistics=ns.character_statistics,
            user_reports=ns.user_reports,
        )

    @property
    def info(self) -> InfoCollections:
        if self._info_ns is None:
            self._info_ns = StrictNamespace(
                self.client["er_game_info"], self.INFO_COLS, "info"
            )
        ns = self._info_ns
        return InfoCollections(
            l10n_info=ns.l10n_info,
            current=ns.current,
            snapshots=ns.snapshots,
            changes=ns.changes,
        )

    def raw(self, version_str: str) -> RawCollections:
        suffix = version_str.replace(".", "_")
        db_name = f"er_game_data_v{suffix}"
        if db_name not in self._raw_cache:
            self._raw_cache[db_name] = StrictNamespace(
                self.client[db_name], self.RAW_COLS, f"raw({version_str})"
            )
        ns = self._raw_cache[db_name]
        return RawCollections(
            game_data_raw=ns.game_data_raw,
            user_checks=ns.user_checks,
            character_statistics=ns.character_statistics,
        )

    @property
    def report(self) -> ReportCollections:
        if self._report_ns is None:
            self._report_ns = StrictNamespace(
                self.client["er_user_report"], self.REPORT_COLS, "report"
            )
        ns = self._report_ns
        return ReportCollections(
            report_jobs=ns.report_jobs,
        )

    def __dir__(self):
        return sorted(set(super().__dir__()) | {"view", "info", "raw", "report"})


def build_db_context(db_url = None) -> DBContext:
    if db_url is None:
        from config import DB_URL
        db_url = DB_URL

    client = MongoClient(db_url)

    return DBContext(client)
