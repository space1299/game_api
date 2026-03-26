"""
사용법:
    from logger import setup_logger
    logger = setup_logger("collector", "/home/user/logs/collector.log")
    logger.info("hello")
    logger.warning("주의")
    logger.error("에러")

특징:
    - 콘솔 + 파일 동시 출력(파일 경로 미지정 시 콘솔만)
    - 파일은 크기 기반 로테이션 (기본 5MB, 5개 보관)
    - 경고 메서드는 표준 logging API인 logger.warning 사용
    - log_file 미지정 시 LOG_FILE 환경변수 값을 자동 사용
"""
import logging
import os
from logging import Logger
from logging.handlers import RotatingFileHandler
from typing import Optional, Union

__all__ = ["setup_logger"]

_LEVEL_MAP = {
    "CRITICAL": logging.CRITICAL,
    "ERROR": logging.ERROR,
    "WARNING": logging.WARNING,
    "INFO": logging.INFO,
    "DEBUG": logging.DEBUG,
    "NOTSET": logging.NOTSET,
}

_DEFAULT_FMT = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
_DEFAULT_DATEFMT = "%Y-%m-%d %H:%M:%S"


def _as_level(level: Union[int, str]) -> int:
    if isinstance(level, int):
        return level
    return _LEVEL_MAP.get(str(level).upper(), logging.INFO)


def _ensure_dir(path: str) -> None:
    d = os.path.dirname(path or "")
    if d and not os.path.exists(d):
        os.makedirs(d, exist_ok=True)


def setup_logger(
    name: str,
    log_file: Optional[str] = None,   # 호출 시점에 LOG_FILE 환경변수로 대체 가능
    level: Union[int, str] = "INFO",
    *,
    console: bool = True,
    # 크기 기반 로테이션 기본값: 5MB / 5개 보관
    max_bytes: int = 5 * 1024 * 1024,
    backup_count: int = 5,
    fmt: str = _DEFAULT_FMT,
    datefmt: str = _DEFAULT_DATEFMT,
    propagate: bool = False,
) -> Logger:
    logger = logging.getLogger(name)

    if logger.handlers:
        logger.setLevel(_as_level(level))
        logger.propagate = propagate
        for h in logger.handlers:
            h.setLevel(_as_level(level))
        return logger

    if log_file is None:
        log_file = os.getenv("ER_DATA_LOG_FILE", None)

    logger.setLevel(_as_level(level))
    logger.propagate = propagate

    formatter = logging.Formatter(fmt=fmt, datefmt=datefmt)

    if console:
        sh = logging.StreamHandler()
        sh.setLevel(_as_level(level))
        sh.setFormatter(formatter)
        logger.addHandler(sh)

    if log_file:
        try:
            _ensure_dir(log_file)
            fh = RotatingFileHandler(
                log_file,
                maxBytes=max_bytes,
                backupCount=backup_count,
                encoding="utf-8",
                delay=True,  
            )
            fh.setLevel(_as_level(level))
            fh.setFormatter(formatter)
            logger.addHandler(fh)
        except Exception:
            pass

    return logger
