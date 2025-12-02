"""Small helpers for consistent CLI messaging and exits."""

from __future__ import annotations

from pathlib import Path
from typing import Any, NoReturn


PREFIX = "[fraud-demo]"


def info(message: str) -> None:
    print(f"{PREFIX} {message}")


def warn(message: str) -> None:
    print(f"{PREFIX} WARNING: {message}")


def error_exit(message: str, *, code: int = 1) -> NoReturn:
    print(f"{PREFIX} ERROR: {message}")
    raise SystemExit(code)


def require_file(path: Path, hint: str | None = None) -> None:
    if not path.exists():
        suffix = f" {hint}" if hint else ""
        error_exit(f"Required file not found at {path}.{suffix}")


def format_exception(exc: Any) -> str:
    return f"{type(exc).__name__}: {exc}"

