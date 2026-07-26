"""Postgres connection pool (ADR 0019: plain psycopg, not an ORM).

A pool (rather than one connection per request opened from scratch) is
still "plain psycopg", not heavier machinery -- ``psycopg_pool`` is
maintained by the same project as the driver itself and exists
specifically so request-path code doesn't pay a fresh TCP+auth
round-trip on every lookup. The pool is a module-level singleton created
once at app startup and closed at shutdown (see app/main.py's lifespan).
"""

from __future__ import annotations

from psycopg_pool import ConnectionPool

_pool: ConnectionPool | None = None


def init_pool(database_url: str) -> ConnectionPool:
    global _pool
    if _pool is not None:
        return _pool
    # open=False: don't block app startup on Postgres being reachable at
    # import time -- opened explicitly right after, so failures surface
    # as a clear startup error rather than a lazy first-request one.
    _pool = ConnectionPool(conninfo=database_url, open=False, min_size=1, max_size=5)
    _pool.open(wait=True, timeout=30)
    return _pool


def get_pool() -> ConnectionPool:
    if _pool is None:
        raise RuntimeError("Connection pool not initialized -- call init_pool() at startup first")
    return _pool


def close_pool() -> None:
    global _pool
    if _pool is not None:
        _pool.close()
        _pool = None
