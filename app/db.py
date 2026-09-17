"""SQLite 连接与表结构。

所有对任务状态的修改都在单个 SQLite 连接的一个事务内完成，
依靠 BEGIN IMMEDIATE 取得的保留锁保证同一时刻只有一个工作器
能从 PENDING 队列领取到给定任务（原子领取）。
"""

from __future__ import annotations

import os
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

# 默认租约时长（秒），任务可用 lease_seconds 覆盖。
DEFAULT_LEASE_SECONDS = 30.0


def db_path() -> Path:
    return Path(os.environ.get("DB_PATH", "./data/tasks.db"))


def get_default_lease_seconds() -> float:
    return float(os.environ.get("DEFAULT_LEASE_SECONDS", DEFAULT_LEASE_SECONDS))


def _connect(path: Path | None = None) -> sqlite3.Connection:
    path = path or db_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(
        str(path),
        timeout=30.0,
        isolation_level=None,  # 手工管理事务
        check_same_thread=False,
    )
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA busy_timeout = 30000")
    return conn


def init_db(path: Path | None = None) -> None:
    with _connect(path) as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS dags (
                id            TEXT PRIMARY KEY,
                name          TEXT NOT NULL,
                created_at    TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS tasks (
                id                  TEXT PRIMARY KEY,
                dag_id              TEXT NOT NULL REFERENCES dags(id),
                task_key            TEXT NOT NULL,
                name                TEXT NOT NULL,
                payload             TEXT,
                max_retries         INTEGER NOT NULL DEFAULT 0,
                lease_seconds       REAL NOT NULL,
                state               TEXT NOT NULL,        -- pending/running/succeeded/failed/blocked
                blocked_reason      TEXT,
                lease_token         TEXT,
                lease_expires_at    TEXT,
                worker_id           TEXT,
                attempt             INTEGER NOT NULL DEFAULT 0,
                created_at          TEXT NOT NULL,
                started_at          TEXT,
                finished_at         TEXT,
                UNIQUE(dag_id, task_key)
            );

            CREATE TABLE IF NOT EXISTS dependencies (
                dag_id     TEXT NOT NULL REFERENCES dags(id),
                task_key   TEXT NOT NULL,
                dep_key    TEXT NOT NULL,
                PRIMARY KEY (dag_id, task_key, dep_key)
            );

            CREATE TABLE IF NOT EXISTS attempts (
                id             INTEGER PRIMARY KEY AUTOINCREMENT,
                task_id        TEXT NOT NULL REFERENCES tasks(id),
                attempt_no     INTEGER NOT NULL,
                lease_token    TEXT NOT NULL UNIQUE,
                worker_id      TEXT,
                leased_at      TEXT NOT NULL,
                expires_at     TEXT NOT NULL,
                heartbeat_at   TEXT,
                state          TEXT NOT NULL,   -- active/expired/succeeded/failed
                error          TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_tasks_dag_state ON tasks(dag_id, state);
            CREATE INDEX IF NOT EXISTS idx_deps_dep ON dependencies(dag_id, dep_key);
            CREATE INDEX IF NOT EXISTS idx_attempts_task ON attempts(task_id, attempt_no);
            """
        )


@contextmanager
def get_conn() -> Iterator[sqlite3.Connection]:
    """返回一个开启外键/WAL 的连接，调用方负责 commit/rollback。"""
    conn = _connect()
    try:
        yield conn
    finally:
        conn.close()
