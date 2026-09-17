"""端到端行为测试。

运行: pytest -q （需先 pip install -r requirements-dev.txt）
"""

from __future__ import annotations

import json
import os
import threading
import time
import uuid
from pathlib import Path

import pytest


@pytest.fixture()
def client(tmp_path, monkeypatch):
    db_file = tmp_path / "test.db"
    monkeypatch.setenv("DB_PATH", str(db_file))
    monkeypatch.setenv("DEFAULT_LEASE_SECONDS", "30")

    from fastapi.testclient import TestClient

    from app.main import app

    with TestClient(app) as c:
        yield c


def submit(client, tasks, name="dag", dag_id=None):
    body = {"name": name, "tasks": tasks}
    if dag_id:
        body["id"] = dag_id
    r = client.post("/dags", json=body)
    assert r.status_code == 201, r.text
    return r.json()


def by_key(dag, key):
    return next(t for t in dag["tasks"] if t["key"] == key)


# ------------------------------------------------------------- 提交校验

def test_submit_valid_dag(client):
    dag = submit(
        client,
        [
            {"key": "a"},
            {"key": "b", "deps": ["a"]},
        ],
    )
    assert dag["task_count"] == 2
    assert dag["states"]["pending"] == 2


def test_submit_rejects_missing_dependency(client):
    r = client.post("/dags", json={"tasks": [{"key": "a", "deps": ["ghost"]}]})
    assert r.status_code == 422
    assert r.json()["error"]["code"] == "missing_dependency"
    assert "ghost" in r.json()["error"]["details"]["missing"]


def test_submit_rejects_self_dependency_cycle(client):
    r = client.post("/dags", json={"tasks": [{"key": "a", "deps": ["a"]}]})
    assert r.status_code == 422
    assert r.json()["error"]["code"] == "cycle_detected"


def test_submit_rejects_long_cycle(client):
    r = client.post(
        "/dags",
        json={"tasks": [{"key": "a", "deps": ["c"]}, {"key": "b", "deps": ["a"]}, {"key": "c", "deps": ["b"]}]},
    )
    assert r.status_code == 422
    assert r.json()["error"]["code"] == "cycle_detected"
    assert r.json()["error"]["details"]["cycle"][0] == r.json()["error"]["details"]["cycle"][-1]


def test_submit_rejects_duplicate_key(client):
    r = client.post("/dags", json={"tasks": [{"key": "a"}, {"key": "a"}]})
    assert r.status_code == 422
    assert r.json()["error"]["code"] == "duplicate_task_key"


# ------------------------------------------------------------- 依赖门控

def test_claim_respects_dependencies(client):
    dag = submit(client, [{"key": "a"}, {"key": "b", "deps": ["a"]}])
    r = client.post("/tasks/claim", json={"limit": 10})
    assert r.status_code == 200
    keys = {t["task_key"] for t in r.json()["claimed"]}
    assert keys == {"a"}

    # a 已在 running 且租约未过期，不重复领取；b 的前置未完成，也不可领
    r = client.post("/tasks/claim", json={"dag_id": dag["id"]})
    assert r.json()["count"] == 0


def test_full_dependency_flow(client):
    dag = submit(
        client,
        [
            {"key": "a"},
            {"key": "b", "deps": ["a"]},
            {"key": "c", "deps": ["a"]},
            {"key": "d", "deps": ["b", "c"]},
        ],
    )
    dag_id = dag["id"]

    # 只有 a 可领
    claimed = client.post("/tasks/claim", json={"limit": 10}).json()["claimed"]
    assert [t["task_key"] for t in claimed] == ["a"]
    token_a = claimed[0]["lease_token"]
    assert claimed[0]["attempt"] == 1

    # 完成 a 后 b、c 同时可领
    assert client.post(f"/tasks/{claimed[0]['task_id']}/complete",
                       json={"lease_token": token_a, "success": True}).status_code == 200
    claimed = client.post("/tasks/claim", json={"dag_id": dag_id, "limit": 10}).json()["claimed"]
    assert {t["task_key"] for t in claimed} == {"b", "c"}
    for t in claimed:
        assert t["attempt"] == 1

    # d 仍被门控
    assert client.post("/tasks/claim", json={"dag_id": dag_id}).json()["count"] == 0

    for t in claimed:
        client.post(f"/tasks/{t['task_id']}/complete",
                    json={"lease_token": t["lease_token"], "success": True})
    d = client.post("/tasks/claim", json={"dag_id": dag_id}).json()["claimed"]
    assert len(d) == 1 and d[0]["task_key"] == "d"


# ------------------------------------------------------------- 尝试与唯一令牌

def test_attempt_increments_and_tokens_unique(client):
    dag = submit(client, [{"key": "a", "lease_seconds": 0.2, "max_retries": 3}])
    t1 = client.post("/tasks/claim", json={}).json()["claimed"][0]
    assert t1["attempt"] == 1
    time.sleep(0.25)
    t2 = client.post("/tasks/claim", json={}).json()["claimed"][0]
    assert t2["task_id"] == t1["task_id"]
    assert t2["attempt"] == 2
    assert t2["lease_token"] != t1["lease_token"]


def test_old_token_operations_return_409_after_reclaim(client):
    submit(client, [{"key": "a", "lease_seconds": 0.2}])
    t1 = client.post("/tasks/claim", json={}).json()["claimed"][0]
    old_token, task_id = t1["lease_token"], t1["task_id"]
    time.sleep(0.25)
    t2 = client.post("/tasks/claim", json={}).json()["claimed"][0]
    assert t2["attempt"] == 2

    # 旧令牌续租 → 409
    r = client.post(f"/tasks/{task_id}/heartbeat",
                    json={"lease_token": old_token})
    assert r.status_code == 409
    assert r.json()["error"]["code"] in ("invalid_lease", "lease_expired")

    # 旧令牌回报结果 → 409，绝不能覆盖新尝试
    r = client.post(f"/tasks/{task_id}/complete",
                    json={"lease_token": old_token, "success": True})
    assert r.status_code == 409

    status = client.get(f"/tasks/{task_id}").json()
    assert status["state"] == "running"
    assert status["attempt"] == 2
    assert status["lease"]["token"] == t2["lease_token"]


def test_heartbeat_extends_lease(client):
    submit(client, [{"key": "a", "lease_seconds": 0.3}])
    t = client.post("/tasks/claim", json={}).json()["claimed"][0]
    time.sleep(0.2)
    r = client.post(f"/tasks/{t['task_id']}/heartbeat",
                    json={"lease_token": t["lease_token"], "extend_seconds": 5})
    assert r.status_code == 200
    time.sleep(0.2)  # 没有心跳的话此刻已过期
    # 租约仍有效：不可再领取，且旧令牌仍可回报
    assert client.post("/tasks/claim", json={}).json()["count"] == 0
    r = client.post(f"/tasks/{t['task_id']}/complete",
                    json={"lease_token": t["lease_token"], "success": True})
    assert r.status_code == 200


def test_wrong_token_returns_409(client):
    submit(client, [{"key": "a"}])
    t = client.post("/tasks/claim", json={}).json()["claimed"][0]
    r = client.post(f"/tasks/{t['task_id']}/complete",
                    json={"lease_token": uuid.uuid4().hex, "success": True})
    assert r.status_code == 409


# ------------------------------------------------------------- 重试与阻塞

def test_lease_expiry_does_not_consume_retry_budget(client):
    """租约过期推高 attempt，但显式失败预算只按失败次数计算。"""
    submit(client, [{"key": "a", "lease_seconds": 0.2, "max_retries": 1}])
    # 三次领取：前两次租约过期，attempt 升到 3，远超 max_retries=1
    client.post("/tasks/claim", json={})
    time.sleep(0.25)
    client.post("/tasks/claim", json={})
    time.sleep(0.25)
    t3 = client.post("/tasks/claim", json={}).json()["claimed"][0]
    assert t3["attempt"] == 3

    # 第一次显式失败：尽管 attempt=3 > max_retries，仍应回到 pending
    r = client.post(f"/tasks/{t3['task_id']}/complete",
                    json={"lease_token": t3["lease_token"], "success": False, "error": "boom"})
    assert r.json()["state"] == "pending"
    assert r.json()["fail_count"] == 1
    assert r.json()["retries_remaining"] == 1

    # 第二次显式失败：失败计数达到上限 → failed
    t4 = client.post("/tasks/claim", json={}).json()["claimed"][0]
    assert t4["attempt"] == 4
    r = client.post(f"/tasks/{t4['task_id']}/complete",
                    json={"lease_token": t4["lease_token"], "success": False, "error": "boom"})
    assert r.json()["state"] == "failed"
    assert r.json()["fail_count"] == 2
    assert r.json()["retries_remaining"] == 0

    status = client.get(f"/tasks/{t4['task_id']}").json()
    assert status["state"] == "failed"
    assert status["attempt"] == 4
    assert status["fail_count"] == 2
    # attempts 历史完整保留：两次 expired + 两次 failed
    assert [a["state"] for a in status["attempts"]] == ["expired", "expired", "failed", "failed"]


def test_retry_then_success(client):
    submit(client, [{"key": "a", "max_retries": 2}])
    t = client.post("/tasks/claim", json={}).json()["claimed"][0]
    r = client.post(f"/tasks/{t['task_id']}/complete",
                    json={"lease_token": t["lease_token"], "success": False, "error": "boom"})
    assert r.json()["state"] == "pending"
    assert r.json()["retries_remaining"] == 2

    t2 = client.post("/tasks/claim", json={}).json()["claimed"][0]
    assert t2["attempt"] == 2
    assert client.post(f"/tasks/{t2['task_id']}/complete",
                       json={"lease_token": t2["lease_token"], "success": True}).status_code == 200


def test_exhausted_retries_blocks_descendants(client):
    dag = submit(
        client,
        [
            {"key": "a", "max_retries": 1},
            {"key": "b", "deps": ["a"]},
            {"key": "c", "deps": ["b"]},
        ],
    )
    dag_id = dag["id"]

    for expected_attempt in (1, 2):
        t = client.post("/tasks/claim", json={}).json()["claimed"][0]
        assert t["attempt"] == expected_attempt
        r = client.post(f"/tasks/{t['task_id']}/complete",
                        json={"lease_token": t["lease_token"], "success": False, "error": "x"})
    assert r.json()["state"] == "failed"
    assert r.json()["blocked_descendants"] == 2

    # 后继永远领取不到
    assert client.post("/tasks/claim", json={"dag_id": dag_id, "limit": 10}).json()["count"] == 0

    full = client.get(f"/dags/{dag_id}").json()
    b = by_key(full, "b")
    c = by_key(full, "c")
    assert b["state"] == "blocked" and "permanently failed" in b["blocked_reason"]
    assert c["state"] == "blocked" and "permanently failed" in c["blocked_reason"]
    assert full["states"]["failed"] == 1 and full["states"]["blocked"] == 2


# ------------------------------------------------------------- 状态接口

def test_status_lists_attempts_and_lease(client):
    submit(client, [{"key": "a", "lease_seconds": 60}])
    t = client.post("/tasks/claim", json={"worker_id": "w-1"}).json()["claimed"][0]
    client.post(f"/tasks/{t['task_id']}/heartbeat",
                json={"lease_token": t["lease_token"]})

    status = client.get(f"/tasks/{t['task_id']}").json()
    assert status["attempt"] == 1
    assert status["state"] == "running"
    assert status["lease"]["token"] == t["lease_token"]
    assert status["lease"]["expired"] is False
    assert status["lease"]["worker_id"] == "w-1"
    att = status["attempts"][0]
    assert att["attempt"] == 1 and att["state"] == "active"
    assert att["heartbeat_at"] is not None

    # 列表接口支持状态过滤
    r = client.get("/tasks", params={"state": "running"})
    assert r.json()["count"] == 1


# ------------------------------------------------------------- 原子领取

def test_concurrent_claim_is_atomic(client):
    submit(client, [{"key": "a"}])
    results = []
    errors = []

    def worker():
        try:
            r = client.post("/tasks/claim", json={"worker_id": f"w-{threading.get_ident()}"})
            results.extend(r.json()["claimed"])
        except Exception as e:  # pragma: no cover
            errors.append(e)

    threads = [threading.Thread(target=worker) for _ in range(10)]
    for th in threads:
        th.start()
    for th in threads:
        th.join()

    assert not errors
    assert len(results) == 1
    assert results[0]["attempt"] == 1


# ------------------------------------------------------------- 旧库迁移

def _create_legacy_db(db_file):
    """按修复前的表结构（无 fail_count 列）构造一个旧库。"""
    import sqlite3

    conn = sqlite3.connect(str(db_file))
    conn.executescript(
        """
        CREATE TABLE dags (
            id TEXT PRIMARY KEY, name TEXT NOT NULL, created_at TEXT NOT NULL
        );
        CREATE TABLE tasks (
            id TEXT PRIMARY KEY, dag_id TEXT NOT NULL, task_key TEXT NOT NULL,
            name TEXT NOT NULL, payload TEXT, max_retries INTEGER NOT NULL DEFAULT 0,
            lease_seconds REAL NOT NULL, state TEXT NOT NULL, blocked_reason TEXT,
            lease_token TEXT, lease_expires_at TEXT, worker_id TEXT,
            attempt INTEGER NOT NULL DEFAULT 0, created_at TEXT NOT NULL,
            started_at TEXT, finished_at TEXT
        );
        CREATE TABLE dependencies (
            dag_id TEXT NOT NULL, task_key TEXT NOT NULL, dep_key TEXT NOT NULL,
            PRIMARY KEY (dag_id, task_key, dep_key)
        );
        CREATE TABLE attempts (
            id INTEGER PRIMARY KEY AUTOINCREMENT, task_id TEXT NOT NULL,
            attempt_no INTEGER NOT NULL, lease_token TEXT NOT NULL UNIQUE,
            worker_id TEXT, leased_at TEXT NOT NULL, expires_at TEXT NOT NULL,
            heartbeat_at TEXT, state TEXT NOT NULL, error TEXT
        );
        """
    )
    conn.execute("INSERT INTO dags VALUES ('d1', 'legacy', '2026-01-01T00:00:00Z')")
    conn.execute(
        """INSERT INTO tasks (id, dag_id, task_key, name, max_retries, lease_seconds,
               state, attempt, created_at)
           VALUES ('t1', 'd1', 'a', 'a', 2, 30, 'pending', 3, '2026-01-01T00:00:00Z')"""
    )
    # 历史：两次显式失败 + 一次租约过期
    for no, st in ((1, "failed"), (2, "expired"), (3, "failed")):
        conn.execute(
            """INSERT INTO attempts (task_id, attempt_no, lease_token, leased_at, expires_at, state)
               VALUES ('t1', ?, ?, '2026-01-01T00:00:00Z', '2026-01-01T00:00:30Z', ?)""",
            (no, f"tok{no}", st),
        )
    conn.commit()
    conn.close()


def test_init_db_migrates_legacy_schema(tmp_path):
    """旧库初始化时补 fail_count 列，并从 attempts 历史回填显式失败次数。"""
    import sqlite3

    from app.db import init_db

    db_file = tmp_path / "legacy.db"
    _create_legacy_db(db_file)

    init_db(db_file)

    conn = sqlite3.connect(str(db_file))
    row = conn.execute("SELECT fail_count, attempt FROM tasks WHERE id = 't1'").fetchone()
    # 回填的是显式失败次数 2，而不是被过期推高的 attempt 3
    assert row == (2, 3)

    # 迁移幂等：再次初始化不会重置运行中任务的计数
    conn.execute("UPDATE tasks SET fail_count = 1 WHERE id = 't1'")
    conn.commit()
    conn.close()
    init_db(db_file)
    conn = sqlite3.connect(str(db_file))
    assert conn.execute("SELECT fail_count FROM tasks WHERE id = 't1'").fetchone()[0] == 1
    conn.close()


def test_migrated_legacy_task_keeps_remaining_budget(tmp_path, monkeypatch):
    """迁移后的旧任务按回填计数继续消耗重试预算。"""
    db_file = tmp_path / "legacy.db"
    _create_legacy_db(db_file)
    monkeypatch.setenv("DB_PATH", str(db_file))

    from fastapi.testclient import TestClient

    from app.main import app

    with TestClient(app) as client:
        task_id = client.get("/tasks", params={"dag_id": "d1"}).json()["tasks"][0]["id"]
        # max_retries=2，已有 2 次显式失败：下一次失败即耗尽预算
        t = client.post("/tasks/claim", json={}).json()["claimed"][0]
        assert t["attempt"] == 4
        r = client.post(f"/tasks/{task_id}/complete",
                        json={"lease_token": t["lease_token"], "success": False, "error": "x"})
        assert r.json()["state"] == "failed"
        assert r.json()["fail_count"] == 3
        assert r.json()["retries_remaining"] == 0
