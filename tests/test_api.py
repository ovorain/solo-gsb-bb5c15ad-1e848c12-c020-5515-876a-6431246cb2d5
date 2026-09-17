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
