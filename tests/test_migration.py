"""端到端测试：在线迁移全生命周期。

运行前由 conftest 建好干净的测试库并加载 01_baseline.sql；
应用通过 DATABASE_URL 指向该库，TestClient 会触发真实 lifespan。
"""
from __future__ import annotations

import time

import psycopg
import pytest
from fastapi.testclient import TestClient


def _wait_phase(client: TestClient, target: str, timeout: float = 20) -> dict:
    deadline = time.time() + timeout
    while True:
        data = client.get("/migration/progress").json()
        if data["state"] == target:
            return data
        assert time.time() < deadline, f"phase stuck at {data['state']}, want {target}"
        time.sleep(0.2)


def test_health_and_initial_progress(client: TestClient):
    assert client.get("/health").json()["phase"] == "init"
    p = client.get("/migration/progress").json()
    assert p["sample_count"] == 12
    assert p["measurement_rows"] == 0


def test_backfill_reaches_ready_with_two_metrics_per_sample(client: TestClient):
    assert client.post("/migration/start").json()["ok"]
    progress = _wait_phase(client, "ready")
    assert progress["consistency"]["ok"]
    assert progress["measured_samples"] == 12
    assert progress["measurement_rows"] == 24
    groups = {}
    for m in client.get("/measurements").json():
        groups.setdefault(m["sample_id"], set()).add(m["metric"])
    assert all(metrics == {"temp", "ph"} for metrics in groups.values())


def test_pause_resume(client: TestClient):
    client.post("/migration/start")
    client.post("/migration/pause")
    # 等待在途批次落库后状态稳定为 paused（或恰好已完成到 ready）
    deadline = time.time() + 10
    state = ""
    while time.time() < deadline:
        state = client.get("/migration/progress").json()["state"]
        if state in ("paused", "ready"):
            break
        time.sleep(0.2)
    assert state in ("paused", "ready")
    client.post("/migration/resume")
    _wait_phase(client, "ready")


def test_direct_sql_changes_survive_backfill(client: TestClient, dsn: str):
    client.post("/migration/start")
    _wait_phase(client, "ready")

    with psycopg.connect(dsn, autocommit=True) as conn:
        # 旧采集程序直接 SQL：改 / 删 / 删后复用 id
        conn.execute("UPDATE samples SET temp=99.90, ph=8.88 WHERE id=3")
        conn.execute("DELETE FROM samples WHERE id=4")
        conn.execute("DELETE FROM samples WHERE id=5")
        conn.execute(
            "INSERT INTO samples (id, temp, ph) OVERRIDING SYSTEM VALUE "
            "VALUES (5, 11.11, 2.22)"
        )
        conn.execute("INSERT INTO samples (temp, ph) VALUES (30.00, 7.70)")

    # 再触发一轮回填（resume 对 ready 合法），验证不覆盖、不复活
    client.post("/migration/resume")
    _wait_phase(client, "ready")

    with psycopg.connect(dsn) as conn:
        row = conn.execute(
            "SELECT value FROM measurements WHERE sample_id=3 AND metric='temp'"
        ).fetchone()
        assert float(row[0]) == 99.90
        assert conn.execute(
            "SELECT count(*) FROM measurements WHERE sample_id=4"
        ).fetchone() == (0,)
        metrics = conn.execute(
            "SELECT metric, value FROM measurements WHERE sample_id=5 ORDER BY metric"
        ).fetchall()
        assert metrics[0][0] == "ph" and float(metrics[0][1]) == 2.22
        assert metrics[1][0] == "temp" and float(metrics[1][1]) == 11.11
        assert conn.execute(
            "SELECT count(*) FROM measurements WHERE sample_id="
            "(SELECT max(id) FROM samples)"
        ).fetchone() == (2,)


def test_two_metric_update_is_atomic(client: TestClient, dsn: str):
    client.post("/migration/start")
    _wait_phase(client, "ready")
    with psycopg.connect(dsn) as conn:
        conn.execute("UPDATE samples SET temp=45.67, ph=7.77 WHERE id=3")
        conn.commit()
        # 第二项违反 NOT NULL：整事务回滚，第一项不落库
        with pytest.raises(psycopg.errors.NotNullViolation):
            conn.execute("BEGIN")
            conn.execute("UPDATE samples SET temp=55.55 WHERE id=3")
            conn.execute("UPDATE samples SET ph=NULL WHERE id=3")
            conn.commit()
        conn.rollback()
        row = conn.execute(
            "SELECT temp, ph FROM samples WHERE id=3"
        ).fetchone()
        assert float(row[0]) == 45.67 and float(row[1]) == 7.77


def test_failed_switch_rolls_back_and_keeps_old_interface(client: TestClient):
    client.post("/migration/start")
    _wait_phase(client, "ready")
    resp = client.post("/migration/switch?hold_seconds=0.2")
    assert resp.status_code == 409
    assert _wait_phase(client, "ready")["state"] == "ready"
    # 旧表仍可整组增改删
    created = client.post("/samples", json={"temp": 1.23, "ph": 4.56}).json()
    sid = created["id"]
    assert client.put(f"/samples/{sid}", json={"temp": 2.34, "ph": 5.67}).status_code == 200
    assert client.delete(f"/samples/{sid}").status_code == 200


def test_switch_then_both_entries_share_one_dataset(client: TestClient, dsn: str):
    client.post("/migration/start")
    _wait_phase(client, "ready")
    assert client.post("/migration/switch").status_code == 200

    with psycopg.connect(dsn) as conn:
        relkind = conn.execute(
            "SELECT relkind FROM pg_class WHERE relname='samples'"
        ).fetchone()[0]
        assert relkind == "v"
        # 旧名称仍可直接 SQL 增改删
        conn.execute("INSERT INTO samples (temp, ph) VALUES (33.33, 7.33)")
        conn.commit()
        new_id = conn.execute("SELECT max(id) FROM samples").fetchone()[0]
        conn.execute(f"UPDATE samples SET temp=34.34, ph=7.34 WHERE id={new_id}")
        conn.commit()
        assert conn.execute(
            "SELECT count(*) FROM measurements WHERE sample_id=%s", (new_id,)
        ).fetchone() == (2,)
        conn.execute(f"DELETE FROM samples WHERE id={new_id}")
        conn.commit()
        assert conn.execute(
            "SELECT count(*) FROM measurements WHERE sample_id=%s", (new_id,)
        ).fetchone() == (0,)

    # 新旧 HTTP 入口读写同一份数据
    created = client.post("/samples", json={"temp": 28.88, "ph": 7.28}).json()
    sid = created["id"]
    group = client.get(f"/measurements/{sid}").json()
    assert {m["metric"] for m in group} == {"temp", "ph"}
    client.put(f"/samples/{sid}", json={"temp": 29.99, "ph": 7.29})
    vals = {m["metric"]: m["value"] for m in client.get(f"/measurements/{sid}").json()}
    assert vals == {"temp": 29.99, "ph": 7.29}
    assert client.delete(f"/measurements/{sid}").status_code == 200
    assert client.get(f"/samples/{sid}").status_code == 404

    # 最终不变量
    p = client.get("/migration/progress").json()
    assert p["state"] == "switched"
    assert p["consistency"]["ok"]
    assert p["measurement_rows"] == 2 * p["sample_count"]
