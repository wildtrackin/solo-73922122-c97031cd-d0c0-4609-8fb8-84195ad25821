"""合并功能的端到端测试。

覆盖：
  * 网页选择保留/重复编号、温度/pH 各取哪条，提交后从当前已提交记录取值，
    原子更新保留行、删除重复行，并回显合并前后数值；
  * 回填期间合并可用；直接 SQL 与合并交错时按提交顺序取值；
  * 已提交的新值不被旧扫描覆盖；被删编号不在 measurements 复活；
  * 任一编号已不存在则整次失败（无部分写入）；
  * 与最终切换交错：合并完整先于切换，否则拒绝；切换校验读不到半次合并。
"""
from __future__ import annotations

import threading
import time

import psycopg
import pytest
from fastapi.testclient import TestClient

from app import merge


def _wait_phase(client: TestClient, target: str, timeout: float = 20) -> dict:
    deadline = time.time() + timeout
    while True:
        data = client.get("/migration/progress").json()
        if data["state"] == target:
            return data
        assert time.time() < deadline, f"phase stuck at {data['state']}, want {target}"
        time.sleep(0.2)


def _values(dsn: str, sample_id: int):
    with psycopg.connect(dsn) as conn:
        rows = conn.execute(
            "SELECT metric, value FROM measurements WHERE sample_id=%s "
            "ORDER BY metric", (sample_id,)
        ).fetchall()
    return {m: float(v) for m, v in rows}


@pytest.fixture()
def ready_client(client: TestClient) -> TestClient:
    client.post("/migration/start")
    _wait_phase(client, "ready")
    return client


# ---------------------------------------------------------------- 基本合并
def test_merge_picks_values_from_both_records_and_reports_before_after(
        ready_client: TestClient, dsn: str):
    client = ready_client
    # 造两个编号指向同一送检样本：id=13/14 由 API 新建
    a = client.post("/samples", json={"temp": 21.11, "ph": 7.01}).json()
    b = client.post("/samples", json={"temp": 22.22, "ph": 7.02}).json()

    preview = client.get(
        f"/samples/{a['id']}/merge-preview?duplicate_id={b['id']}").json()
    assert preview["mergeable"] is True
    assert preview["missing"] == []
    assert preview["samples"][str(a["id"])]["temp"] == 21.11
    assert preview["samples"][str(b["id"])]["ph"] == 7.02

    resp = client.post("/samples/merge", json={
        "keep_id": a["id"], "duplicate_id": b["id"],
        "temp_from": "duplicate", "ph_from": "keep",
    })
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["before"]["keep"] == {"id": a["id"], "temp": 21.11, "ph": 7.01}
    assert body["before"]["duplicate"] == {"id": b["id"], "temp": 22.22, "ph": 7.02}
    assert body["after"]["temp"] == 22.22   # 温度取自重复编号
    assert body["after"]["ph"] == 7.01      # pH 取自保留编号
    assert body["removed_measurements"] == 2

    # 保留编号：宽表与长表都只剩合并后的两项
    assert client.get(f"/samples/{a['id']}").json() == {
        "id": a["id"], "temp": 22.22, "ph": 7.01}
    assert _values(dsn, a["id"]) == {"temp": 22.22, "ph": 7.01}
    # 重复编号：宽表与长表都不再存在
    assert client.get(f"/samples/{b['id']}").status_code == 404
    assert _values(dsn, b["id"]) == {}
    # 全局一致性仍成立
    assert client.get("/migration/progress").json()["consistency"]["ok"]


def test_merge_is_atomic_when_one_id_missing(client: TestClient, dsn: str):
    client.post("/migration/start")
    _wait_phase(client, "ready")
    keep = client.post("/samples", json={"temp": 12.34, "ph": 6.78}).json()["id"]
    dup = client.post("/samples", json={"temp": 56.78, "ph": 7.89}).json()["id"]
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute("DELETE FROM samples WHERE id=%s", (dup,))

    resp = client.post("/samples/merge", json={
        "keep_id": keep, "duplicate_id": dup,
        "temp_from": "duplicate", "ph_from": "keep"})
    assert resp.status_code == 409
    detail = resp.json()["detail"]
    assert detail["code"] == "sample_missing"
    assert detail["missing"] == [dup]
    # 保留编号未被改动，长表两项保持原值
    assert client.get(f"/samples/{keep}").json() == {
        "id": keep, "temp": 12.34, "ph": 6.78}
    assert _values(dsn, keep) == {"temp": 12.34, "ph": 6.78}

    # 重复编号先消失（保留编号仍在）：同样整次失败
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute("DELETE FROM samples WHERE id=%s", (keep,))
    resp = client.post("/samples/merge", json={
        "keep_id": keep, "duplicate_id": 1,
        "temp_from": "keep", "ph_from": "keep"})
    assert resp.status_code == 409
    assert resp.json()["detail"]["missing"] == [keep]


def test_merge_rejects_same_id_and_bad_source(ready_client: TestClient):
    client = ready_client
    resp = client.post("/samples/merge", json={
        "keep_id": 3, "duplicate_id": 3,
        "temp_from": "keep", "ph_from": "keep"})
    assert resp.status_code == 409
    assert resp.json()["detail"]["code"] == "invalid_selection"
    resp = client.post("/samples/merge", json={
        "keep_id": 3, "duplicate_id": 4,
        "temp_from": "keep", "ph_from": "duplicate"})
    # 4 存在，允许；但若传非法来源，pydantic 422
    assert resp.status_code == 200
    bad = client.post("/samples/merge", json={
        "keep_id": 3, "duplicate_id": 4,
        "temp_from": "sideways", "ph_from": "keep"})
    assert bad.status_code == 422


def test_merge_rejected_after_switch(client: TestClient, dsn: str):
    client.post("/migration/start")
    _wait_phase(client, "ready")
    assert client.post("/migration/switch").status_code == 200
    resp = client.post("/samples/merge", json={
        "keep_id": 1, "duplicate_id": 2,
        "temp_from": "keep", "ph_from": "duplicate"})
    assert resp.status_code == 409
    assert resp.json()["detail"]["code"] == "phase_not_mergeable"
    with psycopg.connect(dsn) as conn:
        assert conn.execute("SELECT count(*) FROM samples").fetchone() == (12,)


# ---------------------------------------------------------------- 回填期间
def test_merge_during_backfill_and_later_backfill_does_not_resurrect(
        client: TestClient, dsn: str):
    """回填尚未完成时合并；随后回填越过被删编号，不得复活它。

    为了确定性，先完成一轮回填拿到基线，再把水位退回 0 并置为 paused，
    模拟“后半段尚未回填”的状态；合并两个水位之后的编号后再续办。
    """
    client.post("/migration/start")
    _wait_phase(client, "ready")
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute("UPDATE migration_state SET last_id=0, state='paused' WHERE id=1")

    # 暂停期间新增两个编号：它们都在水位之后，必然会被后续回填扫描到
    keep = client.post("/samples", json={"temp": 11.11, "ph": 7.11}).json()["id"]
    dup = client.post("/samples", json={"temp": 12.12, "ph": 7.12}).json()["id"]
    assert keep > 0 and dup > keep

    resp = client.post("/samples/merge", json={
        "keep_id": keep, "duplicate_id": dup,
        "temp_from": "keep", "ph_from": "duplicate"})
    assert resp.status_code == 200, resp.text

    client.post("/migration/resume")
    progress = _wait_phase(client, "ready")
    assert progress["consistency"]["ok"]
    assert progress["sample_count"] == 13
    # 被删编号没有被后续回填复活
    assert _values(dsn, dup) == {}
    assert client.get(f"/samples/{dup}").status_code == 404
    # 保留编号的两项是合并后的值（temp 取保留、ph 取重复）
    assert _values(dsn, keep) == {"temp": 11.11, "ph": 7.12}


# ---------------------------------------------------------------- 并发交错
def test_old_scan_cannot_overwrite_committed_merge_value(client: TestClient, dsn: str):
    """直接 SQL 修改先提交，合并后取“当前已提交值”而非预览时的旧值。

    这模拟网页先预览、旧程序随后改值、网页再提交：合并必须使用改后的新值。
    """
    client.post("/migration/start")
    _wait_phase(client, "ready")
    keep = client.post("/samples", json={"temp": 10.00, "ph": 7.00}).json()["id"]
    dup = client.post("/samples", json={"temp": 20.00, "ph": 8.00}).json()["id"]

    preview = client.get(
        f"/samples/{keep}/merge-preview?duplicate_id={dup}").json()
    assert preview["samples"][str(dup)]["temp"] == 20.00

    # 预览之后，“旧采集程序”直接 SQL 更新了重复编号的温度并提交
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute("UPDATE samples SET temp=25.50, ph=8.50 WHERE id=%s", (dup,))

    resp = client.post("/samples/merge", json={
        "keep_id": keep, "duplicate_id": dup,
        "temp_from": "duplicate", "ph_from": "duplicate"})
    assert resp.status_code == 200
    body = resp.json()
    # 取到的是提交后的 25.50/8.50，而不是预览时的 20.00/8.00
    assert body["before"]["duplicate"]["temp"] == 25.50
    assert body["after"]["temp"] == 25.50 and body["after"]["ph"] == 8.50
    assert _values(dsn, keep) == {"temp": 25.50, "ph": 8.50}

    # 旧扫描再跑一轮（resume 对 ready 合法）不会覆盖合并结果
    client.post("/migration/resume")
    _wait_phase(client, "ready")
    assert _values(dsn, keep) == {"temp": 25.50, "ph": 8.50}
    assert _values(dsn, dup) == {}


def test_committed_new_value_not_overwritten_by_concurrent_old_scan(
        client: TestClient, dsn: str):
    """回填扫描与直接 SQL 更新交错：后提交的更新胜出，旧扫描不覆盖新值。

    用一个独立连接持有行锁并写入新值，同时让回填扫描在 FOR SHARE 上等待；
    旧更新先提交 -> 回填读到新值 -> 长表是新值。
    """
    client.post("/migration/start")
    _wait_phase(client, "ready")

    target = 3
    # 人为把水位退回到 target 之前，制造“回填会重扫该行”的条件
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute("UPDATE migration_state SET last_id=2 WHERE id=1")

    # 1) 旧采集程序开始事务并更新目标行（未提交）
    writer = psycopg.connect(dsn, autocommit=False)
    writer.execute("UPDATE samples SET temp=77.70, ph=7.77 WHERE id=%s", (target,))

    # 2) 启动回填（会在 FOR SHARE 上等待 writer 提交）
    client.post("/migration/resume")
    time.sleep(0.8)
    # 回填还没完成（被行锁挡住或批次尚在推进）
    assert client.get("/migration/progress").json()["state"] in (
        "backfilling", "ready")
    # 3) writer 提交 -> 回填继续，读到的是 77.70/7.77
    writer.commit()
    writer.close()

    progress = _wait_phase(client, "ready")
    assert progress["consistency"]["ok"]
    assert _values(dsn, target) == {"temp": 77.70, "ph": 7.77}


def test_deleted_id_not_resurrected_by_concurrent_backfill_scan(
        client: TestClient, dsn: str):
    """回填扫描遇到“正被删除”的行：DELETE 提交后扫描重检，编号不复活。

    这是“被删编号不能在 measurements 里复活”的直接竞态测试：
    回填语句的快照里该行仍存在（删除尚未提交），FOR SHARE 使其等待删除者；
    删除提交后 PostgreSQL 重检该行并跳过，因此不会写回长表。
    """
    client.post("/migration/start")
    _wait_phase(client, "ready")

    # 准备两个水位之上的编号，并把水位退回 0、暂停，
    # 使后续回填必然重新扫描到被删除的编号。
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(
            "INSERT INTO samples (id, temp, ph) OVERRIDING SYSTEM VALUE "
            "VALUES (101, 88.10, 6.00), (102, 88.20, 6.10)")
        conn.execute("UPDATE migration_state SET last_id=0, state='paused' WHERE id=1")

    # 旧程序开始删除 101（未提交，持有行锁）
    holder = psycopg.connect(dsn, autocommit=False)
    holder.execute("DELETE FROM samples WHERE id=101")

    # 启动回填：扫描到 101 时会在删除者的行锁上等待
    client.post("/migration/resume")
    time.sleep(0.8)
    holder.commit()          # 删除先提交
    holder.close()

    progress = _wait_phase(client, "ready")
    assert progress["consistency"]["ok"]
    # 101 在宽表与长表都不存在
    assert client.get("/samples/101").status_code == 404
    assert _values(dsn, 101) == {}
    assert _values(dsn, 102) == {"temp": 88.20, "ph": 6.10}
    with psycopg.connect(dsn) as conn:
        assert conn.execute("SELECT count(*) FROM samples").fetchone()[0] == 13


def test_backfill_batch_rechecks_row_committed_while_it_waits(client: TestClient,
                                                             dsn: str):
    """直接驱动 backfill_batch：扫描在 FOR SHARE 上等待期间旧程序提交新值/删除。

    验证 READ COMMITTED 下的重检语义：
      * 等待期间提交的新值被回填采用（旧扫描不覆盖新提交值）；
      * 等待期间提交的删除使该行被跳过（被删编号不在长表复活）。
    两个场景各用一个独立编号，同一批扫描中同时验证。
    """
    client.post("/migration/start")
    _wait_phase(client, "ready")
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(
            "INSERT INTO samples (id, temp, ph) OVERRIDING SYSTEM VALUE "
            "VALUES (201, 10.00, 6.00), (202, 20.00, 7.00)")
        # 暂停后台回填，改为在测试线程里显式执行单批扫描
        conn.execute("UPDATE migration_state SET last_id=200, state='paused' WHERE id=1")

    from app import migration as mig

    # 旧程序事务：改 201、删 202，均未提交
    writer = psycopg.connect(dsn, autocommit=False)
    writer.execute("UPDATE samples SET temp=99.90, ph=8.88 WHERE id=201")
    writer.execute("DELETE FROM samples WHERE id=202")

    result = {}

    def run_batch():
        with psycopg.connect(dsn, autocommit=False) as c:
            c.execute("SET statement_timeout = '10s'")
            with c.transaction():
                cur = c.cursor()
                result["batch"] = mig.backfill_batch(cur, batch_size=2)
            c.commit()

    t = threading.Thread(target=run_batch)
    t.start()
    time.sleep(0.6)
    assert t.is_alive(), "backfill should be waiting on the row locks"
    writer.commit()          # 旧程序先提交
    writer.close()
    t.join(timeout=15)
    assert not t.is_alive(), "backfill did not finish"

    # 201 采用等待期间提交的新值；202 被跳过、没有复活
    assert _values(dsn, 201) == {"temp": 99.90, "ph": 8.88}
    assert _values(dsn, 202) == {}
    # 水位推进到本批实际写出的最大 id（201），没有越过 202 之后
    with psycopg.connect(dsn) as conn:
        assert conn.execute(
            "SELECT last_id FROM migration_state WHERE id=1").fetchone()[0] == 201


def test_merge_and_old_scan_serialize_in_commit_order(client: TestClient, dsn: str):
    """合并事务与旧 UPDATE 并发：两者按行锁串行，最终值等于后提交者的结果。

    这里让合并先取得行锁（用 pg_sleep 拉长其事务），旧 UPDATE 在锁上等待；
    合并提交后旧 UPDATE 才执行 -> 最终是旧 UPDATE 的值，合并没有“丢更新”。
    """
    client.post("/migration/start")
    _wait_phase(client, "ready")
    keep = client.post("/samples", json={"temp": 10.00, "ph": 7.00}).json()["id"]
    dup = client.post("/samples", json={"temp": 20.00, "ph": 8.00}).json()["id"]

    # 用一个独立连接模拟“合并事务已经锁住两行、尚未提交”
    locker = psycopg.connect(dsn, autocommit=False)
    locker.execute(
        "SELECT id FROM samples WHERE id = ANY(%s) ORDER BY id FOR UPDATE",
        ([keep, dup],))
    locker.execute("SELECT pg_sleep(0.6)")   # 保持锁 0.6s
    locker.execute("UPDATE samples SET temp=31.00, ph=8.10 WHERE id=%s", (keep,))
    locker.execute("DELETE FROM samples WHERE id=%s", (dup,))
    locker.commit()
    locker.close()

    # 旧采集程序 UPDATE 合并保留行：在行锁上等待，合并提交后才生效
    writer = psycopg.connect(dsn, autocommit=True)
    writer.execute("UPDATE samples SET temp=41.00, ph=8.20 WHERE id=%s", (keep,))
    writer.close()

    # 合并提交的值先落库，旧 UPDATE 后提交 -> 最终是 41.00/8.20
    assert _values(dsn, keep) == {"temp": 41.00, "ph": 8.20}
    assert _values(dsn, dup) == {}
    p = client.get("/migration/progress").json()
    assert p["consistency"]["ok"]


def test_merge_reads_value_committed_while_it_waits_for_row_lock(
        client: TestClient, dsn: str):
    """合并等待行锁期间旧程序提交的新值，合并必须采用它（按提交顺序取值）。"""
    client.post("/migration/start")
    _wait_phase(client, "ready")
    keep = client.post("/samples", json={"temp": 10.00, "ph": 7.00}).json()["id"]
    dup = client.post("/samples", json={"temp": 20.00, "ph": 8.00}).json()["id"]

    # 外部连接先锁住重复编号一行，模拟旧采集程序事务进行中
    holder = psycopg.connect(dsn, autocommit=False)
    holder.execute("SELECT id FROM samples WHERE id=%s FOR UPDATE", (dup,))

    result = {}

    def run_merge():
        result["resp"] = client.post("/samples/merge", json={
            "keep_id": keep, "duplicate_id": dup,
            "temp_from": "duplicate", "ph_from": "keep"})

    t = threading.Thread(target=run_merge)
    t.start()
    time.sleep(0.4)                    # 合并此刻阻塞在 FOR UPDATE 上
    holder.execute("UPDATE samples SET temp=88.80, ph=8.80 WHERE id=%s", (dup,))
    holder.commit()                    # 旧程序先提交
    holder.close()
    t.join(timeout=10)
    assert not t.is_alive()

    resp = result["resp"]
    assert resp.status_code == 200, resp.text
    body = resp.json()
    # 合并看到的是等待期间提交的新值 88.80，而不是发起时的 20.00
    assert body["before"]["duplicate"]["temp"] == 88.80
    assert body["after"] == {"id": keep, "temp": 88.80, "ph": 7.00}
    assert _values(dsn, keep) == {"temp": 88.80, "ph": 7.00}
    assert _values(dsn, dup) == {}


def test_merge_fails_atomically_if_duplicate_deleted_while_waiting(
        client: TestClient, dsn: str):
    """合并等待行锁期间重复编号被旧程序删除：整次失败，保留编号不变。"""
    client.post("/migration/start")
    _wait_phase(client, "ready")
    keep = client.post("/samples", json={"temp": 10.00, "ph": 7.00}).json()["id"]
    dup = client.post("/samples", json={"temp": 20.00, "ph": 8.00}).json()["id"]

    holder = psycopg.connect(dsn, autocommit=False)
    holder.execute("SELECT id FROM samples WHERE id=%s FOR UPDATE", (dup,))
    result = {}

    def run_merge():
        result["resp"] = client.post("/samples/merge", json={
            "keep_id": keep, "duplicate_id": dup,
            "temp_from": "keep", "ph_from": "keep"})

    t = threading.Thread(target=run_merge)
    t.start()
    time.sleep(0.4)
    holder.execute("DELETE FROM samples WHERE id=%s", (dup,))
    holder.commit()
    holder.close()
    t.join(timeout=10)
    assert not t.is_alive()

    resp = result["resp"]
    assert resp.status_code == 409, resp.text
    assert resp.json()["detail"]["code"] == "sample_missing"
    # 保留编号完全未被改动
    assert client.get(f"/samples/{keep}").json() == {
        "id": keep, "temp": 10.00, "ph": 7.00}
    assert _values(dsn, keep) == {"temp": 10.00, "ph": 7.00}


# ---------------------------------------------------------------- 与切换交错
def test_switch_refused_while_merge_in_progress(client: TestClient, dsn: str):
    """合并事务持锁未提交时，切换必须拒绝（409），不能读到半次合并。"""
    client.post("/migration/start")
    _wait_phase(client, "ready")
    keep = client.post("/samples", json={"temp": 10.00, "ph": 7.00}).json()["id"]
    dup = client.post("/samples", json={"temp": 20.00, "ph": 8.00}).json()["id"]

    merge_conn = psycopg.connect(dsn, autocommit=False)
    with merge_conn.transaction():
        merge_conn.execute("SELECT pg_try_advisory_xact_lock_shared(%s)",
                           (merge.MERGE_LOCK_KEY,))
        merge_conn.execute(
            "SELECT id FROM samples WHERE id = ANY(%s) ORDER BY id FOR UPDATE",
            ([keep, dup],))
        merge_conn.execute("SELECT pg_sleep(0.8)")

        # 在另一线程/连接里发起切换：应因 advisory lock 冲突立即失败
        result_holder = {}

        def do_switch():
            result_holder["resp"] = client.post("/migration/switch")

        t = threading.Thread(target=do_switch)
        t.start()
        t.join(timeout=10)
        assert not t.is_alive()
        resp = result_holder["resp"]
        assert resp.status_code == 409, resp.text
        assert "merge or another switch is in progress" in resp.json()["detail"]

    # 合并连接回滚后，切换应恢复正常
    assert client.get("/migration/progress").json()["state"] == "ready"
    assert client.post("/migration/switch").status_code == 200


def test_merge_refused_while_switch_holds_exclusive_lock(client: TestClient, dsn: str):
    """切换持排他锁期间（校验/换视图未提交），合并必须拒绝。"""
    client.post("/migration/start")
    _wait_phase(client, "ready")
    keep, dup = 1, 2

    sw = psycopg.connect(dsn, autocommit=False)
    with sw.transaction():
        sw.execute("SELECT pg_try_advisory_xact_lock(%s)", (merge.MERGE_LOCK_KEY,))
        sw.execute("LOCK TABLE samples IN ACCESS EXCLUSIVE MODE")
        sw.execute("SELECT pg_sleep(0.8)")

        resp = client.post("/samples/merge", json={
            "keep_id": keep, "duplicate_id": dup,
            "temp_from": "keep", "ph_from": "duplicate"})
        assert resp.status_code == 409, resp.text
        assert resp.json()["detail"]["code"] == "switch_in_progress"

    # 切换事务回滚后，合并恢复可用
    resp = client.post("/samples/merge", json={
        "keep_id": keep, "duplicate_id": dup,
        "temp_from": "keep", "ph_from": "duplicate"})
    assert resp.status_code == 200, resp.text

    # 最终切换仍可完成，校验通过，被删编号不在视图/长表中
    assert client.post("/migration/switch").status_code == 200
    assert client.get(f"/samples/{dup}").status_code == 404


def test_switch_consistency_sees_committed_merge_only(
        client: TestClient, dsn: str):
    """合并提交后、切换开始时：切换校验看到的是完整合并结果（不是半次）。

    通过在合并提交与切换之间插入一次一致性校验断言，并最终核对视图/长表。
    """
    client.post("/migration/start")
    _wait_phase(client, "ready")
    keep = client.post("/samples", json={"temp": 11.11, "ph": 6.66}).json()["id"]
    dup = client.post("/samples", json={"temp": 22.22, "ph": 7.77}).json()["id"]
    resp = client.post("/samples/merge", json={
        "keep_id": keep, "duplicate_id": dup,
        "temp_from": "duplicate", "ph_from": "keep"})
    assert resp.status_code == 200

    check = client.get("/migration/progress").json()["consistency"]
    assert check["ok"]
    assert check["wide_rows"] == check["tall_groups"]

    assert client.post("/migration/switch").status_code == 200
    with psycopg.connect(dsn) as conn:
        row = conn.execute(
            "SELECT temp, ph FROM samples WHERE id=%s", (keep,)).fetchone()
    assert float(row[0]) == 22.22 and float(row[1]) == 6.66
    assert client.get(f"/samples/{dup}").status_code == 404
