r"""在线宽表 -> 指标长表迁移引擎（Expand / Migrate / Contract）。

状态机：
    init -> backfilling <-> paused -> ready -> switching -> switched
                                   \-> (切换失败/进程退出) ready（可重试）

所有进度都在数据库事务内提交，进程在任意时刻被杀都不会产生半迁移：
    * DDL 引导与最终切换都是单事务，未提交即回滚；
    * 回填按批提交，水位 last_id 与本批数据同一事务落库；
    * 已提交的修改在 measurements 中持久存在，重启后续办即可。
"""
from __future__ import annotations

import threading
import time
from pathlib import Path

from psycopg import Cursor
from psycopg.rows import dict_row

from . import db
from .config import settings

SQL_DIR = Path(__file__).resolve().parent.parent / "db" / "migrations"

_wake = threading.Event()
_worker_started = False
_worker_thread: threading.Thread | None = None
_worker_lock = threading.Lock()


# ---------------------------------------------------------------- 引导
def bootstrap(conn) -> None:
    """幂等创建长表/状态表/双写触发器（单事务）。"""
    exists = conn.execute(
        "SELECT 1 FROM information_schema.tables WHERE table_name = 'measurements'"
    ).fetchone()
    if exists is not None:
        return
    conn.execute((SQL_DIR / "02_expand.sql").read_text(encoding="utf-8"))
    conn.commit()


def get_phase(conn) -> str:
    row = conn.execute("SELECT state FROM migration_state WHERE id = 1").fetchone()
    return row[0]


def get_progress(conn) -> dict:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT s.state, s.last_id, s.message, s.updated_at,
                   (SELECT count(*) FROM samples)              AS sample_count,
                   (SELECT count(*) FROM measurements)         AS measurement_rows,
                   (SELECT count(DISTINCT sample_id) FROM measurements) AS measured_samples
            FROM migration_state s WHERE s.id = 1
            """
        )
        row = cur.fetchone()
    return dict(row)


# ---------------------------------------------------------------- 回填
def _set_state(cur: Cursor, state: str, last_id: int | None = None,
               message: str | None = None) -> None:
    if last_id is None and message is None:
        cur.execute("UPDATE migration_state SET state=%s, updated_at=now() WHERE id=1",
                    (state,))
    else:
        cur.execute(
            """UPDATE migration_state
               SET state=%s,
                   last_id=COALESCE(%s, last_id),
                   message=COALESCE(%s, message),
                   updated_at=now()
               WHERE id=1""",
            (state, last_id, message),
        )


def backfill_batch(cur: Cursor, batch_size: int) -> tuple[int, int]:
    """回填一个批次，返回 (本批行数, 新水位)。

    关键：水位以上的行才处理；UPSERT 遇到已由双写触发器写入的指标时执行
    DO UPDATE 同步当前值——直接 SQL 的最新修改胜出，旧扫描永远不会覆盖它；
    已删除的行根本不在扫描结果里，因此不会被“复活”，删除后复用的 id
    会以新行身份按当前值写入。
    """
    cur.execute("SELECT last_id FROM migration_state WHERE id=1")
    last_id = cur.fetchone()[0]

    cur.execute(
        """
        INSERT INTO measurements (sample_id, metric, value)
        SELECT s.id, m.metric,
               CASE m.metric WHEN 'temp' THEN s.temp WHEN 'ph' THEN s.ph END
        FROM samples s
        CROSS JOIN (VALUES ('temp'), ('ph')) AS m(metric)
        WHERE s.id > %s
        ORDER BY s.id
        LIMIT %s
        ON CONFLICT (sample_id, metric) DO UPDATE
            SET value = EXCLUDED.value
        """,
        (last_id, batch_size * 2),
    )
    moved = cur.rowcount // 2

    # 水位推进到“本批处理过的最大 id”；批次为空则推进到现存最大 id，
    # 保证删除空窗不会让回填无法结束。
    cur.execute(
        """
        UPDATE migration_state
        SET last_id = GREATEST(
                last_id,
                COALESCE((SELECT max(id) FROM
                            (SELECT id FROM samples WHERE id > last_id
                             ORDER BY id LIMIT %s) q),
                         last_id)),
            updated_at = now()
        WHERE id = 1
        RETURNING last_id
        """,
        (batch_size,),
    )
    new_watermark = cur.fetchone()[0]
    return moved, new_watermark


def _backfill_loop() -> None:
    # 动态导入并每轮读取当前池：lifespan 重建池后，长驻线程不会持有失效引用
    from . import db

    while True:
        _wake.wait(timeout=max(settings.backfill_idle_seconds, 0.05))
        _wake.clear()
        if db.pool is None:
            time.sleep(0.1)
            continue
        try:
            with db.pool.connection() as conn:
                phase = get_phase(conn)
                if phase in ("paused", "ready", "switched"):
                    if phase == "switched":
                        return
                    continue
                if phase != "backfilling":
                    continue

                try:
                    with conn.transaction():
                        cur = conn.cursor()
                        moved, watermark = backfill_batch(cur, settings.backfill_batch_size)

                        # 水位越过现存最大 id => 回填完成
                        cur.execute("SELECT coalesce(max(id), 0) FROM samples")
                        max_id = cur.fetchone()[0]
                        if watermark >= max_id:
                            # 条件更新：批次执行期间若被暂停，不得覆盖 paused
                            cur.execute(
                                "UPDATE migration_state SET state='ready', "
                                "message=%s, updated_at=now() WHERE id=1 AND state='backfilling'",
                                (f"backfill finished at id {watermark}",),
                            )
                        else:
                            cur.execute(
                                "UPDATE migration_state SET state='backfilling', "
                                "message=%s, updated_at=now() WHERE id=1 AND state='backfilling'",
                                (f"last batch moved {moved} rows",),
                            )
                    conn.commit()
                except Exception as exc:  # 批次失败：保留现场，转 paused 待人工续办
                    conn.rollback()
                    with db.pool.connection() as c2:
                        with c2.transaction():
                            _set_state(c2.cursor(), "paused",
                                       message=f"backfill error: {exc!r}")
                        c2.commit()
        except Exception:
            # 连接池关闭/重启等过渡期：不杀死工作线程，下一轮重试
            time.sleep(0.2)


def _ensure_worker() -> None:
    global _worker_started, _worker_thread
    with _worker_lock:
        if _worker_thread is None or not _worker_thread.is_alive():
            _worker_thread = threading.Thread(
                target=_backfill_loop, name="backfill", daemon=True
            )
            _worker_thread.start()
            _worker_started = True


def start_backfill(conn) -> dict:
    phase = get_phase(conn)
    if phase == "switched":
        return {"ok": False, "detail": "already switched"}
    if phase == "backfilling":
        return {"ok": True, "detail": "already running"}
    with conn.transaction():
        _set_state(conn.cursor(), "backfilling",
                   message="backfill started")
    conn.commit()
    _ensure_worker()
    _wake.set()
    return {"ok": True, "detail": "backfill started"}


def pause_backfill(conn) -> dict:
    phase = get_phase(conn)
    if phase not in ("backfilling",):
        return {"ok": False, "detail": f"cannot pause in phase {phase}"}
    with conn.transaction():
        _set_state(conn.cursor(), "paused", message="paused by operator")
    conn.commit()
    return {"ok": True, "detail": "paused"}


def resume_backfill(conn) -> dict:
    phase = get_phase(conn)
    if phase not in ("paused", "init", "ready"):
        return {"ok": False, "detail": f"cannot resume in phase {phase}"}
    with conn.transaction():
        _set_state(conn.cursor(), "backfilling", message="resumed")
    conn.commit()
    _ensure_worker()
    _wake.set()
    return {"ok": True, "detail": "resumed"}


def resume_on_startup() -> None:
    """进程重启后的续办：backfilling/switching(未提交即已回滚) 自动恢复。"""
    with db.pool.connection() as conn:
        phase = get_phase(conn)
        if phase == "backfilling":
            _ensure_worker()
            _wake.set()
        elif phase == "switching":
            # 上次进程在切换中退出：其事务随进程死亡回滚，旧表与触发器完好
            with conn.transaction():
                _set_state(conn.cursor(), "ready",
                           message="recovered after interrupted switch")
            conn.commit()


# ---------------------------------------------------------------- 校验
def verify_consistency(conn) -> dict:
    """切换前闸门：每样本恰有 temp/ph 两项且与宽表逐值一致。"""
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            WITH wide AS (
                SELECT id, temp, ph FROM samples
            ), tall AS (
                SELECT sample_id,
                       max(value) FILTER (WHERE metric='temp') AS temp,
                       max(value) FILTER (WHERE metric='ph')   AS ph,
                       count(*) AS n
                FROM measurements GROUP BY sample_id
            )
            SELECT
              (SELECT count(*) FROM wide)                                          AS wide_rows,
              (SELECT count(*) FROM tall)                                          AS tall_groups,
              (SELECT count(*) FROM tall WHERE n <> 2)                             AS bad_cardinality,
              (SELECT count(*) FROM wide w FULL JOIN tall t ON t.sample_id = w.id
                 WHERE w.id IS NULL OR t.sample_id IS NULL
                    OR w.temp IS DISTINCT FROM t.temp
                    OR w.ph   IS DISTINCT FROM t.ph)                               AS mismatches
            """
        )
        rows = cur.fetchone()
    result = dict(rows)
    result["ok"] = (
        result["bad_cardinality"] == 0
        and result["mismatches"] == 0
        and result["wide_rows"] == result["tall_groups"]
    )
    return result


# ---------------------------------------------------------------- 切换
def perform_switch(hold_seconds: float | None = None) -> dict:
    """最终切换：全程唯一一次短暂阻塞写入。

    单个事务内：锁表 -> 最终校验 -> DROP 触发器/表 -> 建同名视图+INSTEAD OF 触发器
    -> 置 switched。任何一步失败（含演示用的 hold 后回滚、进程被杀），
    事务回滚，samples 仍是原表，旧接口零影响。

    这里刻意使用“独立短连接并在函数内 close”，而不是连接池：
    回滚后立即关闭连接会同步释放 ACCESS EXCLUSIVE 锁，避免连接池归还时
    的健康检查与等待锁的外部写入互相阻塞。
    """
    from psycopg import connect

    hold = settings.switch_hold_seconds if hold_seconds is None else hold_seconds
    conn = connect(settings.database_url, autocommit=False)
    try:
        phase = get_phase(conn)
        if phase == "switched":
            return {"ok": True, "detail": "already switched"}
        if phase != "ready":
            return {"ok": False,
                    "detail": f"switch requires phase 'ready', got '{phase}'"}

        try:
            with conn.transaction():
                cur = conn.cursor()
                _set_state(cur, "switching", message="switch in progress")

                # 唯一的写阻塞窗口：等待所有在途 DML 提交后取排他锁
                cur.execute("LOCK TABLE samples IN ACCESS EXCLUSIVE MODE")

                check = verify_consistency(conn)
                if not check["ok"]:
                    raise RuntimeError(f"consistency check failed: {check}")

                cur.execute("DROP TRIGGER IF EXISTS samples_to_measurements ON samples")
                cur.execute("DROP TABLE samples")
                cur.execute((SQL_DIR / "03_contract.sql").read_text(encoding="utf-8"))
                _set_state(cur, "switched",
                           message="samples is now a view over measurements")

                if hold and hold > 0:
                    # 演示窗口：锁仍持有，外部写入被阻塞；随后回滚模拟进程崩溃
                    cur.execute("SELECT pg_sleep(%s)", (hold,))
                    raise RuntimeError("simulated crash during switch (rollback)")
            conn.commit()
            return {"ok": True, "detail": "switched: samples is now a view"}
        except Exception as exc:
            conn.rollback()
            # 回滚后恢复为 ready，旧表/双写触发器原样可用，可再次发起切换
            with conn.transaction():
                _set_state(conn.cursor(), "ready",
                           message=f"switch rolled back: {exc!r}")
            conn.commit()
            return {"ok": False,
                    "detail": f"switch rolled back, old interface intact: {exc!r}"}
    finally:
        # 立即断开 => 持有的所有锁同步释放，被阻塞的外部写入继续执行
        conn.close()
