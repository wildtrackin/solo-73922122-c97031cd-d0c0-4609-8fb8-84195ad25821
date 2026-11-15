r"""同一送检样本的“保留编号 + 重复编号”合并（仅限最终切换之前）。

合并是一次单事务操作：
    * 先取共享 advisory lock，与最终切换互斥；
    * 校验迁移阶段仍允许合并（init/backfilling/paused/ready），samples 仍是表；
    * 以 ``SELECT ... FOR UPDATE`` 按 id 顺序锁定两个编号的当前已提交行；
    * 任一编号不存在 -> MergeError，整个事务回滚；
    * 按页面选择从两条当前已提交记录取 temp / ph，原子更新保留行、删除重复行。

锁语义（与旧采集程序直接 SQL、回填扫描交错时）：
    * FOR UPDATE 让合并与直接 SQL 按提交顺序串行化：先提交的一方先被看到，
      合并不会读到旧值再覆盖对方新提交的值。
    * 回填同样用 FOR SHARE 锁定源行（migration.backfill_batch），因此回填扫描
      与合并不能交错：先拿到锁的一方先提交，另一方看到提交后的版本。
      被合并删除的编号不会在回填时被重新写进 measurements。
    * 重复行删除后，旧程序对它的 UPDATE 影响 0 行；DELETE 触发器同步清理
      measurements。

与最终切换的互斥：
    * 合并持共享 advisory lock，最终切换用 ``pg_try_advisory_xact_lock`` 取排他锁，
      取不到立即拒绝（不排队），因此切换只会在“完整合并已提交”之后开始。
    * 切换持排他锁期间，合并取共享锁立即失败并拒绝。
    * 切换的一致性校验与合并不会重叠，不可能读到半次合并。
"""
from __future__ import annotations

from psycopg.rows import dict_row

# pg_advisory_xact_lock 的 key：合并取 shared，最终切换取 exclusive。
MERGE_LOCK_KEY = 7252501

# 允许合并的阶段：最终切换开始后一律拒绝。
ALLOWED_PHASES = ("init", "backfilling", "paused", "ready")

VALID_SOURCES = ("keep", "duplicate")


class MergeError(Exception):
    """合并被拒绝（业务冲突），由 API 映射为 409；事务已回滚。"""

    def __init__(self, code: str, message: str,
                 missing: list[int] | None = None):
        super().__init__(message)
        self.code = code
        self.message = message
        self.missing = missing or []

    def as_detail(self) -> dict:
        return {"code": self.code, "message": self.message,
                "missing": self.missing}


def _phase(cur) -> str:
    cur.execute("SELECT state FROM migration_state WHERE id = 1")
    return cur.fetchone()["state"]


def _samples_is_table(cur) -> bool:
    cur.execute(
        "SELECT c.relkind FROM pg_class c WHERE c.oid = to_regclass('samples')"
    )
    row = cur.fetchone()
    return row is not None and row["relkind"] == "r"


def merge_samples(conn, keep_id: int, duplicate_id: int,
                  temp_from: str, ph_from: str) -> dict:
    """在单事务内合并两个编号，返回合并前后数值。

    值的选择在事务内、拿到行锁之后完成，读取的是两条记录“当前已提交”的值；
    客户端提交的只是“取哪条记录”的选择，不提交具体温度/pH 数值。
    """
    if keep_id == duplicate_id:
        raise MergeError("invalid_selection", "keep_id and duplicate_id must differ")
    for name, source in (("temp", temp_from), ("ph", ph_from)):
        if source not in VALID_SOURCES:
            raise MergeError("invalid_selection",
                             f"{name} source must be 'keep' or 'duplicate'")

    with conn.transaction():
        with conn.cursor(row_factory=dict_row) as cur:

            # 与最终切换互斥：切换持排他锁时这里立即失败，不排队等待。
            cur.execute("SELECT pg_try_advisory_xact_lock_shared(%s) AS got",
                        (MERGE_LOCK_KEY,))
            if not cur.fetchone()["got"]:
                raise MergeError(
                    "switch_in_progress",
                    "final switch is in progress; merge refused "
                    "(retry after it finishes)",
                )

            phase = _phase(cur)
            if phase not in ALLOWED_PHASES:
                raise MergeError(
                    "phase_not_mergeable",
                    f"merge requires phase in {ALLOWED_PHASES}, got '{phase}'",
                )
            if not _samples_is_table(cur):
                raise MergeError(
                    "phase_not_mergeable",
                    "samples is no longer the writable table (switch completed)",
                )

            # 按 id 升序逐行取锁（与回填的 FOR SHARE 顺序一致，避免死锁）。
            ids = sorted((keep_id, duplicate_id))
            rows = {}
            missing = []
            for sid in ids:
                cur.execute(
                    "SELECT id, temp, ph FROM samples WHERE id = %s FOR UPDATE",
                    (sid,),
                )
                row = cur.fetchone()
                if row is None:
                    missing.append(sid)
                else:
                    rows[sid] = dict(row)
            if missing:
                # 任一编号不存在（如被旧程序删除）：整次失败，什么都不改。
                raise MergeError(
                    "sample_missing",
                    f"sample id(s) no longer exist: {missing}; merge aborted",
                    missing=missing,
                )

            # 行锁已持有：以下读取的是“当前已提交”的版本，且到提交前不会被
            # 旧程序的 UPDATE/DELETE 改变（它们会在行锁上等待本事务结束）。
            before = {"keep": rows[keep_id], "duplicate": rows[duplicate_id]}
            temp = before[temp_from]["temp"]
            ph = before[ph_from]["ph"]

            cur.execute(
                "UPDATE samples SET temp = %s, ph = %s WHERE id = %s "
                "RETURNING id, temp, ph",
                (temp, ph, keep_id),
            )
            after = dict(cur.fetchone())

            # 重复编号的 measurements 由 DELETE 触发器同步清理；先计数用于回显。
            cur.execute(
                "SELECT count(*) AS n FROM measurements WHERE sample_id = %s",
                (duplicate_id,),
            )
            removed_measurements = cur.fetchone()["n"]
            cur.execute("DELETE FROM samples WHERE id = %s", (duplicate_id,))
            if cur.rowcount != 1:
                raise MergeError(
                    "sample_missing",
                    f"sample id {duplicate_id} disappeared during merge; aborted",
                    missing=[duplicate_id],
                )

            cur.execute(
                "SELECT sample_id, metric, value FROM measurements "
                "WHERE sample_id = %s ORDER BY metric",
                (keep_id,),
            )
            keep_measurements = [dict(r) for r in cur.fetchall()]

    conn.commit()
    return {
        "ok": True,
        "keep_id": keep_id,
        "duplicate_id": duplicate_id,
        "sources": {"temp": temp_from, "ph": ph_from},
        "before": before,
        "after": after,
        "removed_measurements": removed_measurements,
        "keep_measurements": keep_measurements,
    }
