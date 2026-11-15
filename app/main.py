from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
import psycopg
from psycopg.errors import CheckViolation, UniqueViolation
from psycopg.rows import dict_row

from . import crud, db, merge, migration
from .db import lifespan
from .schemas import MergeIn, SampleIn

app = FastAPI(
    title="宽表 -> 指标长表在线迁移控制台",
    version="1.0.0",
    lifespan=lifespan,
)

STATIC_DIR = Path(__file__).resolve().parent / "static"


@app.get("/")
def index():
    return FileResponse(STATIC_DIR / "index.html")


# ------------------------------------------------ 迁移控制
@app.post("/migration/start")
def migration_start():
    with db.pool.connection() as conn:
        return migration.start_backfill(conn)


@app.post("/migration/pause")
def migration_pause():
    with db.pool.connection() as conn:
        return migration.pause_backfill(conn)


@app.post("/migration/resume")
def migration_resume():
    with db.pool.connection() as conn:
        return migration.resume_backfill(conn)


@app.get("/migration/progress")
def migration_progress():
    with db.pool.connection() as conn:
        progress = migration.get_progress(conn)
        check = migration.verify_consistency(conn)
    return {**progress, "consistency": check}


@app.post("/migration/switch")
def migration_switch(hold_seconds: float | None = None):
    result = migration.perform_switch(hold_seconds=hold_seconds)
    if not result["ok"]:
        raise HTTPException(status_code=409, detail=result["detail"])
    return result


# ------------------------------------------------ 样本合并（切换前均可操作）
@app.get("/samples/{sample_id}/merge-preview")
def merge_preview(sample_id: int, duplicate_id: int):
    """网页选择前的数值预览：读当前已提交的两条记录，不做任何写入。"""
    with db.pool.connection() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            rows = {}
            for sid in (sample_id, duplicate_id):
                cur.execute("SELECT id, temp, ph FROM samples WHERE id = %s", (sid,))
                row = cur.fetchone()
                if row is not None:
                    rows[sid] = dict(row)
            phase = migration.get_phase(conn)
            cur.execute(
                "SELECT sample_id, metric, value FROM measurements "
                "WHERE sample_id = ANY(%s) ORDER BY sample_id, metric",
                ([sample_id, duplicate_id],),
            )
            measurements = [dict(r) for r in cur.fetchall()]
    return {
        "keep_id": sample_id,
        "duplicate_id": duplicate_id,
        "phase": phase,
        "mergeable": phase in merge.ALLOWED_PHASES,
        "samples": rows,
        "missing": [sid for sid in (sample_id, duplicate_id) if sid not in rows],
        "measurements": measurements,
    }


@app.post("/samples/merge")
def samples_merge(body: MergeIn):
    """原子合并：从两条当前已提交记录取值，更新保留样本、删除重复样本。"""
    with db.pool.connection() as conn:
        try:
            return merge.merge_samples(
                conn, body.keep_id, body.duplicate_id,
                body.temp_from, body.ph_from,
            )
        except merge.MergeError as exc:
            conn.rollback()
            raise HTTPException(status_code=409, detail=exc.as_detail()) from exc
        except psycopg.errors.DeadlockDetected as exc:
            # 与旧程序多行写入的锁顺序冲突：PostgreSQL 已回滚整个事务，
            # 合并未产生任何部分写入，安全重试即可。
            conn.rollback()
            raise HTTPException(
                status_code=409,
                detail={"code": "conflict_retry",
                        "message": f"deadlock with concurrent writer; "
                                   f"merge rolled back: {exc}",
                        "missing": []},
            ) from exc


# ------------------------------------------------ 旧入口：宽表 samples
@app.get("/samples")
def samples_list():
    with db.pool.connection() as conn:
        return crud.list_samples(conn)


@app.get("/samples/{sample_id}")
def sample_get(sample_id: int):
    with db.pool.connection() as conn:
        row = crud.get_sample(conn, sample_id)
    if row is None:
        raise HTTPException(404, "sample not found")
    return row


@app.post("/samples", status_code=201)
def sample_create(body: SampleIn):
    with db.pool.connection() as conn:
        try:
            return crud.create_sample(conn, body.temp, body.ph)
        except (CheckViolation, UniqueViolation) as exc:
            conn.rollback()
            raise HTTPException(422, str(exc)) from exc


@app.put("/samples/{sample_id}")
def sample_update(sample_id: int, body: SampleIn):
    with db.pool.connection() as conn:
        row = crud.update_sample(conn, sample_id, body.temp, body.ph)
    if row is None:
        raise HTTPException(404, "sample not found")
    return row


@app.delete("/samples/{sample_id}")
def sample_delete(sample_id: int):
    with db.pool.connection() as conn:
        ok = crud.delete_sample(conn, sample_id)
    if not ok:
        raise HTTPException(404, "sample not found")
    return {"ok": True, "deleted": sample_id}


# ------------------------------------------------ 新入口：长表 measurements
@app.get("/measurements")
def measurements_list():
    with db.pool.connection() as conn:
        return crud.list_measurements(conn)


@app.get("/measurements/{sample_id}")
def measurement_group_get(sample_id: int):
    with db.pool.connection() as conn:
        rows = crud.get_measurement_group(conn, sample_id)
    if not rows:
        raise HTTPException(404, "measurement group not found")
    return rows


@app.post("/measurements", status_code=201)
def measurement_group_create(body: SampleIn):
    # 新增一个样本 = 整组写入 temp/ph 两项；单指标无法构成合法样本
    with db.pool.connection() as conn:
        return crud.create_measurement_group(conn, body.temp, body.ph)


@app.put("/measurements/{sample_id}")
def measurement_group_replace(sample_id: int, body: SampleIn):
    with db.pool.connection() as conn:
        result = crud.replace_measurement_group(conn, sample_id, body.temp, body.ph)
    if result is None:
        raise HTTPException(404, "measurement group not found")
    return result


@app.delete("/measurements/{sample_id}")
def measurement_group_delete(sample_id: int):
    with db.pool.connection() as conn:
        ok = crud.delete_measurement_group(conn, sample_id)
    if not ok:
        raise HTTPException(404, "measurement group not found")
    return {"ok": True, "deleted": sample_id}


@app.get("/health")
def health():
    with db.pool.connection() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute("SELECT state FROM migration_state WHERE id=1")
            row = cur.fetchone()
    return {"ok": True, "phase": row["state"]}


@app.post("/debug/crash")
def debug_crash():
    """仅 ALLOW_CRASH_DEMO=1 时可用：硬退出进程，模拟 kill -9。

    数据库侧效果与真的被杀一致：TCP 连接断开，Postgres 回滚该进程所有
    未提交事务；已提交的批次/修改持久保留，重启后自动续办。
    """
    from .config import settings
    import os

    if not settings.allow_crash_demo:
        raise HTTPException(404, "crash demo disabled")
    # 不优雅关闭、不等待任何事务：模拟崩溃
    os._exit(137)

