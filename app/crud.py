"""新旧两个写入入口的数据访问。

旧入口（/samples/*，宽表语义）与新入口（/measurements/*，长表语义）
在切换前后都指向同一份数据，且不依赖任何定时同步：

    切换前：samples 是表，measurements 由行级双写触发器实时维护；
    切换后：samples 是 measurements 上的视图，INSTEAD OF 触发器把写下沉到长表。

每个写操作都在一个数据库事务内完成，一次修改两项必然一起生效/一起回滚。
"""
from __future__ import annotations

from psycopg.rows import dict_row


def _dict_cursor(conn):
    return conn.cursor(row_factory=dict_row)


# ---------------------------------------------------------------- 旧入口
def list_samples(conn) -> list[dict]:
    with _dict_cursor(conn) as cur:
        cur.execute("SELECT id, temp, ph FROM samples ORDER BY id")
        return [dict(r) for r in cur.fetchall()]


def get_sample(conn, sample_id: int) -> dict | None:
    with _dict_cursor(conn) as cur:
        cur.execute("SELECT id, temp, ph FROM samples WHERE id = %s", (sample_id,))
        row = cur.fetchone()
    return dict(row) if row else None


def create_sample(conn, temp: float, ph: float) -> dict:
    # 一条 INSERT：触发器/视图在同一事务里落 temp、ph 两项
    with _dict_cursor(conn) as cur:
        cur.execute(
            "INSERT INTO samples (temp, ph) VALUES (%s, %s) RETURNING id, temp, ph",
            (temp, ph),
        )
        row = dict(cur.fetchone())
    conn.commit()
    return row


def update_sample(conn, sample_id: int, temp: float, ph: float) -> dict | None:
    with _dict_cursor(conn) as cur:
        cur.execute(
            "UPDATE samples SET temp = %s, ph = %s WHERE id = %s "
            "RETURNING id, temp, ph",
            (temp, ph, sample_id),
        )
        row = cur.fetchone()
    conn.commit()
    return dict(row) if row else None


def delete_sample(conn, sample_id: int) -> bool:
    with conn.cursor() as cur:
        cur.execute("DELETE FROM samples WHERE id = %s", (sample_id,))
        deleted = cur.rowcount > 0
    conn.commit()
    return deleted


# ---------------------------------------------------------------- 新入口
def list_measurements(conn) -> list[dict]:
    with _dict_cursor(conn) as cur:
        cur.execute(
            "SELECT sample_id, metric, value FROM measurements "
            "ORDER BY sample_id, metric"
        )
        rows = [dict(r) for r in cur.fetchall()]
    return rows


def get_measurement_group(conn, sample_id: int) -> list[dict]:
    with _dict_cursor(conn) as cur:
        cur.execute(
            "SELECT sample_id, metric, value FROM measurements "
            "WHERE sample_id = %s ORDER BY metric",
            (sample_id,),
        )
        rows = [dict(r) for r in cur.fetchall()]
    return rows


def create_measurement_group(conn, temp: float, ph: float) -> dict:
    """长表入口新增样本：两行指标在同一事务整组插入。

    切换前后都经 samples 写入（表->触发器，或视图->INSTEAD OF 触发器），
    从而保证两个入口永远操作同一份数据，而不是各写各的再同步。
    """
    with _dict_cursor(conn) as cur:
        cur.execute(
            "INSERT INTO samples (temp, ph) VALUES (%s, %s) RETURNING id",
            (temp, ph),
        )
        sid = cur.fetchone()["id"]
    conn.commit()
    return {"sample_id": sid, "rows": get_measurement_group(conn, sid)}


def replace_measurement_group(conn, sample_id: int,
                              temp: float, ph: float) -> dict | None:
    """整组替换某样本的两项指标（事务内一起生效）。"""
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE samples SET temp = %s, ph = %s WHERE id = %s",
            (temp, ph, sample_id),
        )
        changed = cur.rowcount
    if changed == 0:
        conn.rollback()
        return None
    conn.commit()
    return {"sample_id": sample_id, "rows": get_measurement_group(conn, sample_id)}


def delete_measurement_group(conn, sample_id: int) -> bool:
    with conn.cursor() as cur:
        cur.execute("DELETE FROM samples WHERE id = %s", (sample_id,))
        deleted = cur.rowcount > 0
    conn.commit()
    return deleted
