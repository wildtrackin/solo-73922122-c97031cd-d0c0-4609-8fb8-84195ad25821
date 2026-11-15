from contextlib import asynccontextmanager

from psycopg_pool import ConnectionPool

from .config import settings

# 模块级句柄：lifespan 每次启动创建新池、关闭后置 None。
# （psycopg 的 ConnectionPool 关闭后不可 reopen，测试中会反复进入 lifespan。）
pool: ConnectionPool | None = None


@asynccontextmanager
async def lifespan(app):
    global pool
    pool = ConnectionPool(
        conninfo=settings.database_url,
        min_size=2,
        max_size=10,
        kwargs={"autocommit": False},
        open=True,
    )
    from .migration import bootstrap, resume_on_startup

    with pool.connection() as conn:
        bootstrap(conn)
    resume_on_startup()
    try:
        yield
    finally:
        pool.close()
        pool = None
