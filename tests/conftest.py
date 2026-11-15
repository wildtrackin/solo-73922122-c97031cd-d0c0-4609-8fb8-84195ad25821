import os
import subprocess
import time
from pathlib import Path

import psycopg
import pytest

ROOT = Path(__file__).resolve().parent.parent
TEST_DB = os.getenv("TEST_DATABASE", "warehouse_pytest")
ADMIN_DSN = os.getenv(
    "ADMIN_DATABASE_URL", "postgresql:///postgres?host=%2Ftmp&user=postgres"
)


def _dsn(dbname: str) -> str:
    # unix socket 路径需 URL 编码：/tmp -> %2Ftmp
    return f"postgresql:///{dbname}?host=%2Ftmp&user=postgres"


def _reset_schema(dsn: str) -> None:
    """函数级重置：DROP/CREATE SCHEMA 不触发 checkpoint 信号。

    （部分受限容器禁止跨进程 kill()，DROP DATABASE 的强制 checkpoint 会失败。）
    """
    schema = ROOT / "db" / "init" / "01_baseline.sql"
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute("DROP SCHEMA public CASCADE")
        conn.execute("CREATE SCHEMA public")
        conn.execute("GRANT ALL ON SCHEMA public TO postgres")
    subprocess.run(
        ["psql", "-h", "/tmp", "-U", "postgres", "-d", TEST_DB,
         "-v", "ON_ERROR_STOP=1", "-q", "-f", str(schema)],
        check=True,
    )


@pytest.fixture(scope="session", autouse=True)
def _ensure_database():
    """会话开始时确保测试库存在（仅创建一次，后续用 schema 重置）。"""
    dsn = _dsn(TEST_DB)
    with psycopg.connect(ADMIN_DSN, autocommit=True) as admin:
        exists = admin.execute(
            "SELECT 1 FROM pg_database WHERE datname=%s", (TEST_DB,)
        ).fetchone()
        if not exists:
            admin.execute(f"CREATE DATABASE {TEST_DB}")
    os.environ["DATABASE_URL"] = dsn
    os.environ.setdefault("BACKFILL_BATCH_SIZE", "1")
    os.environ.setdefault("BACKFILL_IDLE_SECONDS", "0.1")
    yield


@pytest.fixture(scope="session")
def dsn() -> str:
    return _dsn(TEST_DB)


@pytest.fixture()
def client(dsn: str):
    # 每个用例独立 schema + 独立 TestClient（lifespan 负责引导与续办）
    _reset_schema(dsn)
    from fastapi.testclient import TestClient

    from app.main import app

    with TestClient(app) as c:
        yield c


def wait_for(url: str, timeout: float = 15) -> None:
    import httpx

    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            if httpx.get(url).status_code == 200:
                return
        except httpx.HTTPError:
            time.sleep(0.3)
    raise TimeoutError(url)
