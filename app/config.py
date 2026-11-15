import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    database_url: str = os.getenv(
        "DATABASE_URL", "postgresql://postgres@/warehouse?host=/tmp"
    )
    backfill_batch_size: int = int(os.getenv("BACKFILL_BATCH_SIZE", "100"))
    backfill_idle_seconds: float = float(os.getenv("BACKFILL_IDLE_SECONDS", "0.5"))
    # >0 时，切换事务在持锁后 sleep 指定秒数再回滚，用于演示“切换中进程退出”
    switch_hold_seconds: float = float(os.getenv("SWITCH_HOLD_SECONDS", "0"))
    # 演示开关：允许调用 /debug/crash 让进程硬退出（等价 kill -9 的数据库语义）
    allow_crash_demo: bool = os.getenv("ALLOW_CRASH_DEMO", "0") == "1"


settings = Settings()
