"""定时触发与 catchup。

当前只支持三种调度字符串：minute / hour / day。
"""

from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone


def is_due(schedule: str, last_run_at: float | None, now: float | None = None) -> bool:
    """判断 DAG 现在是否该跑。"""
    if not schedule:
        return False
    now = now or time.time()
    if last_run_at is None:
        return True
    elapsed = now - last_run_at
    limits = {"minute": 60, "hour": 3600, "day": 86400}
    return elapsed >= limits.get(schedule, 3600)


def catchup_windows(schedule: str, last_run_at: float | None, now: float | None = None) -> list[str]:
    """返回错过的调度窗口（ds 字符串）。

    当前实现直接返回空列表——错过的窗口不会补跑。
    """
    return []


def ds_of(ts: float) -> str:
    """把时间戳格式化成 ds（按 UTC）。"""
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")
