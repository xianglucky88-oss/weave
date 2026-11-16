"""定时触发与 catchup 的调度窗口规则。

只支持三种简化调度字符串：minute / hour / day。所有窗口按 UTC 切分：

    minute  窗口长度 60s   ds 形如 2026-11-14T10:30
    hour    窗口长度 3600s ds 形如 2026-11-14T10
    day     窗口长度 86400s ds 形如 2026-11-14

窗口一律左闭右开 [start, start+period)。ds 是窗口在对应粒度下的唯一标识，
因此 minute/hour 调度即使落在同一个 UTC 日期，也不会互相覆盖。
"""

from __future__ import annotations

import math
import time
from datetime import datetime, timedelta, timezone

# 供 rerun 等模块复用的 datetime 工具（统一走 UTC，避免各处重复 import）。
datetime = datetime
timedelta = timedelta
timezone = timezone

# 每种调度的窗口长度（秒）。
PERIODS: dict[str, int] = {"minute": 60, "hour": 3600, "day": 86400}

# 每种调度的 ds 格式（均为 UTC）。
DS_FORMATS: dict[str, str] = {
    "minute": "%Y-%m-%dT%H:%M",
    "hour": "%Y-%m-%dT%H",
    "day": "%Y-%m-%d",
}

DATE_FMT = "%Y-%m-%d"


class ScheduleError(ValueError):
    """调度字符串或 ds 边界不合法。"""


def validate_schedule(schedule: str) -> None:
    if schedule not in PERIODS:
        raise ScheduleError(
            f"unsupported schedule {schedule!r}; expected one of {sorted(PERIODS)}"
        )


def period_of(schedule: str) -> int:
    validate_schedule(schedule)
    return PERIODS[schedule]


def is_due(schedule: str, last_run_at: float | None, now: float | None = None) -> bool:
    """判断 DAG 现在是否该跑（实时触发，不负责补历史窗口）。"""
    if not schedule:
        return False
    now = now or time.time()
    if last_run_at is None:
        return True
    elapsed = now - last_run_at
    return elapsed >= period_of(schedule)


def window_start(schedule: str, ts: float) -> float:
    """返回 ts 所在窗口的起点 epoch（UTC 向下取整到窗口边界）。"""
    p = period_of(schedule)
    return math.floor(ts / p) * p


def window_end(schedule: str, ts: float) -> float:
    """返回 ts 所在窗口的终点 epoch（开区间）。"""
    return window_start(schedule, ts) + period_of(schedule)


def window_ds(schedule: str, ts: float) -> str:
    """把时间戳格式化成它所属窗口的 ds（按 UTC）。"""
    return datetime.fromtimestamp(window_start(schedule, ts), tz=timezone.utc).strftime(
        DS_FORMATS[schedule]
    )


def ds_of(ts: float) -> str:
    """把时间戳格式化成 UTC 日期 ds（day 粒度的便捷封装）。"""
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime(DATE_FMT)


def iso(ts: float) -> str:
    """epoch -> UTC ISO 字符串，用于对外输出。"""
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_window_ds(schedule: str, ds: str) -> float:
    """把窗口 ds 解析回窗口起点 epoch；非法时抛 ScheduleError。"""
    validate_schedule(schedule)
    try:
        dt = datetime.strptime(ds, DS_FORMATS[schedule]).replace(tzinfo=timezone.utc)
    except ValueError as exc:
        raise ScheduleError(
            f"ds {ds!r} does not match format {DS_FORMATS[schedule]!r} for schedule {schedule!r}"
        ) from exc
    return dt.timestamp()


def parse_bound(schedule: str, text: str) -> tuple[float, float]:
    """把用户输入的边界解析成 (lo 闭, hi 开) 两个 epoch。

    接受两种输入：
      - 精确窗口 ds（与调度粒度一致，如 hour 调度的 2026-11-14T10）；
      - UTC 日期 2026-11-14，表示当天 00:00 到次日 00:00 的整天范围。
    """
    validate_schedule(schedule)
    text = text.strip()
    if "T" in text:
        start = parse_window_ds(schedule, text)
        return start, start + PERIODS[schedule]
    try:
        d = datetime.strptime(text, DATE_FMT).replace(tzinfo=timezone.utc)
    except ValueError as exc:
        raise ScheduleError(f"bound {text!r} must be YYYY-MM-DD or a window ds") from exc
    return d.timestamp(), (d + timedelta(days=1)).timestamp()


def iter_window_starts(schedule: str, lo: float, hi: float) -> list[float]:
    """枚举在 hi 时刻已经关闭、且起点不早于 lo 的窗口起点（升序）。

    窗口 [t, t+p) 在 hi 时刻已关闭当且仅当 t+p <= hi；因此当前仍开放的
    窗口（t+p > hi）永远不会被枚举，保证补跑不触碰未完成的 UTC 窗口。
    """
    p = period_of(schedule)
    t = window_start(schedule, lo)
    if t < lo:
        t += p
    out: list[float] = []
    while t + p <= hi:
        out.append(t)
        t += p
    return out


def catchup_windows(
    schedule: str, last_run_at: float | None, now: float | None = None
) -> list[str]:
    """返回锚点之后已经关闭、因此被漏掉的调度窗口 ds。

    规则：
      - 从未跑过（last_run_at 为 None）时返回空——古老历史不凭空补；
      - 从“上次运行所在窗口”的下一个窗口开始枚举；
      - 只包含已经关闭的窗口（window_end <= now），当前仍开放的窗口留给
        实时触发，因此重复调用本函数不会产生重复 ds；
      - 结果严格按时间升序，服务长期停机时会枚举中间全部窗口。

    这是无状态的底层原语；是否真的补跑还要交给 rerun.plan_reruns 结合
    历史运行状态与 on_exists 策略决定。
    """
    if last_run_at is None:
        return []
    now = now or time.time()
    p = period_of(schedule)
    first = window_start(schedule, last_run_at) + p
    return [
        datetime.fromtimestamp(t, tz=timezone.utc).strftime(DS_FORMATS[schedule])
        for t in iter_window_starts(schedule, first, now)
    ]
