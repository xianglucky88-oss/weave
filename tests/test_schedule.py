"""调度窗口、UTC 边界与无状态 catchup 原语的测试。"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from weave import schedule
from weave.schedule import ScheduleError


def ts(y, m, d, hh=0, mm=0, ss=0):
    return datetime(y, m, d, hh, mm, ss, tzinfo=timezone.utc).timestamp()


def test_periods_and_validation():
    assert schedule.period_of("minute") == 60
    assert schedule.period_of("hour") == 3600
    assert schedule.period_of("day") == 86400
    with pytest.raises(ScheduleError):
        schedule.period_of("weekly")


def test_window_floors_to_utc_boundary():
    t = ts(2026, 11, 14, 23, 59, 59)
    assert schedule.window_start("day", t) == ts(2026, 11, 14)
    assert schedule.window_start("hour", t) == ts(2026, 11, 14, 23)
    assert schedule.window_start("minute", t) == ts(2026, 11, 14, 23, 59)


def test_ds_formats_distinguish_same_utc_day():
    t = ts(2026, 11, 14, 10, 30)
    assert schedule.window_ds("day", t) == "2026-11-14"
    assert schedule.window_ds("hour", t) == "2026-11-14T10"
    assert schedule.window_ds("minute", t) == "2026-11-14T10:30"


def test_utc_midnight_creates_new_window():
    before = ts(2026, 11, 14, 23, 59, 59)
    after = before + 2
    assert schedule.window_ds("day", before) == "2026-11-14"
    assert schedule.window_ds("day", after) == "2026-11-15"
    assert schedule.window_ds("hour", after) == "2026-11-15T00"


def test_catchup_never_ran_returns_empty():
    assert schedule.catchup_windows("day", None, ts(2026, 11, 20)) == []


def test_catchup_enumerates_only_closed_windows_ascending():
    # 停机到 02:05：锚点 23 点窗口之后关闭了 00/01 两个小时窗口；
    # 02 点窗口要到 03:00 才关闭，此刻仍开放，不能补。
    anchor = ts(2026, 11, 14, 23, 30)
    now = ts(2026, 11, 15, 2, 5)
    got = schedule.catchup_windows("hour", anchor, now)
    assert got == ["2026-11-15T00", "2026-11-15T01"]


def test_catchup_includes_window_that_just_closed():
    anchor = ts(2026, 11, 14, 23, 30)
    now = ts(2026, 11, 15, 3, 0)  # 02 点窗口恰好在 03:00 关闭
    got = schedule.catchup_windows("hour", anchor, now)
    assert got == ["2026-11-15T00", "2026-11-15T01", "2026-11-15T02"]


def test_catchup_open_window_not_included():
    anchor = ts(2026, 11, 14, 23, 30)
    # 只过了 10 分钟，新窗口 00 尚未关闭。
    now = ts(2026, 11, 15, 0, 10)
    assert schedule.catchup_windows("hour", anchor, now) == []


def test_catchup_is_idempotent():
    anchor = ts(2026, 11, 14, 23, 30)
    now = ts(2026, 11, 15, 3)
    first = schedule.catchup_windows("hour", anchor, now)
    second = schedule.catchup_windows("hour", anchor, now)
    assert first == second and len(first) == 3


def test_catchup_long_outage_minute_schedule():
    anchor = ts(2026, 11, 14, 10, 0, 30)
    now = ts(2026, 11, 14, 10, 5, 0)  # 关闭了 01..04 四个分钟窗口
    got = schedule.catchup_windows("minute", anchor, now)
    assert got == [
        "2026-11-14T10:01", "2026-11-14T10:02",
        "2026-11-14T10:03", "2026-11-14T10:04",
    ]


def test_catchup_multiple_utc_days():
    anchor = ts(2026, 11, 10, 8)
    now = ts(2026, 11, 14, 9)
    # 11-14 当天窗口在 09:00 仍开放，所以只补 11..13。
    got = schedule.catchup_windows("day", anchor, now)
    assert got == ["2026-11-11", "2026-11-12", "2026-11-13"]


def test_parse_window_ds_roundtrip():
    t = ts(2026, 11, 14, 10, 30)
    ds = schedule.window_ds("hour", t)
    assert schedule.parse_window_ds("hour", ds) == schedule.window_start("hour", t)
    with pytest.raises(ScheduleError):
        schedule.parse_window_ds("hour", "2026-11-14")  # 缺小时段
    with pytest.raises(ScheduleError):
        schedule.parse_window_ds("day", "not-a-date")


def test_parse_bound_date_vs_window_ds():
    lo, hi = schedule.parse_bound("hour", "2026-11-14")
    assert (lo, hi) == (ts(2026, 11, 14), ts(2026, 11, 15))
    lo, hi = schedule.parse_bound("hour", "2026-11-14T10")
    assert (lo, hi) == (ts(2026, 11, 14, 10), ts(2026, 11, 14, 11))
    with pytest.raises(ScheduleError):
        schedule.parse_bound("day", "garbage")


def test_iter_window_starts_partial_edges():
    # 起止都不落在边界上时，只枚举在 hi 之前已经关闭的窗口。
    lo = ts(2026, 11, 14, 10, 15)
    hi = ts(2026, 11, 14, 13, 20)
    got = schedule.iter_window_starts("hour", lo, hi)
    # 11 点窗口 12:00 关闭、12 点窗口 13:00 关闭；13 点窗口 14:00 才关。
    assert got == [ts(2026, 11, 14, 11), ts(2026, 11, 14, 12)]


def test_is_due_unchanged_semantics():
    assert schedule.is_due("day", None) is True
    now = ts(2026, 11, 14)
    assert schedule.is_due("day", now - 86400 - 1, now) is True
    assert schedule.is_due("day", now - 10, now) is False
    assert schedule.is_due("", None, now) is False
