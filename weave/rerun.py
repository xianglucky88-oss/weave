"""历史重跑：按日期范围重新执行 DAG。"""

from __future__ import annotations

from datetime import datetime, timedelta


def daterange(start: str, end: str) -> list[str]:
    """把 [start, end] 展开成日期列表（含两端）。"""
    d0 = datetime.strptime(start, "%Y-%m-%d")
    d1 = datetime.strptime(end, "%Y-%m-%d")
    out: list[str] = []
    while d0 <= d1:
        out.append(d0.strftime("%Y-%m-%d"))
        d0 += timedelta(days=1)
    return out


def plan(dag, start: str, end: str, existing: set[str]) -> list[str]:
    """返回需要重跑的 ds 列表。

    当前实现直接返回空列表——重跑功能尚未接入。
    """
    return []
