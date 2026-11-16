"""SLA 检查与告警摘要。

耗时定义（task_runs 表里每个 attempt 一行）：

- attempt_seconds：单次尝试耗时 = finished_at - started_at；
  任一时刻缺失则为 None（任务仍在 running、运行中断或历史脏数据）。
- busy_seconds（任务重试总耗时）：同一任务所有 attempt_seconds 之和。
  即重试的每一次执行都计入 SLA 耗时，但 attempt 之间的退避等待
  （runner 里的 sleep）不计入——它不是任务执行本身的耗时。
- wall_seconds：首个 attempt 的 started_at 到最后一个 attempt 的
  finished_at 的跨度。运行级总耗时取跨度而不是逐条相加，并行任务的
  重叠时间不会被重复计算。

SLA 判定规则：

- sla_seconds <= 0 表示未设置 SLA，不产生违规项——零 SLA 的含义是
  “不考核”，不是“必须零耗时完成”。
- 判定基于 busy_seconds（重试总耗时），比较使用原始浮点值，仅在输出
  时四舍五入，因此同一份记录永远得到同一个结论。
- 时间戳不完整的 attempt 以 0 计入 busy_seconds 并标记 incomplete；
  若一个任务的所有 attempt 都没有可用时间戳，则无法判定，不产生违规。
- 违规只取决于耗时，不读取也不修改任何运行/任务状态：任务可以成功
  但违规，也可以失败但未违规。运行整体是否失败仍由 runner 的状态
  传播语义单独决定，两者相互独立。
"""

from __future__ import annotations

from collections import defaultdict

# 输出时保留到微秒，避免毫秒级四舍五入翻转边界结论。
_ROUND = 6


def _round(value: float | None) -> float | None:
    return None if value is None else round(value, _ROUND)


def attempt_seconds(record: dict) -> float | None:
    """单次尝试耗时；时间戳缺失或倒挂时返回 None。"""
    started = record.get("started_at")
    finished = record.get("finished_at")
    if started is None or finished is None:
        return None
    delta = finished - started
    if delta < 0:
        # 时钟回拨等异常数据：无法给出可信耗时。
        return None
    return delta


def task_timings(task_records: list[dict]) -> dict[str, dict]:
    """按任务聚合 attempt 记录，返回每个任务的耗时指标。

    输出按任务名排序，保证同一份输入得到字节级稳定的结果。
    """
    grouped: dict[str, list[dict]] = defaultdict(list)
    for rec in task_records:
        grouped[rec["task"]].append(rec)

    out: dict[str, dict] = {}
    for name in sorted(grouped):
        attempts = sorted(grouped[name], key=lambda r: r["attempt"])
        per_attempt: list[float | None] = [attempt_seconds(r) for r in attempts]
        incomplete = any(s is None for s in per_attempt)
        busy = sum(s for s in per_attempt if s is not None)

        starts = [r.get("started_at") for r in attempts if r.get("started_at") is not None]
        finishes = [r.get("finished_at") for r in attempts if r.get("finished_at") is not None]
        wall = (max(finishes) - min(starts)) if starts and finishes else None

        final_status = attempts[-1].get("status")
        out[name] = {
            "task": name,
            "attempts": len(attempts),
            "status": final_status,
            "attempt_seconds": [_round(s) for s in per_attempt],
            "busy_seconds": _round(busy),
            "wall_seconds": _round(wall),
            "incomplete": incomplete,
        }
    return out


def violations(dag, task_records: list[dict]) -> list[dict]:
    """检查任务重试总耗时是否超过其 sla_seconds，返回违规项列表。

    未设置 SLA（<=0）、DAG 中已不存在的任务、以及完全缺失时间戳的
    任务都不产生违规项。输出顺序跟随 DAG 定义中的任务顺序。
    """
    timings = task_timings(task_records)
    sla_by_name = {t.name: t.sla_seconds for t in dag.tasks}

    found: list[dict] = []
    for task in dag.tasks:
        sla = task.sla_seconds
        if sla is None or sla <= 0:
            continue
        timing = timings.get(task.name)
        if timing is None:
            continue
        # 所有 attempt 都缺时间戳：无法判定。
        if all(s is None for s in timing["attempt_seconds"]):
            continue
        # 用原始值比较，避免四舍五入改变边界结论。
        busy_raw = sum(s for s in timing["attempt_seconds"] if s is not None)
        if busy_raw > sla:
            found.append({
                "task": task.name,
                "status": timing["status"],
                "sla_seconds": sla,
                "busy_seconds": timing["busy_seconds"],
                "wall_seconds": timing["wall_seconds"],
                "attempts": timing["attempts"],
                "attempt_seconds": timing["attempt_seconds"],
                "incomplete": timing["incomplete"],
            })
    return found


def summarize(dag, task_records: list[dict]) -> dict:
    """汇总一次运行的耗时、失败任务与 SLA 违规，供 API 展示。

    保留历史字段 total_task_seconds / failed_tasks，新增并行安全的
    wall_seconds 与完整 SLA 结论。所有结论只由入参记录推导，重复
    调用同一份记录结果完全一致。
    """
    timings = task_timings(task_records)
    breaches = violations(dag, task_records)

    # 每个任务只计一次 busy_seconds（已含全部重试），并行任务因此
    # 不会把同一段时间重复相加；若要看真实经过时间则用 wall_seconds。
    total_busy = 0.0
    failed = 0
    for timing in timings.values():
        total_busy += timing["busy_seconds"] or 0.0
        if timing["status"] == "failed":
            failed += 1

    starts = [r.get("started_at") for r in task_records if r.get("started_at") is not None]
    finishes = [r.get("finished_at") for r in task_records if r.get("finished_at") is not None]
    wall = (max(finishes) - min(starts)) if starts and finishes else None

    return {
        "task_count": len(timings),
        "failed_tasks": failed,
        "total_task_seconds": round(total_busy, _ROUND),
        "wall_seconds": _round(wall),
        "sla_violated": bool(breaches),
        "violation_count": len(breaches),
        "violations": breaches,
        "tasks": timings,
    }
