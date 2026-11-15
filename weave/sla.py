"""SLA 检查与告警。"""

from __future__ import annotations


def violations(dag, task_records: list[dict]) -> list[dict]:
    """检查任务耗时是否超过 sla_seconds。

    当前实现返回空列表——告警尚未接入。
    """
    return []


def summarize(dag, task_records: list[dict]) -> dict:
    """汇总一次运行的耗时与状态，供 API 展示。"""
    total = 0.0
    failed = 0
    for rec in task_records:
        if rec.get("started_at") and rec.get("finished_at"):
            total += rec["finished_at"] - rec["started_at"]
        if rec.get("status") == "failed":
            failed += 1
    return {"total_task_seconds": round(total, 3), "failed_tasks": failed}
