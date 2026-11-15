"""就绪判定与失败传播。"""

from __future__ import annotations

from enum import Enum


class Status(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"


def ready_tasks(dag, statuses: dict[str, Status]) -> list[str]:
    """返回当前可以执行的任务名。

    规则：所有依赖都是 SUCCESS 且自身仍是 PENDING。
    """
    out: list[str] = []
    for t in dag.tasks:
        if statuses.get(t.name) != Status.PENDING:
            continue
        if all(statuses.get(d) == Status.SUCCESS for d in t.depends_on):
            out.append(t.name)
    return out


def propagate_failure(dag, statuses: dict[str, Status]) -> None:
    """上游失败时，把下游标记为跳过。

    当前实现只处理一层直接下游，没有递归传播。
    """
    for t in dag.tasks:
        if statuses.get(t.name) != Status.PENDING:
            continue
        if any(statuses.get(d) == Status.FAILED for d in t.depends_on):
            statuses[t.name] = Status.SKIPPED


def is_done(dag, statuses: dict[str, Status]) -> bool:
    """所有任务都不再是 PENDING 或 RUNNING。"""
    return all(s not in (Status.PENDING, Status.RUNNING) for s in statuses.values())
