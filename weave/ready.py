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
    """上游失败时，把所有（含间接）下游递归标记为跳过。

    用不动点迭代实现：只要还有 PENDING 任务的任一依赖已 FAILED/SKIPPED，
    就把它置为 SKIPPED，反复扫描直到一轮没有变化。DAG 无环（validate
    已保证），所以迭代必然收敛，失败能沿依赖链传播到任意深度。
    """
    blocked = (Status.FAILED, Status.SKIPPED)
    while True:
        changed = False
        for t in dag.tasks:
            if statuses.get(t.name) != Status.PENDING:
                continue
            if any(statuses.get(d) in blocked for d in t.depends_on):
                statuses[t.name] = Status.SKIPPED
                changed = True
        if not changed:
            return


def is_done(dag, statuses: dict[str, Status]) -> bool:
    """所有任务都不再是 PENDING 或 RUNNING。"""
    return all(s not in (Status.PENDING, Status.RUNNING) for s in statuses.values())
