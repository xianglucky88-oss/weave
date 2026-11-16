"""就绪判定、实例汇总与失败传播。

这里区分两种状态：

* **逻辑任务状态**（``dict[str, Status]``）：一个任务的所有实例都成功才算
  SUCCESS。下游（包括 reduce）只依赖逻辑状态。
* **实例状态**（task_instances 记录）：用于决定哪些实例还要执行、哪些可以
  跳过，以及对外展示"部分完成"。

空展开（声明了 ``expand`` 但分区列表为空）按成功处理：map 在空集合上
没有工作可做，reduce 应当继续。这个语义由 :func:`rollup_instance_status`
显式表达，避免把"尚未展开"误判成"零实例成功"。
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class Status(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"


@dataclass
class InstanceSummary:
    """某个逻辑任务当前展开下实例的状态汇总。"""

    total: int = 0
    succeeded: int = 0
    failed: int = 0
    running: int = 0
    pending: int = 0

    @property
    def complete(self) -> bool:
        return self.total > 0 and self.succeeded == self.total

    def as_dict(self) -> dict:
        return {
            "total": self.total,
            "succeeded": self.succeeded,
            "failed": self.failed,
            "running": self.running,
            "pending": self.pending,
        }


def summarize_instances(records: list[dict]) -> InstanceSummary:
    """汇总实例记录（每个实例的最新一次尝试，通常只含 active 记录）。"""
    out = InstanceSummary(total=len(records))
    for rec in records:
        status = rec.get("status")
        if status == Status.SUCCESS.value:
            out.succeeded += 1
        elif status == Status.FAILED.value:
            out.failed += 1
        elif status == Status.RUNNING.value:
            out.running += 1
        else:
            out.pending += 1
    return out


def rollup_instance_status(records: list[dict], *, expanded: bool,
                           expected: int | None = None) -> Status:
    """把实例状态汇总成逻辑任务状态。

    ``expanded=False`` 表示任务没有经过展开（单实例语义），由调用方直接
    维护状态；这里只处理已展开的情况。``expected`` 是本次展开的实例数，
    用于识别"历史记录缺失/未落库"的中间态。
    """
    summary = summarize_instances(records)
    if expected is not None and summary.total < expected:
        return Status.RUNNING
    if summary.total == 0:
        return Status.SUCCESS if expanded else Status.PENDING
    if summary.failed:
        return Status.FAILED
    if summary.running or summary.pending:
        return Status.RUNNING
    return Status.SUCCESS if summary.complete else Status.PENDING


def ready_tasks(dag, statuses: dict[str, Status]) -> list[str]:
    """返回当前可以执行的任务名。

    规则：所有依赖都是 SUCCESS 且自身仍是 PENDING。展开成多个实例的任务
    以及后续 reduce 都使用同一规则——reduce 只有在 map 的**全部**实例都
    SUCCESS（逻辑状态 SUCCESS）后才就绪。
    """
    out: list[str] = []
    for t in dag.tasks:
        if statuses.get(t.name) != Status.PENDING:
            continue
        if all(statuses.get(d) == Status.SUCCESS for d in t.depends_on):
            out.append(t.name)
    return out


def reduce_ready(task, statuses: dict[str, Status]) -> bool:
    """reduce/下游任务是否满足完成条件。空展开的 map 也算满足。"""
    return all(statuses.get(d) == Status.SUCCESS for d in task.depends_on)


def propagate_failure(dag, statuses: dict[str, Status]) -> None:
    """上游失败/跳过时，递归把下游标记为跳过。

    失败会沿着依赖链一路传播，而不是只影响直接下游。已经是 SUCCESS 的
    任务不会被改写（历史运行结果保持真实）。
    """
    changed = True
    while changed:
        changed = False
        for t in dag.tasks:
            if statuses.get(t.name) != Status.PENDING:
                continue
            if any(statuses.get(d) in (Status.FAILED, Status.SKIPPED)
                   for d in t.depends_on):
                statuses[t.name] = Status.SKIPPED
                changed = True


def is_done(dag, statuses: dict[str, Status]) -> bool:
    """所有任务都不再是 PENDING 或 RUNNING。"""
    return all(s not in (Status.PENDING, Status.RUNNING) for s in statuses.values())
