"""动态任务展开（map/reduce 扇出）。"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class TaskInstance:
    """一次具体的任务执行。name 用于落库，params 传给脚本。"""

    name: str
    params: dict[str, str]


def expand(task, ds: str) -> list[TaskInstance]:
    """把任务展开成若干实例。

    当前实现不展开：一个任务只产生一个实例。
    """
    return [TaskInstance(name=task.name, params=dict(task.params))]
