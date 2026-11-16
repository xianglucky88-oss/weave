"""DAG 定义：数据结构、拓扑序、兼容门面。

校验契约已迁移到 weave.check：
- load()     只保证「能解析、结构合法」，否则抛 ValueError（稳定消息）
- validate() 返回错误消息列表，是 check.check_dag 的兼容包装
- order()    拓扑序与 runner 的就绪循环同构（见 waves）
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

from .ready import Status, ready_tasks


@dataclass
class Task:
    name: str
    script: str
    depends_on: list[str] = field(default_factory=list)
    retries: int = 0
    sla_seconds: int = 0
    params: dict[str, str] = field(default_factory=dict)


@dataclass
class DAG:
    name: str
    tasks: list[Task]
    # 简单 cron：目前只支持 "每分钟/每小时/每天" 三种字符串。
    schedule: str = ""

    def by_name(self) -> dict[str, Task]:
        return {t.name: t for t in self.tasks}

    def waves(self) -> list[list[str]]:
        """执行波次：与 runner 的就绪循环完全同构。

        每一波 = 所有依赖都已 SUCCESS 的任务（按定义顺序），
        对应 runner 里一轮 while 循环执行的 ready_tasks。
        """
        statuses = {t.name: Status.PENDING for t in self.tasks}
        out: list[list[str]] = []
        done = 0
        while done < len(self.tasks):
            ready = ready_tasks(self, statuses)
            if not ready:
                break
            out.append(ready)
            for n in ready:
                statuses[n] = Status.SUCCESS
            done += len(ready)
        return out

    def order(self) -> list[str]:
        """拓扑序，与 runner 实际采用的执行顺序一致。调用前必须已通过校验。"""
        return [n for wave in self.waves() for n in wave]


def load(path: Path) -> DAG:
    """从 YAML 文件加载 DAG。

    解析或结构不合法时抛 ValueError（稳定的第一条诊断消息）；
    依赖级问题不在此检查，完整契约见 weave.check.check_file。
    """
    from .check import load_definition

    return load_definition(Path(path))


def validate(dag: DAG) -> list[str]:
    """返回错误消息列表；空列表表示通过。

    兼容包装：结构化诊断（错误码/阶段/定位）见 weave.check.check_dag。
    """
    from .check import check_dag

    return [d.message for d in check_dag(dag).errors]
