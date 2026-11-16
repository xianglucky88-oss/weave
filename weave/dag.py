"""DAG 定义：解析 YAML、校验结构、检测依赖环。"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import yaml


@dataclass
class Task:
    name: str
    script: str
    depends_on: list[str] = field(default_factory=list)
    retries: int = 0
    sla_seconds: int = 0
    params: dict[str, str] = field(default_factory=dict)
    # --- 重试/执行语义（均可在 YAML 中按任务覆盖）---
    # 指数退避基数（秒，0 表示立即重试）、乘数、上限、抖动比例(0~1)。
    backoff_base: float = 0.0
    backoff_factor: float = 2.0
    backoff_max: float = 30.0
    retry_jitter: float = 0.0
    # 这些退出码视为永久失败，不再重试。
    fatal_exit_codes: list[int] = field(default_factory=list)
    # 任务级超时（秒）与输出上限（字节）；0 表示沿用运行入口的默认值。
    timeout: int = 0
    max_output: int = 0


@dataclass
class DAG:
    name: str
    tasks: list[Task]
    # 简单 cron：目前只支持 "每分钟/每小时/每天" 三种字符串。
    schedule: str = ""

    def by_name(self) -> dict[str, Task]:
        return {t.name: t for t in self.tasks}

    def order(self) -> list[str]:
        """拓扑序。调用前必须已通过 validate。"""
        indeg = {t.name: len(t.depends_on) for t in self.tasks}
        children: dict[str, list[str]] = {t.name: [] for t in self.tasks}
        for t in self.tasks:
            for dep in t.depends_on:
                children[dep].append(t.name)

        queue = [n for n, d in indeg.items() if d == 0]
        out: list[str] = []
        while queue:
            n = queue.pop(0)
            out.append(n)
            for c in children[n]:
                indeg[c] -= 1
                if indeg[c] == 0:
                    queue.append(c)
        return out


def load(path: Path) -> DAG:
    """从 YAML 文件加载 DAG。"""
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict) or "tasks" not in raw:
        raise ValueError(f"{path}: DAG must contain a 'tasks' list")

    tasks: list[Task] = []
    for item in raw["tasks"]:
        tasks.append(
            Task(
                name=item["name"],
                script=item["script"],
                depends_on=list(item.get("depends_on", [])),
                retries=int(item.get("retries", 0)),
                sla_seconds=int(item.get("sla_seconds", 0)),
                params=dict(item.get("params", {})),
                backoff_base=float(item.get("backoff_base", 0.0)),
                backoff_factor=float(item.get("backoff_factor", 2.0)),
                backoff_max=float(item.get("backoff_max", 30.0)),
                retry_jitter=float(item.get("retry_jitter", 0.0)),
                fatal_exit_codes=[int(c) for c in item.get("fatal_exit_codes", [])],
                timeout=int(item.get("timeout", 0)),
                max_output=int(item.get("max_output", 0)),
            )
        )
    return DAG(name=raw["name"], tasks=tasks, schedule=raw.get("schedule", ""))


def validate(dag: DAG) -> list[str]:
    """返回问题列表；空列表表示通过。"""
    problems: list[str] = []
    names = [t.name for t in dag.tasks]
    if len(names) != len(set(names)):
        problems.append("duplicate task names")

    known = set(names)
    for t in dag.tasks:
        for dep in t.depends_on:
            if dep not in known:
                problems.append(f"task {t.name!r} depends on unknown {dep!r}")

    if not problems:
        order = dag.order()
        if len(order) != len(dag.tasks):
            problems.append("dependency cycle detected")
    return problems
