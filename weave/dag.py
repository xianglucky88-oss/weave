"""DAG 定义：解析 YAML、校验结构、检测依赖环。"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import yaml

from .expand import ExpandSpec, parse_spec, validate_spec


@dataclass
class Task:
    name: str
    script: str
    depends_on: list[str] = field(default_factory=list)
    retries: int = 0
    sla_seconds: int = 0
    params: dict[str, str] = field(default_factory=dict)
    # 动态展开声明；None 表示一个任务只有一个实例。
    expand: ExpandSpec | None = None


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
        raw_expand = item.get("expand")
        tasks.append(
            Task(
                name=item["name"],
                script=item["script"],
                depends_on=list(item.get("depends_on", [])),
                retries=int(item.get("retries", 0)),
                sla_seconds=int(item.get("sla_seconds", 0)),
                params=dict(item.get("params", {})),
                expand=parse_spec(raw_expand) if raw_expand is not None else None,
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
        if t.expand is not None:
            problems.extend(f"task {t.name!r}: {p}" for p in validate_spec(t.expand))
            if t.expand.name and "{key}" not in t.expand.name:
                problems.append(
                    f"task {t.name!r}: expand.name must contain '{{key}}' "
                    "so instances get distinct names"
                )
        if t.params:
            # 避免非标量参数落进环境变量。
            for k, v in t.params.items():
                if isinstance(v, (dict, list)):
                    problems.append(f"task {t.name!r}: param {k!r} must be a scalar")

    if not problems:
        order = dag.order()
        if len(order) != len(dag.tasks):
            problems.append("dependency cycle detected")
    return problems
