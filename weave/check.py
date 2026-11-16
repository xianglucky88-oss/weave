"""发布前检查契约：不执行任务，对 DAG 定义做阶段化校验。

阶段流水线（阶段内聚合全部问题；阶段间按依赖关系短路）：

    parse      读取文件与 YAML 语法
    structure  字段存在性、类型与取值（DAG 级 + 任务级）
    deps       重名、未知/重复/自依赖、依赖环（给出具体环路径）
    scripts    脚本相对 jobs 根目录可达（只依赖 structure，与 deps 独立）
    topology   执行顺序与孤立任务（只依赖 deps）

取舍（README「发布前检查契约」一节有面向用户的说明）：

- 阶段内聚合全部错误，一次看全；阶段间短路——结构/依赖未修复时，
  后续阶段的结果不可靠，报出来只会误导修复顺序。
- 脚本路径一律相对 jobs 根目录解析；绝对路径与 ``..`` 逃逸都是 error。
- 重复依赖是 error（几乎肯定是笔误）；空 DAG 是 error；孤立任务是 warning。
- 拓扑序与 runner 的就绪循环同构，保证「检查给出的顺序 == 实际执行顺序」。
"""

from __future__ import annotations

import os
from collections import Counter, deque
from dataclasses import dataclass, field
from pathlib import Path

import yaml

from .dag import DAG, Task

ERROR = "error"
WARNING = "warning"

PHASES = ("parse", "structure", "deps", "scripts", "topology")

# schedule.is_due 支持的调度字符串。
SCHEDULE_VALUES = ("minute", "hour", "day")

_DAG_FIELDS = ("name", "schedule", "tasks")
_TASK_FIELDS = ("name", "script", "depends_on", "retries", "sla_seconds", "params")

_MISSING = object()


@dataclass(frozen=True)
class Diagnostic:
    """一条诊断。code 稳定不变，自动化可据此分类；message 面向人。"""

    code: str
    severity: str  # ERROR | WARNING
    phase: str     # parse | structure | deps | scripts | topology | repo
    message: str
    task: str | None = None


@dataclass
class PhaseResult:
    name: str
    status: str = "ok"  # ok | failed | skipped
    diagnostics: list[Diagnostic] = field(default_factory=list)
    detail: str = ""    # skipped 时说明原因

    @property
    def errors(self) -> list[Diagnostic]:
        return [d for d in self.diagnostics if d.severity == ERROR]


@dataclass
class CheckReport:
    """单个定义文件的检查报告。phases 永远按 PHASES 顺序包含全部阶段。"""

    source: str
    dag_name: str | None = None
    phases: list[PhaseResult] = field(default_factory=list)
    dag: DAG | None = None
    order: list[str] = field(default_factory=list)
    waves: list[list[str]] = field(default_factory=list)

    @property
    def diagnostics(self) -> list[Diagnostic]:
        return [d for p in self.phases for d in p.diagnostics]

    @property
    def errors(self) -> list[Diagnostic]:
        return [d for d in self.diagnostics if d.severity == ERROR]

    @property
    def warnings(self) -> list[Diagnostic]:
        return [d for d in self.diagnostics if d.severity == WARNING]

    @property
    def ok(self) -> bool:
        return not self.errors


@dataclass
class RepoReport:
    """一次发布前检查的完整结果（一个或多个文件 + 跨文件问题）。"""

    reports: list[CheckReport]
    diagnostics: list[Diagnostic] = field(default_factory=list)  # phase == "repo"

    @property
    def errors(self) -> list[Diagnostic]:
        out = [d for d in self.diagnostics if d.severity == ERROR]
        for r in self.reports:
            out.extend(r.errors)
        return out

    @property
    def warnings(self) -> list[Diagnostic]:
        out = [d for d in self.diagnostics if d.severity == WARNING]
        for r in self.reports:
            out.extend(r.warnings)
        return out

    @property
    def ok(self) -> bool:
        return not self.errors


# ---------------------------------------------------------------- 解析与构建

def _type_name(v) -> str:
    return type(v).__name__


def _is_int(v) -> bool:
    # bool 是 int 的子类，但 `retries: true` 是类型错误。
    return isinstance(v, int) and not isinstance(v, bool)


def _is_scalar(v) -> bool:
    return isinstance(v, (str, int, float, bool))


def _parse_file(path: Path) -> tuple[dict | None, list[Diagnostic]]:
    """parse 阶段：读文件、YAML 语法、根节点类型。失败时 raw 为 None。"""
    try:
        text = path.read_text(encoding="utf-8")
    except OSError as exc:
        return None, [Diagnostic("E_READ", ERROR, "parse",
                                 f"无法读取文件: {exc.strerror or exc}")]
    try:
        raw = yaml.safe_load(text)
    except yaml.YAMLError as exc:
        mark = getattr(exc, "problem_mark", None)
        problem = getattr(exc, "problem", None) or str(exc)
        loc = f"第 {mark.line + 1} 行第 {mark.column + 1} 列: " if mark else ""
        return None, [Diagnostic("E_YAML", ERROR, "parse",
                                 f"YAML 语法错误: {loc}{problem}")]
    if not isinstance(raw, dict):
        return None, [Diagnostic("E_ROOT_TYPE", ERROR, "parse",
                                 "DAG 定义必须是包含 name/tasks 的 mapping，"
                                 f"实际是 {_type_name(raw)}")]
    return raw, []


def _build_task(i: int, item, diags: list[Diagnostic]) -> Task | None:
    """structure 阶段（任务级）：字段全部合法才返回 Task，否则记 error 并跳过。"""
    where = f"tasks[{i}]"
    task_name: str | None = None
    ok = True

    def err(code: str, msg: str) -> None:
        nonlocal ok
        ok = False
        diags.append(Diagnostic(code, ERROR, "structure", f"{where}: {msg}", task_name))

    def warn(code: str, msg: str) -> None:
        diags.append(Diagnostic(code, WARNING, "structure", f"{where}: {msg}", task_name))

    if not isinstance(item, dict):
        err("E_TASK_TYPE", f"任务定义必须是 mapping，实际是 {_type_name(item)}")
        return None

    for key in sorted(set(item) - set(_TASK_FIELDS)):
        warn("W_UNKNOWN_FIELD",
             f"未知字段 {key!r}（已知字段: {', '.join(_TASK_FIELDS)}）——拼写错误会被静默忽略")

    name = item.get("name", _MISSING)
    if name is _MISSING:
        err("E_TASK_NAME", "缺少必填字段 'name'")
    elif not isinstance(name, str):
        err("E_TASK_NAME", f"'name' 必须是字符串，实际是 {_type_name(name)}")
    elif not name.strip():
        err("E_TASK_NAME", "'name' 不能是空白字符串")
    else:
        task_name = name
        where = f"任务 {name!r}"

    script = item.get("script", _MISSING)
    if script is _MISSING:
        err("E_TASK_SCRIPT", "缺少必填字段 'script'")
    elif not isinstance(script, str):
        err("E_TASK_SCRIPT", f"'script' 必须是字符串，实际是 {_type_name(script)}")
    elif not script.strip():
        err("E_TASK_SCRIPT", "'script' 不能是空白字符串")

    deps = item.get("depends_on", [])
    if not isinstance(deps, list):
        err("E_DEPS_TYPE", f"'depends_on' 必须是字符串列表，实际是 {_type_name(deps)}")
        deps = []
    else:
        bad = next((d for d in deps if not isinstance(d, str)), None)
        if bad is not None:
            err("E_DEPS_TYPE",
                f"'depends_on' 必须是字符串列表，元素 {bad!r} 是 {_type_name(bad)}")
            deps = []

    retries = item.get("retries", 0)
    if not _is_int(retries):
        err("E_RETRIES_TYPE", f"'retries' 必须是非负整数，实际是 {_type_name(retries)}")
        retries = 0
    elif retries < 0:
        err("E_RETRIES_RANGE", f"'retries' 不能为负数（{retries}）")
        retries = 0

    sla = item.get("sla_seconds", 0)
    if not _is_int(sla):
        err("E_SLA_TYPE", f"'sla_seconds' 必须是非负整数，实际是 {_type_name(sla)}")
        sla = 0
    elif sla < 0:
        err("E_SLA_RANGE", f"'sla_seconds' 不能为负数（{sla}）")
        sla = 0

    params = item.get("params", {})
    if not isinstance(params, dict):
        err("E_PARAMS_TYPE", f"'params' 必须是 mapping，实际是 {_type_name(params)}")
        params = {}
    else:
        params_ok = True
        for k, v in params.items():
            if not isinstance(k, str):
                err("E_PARAMS_KEY", f"'params' 的键必须是字符串，{k!r} 是 {_type_name(k)}")
                params_ok = False
            elif not _is_scalar(v):
                err("E_PARAMS_VALUE",
                    f"'params.{k}' 必须是标量（字符串/数字/布尔），实际是 {_type_name(v)}")
                params_ok = False
        if not params_ok:
            params = {}

    if not ok:
        return None
    return Task(name=name, script=script, depends_on=list(deps),
                retries=retries, sla_seconds=sla, params=dict(params))


def _build_dag(raw: dict) -> tuple[DAG | None, str | None, list[Diagnostic]]:
    """structure 阶段（DAG 级 + 任务级）。有任何 error 时 dag 为 None。"""
    diags: list[Diagnostic] = []

    def err(code: str, msg: str) -> None:
        diags.append(Diagnostic(code, ERROR, "structure", msg))

    def warn(code: str, msg: str) -> None:
        diags.append(Diagnostic(code, WARNING, "structure", msg))

    for key in sorted(set(raw) - set(_DAG_FIELDS)):
        warn("W_UNKNOWN_FIELD",
             f"DAG 级未知字段 {key!r}（已知字段: {', '.join(_DAG_FIELDS)}）")

    dag_name: str | None = None
    name = raw.get("name", _MISSING)
    if name is _MISSING:
        err("E_DAG_NAME", "缺少必填字段 'name'")
    elif not isinstance(name, str):
        err("E_DAG_NAME", f"'name' 必须是字符串，实际是 {_type_name(name)}")
    elif not name.strip():
        err("E_DAG_NAME", "'name' 不能是空白字符串")
    else:
        dag_name = name

    schedule = raw.get("schedule", "")
    if not isinstance(schedule, str):
        err("E_SCHEDULE_TYPE", f"'schedule' 必须是字符串，实际是 {_type_name(schedule)}")
        schedule = ""
    elif schedule and schedule not in SCHEDULE_VALUES:
        err("E_SCHEDULE_VALUE",
            f"'schedule' 只支持 {'/'.join(SCHEDULE_VALUES)}，实际是 {schedule!r}")

    tasks_raw = raw.get("tasks", _MISSING)
    if tasks_raw is _MISSING:
        err("E_TASKS_MISSING", "缺少必填字段 'tasks'（任务列表）")
        return None, dag_name, diags
    if not isinstance(tasks_raw, list):
        err("E_TASKS_TYPE", f"'tasks' 必须是任务列表，实际是 {_type_name(tasks_raw)}")
        return None, dag_name, diags
    if not tasks_raw:
        err("E_NO_TASKS", "空 DAG：'tasks' 至少要有一个任务")
        return None, dag_name, diags

    tasks: list[Task] = []
    for i, item in enumerate(tasks_raw):
        task = _build_task(i, item, diags)
        if task is not None:
            tasks.append(task)

    if any(d.severity == ERROR for d in diags):
        return None, dag_name, diags
    return DAG(name=dag_name or "", tasks=tasks, schedule=schedule), dag_name, diags


# ---------------------------------------------------------------- 依赖检查

def _sccs(edges: dict[str, set[str]]) -> list[list[str]]:
    """迭代 Tarjan 求强连通分量。遍历顺序确定，输出确定。"""
    index_of: dict[str, int] = {}
    low: dict[str, int] = {}
    on_stack: set[str] = set()
    stack: list[str] = []
    out: list[list[str]] = []
    counter = 0

    for root in sorted(edges):
        if root in index_of:
            continue
        index_of[root] = low[root] = counter
        counter += 1
        stack.append(root)
        on_stack.add(root)
        work = [(root, iter(sorted(edges[root])))]
        while work:
            node, it = work[-1]
            descended = False
            for nxt in it:
                if nxt not in index_of:
                    index_of[nxt] = low[nxt] = counter
                    counter += 1
                    stack.append(nxt)
                    on_stack.add(nxt)
                    work.append((nxt, iter(sorted(edges.get(nxt, ())))))
                    descended = True
                    break
                if nxt in on_stack:
                    low[node] = min(low[node], index_of[nxt])
            if descended:
                continue
            work.pop()
            if work:
                parent = work[-1][0]
                low[parent] = min(low[parent], low[node])
            if low[node] == index_of[node]:
                scc = []
                while True:
                    w = stack.pop()
                    on_stack.discard(w)
                    scc.append(w)
                    if w == node:
                        break
                out.append(scc)
    return out


def _cycle_path(scc: list[str], edges: dict[str, set[str]]) -> list[str]:
    """在 SCC 内找一条具体环路径（a -> b -> a 表示 a 依赖 b、b 依赖 a）。

    BFS + 字典序遍历，输出确定。
    """
    start = min(scc)
    in_scc = set(scc)
    prev: dict[str, str] = {}
    seen = {start}
    queue = deque([start])
    found_via: str | None = None
    while queue and found_via is None:
        node = queue.popleft()
        for nxt in sorted(edges.get(node, ())):
            if nxt not in in_scc:
                continue
            if nxt == start:
                found_via = node
                break
            if nxt not in seen:
                seen.add(nxt)
                prev[nxt] = node
                queue.append(nxt)
    if found_via is None:  # 理论不可达：size>1 的 SCC 必有环
        return [start, start]
    path = [found_via]
    while path[-1] != start:
        path.append(prev[path[-1]])
    path.reverse()
    path.append(start)
    return path


def _check_deps(tasks: list[Task]) -> list[Diagnostic]:
    """deps 阶段：重名、重复/自/未知依赖、依赖环。"""
    diags: list[Diagnostic] = []

    def err(code: str, msg: str, task: str | None = None) -> None:
        diags.append(Diagnostic(code, ERROR, "deps", msg, task))

    counts = Counter(t.name for t in tasks)
    for name in sorted(counts):
        if counts[name] > 1:
            err("E_DUP_NAME", f"任务名 {name!r} 重复定义（{counts[name]} 次）", name)

    known = {t.name for t in tasks}
    for t in tasks:
        dep_counts = Counter(t.depends_on)
        for dep in sorted(dep_counts):
            if dep_counts[dep] > 1:
                err("E_DUP_DEP",
                    f"任务 {t.name!r} 重复声明了依赖 {dep!r}（{dep_counts[dep]} 次）", t.name)
        for dep in sorted(set(t.depends_on)):
            if dep == t.name:
                err("E_SELF_DEP", f"任务 {t.name!r} 依赖了它自己", t.name)
            elif dep not in known:
                err("E_UNKNOWN_DEP", f"任务 {t.name!r} 依赖了未定义的任务 {dep!r}", t.name)

    # 依赖环。图方向为「任务 -> 它的依赖」；未知依赖与自环已单独报告，不进图。
    edges: dict[str, set[str]] = {t.name: set() for t in tasks}
    for t in tasks:
        for dep in t.depends_on:
            if dep in known and dep != t.name:
                edges[t.name].add(dep)
    for scc in _sccs(edges):
        if len(scc) < 2:
            continue
        cycle = _cycle_path(scc, edges)
        err("E_CYCLE", "依赖环（箭头表示「依赖于」）: " + " -> ".join(cycle))
    return diags


# ---------------------------------------------------------------- 脚本可达性

def _check_scripts(tasks: list[Task], jobs_dir: Path) -> list[Diagnostic]:
    """scripts 阶段：脚本路径相对 jobs 根目录解析，且必须可达。"""
    root = Path(os.path.normpath(str(jobs_dir)))
    if not root.is_dir():
        return [Diagnostic("E_JOBS_DIR", ERROR, "scripts",
                           f"脚本根目录不存在或不是目录: {root}")]
    diags: list[Diagnostic] = []
    for t in tasks:
        raw = t.script
        if Path(raw).is_absolute():
            diags.append(Diagnostic(
                "E_SCRIPT_ABSOLUTE", ERROR, "scripts",
                f"任务 {t.name!r}: 脚本路径必须是相对 jobs 根目录的相对路径，"
                f"实际是绝对路径 {raw!r}", t.name))
            continue
        resolved = Path(os.path.normpath(str(root / raw)))
        if root != resolved and root not in resolved.parents:
            diags.append(Diagnostic(
                "E_SCRIPT_ESCAPE", ERROR, "scripts",
                f"任务 {t.name!r}: 脚本路径 {raw!r} 逃逸出 jobs 根目录"
                f"（解析为 {resolved}）", t.name))
            continue
        if not resolved.exists():
            diags.append(Diagnostic(
                "E_SCRIPT_MISSING", ERROR, "scripts",
                f"任务 {t.name!r}: 脚本 {raw!r} 不存在（解析为 {resolved}）", t.name))
        elif not resolved.is_file():
            diags.append(Diagnostic(
                "E_SCRIPT_NOT_FILE", ERROR, "scripts",
                f"任务 {t.name!r}: 脚本 {raw!r} 不是普通文件（解析为 {resolved}）", t.name))
        elif resolved.suffix != ".py":
            diags.append(Diagnostic(
                "W_SCRIPT_SUFFIX", WARNING, "scripts",
                f"任务 {t.name!r}: 脚本 {raw!r} 不是 .py 文件，"
                "执行器会用 Python 运行它", t.name))
    return diags


# ---------------------------------------------------------------- 拓扑摘要

def _topology(dag: DAG) -> tuple[list[str], list[list[str]], list[Diagnostic]]:
    """topology 阶段：执行顺序（与 runner 同构）与孤立任务。"""
    waves = dag.waves()
    order = [n for wave in waves for n in wave]
    diags: list[Diagnostic] = []
    if len(dag.tasks) > 1:  # 单任务 DAG 整体孤立是正常模式，不报
        has_downstream = {dep for t in dag.tasks for dep in t.depends_on}
        for t in dag.tasks:
            if not t.depends_on and t.name not in has_downstream:
                diags.append(Diagnostic(
                    "W_ISOLATED", WARNING, "topology",
                    f"任务 {t.name!r} 是孤立任务（无依赖、无下游），"
                    "可能是漏配了 depends_on", t.name))
    return order, waves, diags


# ---------------------------------------------------------------- 编排

def _has_error(diags: list[Diagnostic]) -> bool:
    return any(d.severity == ERROR for d in diags)


def _finish(report: CheckReport, raw: dict, jobs_dir: Path | None) -> CheckReport:
    """structure 及以后的阶段编排（parse 已由调用方处理）。"""
    dag, dag_name, s_diags = _build_dag(raw)
    report.dag_name = dag_name
    report.phases.append(PhaseResult(
        "structure", "failed" if _has_error(s_diags) else "ok", s_diags))
    if dag is None:
        for ph in ("deps", "scripts", "topology"):
            report.phases.append(PhaseResult(ph, "skipped", detail="structure 阶段未通过"))
        return report
    report.dag = dag

    d_diags = _check_deps(dag.tasks)
    deps_ok = not _has_error(d_diags)
    report.phases.append(PhaseResult("deps", "ok" if deps_ok else "failed", d_diags))

    # scripts 只依赖 structure，与 deps 独立：依赖坏了也要报脚本问题。
    if jobs_dir is None:
        report.phases.append(PhaseResult("scripts", "skipped", detail="未提供 jobs 目录"))
    else:
        sc_diags = _check_scripts(dag.tasks, jobs_dir)
        report.phases.append(PhaseResult(
            "scripts", "failed" if _has_error(sc_diags) else "ok", sc_diags))

    if not deps_ok:
        report.phases.append(PhaseResult("topology", "skipped", detail="deps 阶段未通过"))
    else:
        order, waves, t_diags = _topology(dag)
        report.order, report.waves = order, waves
        report.phases.append(PhaseResult("topology", "ok", t_diags))
    return report


def check_file(path, *, jobs_dir=None) -> CheckReport:
    """检查一个 DAG 定义文件。jobs_dir 为 None 时跳过脚本可达性检查。"""
    path = Path(path)
    report = CheckReport(source=str(path))
    raw, p_diags = _parse_file(path)
    report.phases.append(PhaseResult(
        "parse", "failed" if _has_error(p_diags) else "ok", p_diags))
    if raw is None:
        for ph in ("structure", "deps", "scripts", "topology"):
            report.phases.append(PhaseResult(ph, "skipped", detail="parse 阶段未通过"))
        return report
    return _finish(report, raw, jobs_dir)


def check_dag(dag: DAG, *, jobs_dir=None, source: str = "<memory>") -> CheckReport:
    """检查内存中的 DAG 对象（字段原样送检，类型错误同样会被报告）。"""
    raw = {
        "name": dag.name,
        "schedule": dag.schedule,
        "tasks": [
            {
                "name": t.name,
                "script": t.script,
                "depends_on": t.depends_on,
                "retries": t.retries,
                "sla_seconds": t.sla_seconds,
                "params": t.params,
            }
            for t in dag.tasks
        ],
    }
    report = CheckReport(source=source)
    report.phases.append(PhaseResult("parse", "skipped", detail="内存对象，无需解析"))
    return _finish(report, raw, jobs_dir)


def check_repo(paths, *, jobs_dir=None) -> RepoReport:
    """检查多个定义文件，并做跨文件检查（DAG 重名）。"""
    reports = [check_file(p, jobs_dir=jobs_dir) for p in paths]
    diags: list[Diagnostic] = []
    by_name: dict[str, list[str]] = {}
    for r in reports:
        if r.dag_name:
            by_name.setdefault(r.dag_name, []).append(r.source)
    for name in sorted(by_name):
        files = sorted(by_name[name])
        if len(files) > 1:
            diags.append(Diagnostic(
                "E_DUP_DAG", ERROR, "repo",
                f"DAG 名 {name!r} 在 {len(files)} 个文件中重复定义: {', '.join(files)}"))
    return RepoReport(reports=reports, diagnostics=diags)


def load_definition(path) -> DAG:
    """parse + structure 的严格入口：不合法即抛 ValueError（第一条诊断）。

    只保证结构合法；依赖级问题请用 check_file / check_dag。
    """
    raw, p_diags = _parse_file(Path(path))
    if p_diags:
        raise ValueError(p_diags[0].message)
    dag, _, s_diags = _build_dag(raw)
    errors = [d for d in s_diags if d.severity == ERROR]
    if dag is None or errors:
        raise ValueError(errors[0].message if errors else "invalid DAG definition")
    return dag


def find_dag_file(name: str, dags_dir) -> Path:
    """按 DAG 名定位定义文件。坏文件跳过（由 check_file 单独报告）。"""
    dags_dir = Path(dags_dir)
    for p in sorted(dags_dir.glob("*.yaml")):
        try:
            raw = yaml.safe_load(p.read_text(encoding="utf-8"))
        except (OSError, yaml.YAMLError):
            continue
        if isinstance(raw, dict) and raw.get("name") == name:
            return p
    raise FileNotFoundError(f"在 {dags_dir} 中找不到名为 {name!r} 的 DAG")


# ---------------------------------------------------------------- 输出

def render_text(repo: RepoReport) -> str:
    """人类可读报告：阶段表给出修复顺序，诊断带定位信息。"""
    lines: list[str] = []
    for i, r in enumerate(repo.reports):
        if i:
            lines.append("")
        lines.extend(_render_one(r))
    if repo.diagnostics:
        lines.append("")
        lines.append("跨文件问题:")
        for d in repo.diagnostics:
            lines.append(f"  {d.code}  {d.message}")
    if len(repo.reports) > 1:
        valid = sum(1 for r in repo.reports if r.ok)
        lines.append("")
        summary = f"汇总: {valid}/{len(repo.reports)} 个定义通过"
        if repo.errors:
            summary += f"，{len(repo.errors)} 个错误"
        if repo.warnings:
            summary += f"，{len(repo.warnings)} 个警告"
        lines.append(summary)
    return "\n".join(lines) + "\n"


def _render_one(r: CheckReport) -> list[str]:
    mark = "✓" if r.ok else "✗"
    verdict = "VALID" if r.ok else f"INVALID ({len(r.errors)} 个错误, {len(r.warnings)} 个警告)"
    lines = [f"{mark} {r.source} — {verdict}"]
    if r.dag_name:
        n_tasks = len(r.dag.tasks) if r.dag else 0
        sched = r.dag.schedule if r.dag else ""
        lines.append(f"  DAG: {r.dag_name} (schedule={sched or '-'}, {n_tasks} 个任务)")

    if not r.ok:
        lines.append("  阶段:")
        for p in r.phases:
            status = p.status
            if p.status == "failed":
                status = f"FAILED ({len(p.errors)} 个错误)"
            elif p.status == "skipped":
                status = f"SKIPPED ({p.detail})"
            lines.append(f"    {p.name:<10} {status}")
        if r.errors:
            lines.append("  错误（按阶段顺序修复）:")
            for d in r.errors:
                lines.append(f"    [{d.phase}] {d.code}  {d.message}")
    else:
        lines.append(f"  拓扑序: {' -> '.join(r.order)}")
        widest = max((len(w) for w in r.waves), default=0)
        lines.append(f"  波次: {len(r.waves)}（最大并行 {widest}）")

    if r.warnings:
        lines.append(f"  警告 ({len(r.warnings)}):")
        for d in r.warnings:
            lines.append(f"    [{d.phase}] {d.code}  {d.message}")
    return lines


def to_jsonable(repo: RepoReport) -> dict:
    """机器可读报告：键名稳定，供发布流水线消费。"""
    return {
        "ok": repo.ok,
        "summary": {
            "files": len(repo.reports),
            "valid": sum(1 for r in repo.reports if r.ok),
            "errors": len(repo.errors),
            "warnings": len(repo.warnings),
        },
        "repo_diagnostics": [_diag_to_json(d) for d in repo.diagnostics],
        "files": [
            {
                "source": r.source,
                "dag": r.dag_name,
                "ok": r.ok,
                "phases": [
                    {"name": p.name, "status": p.status, "detail": p.detail}
                    for p in r.phases
                ],
                "diagnostics": [_diag_to_json(d) for d in r.diagnostics],
                "order": r.order,
                "waves": r.waves,
            }
            for r in repo.reports
        ],
    }


def _diag_to_json(d: Diagnostic) -> dict:
    return {
        "code": d.code,
        "severity": d.severity,
        "phase": d.phase,
        "task": d.task,
        "message": d.message,
    }
