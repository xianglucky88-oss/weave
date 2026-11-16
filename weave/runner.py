"""执行一次 DAG 运行。

执行循环以**逻辑任务**为单位推进，以**实例**为单位落地：

* 逻辑任务就绪 = 所有依赖逻辑状态 SUCCESS；下游/reduce 因此只有在 map 的
  全部实例成功（或 map 空展开）后才会被调度。
* 一个任务被展开成实例后，逐个实例执行；单个实例失败不会阻止同任务的其他
  实例执行（部分完成会被完整记录），但会让逻辑任务失败，从而阻断下游。
* 每次实例尝试都追加到 ``task_instances``，历史不被覆盖。

:func:`run_dag` 传入 ``run_id`` 时执行"续跑"：同一 run 内已经成功且参数
快照未变的实例会直接跳过；失败、未执行、参数变化的实例会追加新的 attempt。
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from pathlib import Path

from .dag import DAG, Task
from .executor import run_task
from .expand import (
    ExpansionError,
    TaskInstance,
    expand,
    expansion_digest,
    params_digest,
)
from .ready import (
    Status,
    is_done,
    propagate_failure,
    ready_tasks,
    rollup_instance_status,
    summarize_instances,
)
from .retry import delay_for, policy_for
from .store import Store


@dataclass
class _Execution:
    """一次任务执行的中间结果。"""

    status: Status
    instances: list[TaskInstance]
    digest: str
    executed: int = 0


def run_dag(dag: DAG, store: Store, ds: str, jobs_dir: Path,
            timeout: int = 60, run_id: str | None = None,
            resume: bool = False) -> tuple[str, dict[str, str]]:
    """同步执行一次 DAG，返回 ``(run_id, 逻辑任务状态表)``。

    ``run_id`` 非空时在该 run 上续跑；``resume=True`` 且未给 run_id 时，
    取同 DAG、同 ds 的最近一次 run 续跑。续跑不会新建 run 记录。
    """
    if run_id is None and resume:
        existing = store.find_run(dag.name, ds)
        if existing is None:
            raise ValueError(f"no run to resume for dag={dag.name!r} ds={ds!r}")
        run_id = existing["run_id"]

    if run_id is not None:
        run = store.get_run(run_id)
        if run is None:
            raise ValueError(f"run not found: {run_id}")
        if run["dag"] != dag.name or run["ds"] != ds:
            raise ValueError(
                f"run {run_id} belongs to dag={run['dag']!r} ds={run['ds']!r}, "
                f"not dag={dag.name!r} ds={ds!r}"
            )
        # 上次进程中断遗留的 running 尝试：标记失败，等待本次重新执行。
        store.interrupt_running(run_id)
        previous = store.logical(run_id)
        store.reopen_run(run_id)
        store.prepare_resume(run_id, [t.name for t in dag.tasks])
    else:
        run_id = store.create_run(dag.name, ds)
        store.init_logical(run_id, [t.name for t in dag.tasks])
        previous = {}

    statuses: dict[str, Status] = {t.name: Status.PENDING for t in dag.tasks}
    by_name = dag.by_name()
    # 本次调用中输出发生变化的逻辑任务；用于决定下游是否需要重跑。
    changed: set[str] = set()
    persisted: dict[str, Status] = {}

    def sync_skipped() -> None:
        """把失败传播产生的 SKIPPED 落库。"""
        for name, status in statuses.items():
            if status == Status.SKIPPED and persisted.get(name) != Status.SKIPPED:
                store.set_logical(run_id, name, status.value,
                                  instance_count=0, error="upstream not successful")
                persisted[name] = status

    while not is_done(dag, statuses):
        propagate_failure(dag, statuses)
        sync_skipped()
        ready = ready_tasks(dag, statuses)
        if not ready:
            break

        for name in ready:
            task = by_name[name]
            try:
                result = _run_logical(
                    task, dag, store, run_id, ds, jobs_dir, timeout,
                    statuses, previous.get(name), changed,
                )
            except ExpansionError as exc:
                store.set_logical(run_id, name, "failed", error=str(exc)[:500])
                statuses[name] = Status.FAILED
                changed.add(name)
                persisted[name] = Status.FAILED
                continue

            statuses[name] = result.status
            persisted[name] = result.status
            if result.executed or result.digest != _prev_digest(previous.get(name)):
                changed.add(name)

    # 循环因为"没有就绪任务"而结束时，仍为 PENDING 的任务是被阻塞的，
    # 落库为 SKIPPED，保证历史记录、run 状态、下游判定三者一致。
    for name, status in statuses.items():
        if status == Status.PENDING:
            statuses[name] = Status.SKIPPED
            store.set_logical(run_id, name, Status.SKIPPED.value,
                              instance_count=0, error="blocked: dependencies not satisfied")

    overall = "success" if all(s == Status.SUCCESS for s in statuses.values()) else "failed"
    store.finish_run(run_id, overall)
    return run_id, {k: v.value for k, v in statuses.items()}


def _prev_digest(prev: dict | None) -> str | None:
    return prev.get("expand_digest") if prev else None


def _run_logical(task: Task, dag: DAG, store: Store, run_id: str, ds: str,
                 jobs_dir: Path, timeout: int, statuses: dict[str, Status],
                 prev: dict | None, changed: set[str]) -> _Execution:
    """执行一个逻辑任务（展开 + 逐实例执行），返回汇总结果。"""
    instances = expand(task, ds)
    digest = expansion_digest(instances)

    dep_changed = any(d in changed for d in task.depends_on)
    same_expansion = prev is not None and prev.get("expand_digest") == digest

    # 整个任务可以跳过：上次成功、上游没变、展开结果一致。
    if (prev is not None and prev.get("status") == "success"
            and not dep_changed
            and (task.expand is None or same_expansion)):
        store.set_logical(
            run_id, task.name, Status.SUCCESS.value,
            expand_digest=digest, instance_count=len(instances),
        )
        return _Execution(Status.SUCCESS, instances, digest, executed=0)

    # 展开结果中消失的实例保留历史，但不再是当前展开的一部分。
    if task.expand is not None:
        store.deactivate_missing(run_id, task.name, {i.key for i in instances})

    existing = {
        rec["instance_key"]: rec
        for rec in store.latest_instances(run_id, task.name, active_only=True)
    }
    upstream = _upstream_summary(store, run_id, task, statuses, dag)

    executed = 0
    for index, instance in enumerate(instances):
        pdigest = params_digest(instance.params)
        rec = existing.get(instance.key)
        if rec is not None and rec["params_digest"] != pdigest:
            # 同名实例的定义变了：旧尝试留档，按新参数重新执行。
            store.deactivate_instance(run_id, task.name, instance.key)
            rec = None
        if rec is not None and rec["status"] == "success" and not dep_changed:
            # 同 run 续跑：已成功且上游未变的实例不重复执行。
            continue
        _execute_instance(
            task, instance, index, len(instances), store, run_id, ds,
            jobs_dir, timeout, upstream,
        )
        executed += 1

    records = store.latest_instances(run_id, task.name, active_only=True)
    summary = summarize_instances(records)
    logical = rollup_instance_status(
        records, expanded=task.expand is not None, expected=len(instances),
    )
    error = None
    if logical == Status.FAILED:
        failed = [r for r in records if r["status"] == "failed"]
        error = "; ".join(
            f"{r['instance_key'] or r['instance_name']}: {r.get('error') or 'failed'}"
            for r in failed
        )[:500] or "instance failed"
    store.set_logical(
        run_id, task.name, logical.value,
        expand_digest=digest,
        instance_count=len(instances),
        error=error,
    )
    # 即使没有新执行，实例汇总也是显式可见的。
    _ = summary
    return _Execution(logical, instances, digest, executed=executed)


def _execute_instance(task: Task, instance: TaskInstance, index: int,
                      total: int, store: Store, run_id: str, ds: str,
                      jobs_dir: Path, timeout: int,
                      upstream: dict) -> bool:
    """执行单个实例，失败按策略重试；每次尝试都落库。"""
    policy = policy_for(task)
    env = {
        "WEAVE_TASK": task.name,
        "WEAVE_INSTANCE_KEY": instance.key,
        "WEAVE_INSTANCE_NAME": instance.name,
        "WEAVE_INSTANCE_INDEX": str(index),
        "WEAVE_INSTANCE_COUNT": str(total),
        "WEAVE_UPSTREAM_SUMMARY": json.dumps(upstream, ensure_ascii=False, sort_keys=True),
    }
    pdigest = params_digest(instance.params)

    for attempt in range(1, policy.max_attempts + 1):
        number = store.next_attempt(run_id, task.name, instance.key)
        store.begin_instance(
            run_id, task.name, instance.key, instance.name, number,
            instance.params, pdigest, status="running",
        )
        result = run_task(
            jobs_dir / task.script, run_id, ds, instance.params,
            timeout=timeout, extra_env=env,
        )
        if result.ok:
            store.finish_instance(
                run_id, task.name, instance.key, number, "success",
            )
            return True
        store.finish_instance(
            run_id, task.name, instance.key, number, "failed",
            error=result.output[:500],
        )
        if attempt < policy.max_attempts:
            time.sleep(delay_for(policy, attempt))
    return False


def _upstream_summary(store: Store, run_id: str, task: Task,
                      statuses: dict[str, Status], dag: DAG) -> dict:
    """把上游实例汇总传给下游（reduce 可据此判断"完整"与"部分完成"）。"""
    by_name = dag.by_name()
    out: dict[str, dict] = {}
    for dep in task.depends_on:
        records = store.latest_instances(run_id, dep, active_only=True)
        summary = summarize_instances(records)
        out[dep] = {
            "status": statuses.get(dep, Status.PENDING).value,
            "expanded": by_name[dep].expand is not None,
            "instances": summary.as_dict(),
            "succeeded_keys": [
                r["instance_key"] for r in records if r["status"] == "success"
            ],
        }
    return out
