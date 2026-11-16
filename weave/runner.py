"""执行一次 DAG 运行。"""

from __future__ import annotations

import time
from pathlib import Path

from .dag import DAG
from .executor import run_task
from .expand import expand
from .ready import Status, is_done, propagate_failure, ready_tasks
from .retry import delay_for, policy_for
from .store import Store


def run_dag_with_run_id(
    dag: DAG, store: Store, run_id: str, ds: str, jobs_dir: Path,
    timeout: int = 60,
) -> tuple[str, dict[str, str]]:
    """在已存在的 run_id 上同步执行一次 DAG，返回 (run_id, 任务状态表)。

    run_id 由调用方通过 store.acquire_run 预先获取，这样补跑/手工触发
    可以共用同一套“窗口活动锁”防重复机制。
    """
    statuses: dict[str, Status] = {t.name: Status.PENDING for t in dag.tasks}
    by_name = dag.by_name()

    while not is_done(dag, statuses):
        propagate_failure(dag, statuses)
        ready = ready_tasks(dag, statuses)
        if not ready:
            break

        for name in ready:
            task = by_name[name]
            # 扩展点：一个任务可以展开成多个实例，当前只取第一个。
            instance = expand(task, ds)[0]
            policy = policy_for(task)

            ok = False
            for attempt in range(1, policy.max_attempts + 1):
                store.set_task(run_id, instance.name, attempt, "running")
                result = run_task(jobs_dir / task.script, run_id, ds,
                                  instance.params, timeout=timeout)
                store.set_task(
                    run_id, instance.name, attempt,
                    "success" if result.ok else "failed",
                    error=None if result.ok else result.output[:500],
                )
                if result.ok:
                    ok = True
                    break
                time.sleep(delay_for(policy, attempt))

            statuses[name] = Status.SUCCESS if ok else Status.FAILED

    overall = "success" if all(s == Status.SUCCESS for s in statuses.values()) else "failed"
    store.finish_run(run_id, overall)
    return run_id, {k: v.value for k, v in statuses.items()}


def run_dag(dag: DAG, store: Store, ds: str, jobs_dir: Path,
            timeout: int = 60, *, guard: bool = True) -> tuple[str, dict[str, str]]:
    """同步执行一次 DAG，返回 (run_id, 任务状态表)。

    guard=True（默认）时先抢占 (dag, ds) 活动锁：同一数据日期已有运行在跑
    会抛 RunAlreadyRunning，防止服务重启/手工重复触发产生重复执行。
    guard=False 保留旧的“无条件创建运行”语义。
    """
    if guard:
        run_id = store.acquire_run(dag.name, ds)
    else:
        run_id = store.create_run(dag.name, ds)
    return run_dag_with_run_id(dag, store, run_id, ds, jobs_dir, timeout)
