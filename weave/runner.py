"""执行一次 DAG 运行。

职责边界：
- executor 负责“一次子进程尝试”的明确失败语义；
- retry.run_with_retry 负责“一个任务实例”的退避重试与尝试记录；
- runner 负责 DAG 级编排：就绪判定、失败递归跳过、取消收敛、终态落库。
"""

from __future__ import annotations

import threading
from pathlib import Path

from .dag import DAG
from .executor import FailureKind, run_task
from .expand import expand
from .ready import Status, is_done, propagate_failure, ready_tasks
from .retry import AttemptRecord, policy_for, run_with_retry
from .store import Store

# 落库时错误字段里附带的输出片段长度（完整输出已在 executor 被 max_output 截断）。
ERROR_OUTPUT_SNIPPET = 500


def _error_text(record: AttemptRecord) -> str | None:
    result = record.result
    if result.ok:
        return None
    parts = [result.error or result.kind.value]
    if result.output:
        parts.append("--- output ---")
        parts.append(result.output[:ERROR_OUTPUT_SNIPPET])
    return "\n".join(parts)


def run_dag(dag: DAG, store: Store, ds: str, jobs_dir: Path,
            timeout: int = 60, max_output: int | None = None,
            shutdown: threading.Event | None = None) -> tuple[str, dict[str, str]]:
    """同步执行一次 DAG，返回 (run_id, 任务状态表)。

    ``shutdown`` 是服务停止事件：退避期间置位会立即结束等待，
    正在运行的子进程会被执行层整组终止，DAG 收敛到 canceled 终态，
    不会留下 PENDING 任务或孤儿进程。
    """
    run_id = store.create_run(dag.name, ds)
    statuses: dict[str, Status] = {t.name: Status.PENDING for t in dag.tasks}
    by_name = dag.by_name()
    skipped_persisted: set[str] = set()
    run_canceled = False

    while not is_done(dag, statuses):
        if shutdown is not None and shutdown.is_set():
            run_canceled = True
            break

        propagate_failure(dag, statuses)
        # 把新产生的跳过落库（attempt=0 占位，表示从未尝试）。
        for name, st in statuses.items():
            if st == Status.SKIPPED and name not in skipped_persisted:
                store.skip_task(run_id, name, "upstream task failed or run canceled")
                skipped_persisted.add(name)

        ready = ready_tasks(dag, statuses)
        if not ready:
            break

        for name in ready:
            if shutdown is not None and shutdown.is_set():
                run_canceled = True
                break

            task = by_name[name]
            # 扩展点：一个任务可以展开成多个实例，当前只取第一个。
            instance = expand(task, ds)[0]
            # 任务配置（retries/退避/致命码/超时/输出上限）在此汇成策略，
            # 之后由重试引擎在每次尝试时透传给执行层。
            policy = policy_for(task, timeout=timeout, max_output=max_output)

            def execute(attempt: int, _task=task, _instance=instance) -> object:
                store.set_task(run_id, _instance.name, attempt, "running")
                return run_task(
                    jobs_dir / _task.script, run_id, ds, _instance.params,
                    timeout=policy.timeout,
                    max_output=policy.max_output,
                    cancel=shutdown,
                )

            def on_attempt(record: AttemptRecord, _name=instance.name) -> None:
                r = record.result
                store.set_task(
                    run_id, _name, record.attempt, record.status,
                    error=_error_text(record),
                    kind=r.kind.value,
                    exit_code=r.exit_code,
                    signal=r.signal,
                    timed_out=r.timed_out,
                    killed=r.killed,
                    output_truncated=r.output_truncated,
                    seconds=r.seconds,
                )

            outcome = run_with_retry(policy, execute, cancel=shutdown,
                                     on_attempt=on_attempt)
            if outcome.canceled:
                run_canceled = True
            statuses[name] = Status.SUCCESS if outcome.success else Status.FAILED

        if run_canceled:
            break

    # 取消或异常退出循环时，把仍 PENDING 的任务统一跳过并落库，
    # 保证 run 一定收敛到终态、状态表没有悬挂任务。
    if run_canceled:
        for name in list(statuses):
            if statuses[name] == Status.PENDING:
                statuses[name] = Status.SKIPPED
        propagate_failure(dag, statuses)
    for name, st in statuses.items():
        if st == Status.SKIPPED and name not in skipped_persisted:
            store.skip_task(run_id, name, "upstream task failed or run canceled")
            skipped_persisted.add(name)

    if run_canceled:
        overall = "canceled"
    else:
        overall = "success" if all(s == Status.SUCCESS for s in statuses.values()) else "failed"
    store.finish_run(run_id, overall)
    return run_id, {k: v.value for k, v in statuses.items()}
