"""重试策略、退避与重试引擎。

重试策略的传递链路（配置 -> 执行）：

    dags/*.yaml 中的 retries / backoff_base / backoff_max / backoff_factor /
    retry_jitter / fatal_exit_codes / timeout / max_output
        -> dag.Task 字段（dag.load 解析，带默认值与下限保护）
        -> policy_for(task) 生成 RetryPolicy（纯函数，方便单测）
        -> run_with_retry(policy, execute, ...) 在每次尝试时把
           policy.timeout / policy.max_output 作为参数传给 executor.run_task
        -> executor 返回带 FailureKind 的 ExecResult，由 RetryPolicy.retryable
           决定下一次尝试还是终止

为什么不可重试错误不会被无限放大：

1. ``max_attempts`` 是硬上限，``run_with_retry`` 最多调用 ``max_attempts``
   次 execute，循环本身没有任何自增分支能绕过它；
2. 启动失败（spawn）、被取消（canceled）默认不可重试——再试也起不来，
   或服务正在停，重试只会制造残留；
3. ``fatal_exit_codes`` 里的退出码（默认空，可配置，例如 2、127）立刻终止，
   这类错误通常是参数/语法/环境问题，重试结果完全相同；
4. 退避时长被 ``backoff_max`` 截断，且总尝试次数有限，即使全部失败，
   耗时与子进程数量都有确定上界，不会出现无限重试风暴；
5. 每次尝试都落库一条记录，最终失败的任务在 DAG 状态里是 FAILED 而不是
   挂起，下游会被递归跳过，run 整体一定收敛到终态。
"""

from __future__ import annotations

import random
import threading
import time
from dataclasses import dataclass, field
from typing import Callable

from .executor import ExecResult, FailureKind

# 退避默认参数；0 退避时行为退化为“立即重试”，与旧版语义兼容。
DEFAULT_BACKOFF_BASE = 0.0
DEFAULT_BACKOFF_FACTOR = 2.0
DEFAULT_BACKOFF_MAX = 30.0
DEFAULT_JITTER = 0.0


@dataclass(frozen=True)
class RetryPolicy:
    """一个任务实例的完整重试/执行策略。"""

    max_attempts: int
    backoff_base: float = DEFAULT_BACKOFF_BASE
    backoff_factor: float = DEFAULT_BACKOFF_FACTOR
    backoff_max: float = DEFAULT_BACKOFF_MAX
    retry_jitter: float = DEFAULT_JITTER
    # 这些退出码视为永久失败，不再重试（退出码为正的场景）。
    fatal_exit_codes: frozenset[int] = field(default_factory=frozenset)
    # 透传给执行层的资源约束。
    timeout: int = 60
    max_output: int = 64 * 1024

    def backoff_for(self, attempt: int) -> float:
        """第 ``attempt`` 次尝试（1-based）失败后的退避秒数。

        指数退避：base * factor ** (attempt - 1)，再用 backoff_max 截断，
        最后按 retry_jitter（0~1 的比例）做对称抖动，避免同步重试惊群。
        """
        if self.backoff_base <= 0:
            return 0.0
        raw = self.backoff_base * (self.backoff_factor ** max(0, attempt - 1))
        delay = min(raw, self.backoff_max)
        if self.retry_jitter > 0:
            delay *= 1.0 + self.retry_jitter * (random.random() * 2.0 - 1.0)
        return max(0.0, min(delay, self.backoff_max))

    def retryable(self, result: ExecResult) -> bool:
        """根据失败结局分类判断这次失败是否值得再试一次。"""
        if result.kind == FailureKind.SUCCESS:
            return False
        # 进程没启动 / 服务正在停：重试无意义或不应再启动新进程。
        if result.kind in (FailureKind.SPAWN, FailureKind.CANCELED):
            return False
        if result.kind == FailureKind.EXIT_CODE and result.exit_code in self.fatal_exit_codes:
            return False
        # timeout / signal / 普通非零退出码：默认可重试，由 max_attempts 兜底。
        return True


def policy_for(task, timeout: int | None = None,
               max_output: int | None = None) -> RetryPolicy:
    """根据任务定义生成重试策略。

    CLI/API 级别的 timeout/max_output 作为默认值传入，任务自身的配置优先。
    """
    max_attempts = 1 + max(0, int(getattr(task, "retries", 0)))
    fatal = frozenset(int(c) for c in getattr(task, "fatal_exit_codes", ()) or ())
    task_timeout = getattr(task, "timeout", 0) or 0
    task_max_output = getattr(task, "max_output", 0) or 0
    return RetryPolicy(
        max_attempts=max_attempts,
        backoff_base=max(0.0, float(getattr(task, "backoff_base", DEFAULT_BACKOFF_BASE))),
        backoff_factor=max(1.0, float(getattr(task, "backoff_factor", DEFAULT_BACKOFF_FACTOR))),
        backoff_max=max(0.0, float(getattr(task, "backoff_max", DEFAULT_BACKOFF_MAX))),
        retry_jitter=min(1.0, max(0.0, float(getattr(task, "retry_jitter", DEFAULT_JITTER)))),
        fatal_exit_codes=fatal,
        timeout=int(task_timeout or (timeout if timeout is not None else 60)),
        max_output=int(task_max_output or (max_output if max_output is not None else 64 * 1024)),
    )


def delay_for(policy: RetryPolicy, attempt: int) -> float:
    """兼容旧调用点：第 attempt 次失败后应当等待多久（含抖动）。"""
    return policy.backoff_for(attempt)


@dataclass
class AttemptRecord:
    """一次尝试的结构化记录，供落库与诊断使用。"""

    attempt: int
    result: ExecResult
    will_retry: bool
    backoff_seconds: float

    @property
    def status(self) -> str:
        return "success" if self.result.ok else "failed"


@dataclass
class TaskOutcome:
    """一个任务实例跑完所有尝试（或提前终止）后的汇总。"""

    success: bool
    attempts: list[AttemptRecord]
    last: ExecResult
    canceled: bool = False

    @property
    def attempts_used(self) -> int:
        return len(self.attempts)


def _interruptible_sleep(seconds: float, cancel: threading.Event | None,
                         sleep: Callable[[float], None] = time.sleep) -> bool:
    """可被服务停止事件打断的睡眠。返回 True 表示被取消打断。

    ``sleep`` 可注入（单测用假时钟）；生产路径是 ``time.sleep`` 的分片等待，
    每片最多 0.1s，因此停止事件的响应延迟有上界，不会在长退避里睡死。
    """
    if seconds <= 0:
        return cancel is not None and cancel.is_set()
    if cancel is None:
        sleep(seconds)
        return False

    remaining = seconds
    while remaining > 0:
        if cancel.is_set():
            return True
        slice_ = min(0.1, remaining)
        sleep(slice_)
        remaining -= slice_
    return False


def run_with_retry(
    policy: RetryPolicy,
    execute: Callable[[int], ExecResult],
    *,
    cancel: threading.Event | None = None,
    sleep: Callable[[float], None] = time.sleep,
    on_attempt: Callable[[AttemptRecord], None] | None = None,
) -> TaskOutcome:
    """按策略反复执行 ``execute(attempt_no)`` 直到成功或确定终止。

    - 退避通过 ``sleep`` 注入，单测可替换成假时钟；生产路径在退避前
      先检查 ``cancel``，退避期间事件置位会立即醒来，不再启动新尝试；
    - ``on_attempt`` 在每次尝试结束后回调，runner 用它把尝试记录落库；
    - 不会把异常抛给调用方：execute 的协议是返回 ExecResult，
      启动失败等都已在 executor 内归一化为 FailureKind。
    """
    records: list[AttemptRecord] = []
    last: ExecResult | None = None

    for attempt in range(1, policy.max_attempts + 1):
        if cancel is not None and cancel.is_set():
            # 还没开始就被停止：构造一条 canceled 记录，保证状态可传播。
            last = ExecResult(ok=False, kind=FailureKind.CANCELED, output="",
                              seconds=0.0, error="canceled before attempt started")
            rec = AttemptRecord(attempt, last, will_retry=False, backoff_seconds=0.0)
            records.append(rec)
            if on_attempt:
                on_attempt(rec)
            return TaskOutcome(False, records, last, canceled=True)

        result = execute(attempt)
        last = result
        is_last = attempt == policy.max_attempts
        should_retry = (not is_last) and policy.retryable(result)
        if cancel is not None and cancel.is_set():
            # 尝试结束时服务已停止：不再重试，按取消收敛。
            should_retry = False
        backoff = policy.backoff_for(attempt) if should_retry else 0.0

        rec = AttemptRecord(attempt, result, will_retry=should_retry,
                            backoff_seconds=backoff)
        records.append(rec)
        if on_attempt:
            on_attempt(rec)

        if result.ok:
            return TaskOutcome(True, records, result)
        if not should_retry:
            return TaskOutcome(False, records, result,
                               canceled=cancel is not None and cancel.is_set())

        interrupted = _interruptible_sleep(backoff, cancel, sleep)
        if interrupted:
            # 退避期间服务停止：不再发起下一次尝试，把取消如实带回。
            return TaskOutcome(False, records, result, canceled=True)

    # 循环理论上总会在内部 return；走到这里说明策略配置异常，按失败兜底。
    return TaskOutcome(False, records, last, canceled=False)
