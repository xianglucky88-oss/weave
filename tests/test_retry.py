"""重试策略与重试引擎测试：退避、分类、不可重试放大、取消打断。"""

from __future__ import annotations

import threading

from weave.executor import ExecResult, FailureKind
from weave.retry import (
    AttemptRecord,
    RetryPolicy,
    delay_for,
    policy_for,
    run_with_retry,
)


def _fail(kind: FailureKind, exit_code: int | None = 1) -> ExecResult:
    return ExecResult(ok=False, kind=kind, output="", seconds=0.0,
                      exit_code=exit_code)


def _ok() -> ExecResult:
    return ExecResult(ok=True, kind=FailureKind.SUCCESS, output="",
                      seconds=0.0, exit_code=0)


# ---------- 退避 ----------

def test_backoff_is_exponential_and_capped():
    p = RetryPolicy(max_attempts=5, backoff_base=1.0, backoff_factor=2.0,
                    backoff_max=10.0, retry_jitter=0.0)
    assert delay_for(p, 1) == 1.0
    assert delay_for(p, 2) == 2.0
    assert delay_for(p, 3) == 4.0
    assert delay_for(p, 4) == 8.0
    # 第 5 次理论 16s，被 max 截断为 10s
    assert delay_for(p, 5) == 10.0


def test_zero_base_means_no_delay():
    p = RetryPolicy(max_attempts=3)
    assert delay_for(p, 1) == 0.0
    assert delay_for(p, 2) == 0.0


def test_jitter_stays_within_bounds():
    p = RetryPolicy(max_attempts=4, backoff_base=1.0, backoff_max=100.0,
                    retry_jitter=0.5)
    for _ in range(50):
        d = delay_for(p, 1)
        assert 0.5 <= d <= 1.5


# ---------- 失败分类 ----------

def test_spawn_and_cancel_are_never_retried():
    p = RetryPolicy(max_attempts=5)
    assert not p.retryable(_fail(FailureKind.SPAWN))
    assert not p.retryable(_fail(FailureKind.CANCELED))
    assert not p.retryable(_ok())


def test_fatal_exit_codes_stop_immediately():
    p = RetryPolicy(max_attempts=5, fatal_exit_codes=frozenset({2, 127}))
    assert not p.retryable(_fail(FailureKind.EXIT_CODE, exit_code=2))
    assert not p.retryable(_fail(FailureKind.EXIT_CODE, exit_code=127))
    # 普通退出码仍可重试
    assert p.retryable(_fail(FailureKind.EXIT_CODE, exit_code=1))
    # 超时/信号默认可重试
    assert p.retryable(_fail(FailureKind.TIMEOUT, exit_code=None))
    assert p.retryable(_fail(FailureKind.SIGNAL, exit_code=-15))


# ---------- 引擎行为 ----------

def test_engine_retries_then_succeeds():
    p = RetryPolicy(max_attempts=3)  # 无退避，测试快
    calls = []

    def execute(attempt):
        calls.append(attempt)
        return _ok() if attempt == 3 else _fail(FailureKind.EXIT_CODE)

    outcome = run_with_retry(p, execute)
    assert outcome.success
    assert calls == [1, 2, 3]
    assert outcome.attempts_used == 3
    assert [r.attempt for r in outcome.attempts] == [1, 2, 3]
    assert outcome.attempts[0].will_retry
    assert not outcome.attempts[-1].will_retry


def test_engine_caps_attempts_and_reports_last_failure():
    p = RetryPolicy(max_attempts=3)
    calls = []

    def execute(attempt):
        calls.append(attempt)
        return _fail(FailureKind.EXIT_CODE)

    outcome = run_with_retry(p, execute)
    assert not outcome.success
    assert calls == [1, 2, 3]
    assert outcome.last.kind == FailureKind.EXIT_CODE


def test_engine_does_not_amplify_non_retryable_error():
    p = RetryPolicy(max_attempts=10)
    calls = []

    def execute(attempt):
        calls.append(attempt)
        return _fail(FailureKind.SPAWN)

    outcome = run_with_retry(p, execute)
    assert not outcome.success
    assert calls == [1]  # 只尝试一次，没有重试风暴
    assert outcome.last.kind == FailureKind.SPAWN


def test_fatal_exit_code_bypasses_remaining_attempts():
    p = RetryPolicy(max_attempts=5, fatal_exit_codes=frozenset({2}))
    calls = []

    def execute(attempt):
        calls.append(attempt)
        return _fail(FailureKind.EXIT_CODE, exit_code=2)

    outcome = run_with_retry(p, execute)
    assert not outcome.success
    assert calls == [1]


def test_backoff_sleeps_are_driven_by_policy():
    p = RetryPolicy(max_attempts=3, backoff_base=4.0, backoff_factor=2.0,
                    backoff_max=100.0)
    slept: list[float] = []

    def execute(attempt):
        return _ok() if attempt == 3 else _fail(FailureKind.EXIT_CODE)

    outcome = run_with_retry(
        p, execute,
        sleep=lambda s: slept.append(s),  # 不睡，只记录退避时长
    )
    assert outcome.success
    # 两次失败后各退避一次：第 1 次失败 4s，第 2 次失败 8s
    assert slept == [4.0, 8.0]


def test_cancel_during_backoff_interrupts_retry():
    p = RetryPolicy(max_attempts=5, backoff_base=60.0)  # 长退避
    cancel = threading.Event()
    calls = []

    def execute(attempt):
        calls.append(attempt)
        if attempt == 1:
            cancel.set()  # 模拟退避期间服务停止
        return _fail(FailureKind.EXIT_CODE)

    outcome = run_with_retry(p, execute, cancel=cancel)
    assert not outcome.success
    assert outcome.canceled
    assert calls == [1]  # 没有发起第二次尝试


def test_cancel_before_first_attempt_records_canceled():
    p = RetryPolicy(max_attempts=3)
    cancel = threading.Event()
    cancel.set()
    calls = []

    outcome = run_with_retry(p, lambda a: calls.append(a) or _ok(),
                             cancel=cancel)
    assert not outcome.success
    assert outcome.canceled
    assert calls == []
    assert outcome.attempts[0].result.kind == FailureKind.CANCELED


def test_on_attempt_callback_fires_for_every_try():
    p = RetryPolicy(max_attempts=2)
    seen: list[AttemptRecord] = []

    outcome = run_with_retry(
        p,
        lambda a: _fail(FailureKind.EXIT_CODE),
        on_attempt=seen.append,
    )
    assert not outcome.success
    assert len(seen) == 2
    assert [r.attempt for r in seen] == [1, 2]


# ---------- 策略从任务配置传递 ----------

class _FakeTask:
    def __init__(self, **kw):
        self.retries = kw.get("retries", 0)
        self.backoff_base = kw.get("backoff_base", 0.0)
        self.backoff_factor = kw.get("backoff_factor", 2.0)
        self.backoff_max = kw.get("backoff_max", 30.0)
        self.retry_jitter = kw.get("retry_jitter", 0.0)
        self.fatal_exit_codes = kw.get("fatal_exit_codes", [])
        self.timeout = kw.get("timeout", 0)
        self.max_output = kw.get("max_output", 0)


def test_policy_for_carries_task_config_and_defaults():
    t = _FakeTask(retries=2, backoff_base=3.0, fatal_exit_codes=[7],
                  timeout=15, max_output=2048)
    p = policy_for(t, timeout=60, max_output=4096)
    assert p.max_attempts == 3
    assert p.backoff_base == 3.0
    assert p.fatal_exit_codes == frozenset({7})
    # 任务级配置优先于入口默认值
    assert p.timeout == 15
    assert p.max_output == 2048

    t2 = _FakeTask()
    p2 = policy_for(t2, timeout=60, max_output=4096)
    assert p2.max_attempts == 1
    # 任务未配置时沿用入口默认值
    assert p2.timeout == 60
    assert p2.max_output == 4096
