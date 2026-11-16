"""SLA 计算与告警摘要的单元测试。

所有用例使用固定时间戳的手工记录，不依赖真实执行与墙钟，
同一份输入重复计算必须得到完全一致的结论。
"""

from __future__ import annotations

from weave.dag import DAG, Task
from weave.sla import attempt_seconds, summarize, task_timings, violations


def _attempt(task, attempt, started, finished, status="success"):
    return {
        "run_id": "r1", "task": task, "attempt": attempt, "status": status,
        "started_at": started, "finished_at": finished, "error": None,
    }


def _dag(**sla):
    tasks = []
    for name, secs in sla.items():
        tasks.append(Task(name=name, script=f"{name}.py", sla_seconds=secs))
    return DAG(name="d", tasks=tasks)


def test_attempt_seconds_handles_missing_and_clock_skew():
    assert attempt_seconds(_attempt("a", 1, 100.0, 103.0)) == 3.0
    # 缺失 finished（仍在 running / 中断）。
    assert attempt_seconds({"started_at": 100.0, "finished_at": None}) is None
    # 两个时间戳都缺。
    assert attempt_seconds({"started_at": None, "finished_at": None}) is None
    # 时钟倒挂不产生负耗时。
    assert attempt_seconds(_attempt("a", 1, 110.0, 100.0)) is None


def test_retry_total_counts_every_attempt_but_not_backoff():
    # 两次 attempt 各 4s（共 8s），中间 5s 退避不落在任何行里，
    # 因此 SLA 口径的 busy_seconds 必须是 8 而不是 13。
    records = [
        _attempt("a", 1, 0.0, 4.0, status="failed"),
        _attempt("a", 2, 9.0, 13.0, status="success"),
    ]
    dag = _dag(a=7)
    timing = task_timings(records)["a"]
    assert timing["busy_seconds"] == 8.0
    assert timing["wall_seconds"] == 13.0
    assert timing["attempts"] == 2
    assert timing["status"] == "success"

    breaches = violations(dag, records)
    assert [b["task"] for b in breaches] == ["a"]
    assert breaches[0]["sla_seconds"] == 7
    assert breaches[0]["busy_seconds"] == 8.0


def test_single_attempt_within_sla_is_not_violation():
    records = [_attempt("a", 1, 0.0, 4.0)]
    assert violations(_dag(a=10), records) == []
    # 边界：恰好等于 SLA 不算违规。
    assert violations(_dag(a=4), records) == []
    assert [b["task"] for b in violations(_dag(a=3), records)] == ["a"]


def test_zero_sla_means_disabled_not_zero_budget():
    # 零 SLA 不产生任何违规，即使耗时很长。
    records = [_attempt("a", 1, 0.0, 999.0)]
    assert violations(_dag(a=0), records) == []
    summary = summarize(_dag(a=0), records)
    assert summary["sla_violated"] is False
    assert summary["violation_count"] == 0


def test_missing_timestamps_are_incomplete_but_not_false_violation():
    # 第一次 attempt 还在 running（无 finished_at），第二次成功 2s。
    records = [
        _attempt("a", 1, 0.0, None, status="running"),
        _attempt("a", 2, 10.0, 12.0),
    ]
    timing = task_timings(records)["a"]
    assert timing["incomplete"] is True
    assert timing["busy_seconds"] == 2.0
    assert timing["attempt_seconds"] == [None, 2.0]

    # 全部时间戳缺失：无法判定，不能报违规也不能报 0 秒达标。
    blind = [{"task": "a", "attempt": 1, "status": "running",
              "started_at": None, "finished_at": None}]
    assert violations(_dag(a=1), blind) == []
    assert task_timings(blind)["a"]["busy_seconds"] == 0.0
    assert task_timings(blind)["a"]["wall_seconds"] is None


def test_parallel_tasks_wall_time_does_not_double_count():
    # a、b 并行执行，时间窗完全重叠（各 5s）。
    records = [
        _attempt("a", 1, 0.0, 5.0),
        _attempt("b", 1, 0.0, 5.0),
    ]
    summary = summarize(_dag(a=0, b=0), records)
    # 忙碌时间按任务相加 = 10；墙钟跨度 = 5，不重复计重叠段。
    assert summary["total_task_seconds"] == 10.0
    assert summary["wall_seconds"] == 5.0
    assert summary["task_count"] == 2


def test_violation_is_independent_of_task_and_run_status():
    # 成功但超时：仍然是违规。
    ok_but_slow = [_attempt("a", 1, 0.0, 9.0, status="success")]
    breaches = violations(_dag(a=5), ok_but_slow)
    assert len(breaches) == 1
    assert breaches[0]["status"] == "success"

    # 失败但没超时：失败与 SLA 无关，不产生违规。
    failed_but_fast = [_attempt("b", 1, 0.0, 1.0, status="failed")]
    summary = summarize(_dag(b=5), failed_but_fast)
    assert summary["sla_violated"] is False
    assert summary["failed_tasks"] == 1

    # 失败任务只按最终状态计一次，重试的中间 failed 行不重复计数。
    retried = [
        _attempt("b", 1, 0.0, 1.0, status="failed"),
        _attempt("b", 2, 2.0, 3.0, status="failed"),
    ]
    assert summarize(_dag(b=0), retried)["failed_tasks"] == 1


def test_violations_follow_dag_order_and_ignore_unknown_tasks():
    records = [
        _attempt("ghost", 1, 0.0, 99.0),
        _attempt("b", 1, 0.0, 9.0),
        _attempt("a", 1, 0.0, 8.0),
    ]
    dag = DAG(name="d", tasks=[
        Task(name="a", script="a.py", sla_seconds=1),
        Task(name="b", script="b.py", sla_seconds=1),
    ])
    breaches = violations(dag, records)
    # 顺序跟随 DAG；库里已不存在的 ghost 任务不参与判定。
    assert [b["task"] for b in breaches] == ["a", "b"]


def test_deterministic_output_for_same_records():
    records = [
        _attempt("a", 1, 0.0, 6.0, status="failed"),
        _attempt("a", 2, 7.0, 10.0),
        _attempt("b", 1, 0.0, 2.0),
    ]
    dag = DAG(name="d", tasks=[
        Task(name="b", script="b.py", sla_seconds=5),
        Task(name="a", script="a.py", sla_seconds=7),
    ])
    assert summarize(dag, records) == summarize(dag, records)
    assert violations(dag, records) == violations(dag, records)
