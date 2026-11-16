"""就绪判定与失败传播的测试。"""

from __future__ import annotations

from weave.dag import DAG, Task
from weave.ready import Status, is_done, propagate_failure, ready_tasks


def make_dag():
    return DAG(name="d", tasks=[
        Task(name="a", script="a.py"),
        Task(name="b", script="b.py", depends_on=["a"]),
        Task(name="c", script="c.py", depends_on=["b"]),
    ])


def test_ready_respects_dependencies():
    d = make_dag()
    st = {"a": Status.PENDING, "b": Status.PENDING, "c": Status.PENDING}
    assert ready_tasks(d, st) == ["a"]

    st["a"] = Status.SUCCESS
    assert ready_tasks(d, st) == ["b"]


def test_failure_skips_direct_downstream():
    d = make_dag()
    st = {"a": Status.FAILED, "b": Status.PENDING, "c": Status.PENDING}
    propagate_failure(d, st)
    assert st["b"] == Status.SKIPPED


def test_failure_propagates_transitively():
    """失败会沿依赖链递归传播，隔代下游同样被跳过。"""
    d = make_dag()
    st = {"a": Status.FAILED, "b": Status.PENDING, "c": Status.PENDING}
    propagate_failure(d, st)
    assert st["b"] == Status.SKIPPED
    assert st["c"] == Status.SKIPPED


def test_is_done():
    d = make_dag()
    assert not is_done(d, {"a": Status.SUCCESS, "b": Status.PENDING, "c": Status.PENDING})
    assert is_done(d, {"a": Status.SUCCESS, "b": Status.SKIPPED, "c": Status.SKIPPED})
