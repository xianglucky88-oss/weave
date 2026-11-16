"""DAG 解析与校验的测试（兼容门面 + 拓扑序与 runner 一致性）。"""

from __future__ import annotations

import pytest

from weave import dag as dagmod
from weave.check import check_dag
from weave.dag import DAG, Task, load, validate
from weave.ready import Status, ready_tasks


def write(tmp_path, text):
    p = tmp_path / "d.yaml"
    p.write_text(text, encoding="utf-8")
    return p


def codes(dag):
    return [d.code for d in check_dag(dag).diagnostics]


def test_load_and_order(tmp_path):
    p = write(tmp_path, """
name: demo
tasks:
  - name: a
    script: a.py
  - name: b
    script: b.py
    depends_on: [a]
""")
    d = load(p)
    assert d.name == "demo"
    assert validate(d) == []
    assert d.order() == ["a", "b"]


def test_cycle_detected(tmp_path):
    p = write(tmp_path, """
name: cyc
tasks:
  - name: a
    script: a.py
    depends_on: [b]
  - name: b
    script: b.py
    depends_on: [a]
""")
    d = load(p)
    assert "E_CYCLE" in codes(d)
    assert validate(d)  # 兼容包装：非空列表


def test_unknown_dependency(tmp_path):
    p = write(tmp_path, """
name: bad
tasks:
  - name: a
    script: a.py
    depends_on: [missing]
""")
    d = load(p)
    assert "E_UNKNOWN_DEP" in codes(d)


def test_duplicate_names(tmp_path):
    p = write(tmp_path, """
name: dup
tasks:
  - name: a
    script: a.py
  - name: a
    script: b.py
""")
    d = load(p)
    assert "E_DUP_NAME" in codes(d)


def test_load_raises_stable_valueerror_on_bad_structure(tmp_path):
    p = write(tmp_path, """
name: bad
tasks:
  - name: a
    script: a.py
    retries: not-a-number
""")
    with pytest.raises(ValueError, match="retries"):
        load(p)


def test_order_matches_runner_wave_semantics():
    """Kahn FIFO 会给 p2,p3,x,y；runner 的波次语义是 p2,p3,y,x。"""
    dag = DAG(name="w", tasks=[
        Task(name="y", script="a.py", depends_on=["p3"]),
        Task(name="x", script="a.py", depends_on=["p2"]),
        Task(name="p2", script="a.py"),
        Task(name="p3", script="a.py"),
    ])
    assert dag.order() == ["p2", "p3", "y", "x"]
    assert dag.waves() == [["p2", "p3"], ["y", "x"]]


def test_order_matches_ready_loop_on_sample_dags():
    """随机无环图上，order() 与 runner 的就绪循环逐步对比。"""
    import random

    rng = random.Random(20261116)
    for _ in range(50):
        n = rng.randint(1, 8)
        names = [f"t{i}" for i in range(n)]
        tasks = []
        for i, name in enumerate(names):
            deps = sorted(rng.sample(names[:i], rng.randint(0, i))) if i else []
            tasks.append(Task(name=name, script="a.py", depends_on=deps))
        dag = DAG(name="r", tasks=tasks)

        statuses = {t.name: Status.PENDING for t in tasks}
        seq: list[str] = []
        while len(seq) < n:
            ready = ready_tasks(dag, statuses)
            assert ready, "无环图不应卡死"
            seq.extend(ready)
            for x in ready:
                statuses[x] = Status.SUCCESS
        assert dag.order() == seq
