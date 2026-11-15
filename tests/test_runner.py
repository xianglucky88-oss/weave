"""端到端：用临时目录里的脚本跑一个小 DAG。"""

from __future__ import annotations

from pathlib import Path

from weave.dag import DAG, Task
from weave.runner import run_dag
from weave.store import Store


def test_success_path(tmp_path):
    jobs = tmp_path / "jobs"
    jobs.mkdir()
    (jobs / "ok.py").write_text("print('ok')\n", encoding="utf-8")

    dag = DAG(name="t", tasks=[
        Task(name="a", script="ok.py"),
        Task(name="b", script="ok.py", depends_on=["a"]),
    ])
    store = Store(tmp_path / "db.sqlite")
    run_id, statuses = run_dag(dag, store, "2026-11-14", jobs, timeout=30)

    assert statuses == {"a": "success", "b": "success"}
    detail = store.run_detail(run_id)
    assert detail["run"]["status"] == "success"


def test_failure_marks_run_failed(tmp_path):
    jobs = tmp_path / "jobs"
    jobs.mkdir()
    (jobs / "bad.py").write_text("import sys; sys.exit(2)\n", encoding="utf-8")
    (jobs / "ok.py").write_text("print('ok')\n", encoding="utf-8")

    dag = DAG(name="t2", tasks=[
        Task(name="a", script="bad.py"),
        Task(name="b", script="ok.py", depends_on=["a"]),
    ])
    store = Store(tmp_path / "db2.sqlite")
    run_id, statuses = run_dag(dag, store, "2026-11-14", jobs, timeout=30)

    assert statuses["a"] == "failed"
    assert statuses["b"] == "skipped"
    assert store.run_detail(run_id)["run"]["status"] == "failed"


def test_retry_exhausted(tmp_path):
    jobs = tmp_path / "jobs"
    jobs.mkdir()
    (jobs / "bad.py").write_text("import sys; sys.exit(1)\n", encoding="utf-8")

    dag = DAG(name="t3", tasks=[Task(name="a", script="bad.py", retries=2)])
    store = Store(tmp_path / "db3.sqlite")
    run_id, _ = run_dag(dag, store, "2026-11-14", jobs, timeout=30)

    attempts = [t for t in store.run_detail(run_id)["tasks"] if t["task"] == "a"]
    assert len(attempts) == 3
