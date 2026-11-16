"""端到端：用临时目录里的脚本跑一个小 DAG。"""

from __future__ import annotations

import sqlite3
import threading
import time
from pathlib import Path

from weave.dag import DAG, Task
from weave.runner import run_dag
from weave.store import Store


def _jobs(tmp_path: Path) -> Path:
    jobs = tmp_path / "jobs"
    jobs.mkdir(exist_ok=True)
    return jobs


def test_success_path(tmp_path):
    jobs = _jobs(tmp_path)
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
    jobs = _jobs(tmp_path)
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
    jobs = _jobs(tmp_path)
    (jobs / "bad.py").write_text("import sys; sys.exit(1)\n", encoding="utf-8")

    dag = DAG(name="t3", tasks=[Task(name="a", script="bad.py", retries=2)])
    store = Store(tmp_path / "db3.sqlite")
    run_id, _ = run_dag(dag, store, "2026-11-14", jobs, timeout=30)

    attempts = [t for t in store.run_detail(run_id)["tasks"] if t["task"] == "a"]
    assert len(attempts) == 3


def test_failure_skips_entire_downstream_chain(tmp_path):
    """最后一次尝试失败后，直接和间接下游都必须被跳过并落库。"""
    jobs = _jobs(tmp_path)
    (jobs / "bad.py").write_text("import sys; sys.exit(1)\n", encoding="utf-8")
    (jobs / "ok.py").write_text("print('ok')\n", encoding="utf-8")

    dag = DAG(name="chain", tasks=[
        Task(name="a", script="bad.py", retries=1),
        Task(name="b", script="ok.py", depends_on=["a"]),
        Task(name="c", script="ok.py", depends_on=["b"]),
    ])
    store = Store(tmp_path / "db.sqlite")
    run_id, statuses = run_dag(dag, store, "ds", jobs, timeout=30)

    assert statuses == {"a": "failed", "b": "skipped", "c": "skipped"}
    detail = store.run_detail(run_id)
    skipped = {t["task"]: t for t in detail["tasks"] if t["status"] == "skipped"}
    assert set(skipped) == {"b", "c"}
    # 跳过的任务以 attempt=0 占位，且记录了原因
    assert all(t["attempt"] == 0 and t["error"] for t in skipped.values())
    assert detail["run"]["status"] == "failed"


def test_retry_with_backoff_then_success(tmp_path):
    jobs = _jobs(tmp_path)
    marker = tmp_path / "tries.txt"
    (jobs / "flaky.py").write_text(
        "from pathlib import Path\n"
        f"p = Path({str(marker)!r})\n"
        "n = int(p.read_text()) if p.exists() else 0\n"
        "p.write_text(str(n + 1))\n"
        "import sys; sys.exit(0 if n >= 2 else 1)\n",
        encoding="utf-8",
    )
    dag = DAG(name="flaky", tasks=[
        Task(name="a", script="flaky.py", retries=3, backoff_base=0.05,
             backoff_factor=2.0),
    ])
    store = Store(tmp_path / "db.sqlite")
    run_id, statuses = run_dag(dag, store, "ds", jobs, timeout=30)

    assert statuses["a"] == "success"
    rows = [t for t in store.run_detail(run_id)["tasks"] if t["task"] == "a"]
    assert [r["status"] for r in rows] == ["failed", "failed", "success"]
    # 退避时长按策略落库到尝试记录的计算路径（这里断言两次失败确实发生）
    assert rows[0]["kind"] == "exit_code" and rows[0]["exit_code"] == 1
    assert rows[2]["kind"] == "success"


def test_spawn_failure_is_failed_without_retry_storm(tmp_path, monkeypatch):
    jobs = _jobs(tmp_path)
    (jobs / "whatever.py").write_text("print('x')\n", encoding="utf-8")
    # 解释器不可用 => Popen 阶段 OSError，归类为 spawn；即使配了很多重试
    # 也只尝试一次，不会重试放大。
    import sys as sysmod
    monkeypatch.setattr(sysmod, "executable", "/nonexistent/pythonxyz")
    dag = DAG(name="spawn", tasks=[
        Task(name="a", script="whatever.py", retries=5),
    ])
    store = Store(tmp_path / "db.sqlite")
    run_id, statuses = run_dag(dag, store, "ds", jobs, timeout=30)

    assert statuses["a"] == "failed"
    rows = [t for t in store.run_detail(run_id)["tasks"] if t["task"] == "a"]
    assert len(rows) == 1
    assert rows[0]["kind"] == "spawn"
    assert rows[0]["error"] and "failed to start" in rows[0]["error"]


def test_fatal_exit_code_is_not_retried(tmp_path):
    jobs = _jobs(tmp_path)
    (jobs / "fatal.py").write_text("import sys; sys.exit(2)\n", encoding="utf-8")
    dag = DAG(name="fatal", tasks=[
        Task(name="a", script="fatal.py", retries=4, fatal_exit_codes=[2]),
    ])
    store = Store(tmp_path / "db.sqlite")
    run_id, _ = run_dag(dag, store, "ds", jobs, timeout=30)

    rows = [t for t in store.run_detail(run_id)["tasks"] if t["task"] == "a"]
    assert len(rows) == 1
    assert rows[0]["kind"] == "exit_code"
    assert rows[0]["exit_code"] == 2


def test_timeout_is_recorded_and_task_fails(tmp_path):
    jobs = _jobs(tmp_path)
    (jobs / "slow.py").write_text("import time; time.sleep(30)\n", encoding="utf-8")
    dag = DAG(name="slow", tasks=[
        Task(name="a", script="slow.py", retries=1, timeout=1),
    ])
    start = time.monotonic()
    store = Store(tmp_path / "db.sqlite")
    run_id, statuses = run_dag(dag, store, "ds", jobs, timeout=30)
    elapsed = time.monotonic() - start

    assert statuses["a"] == "failed"
    assert elapsed < 10
    rows = [t for t in store.run_detail(run_id)["tasks"] if t["task"] == "a"]
    assert len(rows) == 2  # 初始尝试 + 1 次重试，都超时
    assert all(r["kind"] == "timeout" and r["timed_out"] for r in rows)
    assert all(r["killed"] for r in rows)


def test_shutdown_during_backoff_cancels_run(tmp_path):
    jobs = _jobs(tmp_path)
    (jobs / "bad.py").write_text("import sys; sys.exit(1)\n", encoding="utf-8")
    dag = DAG(name="cancel", tasks=[
        Task(name="a", script="bad.py", retries=5, backoff_base=30.0),
    ])
    shutdown = threading.Event()

    def trigger():
        time.sleep(0.3)
        shutdown.set()

    threading.Thread(target=trigger, daemon=True).start()
    store = Store(tmp_path / "db.sqlite")
    start = time.monotonic()
    run_id, statuses = run_dag(dag, store, "ds", jobs, timeout=30,
                               shutdown=shutdown)
    elapsed = time.monotonic() - start

    assert elapsed < 5  # 没有等 30s 退避
    assert statuses["a"] == "failed"
    assert store.run_detail(run_id)["run"]["status"] == "canceled"


def test_shutdown_skips_pending_downstream(tmp_path):
    jobs = _jobs(tmp_path)
    (jobs / "slow.py").write_text("import time; time.sleep(30)\n", encoding="utf-8")
    (jobs / "ok.py").write_text("print('ok')\n", encoding="utf-8")
    dag = DAG(name="cancel2", tasks=[
        Task(name="a", script="slow.py", timeout=5),
        Task(name="b", script="ok.py", depends_on=["a"]),
    ])
    shutdown = threading.Event()

    def trigger():
        time.sleep(0.3)
        shutdown.set()

    threading.Thread(target=trigger, daemon=True).start()
    store = Store(tmp_path / "db.sqlite")
    run_id, statuses = run_dag(dag, store, "ds", jobs, timeout=30,
                               shutdown=shutdown)

    assert statuses["a"] == "failed"
    assert statuses["b"] == "skipped"
    assert store.run_detail(run_id)["run"]["status"] == "canceled"


def test_duplicate_run_id_is_regenerated(tmp_path, monkeypatch):
    store = Store(tmp_path / "db.sqlite")
    # 先占用一个 id，再让 uuid 生成器先返回同一个、随后返回新值，
    # 验证碰撞时会重新分配而不是覆盖既有 run。
    existing = store.create_run("other", "ds")
    seq = [existing, "newunique001"]
    monkeypatch.setattr(
        "weave.store.uuid.uuid4",
        lambda: _FakeUUID(seq.pop(0)),
    )

    run_id = store.create_run("dag-x", "ds")
    assert run_id == "newunique001"
    detail = store.run_detail(existing)
    assert detail["run"]["dag"] == "other"  # 旧记录未被覆盖


class _FakeUUID:
    def __init__(self, hex12: str):
        # store 取 uuid4().hex[:12]，补齐到 32 位保持与真实 uuid 同形。
        self.hex = hex12[:12].ljust(32, "0")


def test_attempt_records_carry_failure_semantics(tmp_path):
    jobs = _jobs(tmp_path)
    (jobs / "bad.py").write_text("import sys; sys.exit(7)\n", encoding="utf-8")
    dag = DAG(name="sem", tasks=[Task(name="a", script="bad.py")])
    store = Store(tmp_path / "db.sqlite")
    run_id, _ = run_dag(dag, store, "ds", jobs, timeout=30)

    row = [t for t in store.run_detail(run_id)["tasks"] if t["task"] == "a"][0]
    assert row["status"] == "failed"
    assert row["kind"] == "exit_code"
    assert row["exit_code"] == 7
    assert row["timed_out"] == 0
    assert row["seconds"] is not None and row["seconds"] >= 0


def test_old_database_is_migrated(tmp_path):
    """老 schema（没有 kind 等列）的库打开时自动加列，旧数据仍可读。"""
    db = tmp_path / "old.sqlite"
    conn = sqlite3.connect(db)
    conn.executescript(
        "CREATE TABLE runs (run_id TEXT PRIMARY KEY, dag TEXT NOT NULL, ds TEXT NOT NULL,"
        " status TEXT NOT NULL, started_at REAL NOT NULL, finished_at REAL);"
        "CREATE TABLE task_runs (run_id TEXT NOT NULL, task TEXT NOT NULL, attempt INTEGER NOT NULL,"
        " status TEXT NOT NULL, started_at REAL, finished_at REAL, error TEXT,"
        " PRIMARY KEY (run_id, task, attempt));"
        "INSERT INTO runs VALUES ('r1', 'd', 'ds', 'success', 1.0, 2.0);"
        "INSERT INTO task_runs VALUES ('r1', 'a', 1, 'success', 1.0, 2.0, NULL);"
    )
    conn.commit()
    conn.close()

    store = Store(db)  # 不应报错
    detail = store.run_detail("r1")
    assert detail["run"]["status"] == "success"
    assert detail["tasks"][0]["kind"] == ""  # 迁移补的默认值
