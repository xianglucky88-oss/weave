"""端到端：动态展开的实例级执行、重试、续跑、展开变化与 reduce 完成条件。"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import pytest

from weave.dag import DAG, Task
from weave.expand import parse_spec
from weave.ready import Status, propagate_failure, ready_tasks
from weave.runner import run_dag
from weave.store import Store

# ---------------------------------------------------------------- helpers


def job(jobs: Path, name: str, body: str) -> str:
    (jobs / name).write_text(body, encoding="utf-8")
    return name


def recorder(jobs: Path, name: str = "record.py") -> Path:
    """每个实例把自己的 key 追加到文件，用于断言"到底跑了哪些实例"。"""
    return jobs / job(jobs, name, """
import json, os, pathlib
log = pathlib.Path(os.environ["LOG"])
key = os.environ.get("WEAVE_INSTANCE_KEY", "")
with log.open("a", encoding="utf-8") as fh:
    fh.write(json.dumps({
        "task": os.environ.get("WEAVE_TASK"),
        "key": key,
        "name": os.environ.get("WEAVE_INSTANCE_NAME"),
        "index": os.environ.get("WEAVE_INSTANCE_INDEX"),
        "count": os.environ.get("WEAVE_INSTANCE_COUNT"),
        "shard": os.environ.get("WEAVE_PARAM_SHARD"),
        "upstream": json.loads(os.environ.get("WEAVE_UPSTREAM_SUMMARY", "{}")),
    }) + "\\n")
print("ran", key)
""")


def lines(log: Path) -> list[dict]:
    if not log.exists():
        return []
    return [json.loads(x) for x in log.read_text(encoding="utf-8").splitlines()]


def map_reduce_dag(jobs: Path, *, items, reduce_script="record.py",
                   map_retries=0, map_script="record.py") -> DAG:
    return DAG(name="mr", tasks=[
        Task(name="map", script=map_script, retries=map_retries,
             expand=parse_spec({"items": items, "key": "shard"})),
        Task(name="reduce", script=reduce_script, depends_on=["map"]),
    ])


# ---------------------------------------------------------------- 基本扇出


def test_every_instance_runs_and_reduce_sees_complete_summary(tmp_path):
    jobs = tmp_path / "jobs"
    jobs.mkdir()
    recorder(jobs)
    log = tmp_path / "log.jsonl"
    os.environ["LOG"] = str(log)

    dag = map_reduce_dag(jobs, items=[{"shard": "a"}, {"shard": "b"}, {"shard": "c"}])
    store = Store(tmp_path / "db.sqlite")
    run_id, statuses = run_dag(dag, store, "2026-11-14", jobs, timeout=30)

    assert statuses == {"map": "success", "reduce": "success"}
    entries = lines(log)
    map_entries = [e for e in entries if e["task"] == "map"]
    assert sorted(e["key"] for e in map_entries) == ["a", "b", "c"]
    assert sorted(e["name"] for e in map_entries) == ["map[a]", "map[b]", "map[c]"]
    assert {e["shard"] for e in map_entries} == {"a", "b", "c"}
    assert [e["count"] for e in map_entries] == ["3"] * 3

    reduce = [e for e in entries if e["task"] == "reduce"][0]
    assert reduce["upstream"]["map"]["status"] == "success"
    assert reduce["upstream"]["map"]["instances"] == {
        "total": 3, "succeeded": 3, "failed": 0, "running": 0, "pending": 0,
    }
    assert sorted(reduce["upstream"]["map"]["succeeded_keys"]) == ["a", "b", "c"]

    detail = store.run_detail(run_id)
    assert detail["run"]["status"] == "success"
    logical = {r["task"]: r for r in detail["logical_tasks"]}
    assert logical["map"]["status"] == "success"
    assert logical["map"]["instance_count"] == 3
    assert all(i["status"] == "success" for i in detail["instances"])


def test_all_instances_attempted_even_if_one_fails(tmp_path):
    """实例级失败不阻止兄弟实例完成，但逻辑任务失败、下游被跳过。"""
    jobs = tmp_path / "jobs"
    jobs.mkdir()
    (jobs / "map.py").write_text("""
import os, sys
if os.environ["WEAVE_INSTANCE_KEY"] == "b":
    print("boom", file=sys.stderr); sys.exit(3)
print("ok")
""", encoding="utf-8")
    (jobs / "ok.py").write_text("print('ok')\n", encoding="utf-8")

    dag = DAG(name="mr", tasks=[
        Task(name="map", script="map.py",
             expand=parse_spec({"items": [{"shard": "a"}, {"shard": "b"},
                                          {"shard": "c"}], "key": "shard"})),
        Task(name="reduce", script="ok.py", depends_on=["map"]),
    ])
    store = Store(tmp_path / "db.sqlite")
    run_id, statuses = run_dag(dag, store, "2026-11-14", jobs, timeout=30)

    assert statuses == {"map": "failed", "reduce": "skipped"}
    detail = store.run_detail(run_id)
    inst = {i["instance_key"]: i for i in detail["instances"]}
    assert inst["a"]["status"] == "success"
    assert inst["b"]["status"] == "failed"
    assert "boom" in inst["b"]["error"]
    assert inst["c"]["status"] == "success"
    logical = {r["task"]: r for r in detail["logical_tasks"]}
    assert logical["map"]["status"] == "failed"
    assert "b" in logical["map"]["error"]
    assert detail["run"]["status"] == "failed"


def test_instance_retry_is_recorded_per_instance(tmp_path):
    jobs = tmp_path / "jobs"
    jobs.mkdir()
    (jobs / "flaky.py").write_text("""
import os, pathlib, sys
marker = pathlib.Path(os.environ["MARKER"])
if os.environ["WEAVE_INSTANCE_KEY"] == "a" and not marker.exists():
    marker.write_text("x"); sys.exit(1)
print("ok")
""", encoding="utf-8")
    dag = DAG(name="r", tasks=[
        Task(name="map", script="flaky.py", retries=2,
             expand=parse_spec({"items": [{"shard": "a"}, {"shard": "b"}],
                                "key": "shard"})),
    ])
    store = Store(tmp_path / "db.sqlite")
    os.environ["MARKER"] = str(tmp_path / "marker")
    run_id, statuses = run_dag(dag, store, "2026-11-14", jobs, timeout=30)

    assert statuses == {"map": "success"}
    attempts = store.instance_attempts(run_id, "map")
    a = [x for x in attempts if x["instance_key"] == "a"]
    b = [x for x in attempts if x["instance_key"] == "b"]
    assert [x["status"] for x in a] == ["failed", "success"]
    assert [x["attempt"] for x in a] == [1, 2]
    assert [x["status"] for x in b] == ["success"]


# ---------------------------------------------------------------- 续跑


def test_resume_same_run_skips_successful_instances(tmp_path):
    jobs = tmp_path / "jobs"
    jobs.mkdir()
    recorder(jobs)
    log = tmp_path / "log.jsonl"
    os.environ["LOG"] = str(log)

    dag = map_reduce_dag(jobs, items=[{"shard": "a"}, {"shard": "b"}])
    store = Store(tmp_path / "db.sqlite")
    run_id, statuses = run_dag(dag, store, "2026-11-14", jobs, timeout=30)
    assert statuses == {"map": "success", "reduce": "success"}
    first = len(lines(log))

    run_id2, statuses2 = run_dag(dag, store, "2026-11-14", jobs, timeout=30,
                                 run_id=run_id)
    assert run_id2 == run_id
    assert statuses2 == {"map": "success", "reduce": "success"}
    # 没有任何实例被重新执行，历史也没有新增 attempt
    assert len(lines(log)) == first
    attempts = store.instance_attempts(run_id)
    assert len(attempts) == first  # map a/b + reduce
    assert store.run_detail(run_id)["run"]["status"] == "success"


def test_resume_after_partial_failure_reruns_only_missing(tmp_path):
    """模拟：run 在第 3 个实例前中断；续跑只补跑未成功的实例。"""
    jobs = tmp_path / "jobs"
    jobs.mkdir()
    recorder(jobs)
    log = tmp_path / "log.jsonl"
    os.environ["LOG"] = str(log)

    dag = map_reduce_dag(jobs, items=[{"shard": "a"}, {"shard": "b"},
                                      {"shard": "c"}])
    store = Store(tmp_path / "db.sqlite")
    run_id = store.create_run(dag.name, "2026-11-14")
    store.init_logical(run_id, ["map", "reduce"])
    # 预置 a 已成功、b 失败、c 从未执行；reduce 尚未执行
    from weave.expand import expand, params_digest
    inst = {i.key: i for i in expand(dag.by_name()["map"], "2026-11-14")}
    for key, status in (("a", "success"), ("b", "failed")):
        store.begin_instance(run_id, "map", key, inst[key].name, 1,
                             inst[key].params, params_digest(inst[key].params),
                             status=status)
        store.finish_instance(run_id, "map", key, 1, status,
                              error=None if status == "success" else "old failure")

    run_id2, statuses = run_dag(dag, store, "2026-11-14", jobs, timeout=30,
                                run_id=run_id)
    assert run_id2 == run_id
    assert statuses == {"map": "success", "reduce": "success"}
    keys = [e["key"] for e in lines(log) if e["task"] == "map"]
    assert sorted(keys) == ["b", "c"]  # a 被跳过，未重复执行
    attempts = store.instance_attempts(run_id, "map")
    by_key = {}
    for x in attempts:
        by_key.setdefault(x["instance_key"], []).append(x["attempt"])
    assert by_key == {"a": [1], "b": [1, 2], "c": [1]}


def test_resume_with_changed_expansion_keeps_history_and_runs_new(tmp_path):
    jobs = tmp_path / "jobs"
    jobs.mkdir()
    recorder(jobs)
    log = tmp_path / "log.jsonl"
    os.environ["LOG"] = str(log)

    dag1 = map_reduce_dag(jobs, items=[{"shard": "a"}, {"shard": "b"}])
    store = Store(tmp_path / "db.sqlite")
    run_id, _ = run_dag(dag1, store, "2026-11-14", jobs, timeout=30)
    assert sorted(e["key"] for e in lines(log) if e["task"] == "map") == ["a", "b"]

    log.unlink()
    # 同一 run 附带新 DAG：b 消失，c 新增，a 参数变化
    dag2 = map_reduce_dag(jobs, items=[{"shard": "a", "v": 2}, {"shard": "c"}])
    _, statuses = run_dag(dag2, store, "2026-11-14", jobs, timeout=30,
                          run_id=run_id)
    assert statuses == {"map": "success", "reduce": "success"}
    ran = sorted(e["key"] for e in lines(log) if e["task"] == "map")
    assert ran == ["a", "c"]  # b 不再活跃；a 因参数快照变化而重跑

    detail = store.run_detail(run_id)
    active = {i["instance_key"]: i for i in detail["instances"] if i["task"] == "map"}
    assert set(active) == {"a", "c"}
    assert active["a"]["attempt"] == 2
    assert active["c"]["attempt"] == 1
    # b 的历史尝试保留，只是不活跃
    all_keys = {(i["instance_key"], i["active"])
                for i in detail["tasks"] if i["task"] == "map"}
    assert ("b", 0) in all_keys
    logical = {r["task"]: r for r in detail["logical_tasks"]}
    assert logical["map"]["instance_count"] == 2
    assert detail["run"]["status"] == "success"


def test_resume_reruns_downstream_when_upstream_changed(tmp_path):
    jobs = tmp_path / "jobs"
    jobs.mkdir()
    recorder(jobs)
    log = tmp_path / "log.jsonl"
    os.environ["LOG"] = str(log)

    dag1 = map_reduce_dag(jobs, items=[{"shard": "a"}])
    store = Store(tmp_path / "db.sqlite")
    run_id, _ = run_dag(dag1, store, "2026-11-14", jobs, timeout=30)
    log.unlink()

    dag2 = map_reduce_dag(jobs, items=[{"shard": "a"}, {"shard": "b"}])
    _, statuses = run_dag(dag2, store, "2026-11-14", jobs, timeout=30,
                          run_id=run_id)
    assert statuses == {"map": "success", "reduce": "success"}
    entries = lines(log)
    assert [e["key"] for e in entries if e["task"] == "map"] == ["b"]
    # a 已成功未重跑；但 map 新增实例使输出变化，reduce 必须重跑
    reduce_entries = [e for e in entries if e["task"] == "reduce"]
    assert len(reduce_entries) == 1
    assert reduce_entries[0]["upstream"]["map"]["instances"]["total"] == 2


def test_resume_refuses_mismatched_run(tmp_path):
    jobs = tmp_path / "jobs"
    jobs.mkdir()
    (jobs / "ok.py").write_text("print('ok')\n", encoding="utf-8")
    dag = DAG(name="x", tasks=[Task(name="a", script="ok.py")])
    store = Store(tmp_path / "db.sqlite")
    run_id, _ = run_dag(dag, store, "2026-11-14", jobs, timeout=30)
    with pytest.raises(ValueError, match="belongs to"):
        run_dag(dag, store, "2026-11-15", jobs, timeout=30, run_id=run_id)


# ---------------------------------------------------------------- 空展开 / reduce


def test_empty_expansion_succeeds_and_unblocks_reduce(tmp_path):
    jobs = tmp_path / "jobs"
    jobs.mkdir()
    recorder(jobs)
    log = tmp_path / "log.jsonl"
    os.environ["LOG"] = str(log)

    dag = map_reduce_dag(jobs, items=[])
    store = Store(tmp_path / "db.sqlite")
    run_id, statuses = run_dag(dag, store, "2026-11-14", jobs, timeout=30)

    assert statuses == {"map": "success", "reduce": "success"}
    entries = lines(log)
    assert [e["task"] for e in entries] == ["reduce"]
    assert entries[0]["upstream"]["map"]["instances"]["total"] == 0
    detail = store.run_detail(run_id)
    assert detail["run"]["status"] == "success"
    logical = {r["task"]: r for r in detail["logical_tasks"]}
    assert logical["map"]["status"] == "success"
    assert logical["map"]["instance_count"] == 0


def test_reduce_not_ready_until_all_instances_succeed():
    dag = DAG(name="d", tasks=[
        Task(name="map", script="m.py", expand=parse_spec(
            {"items": [{"id": 1}, {"id": 2}]})),
        Task(name="reduce", script="r.py", depends_on=["map"]),
    ])
    st = {"map": Status.SUCCESS, "reduce": Status.PENDING}
    assert ready_tasks(dag, st) == ["reduce"]
    st = {"map": Status.FAILED, "reduce": Status.PENDING}
    propagate_failure(dag, st)
    assert st["reduce"] == Status.SKIPPED


# ---------------------------------------------------------------- 超时


def test_instance_timeout_marks_only_that_instance_failed(tmp_path):
    jobs = tmp_path / "jobs"
    jobs.mkdir()
    (jobs / "slow.py").write_text("""
import os, sys, time
if os.environ["WEAVE_INSTANCE_KEY"] == "b":
    time.sleep(5)
print("ok")
""", encoding="utf-8")
    dag = DAG(name="t", tasks=[
        Task(name="map", script="slow.py", expand=parse_spec(
            {"items": [{"id": "a"}, {"id": "b"}], "key": "id"})),
    ])
    store = Store(tmp_path / "db.sqlite")
    run_id, statuses = run_dag(dag, store, "2026-11-14", jobs, timeout=1)
    assert statuses == {"map": "failed"}
    inst = {i["instance_key"]: i for i in store.run_detail(run_id)["instances"]}
    assert inst["a"]["status"] == "success"
    assert inst["b"]["status"] == "failed"
    assert "timeout" in inst["b"]["error"]


def test_expansion_error_fails_run_without_crashing(tmp_path):
    jobs = tmp_path / "jobs"
    jobs.mkdir()
    (jobs / "ok.py").write_text("print('ok')\n", encoding="utf-8")
    dag = DAG(name="e", tasks=[
        Task(name="map", script="ok.py", expand=parse_spec(
            {"path": str(tmp_path / "nope_{ds}.json")})),
        Task(name="reduce", script="ok.py", depends_on=["map"]),
    ])
    store = Store(tmp_path / "db.sqlite")
    run_id, statuses = run_dag(dag, store, "2026-11-14", jobs, timeout=30)
    assert statuses == {"map": "failed", "reduce": "skipped"}
    detail = store.run_detail(run_id)
    assert detail["run"]["status"] == "failed"
    logical = {r["task"]: r for r in detail["logical_tasks"]}
    assert "expand source not found" in logical["map"]["error"]
    assert logical["reduce"]["status"] == "skipped"
