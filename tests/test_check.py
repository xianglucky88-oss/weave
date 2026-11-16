"""发布前检查契约的测试：阶段、诊断、退出码、输出。"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from weave import cli
from weave.check import check_dag, check_file, check_repo
from weave.dag import DAG, Task


def write(tmp_path, text, name="d.yaml"):
    p = tmp_path / name
    p.write_text(text, encoding="utf-8")
    return p


@pytest.fixture
def jobs(tmp_path):
    j = tmp_path / "jobs"
    j.mkdir()
    for s in ("a.py", "b.py", "c.py"):
        (j / s).write_text(f"print({s!r})\n", encoding="utf-8")
    return j


def codes(report):
    return [d.code for d in report.diagnostics]


def phase(report, name):
    return next(p for p in report.phases if p.name == name)


VALID = """
name: ok_daily
schedule: day
tasks:
  - name: a
    script: a.py
  - name: b
    script: b.py
    depends_on: [a]
"""

# ---------------------------------------------------------------- parse 阶段

def test_yaml_syntax_error_locates_and_skips(tmp_path, jobs):
    p = write(tmp_path, "name: x\ntasks:\n  - name: a\n    script: a.py\n   bad indent\n")
    r = check_file(p, jobs_dir=jobs)
    assert "E_YAML" in codes(r)
    assert phase(r, "parse").status == "failed"
    for ph in ("structure", "deps", "scripts", "topology"):
        assert phase(r, ph).status == "skipped"


def test_root_must_be_mapping(tmp_path, jobs):
    p = write(tmp_path, "- just\n- a\n- list\n")
    r = check_file(p, jobs_dir=jobs)
    assert "E_ROOT_TYPE" in codes(r)


def test_unreadable_file(tmp_path, jobs):
    r = check_file(tmp_path / "nope.yaml", jobs_dir=jobs)
    assert "E_READ" in codes(r)
    assert not r.ok


# ---------------------------------------------------------------- structure 阶段

def test_missing_dag_name(tmp_path, jobs):
    p = write(tmp_path, "tasks:\n  - name: a\n    script: a.py\n")
    r = check_file(p, jobs_dir=jobs)
    assert "E_DAG_NAME" in codes(r)
    assert phase(r, "deps").status == "skipped"


def test_empty_dag_is_an_error(tmp_path, jobs):
    p = write(tmp_path, "name: empty\ntasks: []\n")
    r = check_file(p, jobs_dir=jobs)
    assert "E_NO_TASKS" in codes(r)
    assert not r.ok


def test_tasks_must_be_a_list(tmp_path, jobs):
    p = write(tmp_path, "name: x\ntasks: not-a-list\n")
    assert "E_TASKS_TYPE" in codes(check_file(p, jobs_dir=jobs))


def test_task_field_type_errors_are_located(tmp_path, jobs):
    p = write(tmp_path, """
name: types
tasks:
  - name: 123
    script: a.py
  - name: b
  - name: c
    script: a.py
    depends_on: a
  - name: d
    script: a.py
    retries: many
  - name: e
    script: a.py
    retries: -1
  - name: f
    script: a.py
    params: [k, v]
  - just-a-string
""")
    r = check_file(p, jobs_dir=jobs)
    got = codes(r)
    for code in ("E_TASK_NAME", "E_TASK_SCRIPT", "E_DEPS_TYPE",
                 "E_RETRIES_TYPE", "E_RETRIES_RANGE", "E_PARAMS_TYPE", "E_TASK_TYPE"):
        assert code in got, f"{code} missing in {got}"
    # 定位信息：消息里能找到任务名或索引
    msgs = "\n".join(d.message for d in r.diagnostics)
    assert "任务 'b'" in msgs and "tasks[6]" in msgs
    # 结构错了，后续阶段全部短路
    for ph in ("deps", "scripts", "topology"):
        assert phase(r, ph).status == "skipped"


def test_params_key_and_value_types(tmp_path, jobs):
    p = write(tmp_path, """
name: p
tasks:
  - name: a
    script: a.py
    params:
      1: x
      k: [nested]
""")
    r = check_file(p, jobs_dir=jobs)
    assert "E_PARAMS_KEY" in codes(r)
    assert "E_PARAMS_VALUE" in codes(r)


def test_unknown_field_warns_about_typos(tmp_path, jobs):
    p = write(tmp_path, """
name: typo
tasks:
  - name: a
    script: a.py
  - name: b
    script: b.py
    depend_on: [a]
""")
    r = check_file(p, jobs_dir=jobs)
    assert "W_UNKNOWN_FIELD" in codes(r)
    assert r.ok  # warning 不影响通过
    assert any("depend_on" in d.message for d in r.warnings)


def test_unknown_schedule_value(tmp_path, jobs):
    p = write(tmp_path, "name: s\nschedule: hourly\ntasks:\n  - name: a\n    script: a.py\n")
    assert "E_SCHEDULE_VALUE" in codes(check_file(p, jobs_dir=jobs))


# ---------------------------------------------------------------- deps 阶段

def test_duplicate_names_reported_with_count(tmp_path, jobs):
    p = write(tmp_path, """
name: dup
tasks:
  - name: a
    script: a.py
  - name: a
    script: b.py
  - name: a
    script: c.py
""")
    r = check_file(p, jobs_dir=jobs)
    d = next(d for d in r.diagnostics if d.code == "E_DUP_NAME")
    assert "'a'" in d.message and "3" in d.message


def test_unknown_dependency_is_located(tmp_path, jobs):
    p = write(tmp_path, """
name: u
tasks:
  - name: a
    script: a.py
    depends_on: [ghost]
""")
    r = check_file(p, jobs_dir=jobs)
    d = next(d for d in r.diagnostics if d.code == "E_UNKNOWN_DEP")
    assert d.task == "a" and "ghost" in d.message


def test_duplicate_dependency_is_an_error(tmp_path, jobs):
    p = write(tmp_path, """
name: dd
tasks:
  - name: a
    script: a.py
  - name: b
    script: b.py
    depends_on: [a, a]
""")
    r = check_file(p, jobs_dir=jobs)
    assert "E_DUP_DEP" in codes(r)
    assert not r.ok


def test_self_dependency(tmp_path, jobs):
    p = write(tmp_path, """
name: self
tasks:
  - name: a
    script: a.py
    depends_on: [a]
""")
    r = check_file(p, jobs_dir=jobs)
    assert "E_SELF_DEP" in codes(r)
    # 自环不重复报 E_CYCLE
    assert "E_CYCLE" not in codes(r)


def test_cycle_reports_concrete_path(tmp_path, jobs):
    p = write(tmp_path, """
name: cyc
tasks:
  - name: a
    script: a.py
    depends_on: [c]
  - name: b
    script: b.py
    depends_on: [a]
  - name: c
    script: c.py
    depends_on: [b]
""")
    r = check_file(p, jobs_dir=jobs)
    d = next(d for d in r.diagnostics if d.code == "E_CYCLE")
    # 箭头方向为「依赖于」：a 依赖 c，c 依赖 b，b 依赖 a
    assert "a -> c -> b -> a" in d.message
    assert phase(r, "topology").status == "skipped"


def test_multiple_cycles_all_reported(tmp_path, jobs):
    p = write(tmp_path, """
name: multi
tasks:
  - name: a
    script: a.py
    depends_on: [b]
  - name: b
    script: b.py
    depends_on: [a]
  - name: x
    script: a.py
    depends_on: [y]
  - name: y
    script: b.py
    depends_on: [x]
""")
    r = check_file(p, jobs_dir=jobs)
    cycles = [d for d in r.diagnostics if d.code == "E_CYCLE"]
    assert len(cycles) == 2


def test_deps_failed_but_scripts_still_checked(tmp_path, jobs):
    """deps 与 scripts 独立：依赖坏了，脚本问题照样报。"""
    p = write(tmp_path, """
name: both
tasks:
  - name: a
    script: missing.py
    depends_on: [ghost]
""")
    r = check_file(p, jobs_dir=jobs)
    assert phase(r, "deps").status == "failed"
    assert phase(r, "scripts").status == "failed"
    assert "E_UNKNOWN_DEP" in codes(r)
    assert "E_SCRIPT_MISSING" in codes(r)


# ---------------------------------------------------------------- scripts 阶段

def test_script_missing_shows_resolved_path(tmp_path, jobs):
    p = write(tmp_path, """
name: m
tasks:
  - name: a
    script: nope.py
""")
    r = check_file(p, jobs_dir=jobs)
    d = next(d for d in r.diagnostics if d.code == "E_SCRIPT_MISSING")
    assert str(jobs / "nope.py") in d.message


def test_absolute_script_path_rejected(tmp_path, jobs):
    p = write(tmp_path, f"""
name: abs
tasks:
  - name: a
    script: {jobs}/a.py
""")
    assert "E_SCRIPT_ABSOLUTE" in codes(check_file(p, jobs_dir=jobs))


def test_script_escaping_jobs_root_rejected(tmp_path, jobs):
    p = write(tmp_path, """
name: esc
tasks:
  - name: a
    script: ../../etc/passwd
""")
    assert "E_SCRIPT_ESCAPE" in codes(check_file(p, jobs_dir=jobs))


def test_script_subdirectory_ok(tmp_path, jobs):
    (jobs / "sub").mkdir()
    (jobs / "sub" / "x.py").write_text("print('x')\n", encoding="utf-8")
    p = write(tmp_path, """
name: sub
tasks:
  - name: a
    script: sub/x.py
""")
    r = check_file(p, jobs_dir=jobs)
    assert r.ok, [d.message for d in r.errors]


def test_script_directory_and_suffix(tmp_path, jobs):
    (jobs / "dir.py").mkdir()
    (jobs / "run.sh").write_text("echo hi\n", encoding="utf-8")
    p = write(tmp_path, """
name: misc
tasks:
  - name: a
    script: dir.py
  - name: b
    script: run.sh
""")
    r = check_file(p, jobs_dir=jobs)
    assert "E_SCRIPT_NOT_FILE" in codes(r)
    assert "W_SCRIPT_SUFFIX" in codes(r)


def test_missing_jobs_dir(tmp_path):
    p = write(tmp_path, VALID)
    r = check_file(p, jobs_dir=tmp_path / "no-such-dir")
    assert "E_JOBS_DIR" in codes(r)


# ---------------------------------------------------------------- topology 阶段

def test_valid_dag_has_order_and_waves(tmp_path, jobs):
    p = write(tmp_path, VALID)
    r = check_file(p, jobs_dir=jobs)
    assert r.ok
    assert r.order == ["a", "b"]
    assert r.waves == [["a"], ["b"]]
    assert phase(r, "topology").status == "ok"


def test_isolated_task_warns(tmp_path, jobs):
    p = write(tmp_path, """
name: iso
tasks:
  - name: a
    script: a.py
  - name: b
    script: b.py
    depends_on: [a]
  - name: stray
    script: c.py
""")
    r = check_file(p, jobs_dir=jobs)
    assert r.ok
    d = next(d for d in r.diagnostics if d.code == "W_ISOLATED")
    assert d.task == "stray"


def test_single_task_dag_not_flagged_isolated(tmp_path, jobs):
    p = write(tmp_path, "name: one\ntasks:\n  - name: a\n    script: a.py\n")
    r = check_file(p, jobs_dir=jobs)
    assert r.ok and not r.warnings


# ---------------------------------------------------------------- 跨文件

def test_duplicate_dag_names_across_files(tmp_path, jobs):
    a = write(tmp_path, VALID, "a.yaml")
    b = write(tmp_path, VALID, "b.yaml")  # 同名 ok_daily
    repo = check_repo([a, b], jobs_dir=jobs)
    assert "E_DUP_DAG" in [d.code for d in repo.diagnostics]
    assert not repo.ok


# ---------------------------------------------------------------- CLI 契约

def test_cli_exit_codes(tmp_path, jobs, capsys):
    ok = write(tmp_path, VALID, "ok.yaml")
    bad = write(tmp_path, "name: b\ntasks:\n  - name: a\n    script: a.py\n    depends_on: [ghost]\n", "bad.yaml")

    assert cli.main(["validate", "--file", str(ok), "--jobs-dir", str(jobs)]) == 0
    assert cli.main(["validate", "--file", str(bad), "--jobs-dir", str(jobs)]) == 1
    assert cli.main(["validate", "--file", str(tmp_path / "nope.yaml")]) == 2
    assert cli.main(["validate", "no_such_dag", "--dags-dir", str(tmp_path)]) == 2
    capsys.readouterr()


def test_cli_validate_all_on_repo(capsys):
    """仓库自带的两个 DAG 定义都应是合法的（broken_daily 是运行时失败）。"""
    assert cli.main(["validate", "--all"]) == 0
    out = capsys.readouterr().out
    assert "orders_daily" in out and "broken_daily" in out


def test_cli_validate_by_name_and_dags_dir(tmp_path, jobs, capsys):
    write(tmp_path, VALID)
    assert cli.main(["validate", "ok_daily", "--dags-dir", str(tmp_path),
                     "--jobs-dir", str(jobs)]) == 0
    out = capsys.readouterr().out
    assert "拓扑序: a -> b" in out


def test_cli_json_format_is_machine_readable(tmp_path, jobs, capsys):
    bad = write(tmp_path, "name: b\ntasks:\n  - name: a\n    script: nope.py\n")
    rc = cli.main(["validate", "--file", str(bad), "--jobs-dir", str(jobs),
                   "--format", "json"])
    assert rc == 1
    doc = json.loads(capsys.readouterr().out)
    assert doc["ok"] is False
    assert doc["summary"]["errors"] >= 1
    f = doc["files"][0]
    assert {p["name"] for p in f["phases"]} == {"parse", "structure", "deps", "scripts", "topology"}
    diag = f["diagnostics"][0]
    assert set(diag) == {"code", "severity", "phase", "task", "message"}


def test_cli_strict_turns_warnings_into_failure(tmp_path, jobs, capsys):
    p = write(tmp_path, """
name: w
tasks:
  - name: a
    script: a.py
  - name: stray
    script: b.py
""")
    assert cli.main(["validate", "--file", str(p), "--jobs-dir", str(jobs)]) == 0
    assert cli.main(["validate", "--file", str(p), "--jobs-dir", str(jobs), "--strict"]) == 1
    capsys.readouterr()


def test_cli_run_refuses_invalid_definition(tmp_path, jobs, capsys):
    write(tmp_path, "name: b\ntasks:\n  - name: a\n    script: ghost.py\n")
    rc = cli.main(["run", "b", "--dags-dir", str(tmp_path), "--jobs-dir", str(jobs)])
    assert rc == 1
    err = capsys.readouterr().err
    assert "E_SCRIPT_MISSING" in err


def test_cli_list_survives_broken_files(capsys):
    assert cli.main(["list"]) == 0
    out = capsys.readouterr().out
    assert "orders_daily" in out


def test_check_dag_on_in_memory_object():
    dag = DAG(name="m", tasks=[
        Task(name="a", script="a.py", depends_on=["nope"]),
    ])
    r = check_dag(dag)
    assert "E_UNKNOWN_DEP" in codes(r)
    assert phase(r, "scripts").status == "skipped"  # 未提供 jobs 目录
