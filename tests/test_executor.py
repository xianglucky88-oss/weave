"""执行层失败语义测试：启动失败、退出码、信号、超时清理、输出截断、取消。"""

from __future__ import annotations

import os
import signal
import threading
import time
from pathlib import Path

from weave.executor import FailureKind, run_task
from weave import executor as execmod
import sys as sysmod


def _write(jobs: Path, name: str, body: str) -> Path:
    p = jobs / name
    p.write_text(body, encoding="utf-8")
    return p


def _alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def test_success_result(tmp_path):
    jobs = tmp_path / "jobs"
    jobs.mkdir()
    script = _write(jobs, "ok.py", "print('hello')\n")
    r = run_task(script, "run1", "2026-11-16", {}, timeout=10)
    assert r.ok
    assert r.kind == FailureKind.SUCCESS
    assert r.exit_code == 0
    assert "hello" in r.output
    assert not r.output_truncated


def test_nonzero_exit_is_classified(tmp_path):
    jobs = tmp_path / "jobs"
    jobs.mkdir()
    script = _write(jobs, "bad.py", "import sys; sys.exit(3)\n")
    r = run_task(script, "run1", "ds", {}, timeout=10)
    assert not r.ok
    assert r.kind == FailureKind.EXIT_CODE
    assert r.exit_code == 3
    assert "3" in r.error


def test_missing_script_is_spawn_failure(tmp_path, monkeypatch):
    jobs = tmp_path / "jobs"
    jobs.mkdir()
    # 解释器本身不可用 => Popen 直接抛 OSError，进程从未启动。
    monkeypatch.setattr(sysmod, "executable", "/nonexistent/pythonxyz")
    r = run_task(jobs / "nope.py", "run1", "ds", {}, timeout=10)
    assert not r.ok
    assert r.kind == FailureKind.SPAWN
    # 启动失败不应返回退出码，也不应标记超时
    assert r.exit_code is None
    assert not r.timed_out


def test_signal_termination_is_classified(tmp_path):
    jobs = tmp_path / "jobs"
    jobs.mkdir()
    script = _write(
        jobs, "sig.py",
        "import os, signal, time\n"
        "os.kill(os.getpid(), signal.SIGTERM)\n"
        "time.sleep(5)\n",
    )
    r = run_task(script, "run1", "ds", {}, timeout=10)
    assert not r.ok
    assert r.kind == FailureKind.SIGNAL
    assert r.signal == signal.SIGTERM


def test_timeout_kills_process_group_and_reaps(tmp_path, monkeypatch):
    jobs = tmp_path / "jobs"
    jobs.mkdir()
    # 父进程 sleep，孙进程也 sleep；只杀父进程会留下孙进程。
    script = _write(
        jobs, "slow.py",
        "import subprocess, sys, time\n"
        "subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(30)'])\n"
        "time.sleep(30)\n",
    )
    # 缩短 SIGTERM 后的宽限期，测试跑得快一点。
    monkeypatch.setattr(execmod, "KILL_GRACE_SECONDS", 0.2)

    start = time.monotonic()
    r = run_task(script, "run1", "ds", {}, timeout=1)
    elapsed = time.monotonic() - start

    assert not r.ok
    assert r.kind == FailureKind.TIMEOUT
    assert r.timed_out
    assert r.killed
    assert elapsed < 5  # 不能等到 sleep 自然结束
    # 找到该脚本派生的所有 sleep 进程并确认已退出（含同组孙进程）。
    import subprocess
    out = subprocess.run(["pgrep", "-f", "time.sleep(30)"],
                         capture_output=True, text=True)
    pids = [int(x) for x in out.stdout.split()]
    assert all(not _alive(p) for p in pids)


def test_timeout_sigkill_when_process_ignores_term(tmp_path, monkeypatch):
    jobs = tmp_path / "jobs"
    jobs.mkdir()
    script = _write(
        jobs, "stubborn.py",
        "import signal, time\n"
        "signal.signal(signal.SIGTERM, signal.SIG_IGN)\n"
        "time.sleep(30)\n",
    )
    monkeypatch.setattr(execmod, "KILL_GRACE_SECONDS", 0.2)
    r = run_task(script, "run1", "ds", {}, timeout=1)
    assert r.kind == FailureKind.TIMEOUT
    assert r.killed


def test_output_is_bounded_and_marked(tmp_path):
    jobs = tmp_path / "jobs"
    jobs.mkdir()
    script = _write(
        jobs, "loud.py",
        "print('x' * 100000)\n",
    )
    r = run_task(script, "run1", "ds", {}, timeout=10, max_output=1024)
    assert r.output_truncated
    assert "truncated" in r.output
    # 保留内容有界（标记文本之外的正文不超过上限）。
    assert len(r.output) < 1024 + 200


def test_cancel_event_kills_running_process(tmp_path):
    jobs = tmp_path / "jobs"
    jobs.mkdir()
    script = _write(jobs, "slow.py", "import time; time.sleep(30)\n")
    cancel = threading.Event()

    def trigger():
        time.sleep(0.3)
        cancel.set()

    threading.Thread(target=trigger, daemon=True).start()
    r = run_task(script, "run1", "ds", {}, timeout=30, cancel=cancel)
    assert not r.ok
    assert r.kind == FailureKind.CANCELED
    assert r.killed


def test_params_and_context_are_passed(tmp_path):
    jobs = tmp_path / "jobs"
    jobs.mkdir()
    script = _write(
        jobs, "env.py",
        "import os\n"
        "print(os.environ['WEAVE_RUN_ID'], os.environ['WEAVE_DS'],\n"
        "      os.environ['WEAVE_PARAM_REGION'])\n",
    )
    r = run_task(script, "rid-42", "2026-11-16", {"region": "cn"}, timeout=10)
    assert r.ok
    assert "rid-42 2026-11-16 cn" in r.output
