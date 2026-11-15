"""任务执行：把任务脚本跑成子进程。"""

from __future__ import annotations

import os
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path


@dataclass
class ExecResult:
    ok: bool
    exit_code: int
    output: str
    seconds: float


def run_task(script: Path, run_id: str, ds: str, params: dict[str, str],
             timeout: int = 60, workdir: Path | None = None) -> ExecResult:
    """执行一个任务脚本。

    通过环境变量把运行上下文传给脚本，脚本只需读环境变量即可，
    不需要知道调度器的存在。
    """
    env = os.environ.copy()
    env["WEAVE_RUN_ID"] = run_id
    env["WEAVE_DS"] = ds
    for k, v in params.items():
        env[f"WEAVE_PARAM_{k.upper()}"] = str(v)

    import time
    start = time.time()
    try:
        proc = subprocess.run(
            [sys.executable, str(script)],
            capture_output=True,
            text=True,
            timeout=timeout,
            env=env,
            cwd=str(workdir) if workdir else None,
        )
        elapsed = time.time() - start
        out = (proc.stdout or "") + (proc.stderr or "")
        return ExecResult(proc.returncode == 0, proc.returncode, out.strip(), elapsed)
    except subprocess.TimeoutExpired as exc:
        elapsed = time.time() - start
        return ExecResult(False, -1, f"timeout after {timeout}s: {exc}", elapsed)
