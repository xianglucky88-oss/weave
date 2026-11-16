"""任务执行：把任务脚本跑成子进程，并给出明确的失败语义。

执行结果不再只用一个 ``ok`` 布尔值表达，而是用 ``FailureKind`` 明确分类：

- ``success``    正常退出且退出码为 0
- ``spawn``      进程根本没启动起来（脚本缺失、权限不足、解释器不可用等）
- ``exit_code``  进程正常结束，但退出码非 0
- ``signal``     进程被外部信号杀死
- ``timeout``    超过时限，由本模块杀死整个进程组后回收
- ``canceled``   服务停止事件被置位，主动杀死正在运行的进程

超时/取消时通过 ``start_new_session=True`` 让子进程成为新会话/进程组的
组长，再用 ``killpg`` 整组清理，避免只杀父进程、孙进程残留的问题。
"""

from __future__ import annotations

import os
import signal
import subprocess
import sys
import threading
import time
from dataclasses import dataclass
from enum import Enum
from pathlib import Path

# 输出捕获的默认上限（字节）与杀进程后的宽限期（秒）。
DEFAULT_MAX_OUTPUT = 64 * 1024
KILL_GRACE_SECONDS = 2.0
POLL_INTERVAL = 0.05


class FailureKind(str, Enum):
    """一次尝试的结局分类。值会直接落库，便于检索统计。"""

    SUCCESS = "success"
    SPAWN = "spawn"
    EXIT_CODE = "exit_code"
    SIGNAL = "signal"
    TIMEOUT = "timeout"
    CANCELED = "canceled"


@dataclass
class ExecResult:
    """一次子进程尝试的完整结果。

    与旧版的区别：
    - ``ok`` 之外带结构化的 ``kind``，调用方可以按类别决定是否重试；
    - ``output`` 永远有界，超长时 ``output_truncated`` 为 True 并带截断标记；
    - ``error`` 是一句话诊断，不再把整段输出塞进错误字段；
    - ``timed_out`` / ``killed`` / ``signal`` 如实记录终止过程。
    """

    ok: bool
    kind: FailureKind
    output: str
    seconds: float
    exit_code: int | None = None
    signal: int | None = None
    timed_out: bool = False
    killed: bool = False
    output_truncated: bool = False
    error: str | None = None

    def to_record(self) -> dict:
        """转成落库字段（见 store.Store.set_task）。"""
        return {
            "kind": self.kind.value,
            "exit_code": self.exit_code,
            "signal": self.signal,
            "timed_out": self.timed_out,
            "killed": self.killed,
            "output_truncated": self.output_truncated,
            "seconds": round(self.seconds, 3),
        }


class _BoundedReader(threading.Thread):
    """后台读取一个管道，只保留前 ``limit`` 字节，其余继续抽干但丢弃。

    必须持续抽干管道，否则子进程写满管道缓冲区后会阻塞，造成假死。
    """

    def __init__(self, pipe, limit: int) -> None:
        super().__init__(daemon=True)
        self._pipe = pipe
        self._limit = limit
        self._chunks: list[bytes] = []
        self._kept = 0
        self.total_bytes = 0
        self.truncated = False

    def run(self) -> None:
        try:
            while True:
                chunk = self._pipe.read(4096)
                if not chunk:
                    break
                self.total_bytes += len(chunk)
                budget = self._limit - self._kept
                if budget > 0:
                    head = chunk[:budget]
                    self._chunks.append(head)
                    self._kept += len(head)
                    if len(chunk) > len(head):
                        self.truncated = True
                else:
                    self.truncated = True
        finally:
            self._pipe.close()

    def text(self) -> str:
        raw = b"".join(self._chunks)
        text = raw.decode("utf-8", errors="replace")
        if self.truncated:
            dropped = self.total_bytes - self._kept
            text += f"\n...[output truncated: kept {self._kept} of {self.total_bytes} bytes, {dropped} dropped]..."
        return text


def _kill_process_group(proc: subprocess.Popen, grace: float = KILL_GRACE_SECONDS) -> bool:
    """先 SIGTERM 整组、宽限期后 SIGKILL 整组，并确保 reap。

    返回 True 表示确实动过杀手。进程已退出/组已不存在时返回 False。
    """
    try:
        pgid = os.getpgid(proc.pid)
    except ProcessLookupError:
        # 子进程已经退出，只需兜底 reap。
        proc.wait()
        return False

    try:
        os.killpg(pgid, signal.SIGTERM)
    except ProcessLookupError:
        proc.wait()
        return True

    try:
        proc.wait(timeout=grace)
    except subprocess.TimeoutExpired:
        # 宽限期内不退出 => SIGKILL 强杀，同样整组，再 reap 防止僵尸。
        try:
            os.killpg(pgid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        proc.wait()
    return True


def run_task(script: Path, run_id: str, ds: str, params: dict[str, str],
             timeout: int = 60, workdir: Path | None = None,
             max_output: int = DEFAULT_MAX_OUTPUT,
             cancel: threading.Event | None = None) -> ExecResult:
    """执行一个任务脚本。

    通过环境变量把运行上下文传给脚本，脚本只需读环境变量即可，
    不需要知道调度器的存在。

    - ``timeout``：秒级硬时限，超时后杀死整个进程组；
    - ``max_output``：stdout/stderr 合并保留的最大字节数，超出部分抽干丢弃；
    - ``cancel``：服务停止事件，置位时同样走整组终止流程，返回 ``canceled``。
    """
    env = os.environ.copy()
    env["WEAVE_RUN_ID"] = run_id
    env["WEAVE_DS"] = ds
    for k, v in params.items():
        env[f"WEAVE_PARAM_{k.upper()}"] = str(v)

    start = time.monotonic()
    try:
        # start_new_session：子进程成为新会话组长，pgid == pid，
        # 它派生的孙进程默认也在这个组里，killpg 可以一网打尽。
        proc = subprocess.Popen(
            [sys.executable, str(script)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=env,
            cwd=str(workdir) if workdir else None,
            start_new_session=True,
        )
    except OSError as exc:
        # 脚本不存在、没有执行权限、解释器不可用等：进程从未启动，
        # 不抛异常，交给上层按 spawn 类失败处理（不可重试）。
        return ExecResult(
            ok=False,
            kind=FailureKind.SPAWN,
            output="",
            seconds=time.monotonic() - start,
            error=f"failed to start {script.name}: {exc.__class__.__name__}: {exc}",
        )

    out_reader = _BoundedReader(proc.stdout, max_output)
    err_reader = _BoundedReader(proc.stderr, max_output)
    out_reader.start()
    err_reader.start()

    timed_out = False
    canceled = False
    deadline = start + timeout
    while True:
        rc = proc.poll()
        if rc is not None:
            break
        now = time.monotonic()
        if now >= deadline:
            timed_out = True
            break
        if cancel is not None and cancel.is_set():
            canceled = True
            break
        time.sleep(min(POLL_INTERVAL, deadline - now))

    killed = False
    if timed_out or canceled:
        killed = _kill_process_group(proc)

    # 无论正常还是被杀，都 wait 一次确保回收，不留僵尸/残留。
    proc.wait()
    out_reader.join()
    err_reader.join()
    elapsed = time.monotonic() - start

    output = (out_reader.text() + err_reader.text()).strip()
    truncated = out_reader.truncated or err_reader.truncated

    if timed_out:
        return ExecResult(
            ok=False,
            kind=FailureKind.TIMEOUT,
            output=output,
            seconds=elapsed,
            exit_code=proc.returncode,
            signal=(-proc.returncode if proc.returncode is not None and proc.returncode < 0 else None),
            timed_out=True,
            killed=killed,
            output_truncated=truncated,
            error=f"timeout after {timeout}s; process group killed (exit={proc.returncode})",
        )

    if canceled:
        return ExecResult(
            ok=False,
            kind=FailureKind.CANCELED,
            output=output,
            seconds=elapsed,
            exit_code=proc.returncode,
            signal=(-proc.returncode if proc.returncode is not None and proc.returncode < 0 else None),
            killed=killed,
            output_truncated=truncated,
            error=f"service shutdown; process group killed (exit={proc.returncode})",
        )

    if proc.returncode == 0:
        return ExecResult(
            ok=True,
            kind=FailureKind.SUCCESS,
            output=output,
            seconds=elapsed,
            exit_code=0,
            output_truncated=truncated,
        )

    if proc.returncode < 0:
        sig = -proc.returncode
        try:
            sig_name = signal.Signals(sig).name
        except ValueError:
            sig_name = str(sig)
        return ExecResult(
            ok=False,
            kind=FailureKind.SIGNAL,
            output=output,
            seconds=elapsed,
            exit_code=proc.returncode,
            signal=sig,
            output_truncated=truncated,
            error=f"killed by signal {sig} ({sig_name})",
        )

    return ExecResult(
        ok=False,
        kind=FailureKind.EXIT_CODE,
        output=output,
        seconds=elapsed,
        exit_code=proc.returncode,
        output_truncated=truncated,
        error=f"exited with non-zero code {proc.returncode}",
    )
