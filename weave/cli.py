"""命令行入口。"""

from __future__ import annotations

import argparse
import json
import os
import signal
import sys
import threading
import time
from pathlib import Path

from . import schedule
from .api import serve
from .dag import load, validate
from .runner import run_dag
from .store import Store

ROOT = Path(__file__).resolve().parent.parent
DAGS = ROOT / "dags"
JOBS = ROOT / "jobs"

# 运行数据放在临时目录，可用 WEAVE_DB 覆盖。
DB_PATH = Path(os.environ.get("WEAVE_DB", "/tmp/weave/weave.db"))


def find_dag(name: str):
    """按 DAG 名查找文件（文件名与 name 字段不一定相同）。"""
    for p in sorted(DAGS.glob("*.yaml")):
        d = load(p)
        if d.name == name:
            return d
    raise FileNotFoundError(f"DAG {name!r} not found in {DAGS}")


def cmd_list(_args) -> int:
    for p in sorted(DAGS.glob("*.yaml")):
        d = load(p)
        problems = validate(d)
        status = "OK" if not problems else "INVALID: " + "; ".join(problems)
        print(f"{d.name:24} schedule={d.schedule or '-':8} tasks={len(d.tasks):2}  {status}")
    return 0


def cmd_validate(args) -> int:
    d = find_dag(args.dag)
    problems = validate(d)
    if problems:
        print("INVALID:", problems)
        return 1
    print(f"OK {d.name} 拓扑序: {' -> '.join(d.order())}")
    return 0


def cmd_run(args) -> int:
    d = find_dag(args.dag)
    problems = validate(d)
    if problems:
        print("INVALID:", problems, file=sys.stderr)
        return 1
    store = Store(DB_PATH)
    ds = args.ds or schedule.ds_of(time.time())

    # SIGTERM/SIGINT 置位停止事件：runner 会中断退避、终止在跑的子进程，
    # 并把 run 收敛到 canceled，而不是被强杀后留下残留进程和悬挂状态。
    shutdown = threading.Event()

    def _ask_stop(signum, _frame):
        print(f"\nreceived signal {signum}, shutting down ...", file=sys.stderr)
        shutdown.set()

    signal.signal(signal.SIGTERM, _ask_stop)
    signal.signal(signal.SIGINT, _ask_stop)

    run_id, statuses = run_dag(d, store, ds, JOBS, timeout=args.timeout,
                               max_output=args.max_output, shutdown=shutdown)
    print(json.dumps({"run_id": run_id, "ds": ds, "statuses": statuses},
                     ensure_ascii=False, indent=2))
    if shutdown.is_set():
        return 130
    return 0 if all(v == "success" for v in statuses.values()) else 1


def cmd_serve(args) -> int:
    store = Store(DB_PATH)
    serve(store, DAGS, JOBS, port=args.port)
    return 0


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="weave")
    sub = ap.add_subparsers(dest="cmd", required=True)

    sub.add_parser("list").set_defaults(func=cmd_list)

    v = sub.add_parser("validate")
    v.add_argument("dag")
    v.set_defaults(func=cmd_validate)

    r = sub.add_parser("run")
    r.add_argument("dag")
    r.add_argument("--ds", default="")
    r.add_argument("--timeout", type=int, default=60)
    r.add_argument("--max-output", type=int, default=None,
                   help="单个任务保留的输出字节上限，默认由执行层决定(64KiB)")
    r.set_defaults(func=cmd_run)

    s = sub.add_parser("serve")
    s.add_argument("--port", type=int, default=9700)
    s.set_defaults(func=cmd_serve)

    args = ap.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
