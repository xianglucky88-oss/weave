"""命令行入口。

退出码契约（发布流程据此阻断坏定义）：

    0  定义合法（允许 warning）
    1  定义非法（存在 error 级诊断；run 子命令另含「任务执行失败」）
    2  检查本身无法执行（参数错误、文件不存在、DAG 名未找到）
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path

from . import schedule
from .api import serve
from .check import RepoReport, check_file, check_repo, find_dag_file, render_text, to_jsonable
from .runner import run_dag
from .store import Store

ROOT = Path(__file__).resolve().parent.parent
DAGS = ROOT / "dags"
JOBS = ROOT / "jobs"

# 运行数据放在临时目录，可用 WEAVE_DB 覆盖。
DB_PATH = Path(os.environ.get("WEAVE_DB", "/tmp/weave/weave.db"))

EXIT_OK = 0
EXIT_INVALID = 1
EXIT_USAGE = 2


def cmd_list(_args) -> int:
    for p in sorted(DAGS.glob("*.yaml")):
        r = check_file(p, jobs_dir=JOBS)
        name = r.dag_name or f"<{p.name} 无法解析>"
        n_tasks = len(r.dag.tasks) if r.dag else 0
        sched = (r.dag.schedule if r.dag else "") or "-"
        if not r.ok:
            status = f"INVALID ({len(r.errors)} 个错误)"
        elif r.warnings:
            status = f"OK ({len(r.warnings)} 个警告)"
        else:
            status = "OK"
        print(f"{name:24} schedule={sched:8} tasks={n_tasks:2}  {status}")
    return EXIT_OK


def cmd_validate(args) -> int:
    if args.file and args.dag:
        print("error: DAG 名与 --file 只能二选一", file=sys.stderr)
        return EXIT_USAGE
    dags_dir = Path(args.dags_dir).resolve()
    jobs_dir = Path(args.jobs_dir).resolve()
    try:
        if args.file:
            target = Path(args.file)
            if not target.is_file():
                raise FileNotFoundError(f"定义文件不存在: {target}")
            paths = [target]
        elif args.dag:
            paths = [find_dag_file(args.dag, dags_dir)]
        else:
            paths = sorted(dags_dir.glob("*.yaml"))
            if not paths:
                raise FileNotFoundError(f"{dags_dir} 中没有 .yaml 定义文件")
    except FileNotFoundError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return EXIT_USAGE

    repo = check_repo(paths, jobs_dir=jobs_dir)
    if args.format == "json":
        print(json.dumps(to_jsonable(repo), ensure_ascii=False, indent=2))
    else:
        sys.stdout.write(render_text(repo))
    if repo.errors or (args.strict and repo.warnings):
        return EXIT_INVALID
    return EXIT_OK


def cmd_run(args) -> int:
    dags_dir = Path(args.dags_dir).resolve()
    jobs_dir = Path(args.jobs_dir).resolve()
    try:
        path = find_dag_file(args.dag, dags_dir)
    except FileNotFoundError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return EXIT_USAGE
    report = check_file(path, jobs_dir=jobs_dir)
    if not report.ok:
        print(f"定义未通过发布前检查，拒绝执行 {args.dag!r}:", file=sys.stderr)
        sys.stderr.write(render_text(RepoReport(reports=[report])))
        return EXIT_INVALID
    store = Store(DB_PATH)
    ds = args.ds or schedule.ds_of(time.time())
    run_id, statuses = run_dag(report.dag, store, ds, jobs_dir, timeout=args.timeout)
    print(json.dumps({"run_id": run_id, "ds": ds, "statuses": statuses},
                     ensure_ascii=False, indent=2))
    return EXIT_OK if all(v == "success" for v in statuses.values()) else EXIT_INVALID


def cmd_serve(args) -> int:
    store = Store(DB_PATH)
    serve(store, DAGS, JOBS, port=args.port)
    return EXIT_OK


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="weave")
    sub = ap.add_subparsers(dest="cmd", required=True)

    sub.add_parser("list", help="列出所有 DAG 及其校验状态").set_defaults(func=cmd_list)

    v = sub.add_parser("validate", help="发布前检查：不执行任务，校验 DAG 定义")
    v.add_argument("dag", nargs="?", help="DAG 名（缺省时检查 --dags-dir 下全部定义）")
    v.add_argument("--file", help="直接指定定义文件")
    v.add_argument("--all", action="store_true", help="检查全部定义（缺省行为）")
    v.add_argument("--dags-dir", default=str(DAGS), help="DAG 定义目录（默认仓库 dags/）")
    v.add_argument("--jobs-dir", default=str(JOBS), help="脚本根目录（默认仓库 jobs/）")
    v.add_argument("--strict", action="store_true", help="警告也视为不通过")
    v.add_argument("--format", choices=["text", "json"], default="text",
                   help="输出格式（json 供发布流水线消费）")
    v.set_defaults(func=cmd_validate)

    r = sub.add_parser("run", help="先通过发布前检查，再执行一次 DAG")
    r.add_argument("dag")
    r.add_argument("--ds", default="")
    r.add_argument("--timeout", type=int, default=60)
    r.add_argument("--dags-dir", default=str(DAGS))
    r.add_argument("--jobs-dir", default=str(JOBS))
    r.set_defaults(func=cmd_run)

    s = sub.add_parser("serve")
    s.add_argument("--port", type=int, default=9700)
    s.set_defaults(func=cmd_serve)

    args = ap.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
