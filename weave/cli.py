"""命令行入口。"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path

from . import rerun, schedule
from .api import serve
from .dag import load, validate
from .rerun import RerunConfigError, execute_plan, plan_reruns
from .runner import run_dag
from .schedule import ScheduleError
from .store import DEFAULT_STALE_AFTER, RunAlreadyRunning, Store
from .rerun import POLICIES

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


def _open_store(args) -> Store:
    return Store(DB_PATH, stale_after=args.stale_after)


def _print_window_lines(plan) -> None:
    print(f"-- 窗口评估（共 {len(plan.decisions)} 个，"
          f"补跑 {len(plan.run_windows)} / 跳过 {len(plan.skip_windows)}）--")
    for d in plan.decisions:
        tag = "RUN " if d.action == "run" else "SKIP"
        print(f"  [{tag}] {d.ds:16} state={d.state:12} run={d.run_id or '-'}")
        print(f"         原因: {d.reason}")
    for note in plan.notes:
        print(f"  注: {note}")


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


def _canonical_ds(d, raw_ds: str, at: float) -> str:
    """把用户给的 ds（精确窗口 ds 或 UTC 日期）规范到 DAG 调度粒度。

    DAG 没有声明 schedule 时退回到 day 粒度的 UTC 日期，保持旧行为可用。
    """
    sched = d.schedule or "day"
    if raw_ds:
        lo, _ = schedule.parse_bound(sched, raw_ds)
        return schedule.window_ds(sched, lo)
    return schedule.window_ds(sched, at)


def cmd_run(args) -> int:
    d = find_dag(args.dag)
    problems = validate(d)
    if problems:
        print("INVALID:", problems, file=sys.stderr)
        return 1
    store = _open_store(args)
    try:
        ds = _canonical_ds(d, args.ds, time.time())
    except ScheduleError as exc:
        print(json.dumps({"error": str(exc)}, ensure_ascii=False), file=sys.stderr)
        return 2
    try:
        run_id, statuses = run_dag(
            d, store, ds, JOBS, timeout=args.timeout, guard=not args.no_guard
        )
    except RunAlreadyRunning as exc:
        print(json.dumps(
            {"error": "window already running", "ds": ds,
             "run_id": exc.run_id, "reason": str(exc)},
            ensure_ascii=False, indent=2), file=sys.stderr)
        return 3
    print(json.dumps({"run_id": run_id, "ds": ds, "statuses": statuses},
                     ensure_ascii=False, indent=2))
    return 0 if all(v == "success" for v in statuses.values()) else 1


def _plan_and_execute(args, start, end) -> int:
    d = find_dag(args.dag)
    problems = validate(d)
    if problems:
        print("INVALID:", problems, file=sys.stderr)
        return 1
    store = _open_store(args)
    try:
        plan = plan_reruns(
            d, store, start=start, end=end, on_exists=args.on_exists,
            stale_after=args.stale_after, max_windows=args.max_windows,
        )
    except (RerunConfigError, ScheduleError) as exc:
        print(json.dumps({"error": str(exc)}, ensure_ascii=False), file=sys.stderr)
        return 2

    if args.explain:
        _print_window_lines(plan)

    if args.dry_run:
        if args.explain:
            print(f"(dry-run) 计划补跑 {len(plan.run_windows)} 个窗口，未执行任何任务")
        else:
            payload = plan.to_dict()
            payload["dry_run"] = True
            print(json.dumps(payload, ensure_ascii=False, indent=2))
        return 0

    if not plan.run_windows:
        if args.explain:
            print("没有需要补跑的窗口")
        else:
            print(json.dumps(plan.to_dict(), ensure_ascii=False, indent=2))
        return 0

    report = execute_plan(
        plan, d, store, JOBS, timeout=args.timeout,
        continue_on_fail=args.continue_on_fail,
    )

    if args.explain:
        print("-- 执行结果 --")
        for e in report.entries:
            print(f"  [{e.status:15}] {e.ds:16} run={e.run_id or '-'}  {e.reason}")
        if report.not_executed:
            print("  未执行（等待下次触发恢复）:", ", ".join(report.not_executed))
        if report.stop_reason:
            print("  中止原因:", report.stop_reason)
        return _exit_code(report)

    out = {"plan": plan.to_dict(), "execution": report.to_dict()}
    print(json.dumps(out, ensure_ascii=False, indent=2))
    return _exit_code(report)


def _exit_code(report) -> int:
    if any(e.status in ("failed", "interrupted") for e in report.entries):
        return 1
    if report.stopped_early:
        return 1
    return 0


def cmd_catchup(args) -> int:
    return _plan_and_execute(args, start=None, end=None)


def cmd_rerun(args) -> int:
    return _plan_and_execute(args, start=args.start, end=args.end)


def cmd_serve(args) -> int:
    store = _open_store(args)
    serve(store, DAGS, JOBS, port=args.port)
    return 0


def _add_window_args(p, *, need_range: bool) -> None:
    p.add_argument("dag")
    if need_range:
        p.add_argument("start", help="起始边界（UTC 日期 YYYY-MM-DD 或精确窗口 ds，闭区间）")
        p.add_argument("end", help="结束边界（UTC 日期 YYYY-MM-DD 或精确窗口 ds，闭区间）")
    p.add_argument(
        "--on-exists", choices=POLICIES, default="retry",
        help="已有运行的处理：skip=全部跳过，retry=成功跳过/失败重试（默认），force=全部重生成",
    )
    p.add_argument("--timeout", type=int, default=60)
    p.add_argument("--stale-after", type=float, default=DEFAULT_STALE_AFTER,
                   help=f"running 超过该秒数视为崩溃残留（默认 {DEFAULT_STALE_AFTER}）")
    p.add_argument("--max-windows", type=int, default=rerun.DEFAULT_MAX_WINDOWS,
                   help="单次计划候选窗口数上限，防止停机过久产生超大计划")
    p.add_argument("--dry-run", action="store_true", help="只生成计划不执行")
    p.add_argument("--explain", action="store_true",
                   help="人类可读输出：逐窗口解释为何补跑或排除")
    p.add_argument("--continue-on-fail", action="store_true",
                   help="某窗口失败后继续后续窗口（默认遇错即停，重新触发可恢复）")


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
    r.add_argument("--stale-after", type=float, default=DEFAULT_STALE_AFTER)
    r.add_argument("--no-guard", action="store_true",
                   help="关闭窗口活动锁（不推荐，保留给旧调用方式）")
    r.set_defaults(func=cmd_run)

    c = sub.add_parser("catchup", help="按上次运行自动补齐漏掉的调度窗口")
    _add_window_args(c, need_range=False)
    c.set_defaults(func=cmd_catchup)

    rr = sub.add_parser("rerun", help="在显式 UTC 日期范围内重跑")
    _add_window_args(rr, need_range=True)
    rr.set_defaults(func=cmd_rerun)

    s = sub.add_parser("serve")
    s.add_argument("--port", type=int, default=9700)
    s.add_argument("--stale-after", type=float, default=DEFAULT_STALE_AFTER)
    s.set_defaults(func=cmd_serve)

    args = ap.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
