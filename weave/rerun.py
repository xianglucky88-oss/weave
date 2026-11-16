"""历史重跑与漏跑补齐：从调度窗口到可执行、可解释的 rerun 计划。

核心链路：

    schedule 的 UTC 窗口规则
        -> 枚举候选窗口（catchup 锚点 / 用户日期范围）
        -> 查每个窗口的最新运行状态
        -> 按 on_exists 策略给出 skip / run 决策（每条都带 reason）
        -> execute_plan 按时间顺序加锁执行，支持中途失败恢复与重复提交防护

策略 on_exists：
    skip   已有终结运行（成功/失败/中断）的窗口一律跳过
    retry  成功跳过，失败/中断/崩溃残留重新执行（默认，补漏跑的常规选择）
    force  无视已有结果，全部重新生成
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable

from . import schedule
from .store import RunAlreadyRunning, Store

# 候选窗口数的安全上限，防止服务停机数年导致的超大计划。
DEFAULT_MAX_WINDOWS = 10_000

POLICIES = ("skip", "retry", "force")

# 窗口最新运行状态 -> 规范化标签。
STATE_NEVER_RUN = "never_run"
STATE_SUCCESS = "success"
STATE_FAILED = "failed"
STATE_INTERRUPTED = "interrupted"
STATE_LOCKED = "locked"          # 有正在执行且未过期的运行
STATE_STALE = "stale"            # running 但超过 stale_after，视为崩溃残留


class RerunConfigError(ValueError):
    """重跑参数不合法（日期格式、策略、调度缺失等）。"""


@dataclass
class WindowDecision:
    """单个窗口的评估结论。action 只有 run / skip 两种。"""

    ds: str
    state: str
    action: str
    reason: str
    window_start: float
    window_end: float
    run_id: str | None = None
    is_open: bool = False

    def to_dict(self) -> dict:
        d = dict(self.__dict__)
        d["window_start_iso"] = schedule.iso(self.window_start)
        d["window_end_iso"] = schedule.iso(self.window_end)
        return d


@dataclass
class RerunPlan:
    """完整的补跑/重跑计划（纯数据，可序列化输出，也可先 dry-run 再执行）。"""

    dag: str
    schedule_name: str
    mode: str                      # catchup | rerun
    on_exists: str
    now: float
    lo: float | None
    hi: float | None
    anchor_run_at: float | None
    anchor_ds: str | None
    decisions: list[WindowDecision] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)
    truncated: bool = False
    max_windows: int = DEFAULT_MAX_WINDOWS

    @property
    def run_windows(self) -> list[WindowDecision]:
        return [d for d in self.decisions if d.action == "run"]

    @property
    def skip_windows(self) -> list[WindowDecision]:
        return [d for d in self.decisions if d.action == "skip"]

    def to_dict(self) -> dict:
        return {
            "dag": self.dag,
            "schedule": self.schedule_name,
            "mode": self.mode,
            "on_exists": self.on_exists,
            "now": self.now,
            "now_iso": schedule.iso(self.now),
            "range": {
                "lo": self.lo,
                "hi": self.hi,
                "lo_iso": schedule.iso(self.lo) if self.lo is not None else None,
                "hi_iso": schedule.iso(self.hi) if self.hi is not None else None,
            },
            "anchor": {
                "last_run_at": self.anchor_run_at,
                "last_run_iso": (
                    schedule.iso(self.anchor_run_at)
                    if self.anchor_run_at is not None
                    else None
                ),
                "anchor_ds": self.anchor_ds,
            },
            "summary": {
                "windows_evaluated": len(self.decisions),
                "to_run": len(self.run_windows),
                "skipped": len(self.skip_windows),
                "truncated": self.truncated,
            },
            "notes": list(self.notes),
            "windows": [d.to_dict() for d in self.decisions],
        }


def daterange(start: str, end: str) -> list[str]:
    """把 [start, end] 展开成 UTC 日期列表（含两端）。"""
    d0 = schedule.datetime.strptime(start, schedule.DATE_FMT)
    d1 = schedule.datetime.strptime(end, schedule.DATE_FMT)
    out: list[str] = []
    while d0 <= d1:
        out.append(d0.strftime(schedule.DATE_FMT))
        d0 += schedule.timedelta(days=1)
    return out


def _classify_state(rec: dict | None, now: float, stale_after: float) -> str:
    if rec is None:
        return STATE_NEVER_RUN
    status = rec.get("status")
    if status == "running":
        return STATE_STALE if rec.get("age", 0.0) > stale_after else STATE_LOCKED
    if status in ("success", "failed", "interrupted"):
        return status
    return status or "unknown"


def _decide(
    ds: str,
    state: str,
    on_exists: str,
    rec: dict | None,
    is_open: bool,
    stale_after: float,
) -> tuple[str, str]:
    """返回 (action, reason)。"""
    if is_open:
        return (
            "skip",
            "窗口尚未关闭（UTC 边界未到），当前数据不完整，留给实时触发而非补跑",
        )
    if state == STATE_LOCKED:
        rid = rec["run_id"] if rec else "?"
        return (
            "skip",
            f"运行 {rid} 正在执行该窗口，拒绝重复提交（等其结束后再评估）",
        )
    if state == STATE_NEVER_RUN:
        return "run", "该窗口从未执行过，属于漏跑窗口，需要补跑"
    if state == STATE_STALE:
        rid = rec["run_id"] if rec else "?"
        return (
            "run",
            f"运行 {rid} 停留在 running 超过 {int(stale_after)}s，"
            "判定为进程崩溃残留，先接管（标记 interrupted）再重新执行",
        )

    rid = rec["run_id"] if rec else "?"
    if state == STATE_SUCCESS:
        if on_exists == "force":
            return "run", f"已有成功运行 {rid}，但策略=force，按要求重新生成"
        return "skip", f"已有成功运行 {rid}，策略={on_exists}，无需重复执行"
    if state in (STATE_FAILED, STATE_INTERRUPTED):
        label = "失败" if state == STATE_FAILED else "中断"
        if on_exists == "skip":
            return "skip", f"上次运行{label}（{rid}），策略=skip，保留现状不自动重试"
        if on_exists == "force":
            return "run", f"上次运行{label}（{rid}），策略=force，重新生成该窗口"
        return "run", f"上次运行{label}（{rid}），策略=retry，重新执行该窗口"
    # 未知状态：保守处理。
    if on_exists == "skip":
        return "skip", f"运行 {rid} 状态为 {state}，策略=skip，保守跳过"
    return "run", f"运行 {rid} 状态为 {state}（非成功终结态），策略={on_exists}，重新执行"


def plan_reruns(
    dag,
    store: Store,
    start: str | None = None,
    end: str | None = None,
    on_exists: str = "retry",
    now: float | None = None,
    stale_after: float | None = None,
    max_windows: int = DEFAULT_MAX_WINDOWS,
) -> RerunPlan:
    """生成补跑/重跑计划。纯规划，不执行任何任务、不写运行表。

    - 不传 start/end：catchup 模式，以该 DAG 最近一次运行所在窗口为锚点
      （锚点窗口本身也重新评估，保证失败窗口能被重试）；
    - 传 start/end：rerun 模式，范围是闭区间，接受 UTC 日期或精确窗口 ds；
    - 每个候选窗口都产出一条带 state/action/reason 的决策记录。
    """
    now = now if now is not None else schedule.time.time()
    stale_after = stale_after if stale_after is not None else store.stale_after
    if on_exists not in POLICIES:
        raise RerunConfigError(
            f"on_exists must be one of {POLICIES}, got {on_exists!r}"
        )
    sched = getattr(dag, "schedule", "")
    if not sched:
        raise RerunConfigError(f"DAG {getattr(dag, 'name', '?')!r} 没有声明 schedule")
    schedule.validate_schedule(sched)

    mode = "rerun" if (start or end) else "catchup"
    notes: list[str] = []
    anchor_at = store.last_run_at(dag.name)
    anchor_ds = (
        schedule.window_ds(sched, anchor_at) if anchor_at is not None else None
    )

    lo: float | None
    hi: float | None
    if mode == "rerun":
        if start:
            lo, _ = schedule.parse_bound(sched, start)
        elif anchor_at is not None:
            lo = schedule.window_start(sched, anchor_at)
            notes.append("未提供 start，下界回退为最近一次运行所在窗口")
        else:
            raise RerunConfigError("rerun 模式必须提供 start，或该 DAG 至少有一次历史运行")
        if end:
            _, hi = schedule.parse_bound(sched, end)
        else:
            hi = now
            notes.append("未提供 end，上界取当前时间")
        if hi <= lo:
            raise RerunConfigError("日期范围为空：end 必须晚于 start")
        if hi > now:
            notes.append("范围包含未来时间，已把上界截到当前时刻（未来窗口尚不可执行）")
            hi = now
    else:
        if anchor_at is None:
            notes.append(
                "该 DAG 从未运行过，没有补跑锚点；如需补齐历史请用 rerun 显式给出日期范围"
            )
            return RerunPlan(
                dag=dag.name, schedule_name=sched, mode=mode, on_exists=on_exists,
                now=now, lo=None, hi=None, anchor_run_at=None, anchor_ds=None,
                notes=notes, max_windows=max_windows,
            )
        # 锚点窗口本身纳入评估：它可能正是失败需要重试的那一天。
        lo = schedule.window_start(sched, anchor_at)
        hi = now

    starts = schedule.iter_window_starts(sched, lo, hi)

    # 当前仍开放的窗口单独评估（最多一个），用于解释 UTC 边界行为；
    # 只要它落在 [lo, hi) 内就纳入，哪怕范围内没有任何已关闭窗口。
    open_start = schedule.window_start(sched, now)
    include_open = lo <= open_start < hi
    if include_open:
        starts.append(open_start)

    truncated = len(starts) > max_windows
    if truncated:
        starts = starts[:max_windows]
        notes.append(
            f"候选窗口超过安全上限 {max_windows}，已截断；请缩小日期范围分批补跑"
        )
    open_in_slice = include_open and open_start in starts

    ds_values = [
        schedule.datetime.fromtimestamp(t, tz=schedule.timezone.utc).strftime(
            schedule.DS_FORMATS[sched]
        )
        for t in starts
    ]
    latest = store.latest_run_per_ds(dag.name, ds_values)

    decisions: list[WindowDecision] = []
    period = schedule.period_of(sched)
    for t, ds in zip(starts, ds_values):
        rec = latest.get(ds)
        is_open = open_in_slice and t == open_start
        state = _classify_state(rec, now, stale_after)
        action, reason = _decide(ds, state, on_exists, rec, is_open, stale_after)
        decisions.append(
            WindowDecision(
                ds=ds,
                state=state,
                action=action,
                reason=reason,
                window_start=t,
                window_end=t + period,
                run_id=rec["run_id"] if rec else None,
                is_open=is_open,
            )
        )

    if mode == "catchup" and anchor_at is not None:
        anchor_end = schedule.window_end(sched, anchor_at)
        if anchor_end > now:
            notes.append(
                "最近一次运行所在窗口尚未关闭，计划只评估该窗口，不产生新的漏跑窗口"
            )
    if not decisions:
        notes.append("范围内没有任何已关闭窗口，无需补跑")

    return RerunPlan(
        dag=dag.name, schedule_name=sched, mode=mode, on_exists=on_exists,
        now=now, lo=lo, hi=hi, anchor_run_at=anchor_at, anchor_ds=anchor_ds,
        decisions=decisions, notes=notes, truncated=truncated,
        max_windows=max_windows,
    )


# 向后兼容的旧入口：按给定日期集合与“已存在 ds 集合”规划。
def plan(dag, start: str, end: str, existing: set[str]) -> list[str]:
    """返回需要重跑的 ds 列表（day 粒度、retry 语义的便捷封装）。"""
    sched = getattr(dag, "schedule", "") or "day"
    lo, _ = schedule.parse_bound(sched, start)
    _, hi = schedule.parse_bound(sched, end)
    return [
        schedule.datetime.fromtimestamp(t, tz=schedule.timezone.utc).strftime(
            schedule.DS_FORMATS[sched]
        )
        for t in schedule.iter_window_starts(sched, lo, hi)
        if schedule.window_ds(sched, t) not in existing
    ]


@dataclass
class WindowExecution:
    ds: str
    run_id: str | None
    status: str                    # success | failed | skipped_locked | interrupted
    reason: str
    task_statuses: dict = field(default_factory=dict)


@dataclass
class ExecutionReport:
    dag: str
    mode: str
    on_exists: str
    entries: list[WindowExecution]
    stopped_early: bool
    stop_reason: str | None
    not_executed: list[str]

    @property
    def summary(self) -> dict:
        return {
            "executed": sum(
                1 for e in self.entries
                if e.status in ("success", "failed", "interrupted")),
            "succeeded": sum(1 for e in self.entries if e.status == "success"),
            "failed": sum(1 for e in self.entries if e.status == "failed"),
            "skipped_locked": sum(
                1 for e in self.entries if e.status == "skipped_locked"),
            "interrupted": sum(
                1 for e in self.entries if e.status == "interrupted"),
            "not_executed": len(self.not_executed),
        }

    def to_dict(self) -> dict:
        return {
            "dag": self.dag,
            "mode": self.mode,
            "on_exists": self.on_exists,
            "stopped_early": self.stopped_early,
            "stop_reason": self.stop_reason,
            "summary": self.summary,
            "results": [e.__dict__ for e in self.entries],
            "not_executed": list(self.not_executed),
        }


def execute_plan(
    plan: RerunPlan,
    dag,
    store: Store,
    jobs_dir,
    timeout: int = 60,
    continue_on_fail: bool = False,
    runner: Callable | None = None,
) -> ExecutionReport:
    """按窗口时间升序执行计划里 action=run 的窗口。

    交互约束：
      - 每个窗口先 acquire_run 拿活动锁，重复提交/并发触发直接被挡下；
      - 崩溃残留的 running 窗口由 acquire_run 接管后重跑；
      - 默认遇错即停（continue_on_fail=False），剩余窗口留待下次调用——
        而计划是根据 store 现算的，重新触发会自动跳过已成功的窗口，
        从而实现幂等的“计划中途失败恢复”；
      - 窗口严格按 ds 升序执行，保证补数顺序与业务日期一致。
    """
    from .runner import run_dag_with_run_id

    runner = runner or run_dag_with_run_id
    entries: list[WindowExecution] = []
    not_executed: list[str] = []
    stopped_early = False
    stop_reason: str | None = None

    for dec in plan.decisions:
        if dec.action != "run":
            continue
        if stopped_early:
            not_executed.append(dec.ds)
            continue
        try:
            run_id = store.acquire_run(plan.dag, dec.ds)
        except RunAlreadyRunning as exc:
            # 计划生成后、执行前被别的触发抢先占锁：不重复跑。
            entries.append(
                WindowExecution(
                    ds=dec.ds, run_id=exc.run_id, status="skipped_locked",
                    reason=f"窗口已被运行 {exc.run_id} 占用，本次触发跳过",
                )
            )
            continue

        try:
            _rid, statuses = runner(dag, store, run_id, dec.ds, jobs_dir, timeout)
        except Exception as exc:  # 执行器自身崩溃：释放窗口锁，便于下次接管
            store.mark_run_interrupted(run_id)
            entries.append(
                WindowExecution(
                    ds=dec.ds, run_id=run_id, status="interrupted",
                    reason=f"执行器异常中断，已释放窗口锁：{exc!r}",
                )
            )
            stopped_early = True
            stop_reason = f"窗口 {dec.ds} 执行器异常：{exc!r}"
            continue

        overall = "success" if all(v == "success" for v in statuses.values()) else "failed"
        entries.append(
            WindowExecution(
                ds=dec.ds, run_id=run_id, status=overall,
                reason="补跑执行完成" if overall == "success" else "补跑执行失败",
                task_statuses=statuses,
            )
        )
        if overall == "failed" and not continue_on_fail:
            stopped_early = True
            stop_reason = f"窗口 {dec.ds} 执行失败，按默认策略停止后续窗口"

    if stopped_early and not_executed:
        plan.notes.append(
            "存在未执行窗口；修复后重新触发同一命令即可，已成功的窗口会自动跳过"
        )

    return ExecutionReport(
        dag=plan.dag, mode=plan.mode, on_exists=plan.on_exists,
        entries=entries, stopped_early=stopped_early,
        stop_reason=stop_reason, not_executed=not_executed,
    )
