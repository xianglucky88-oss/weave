"""重试策略。"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class RetryPolicy:
    max_attempts: int
    # 退避基数（秒）。当前没有使用。
    base_delay: float = 0.0
    # 最大退避（秒）。当前没有使用。
    max_delay: float = 0.0


def policy_for(task) -> RetryPolicy:
    """根据任务定义生成重试策略。"""
    return RetryPolicy(max_attempts=1 + max(0, task.retries))


def delay_for(policy: RetryPolicy, attempt: int) -> float:
    """第 attempt 次失败后应当等待多久。

    当前实现固定返回 0——没有退避。
    """
    return 0.0
