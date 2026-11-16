"""weave —— 轻量 DAG 调度器。

模块划分（每个模块对应一个可扩展点）：
    dag       DAG 数据结构与拓扑序
    check     发布前检查契约（阶段化校验、诊断、报告）
    ready     就绪判定与失败传播
    schedule  定时触发与 catchup
    expand    动态任务展开（map/reduce）
    retry     重试与退避
    rerun     历史重跑
    sla       SLA 与告警
    store     运行状态持久化
    executor  任务执行
    api       HTTP 查询接口
"""

__version__ = "0.4.0"
