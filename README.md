# weave

轻量 DAG 调度器：解析依赖、判定就绪、执行任务、记录状态。

## 快速开始

```bash
pip install -r requirements.txt
make demo     # 跑一个正常 DAG 和一个失败 DAG
make verify   # 断言结果
make test     # 单元测试
python3 -m weave serve --port 9700   # HTTP 查询接口
```

## 结构

```
weave/dag.py        DAG 解析、校验、拓扑序
weave/ready.py      就绪判定、失败传播
weave/schedule.py   定时触发与 catchup
weave/expand.py     动态任务展开
weave/retry.py      重试与退避
weave/rerun.py      历史重跑
weave/sla.py        SLA 检查
weave/store.py      SQLite 状态存储
weave/executor.py   子进程执行任务
weave/api.py        HTTP 接口
dags/               DAG 定义
jobs/               任务脚本
```

## 写一个任务

任务就是一个 Python 脚本，通过环境变量拿到上下文：

- `WEAVE_RUN_ID`：本次运行 ID
- `WEAVE_DS`：数据日期
- `WEAVE_PARAM_<NAME>`：DAG 里声明的参数

## 查询运行与 SLA

`GET /runs` 支持以下查询参数（非法值返回 400）：

| 参数 | 含义 |
| --- | --- |
| `dag` | 按 DAG 名过滤，未知 DAG 返回空结果 |
| `status` | `running` / `success` / `failed` |
| `start_at` / `end_at` | 按 `started_at` 过滤的闭区间，epoch 秒或 ISO 8601 |
| `sla` | `violated` 只看超时运行，`ok` 只看未超时 |
| `limit` / `offset` | 分页，`limit` 1..200，默认 20；offset 越界返回空列表但保留 `total` |

响应固定带 `total / limit / offset`，每条运行内嵌 `sla` 摘要（违规项、
墙钟耗时、忙碌耗时、失败任务数）。`GET /status/<run_id>` 的明细里也带
同样的 SLA 摘要。

耗时口径（`weave/sla.py`）：

- `busy_seconds`：任务重试总耗时——每次 attempt 的耗时都计入，
  attempt 之间的退避等待不计入；SLA 按它判定。
- `wall_seconds`：首个 attempt 开始到最后一个 attempt 结束的跨度，
  并行任务的重叠时间不会被重复计算。
- `sla_seconds <= 0` 表示不考核，永远不产生违规。
- 时间戳缺失（任务仍在运行/中断）的 attempt 标记为 `incomplete`，
  全部缺失时无法判定，不误报违规。
- SLA 违规与运行状态相互独立：成功的运行也可能违规，失败的运行
  不一定违规；runner 的状态语义不被修改。

查询在独立只读快照连接里执行，查询期间新写入的运行不会改变本次
分页结果，下一次查询才能看到。

## 扩展方式

- 新的调度语义：改 `schedule.py` 的 `is_due` / `catchup_windows`
- 新的重试策略：改 `retry.py` 的 `delay_for`
- 任务扇出：改 `expand.py`，`runner` 已经按实例列表执行
- 新的存储后端：`store.py` 的接口保持不变即可替换
