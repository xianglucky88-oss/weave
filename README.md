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
weave/dag.py        DAG 解析、校验、拓扑序（含重试/退避/超时/输出上限配置）
weave/ready.py      就绪判定、失败递归传播
weave/schedule.py   定时触发与 catchup
weave/expand.py     动态任务展开
weave/retry.py      重试策略、指数退避、失败分类与重试引擎
weave/rerun.py      历史重跑
weave/sla.py        SLA 检查
weave/store.py      SQLite 状态存储（run_id 去重、尝试记录、旧库迁移）
weave/executor.py   子进程执行：五类失败语义、进程组清理、有界输出
weave/api.py        HTTP 接口（带停止事件）
dags/               DAG 定义
jobs/               任务脚本
```

## 失败语义协议

执行层（`executor`）对每次尝试返回结构化的 `ExecResult`，用 `FailureKind`
区分结局，而不是只给一个 `ok` 布尔值：

| kind        | 含义                                          | 默认是否重试 |
|-------------|-----------------------------------------------|--------------|
| `success`   | 退出码 0                                      | —            |
| `spawn`     | 进程未启动（解释器不可用等 OSError）          | 否           |
| `exit_code` | 正常结束但退出码非 0                          | 是           |
| `signal`    | 被外部信号杀死（记录具体信号）                | 是           |
| `timeout`   | 超时后杀死整个进程组并回收                    | 是           |
| `canceled`  | 服务停止事件置位，主动终止                    | 否           |

超时与取消都通过 `start_new_session=True` 让子进程成为新进程组组长，
再 `SIGTERM` 整组、宽限期后 `SIGKILL` 整组并 `wait` 回收，孙进程不会残留。
输出由后台线程持续抽干管道、只保留前 `max_output` 字节，并置
`output_truncated` 标记，长输出不会撑爆内存或堵死管道。

## 重试策略如何从配置传到执行

```
dags/*.yaml（retries/backoff_*/fatal_exit_codes/timeout/max_output）
  -> dag.Task 字段（load 解析，带默认值与下限保护）
  -> retry.policy_for(task, 入口 timeout/max_output) 生成不可变 RetryPolicy
  -> retry.run_with_retry 每次尝试把 policy.timeout / policy.max_output
     透传给 executor.run_task，并按 ExecResult.kind 决定是否再来一次
  -> 每次尝试经 on_attempt 回调落库（kind/exit_code/signal/seconds 等）
```

为什么不可重试错误不会被无限放大：

1. `max_attempts = 1 + retries` 是硬上限，引擎最多调用这么多次 execute；
2. `spawn` / `canceled` 直接终止——重试无意义，或服务正在停不应再拉起进程；
3. `fatal_exit_codes`（如 2、127 这类参数/语法错误）命中即停；
4. 退避被 `backoff_max` 截断、带抖动，总尝试次数与耗时都有确定上界；
5. 每次尝试都落库，最终失败的任务为 `failed`，下游递归 `skipped`，
   run 一定收敛到 `success` / `failed` / `canceled` 终态。

退避期间收到 SIGTERM/SIGINT：停止事件打断分片睡眠，不再发起新尝试，
在跑的子进程被整组终止，run 落为 `canceled`，PENDING 下游统一跳过。

## 写一个任务

任务就是一个 Python 脚本，通过环境变量拿到上下文：

- `WEAVE_RUN_ID`：本次运行 ID
- `WEAVE_DS`：数据日期
- `WEAVE_PARAM_<NAME>`：DAG 里声明的参数

## 扩展方式

- 新的调度语义：改 `schedule.py` 的 `is_due` / `catchup_windows`
- 新的重试策略：改 `retry.py` 的 `RetryPolicy.backoff_for` / `retryable`，
  或在 YAML 里按任务调整 `backoff_*` 与 `fatal_exit_codes`
- 任务扇出：改 `expand.py`，`runner` 已经按实例列表执行
- 新的存储后端：`store.py` 的接口保持不变即可替换
