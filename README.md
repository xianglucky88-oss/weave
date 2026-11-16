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

## 扩展方式

- 新的调度语义：改 `schedule.py` 的 `is_due` / `catchup_windows` / 窗口规则
- 新的重试策略：改 `retry.py` 的 `delay_for`
- 任务扇出：改 `expand.py`，`runner` 已经按实例列表执行
- 新的存储后端：`store.py` 的接口保持不变即可替换

## 漏跑补齐与历史重跑

补跑不是“按时间差循环调 `run_dag`”，而是一条可解释、可恢复、防重复的规则链：

```
UTC 调度窗口 → 候选窗口枚举 → 查每个窗口的最新运行 → on_exists 策略决策
            → 窗口活动锁 → 按 ds 升序执行 → 失败可恢复
```

### 窗口与 ds（全部按 UTC）

| schedule | 窗口长度 | ds 形式 | 示例 |
| --- | --- | --- | --- |
| minute | 60s | `YYYY-MM-DDTHH:MM` | `2026-11-14T10:30` |
| hour | 3600s | `YYYY-MM-DDTHH` | `2026-11-14T10` |
| day | 86400s | `YYYY-MM-DD` | `2026-11-14` |

窗口左闭右开 `[start, end)`。只有 `end <= now` 的**已关闭窗口**才会补；
当前仍开放的窗口数据不完整，会在计划里标记 `is_open=true` 并说明原因，
留给实时触发。跨 UTC 午夜会自然切到新窗口，minute/hour 同一天的不同窗口
ds 不同，不会互相覆盖。

### 两个入口

```bash
# 1) catchup：以该 DAG 最近一次运行所在窗口为锚点自动补齐（锚点本身也重新评估）
python3 -m weave catchup orders_daily --explain          # 直接执行
python3 -m weave catchup orders_daily --dry-run          # 只看计划，不执行

# 2) rerun：显式 UTC 日期范围（闭区间，可给日期或精确窗口 ds）
python3 -m weave rerun orders_daily 2026-11-10 2026-11-14 --on-exists retry
```

### `--on-exists` 策略：已存在的运行怎么处理

| 窗口最新状态 | skip | retry（默认） | force |
| --- | --- | --- | --- |
| 从未运行 | 补跑 | 补跑 | 补跑 |
| 成功 | 跳过 | 跳过 | 重新生成 |
| 失败 / 中断 | 跳过 | 重新执行 | 重新生成 |
| running 且未过期（活动锁） | 跳过（任何策略都不重复跑） | 同左 | 同左 |
| running 但超过 `--stale-after`（崩溃残留） | 跳过 | 接管后重跑 | 接管后重跑 |

### 交互约束如何落地

- **服务长期停机**：`catchup` 枚举锚点之后到当前时刻之间所有已关闭窗口，
  停机多久就补多少；`--max-windows` 限制单次计划规模，超限截断并提示分批。
- **部分窗口已成功**：retry 下成功窗口自动跳过，只补失败和漏跑的窗口。
- **窗口执行顺序**：严格按 ds 升序，保证业务日期顺序。
- **重复提交 / 服务重启**：`store.acquire_run` 在
  `(dag, ds) WHERE status='running'` 上有部分唯一索引 + 进程内锁，
  同一数据日期的并发/重复触发会被挡下（CLI 退出码 3，HTTP 409）。
- **计划中途失败恢复**：默认遇错即停并登记 `not_executed`；修复后重新触发
  同一命令，计划根据 store 现算，已成功的窗口跳过、失败的重试，幂等收敛。
  加 `--continue-on-fail` 可在单窗口失败后继续后续窗口。
- **进程崩溃**：超过 `--stale-after`（默认 3600s）仍 running 的窗口在下次
  规划时识别为 `stale`，执行前先改判 `interrupted` 再接管重跑。

### 输出如何解释每个窗口

CLI 不加 `--explain` 时输出纯 JSON（`plan.windows[*]` 每个窗口都带
`state / action / reason / run_id / 窗口起止 ISO 时间`，外加 `summary`、
`notes` 和执行报告）；加 `--explain` 输出人类可读的逐窗口说明。HTTP 接口：

```
POST /catchup/<dag>   {"on_exists":"retry","dry_run":false,"continue_on_fail":false}
POST /rerun/<dag>     {"start":"2026-11-10","end":"2026-11-14","on_exists":"retry"}
POST /run/<dag>       {"ds":"2026-11-14"}      # 同样受活动锁保护，冲突返回 409
```

