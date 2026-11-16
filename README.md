# weave

轻量 DAG 调度器：解析依赖、判定就绪、执行任务、记录状态。

## 快速开始

```bash
pip install -r requirements.txt
make demo     # 跑一个正常 DAG 和一个失败 DAG
make verify   # 断言结果
make test     # 单元测试
make check    # 发布前检查：校验 dags/ 下全部定义
python3 -m weave serve --port 9700   # HTTP 查询接口
```

## 发布前检查契约

`weave validate` 不执行任务，对 DAG 定义做阶段化校验，供发布流程卡点：

```bash
python3 -m weave validate              # 检查 dags/ 下全部定义（= --all）
python3 -m weave validate orders_daily # 按 DAG 名检查
python3 -m weave validate --file dags/orders.yaml
python3 -m weave validate --format json   # 机器可读输出
python3 -m weave validate --strict        # 警告也视为不通过
```

**退出码**（自动化发布据此阻断）：

| 退出码 | 含义 |
|--------|------|
| 0 | 定义合法（允许 warning） |
| 1 | 定义非法（存在 error 级诊断） |
| 2 | 检查本身无法执行（参数错误、文件不存在、DAG 名未找到） |

**阶段流水线**：`parse → structure → deps → scripts → topology`。
阶段内聚合全部错误；阶段间按依赖短路——结构没修好时不报依赖问题，
依赖没修好时不给拓扑序，避免误导修复顺序。`scripts` 只依赖 `structure`，
依赖坏了也会照常报告脚本问题。输出按阶段分组，自上而下就是修复顺序。

**取舍决策**（固定为契约，不逐案讨论）：

- 脚本路径一律相对 jobs 根目录解析；绝对路径（`E_SCRIPT_ABSOLUTE`）与
  `..` 逃逸（`E_SCRIPT_ESCAPE`）都是 error。
- 重复依赖是 error（`E_DUP_DEP`，几乎肯定是笔误）；空 DAG 是 error（`E_NO_TASKS`）。
- 孤立任务（无依赖、无下游）是 warning（`W_ISOLATED`）；单任务 DAG 不报。
- 依赖环给出具体路径（`E_CYCLE: a -> c -> b -> a`，箭头表示「依赖于」）。
- YAML 类型错误定位到任务与字段（`E_TASK_NAME`、`E_DEPS_TYPE`、`E_RETRIES_TYPE` 等）。
- 未知字段是 warning（`W_UNKNOWN_FIELD`）——`depend_on` 之类的拼写错误
  以前会被静默忽略。
- 拓扑摘要的顺序与 runner 实际执行顺序一致（同一套就绪循环），
  「检查通过的顺序 == 运行顺序」。

**错误码**（稳定，供自动化分类；`E_` 为 error，`W_` 为 warning）：

| 阶段 | 错误码 |
|------|--------|
| parse | `E_READ` `E_YAML` `E_ROOT_TYPE` |
| structure | `E_DAG_NAME` `E_SCHEDULE_TYPE` `E_SCHEDULE_VALUE` `E_TASKS_MISSING` `E_TASKS_TYPE` `E_NO_TASKS` `E_TASK_TYPE` `E_TASK_NAME` `E_TASK_SCRIPT` `E_DEPS_TYPE` `E_RETRIES_TYPE` `E_RETRIES_RANGE` `E_SLA_TYPE` `E_SLA_RANGE` `E_PARAMS_TYPE` `E_PARAMS_KEY` `E_PARAMS_VALUE` `W_UNKNOWN_FIELD` |
| deps | `E_DUP_NAME` `E_DUP_DEP` `E_SELF_DEP` `E_UNKNOWN_DEP` `E_CYCLE` |
| scripts | `E_JOBS_DIR` `E_SCRIPT_ABSOLUTE` `E_SCRIPT_ESCAPE` `E_SCRIPT_MISSING` `E_SCRIPT_NOT_FILE` `W_SCRIPT_SUFFIX` |
| topology | `W_ISOLATED` |
| 跨文件 | `E_DUP_DAG`（多个文件定义同名 DAG，`--all` 时检查） |

`run` 子命令同样先过这套检查，定义非法直接拒绝执行（退出码 1）。


## 结构

```
weave/dag.py        DAG 数据结构、拓扑序（与 runner 同构）
weave/check.py      发布前检查契约：阶段化校验、诊断、报告
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

- 新的调度语义：改 `schedule.py` 的 `is_due` / `catchup_windows`
- 新的重试策略：改 `retry.py` 的 `delay_for`
- 任务扇出：改 `expand.py`，`runner` 已经按实例列表执行
- 新的存储后端：`store.py` 的接口保持不变即可替换
