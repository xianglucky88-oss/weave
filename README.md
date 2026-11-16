# weave

轻量 DAG 调度器：解析依赖、判定就绪、执行任务、记录状态。

## 快速开始

```bash
pip install -r requirements.txt
make demo            # 正常 DAG、失败 DAG、动态展开 + 续跑
make verify          # 断言正常运行与展开结果
make verify-resume   # 断言部分完成 / 中断续跑的一致性
make test            # 单元测试
python3 -m weave serve --port 9700   # HTTP 查询接口
```

## 结构

```
weave/dag.py        DAG 解析、校验、拓扑序
weave/ready.py      就绪判定、实例汇总、失败传播
weave/schedule.py   定时触发与 catchup
weave/expand.py     动态任务展开（map/reduce）
weave/retry.py      重试与退避
weave/rerun.py      历史重跑
weave/sla.py        SLA 检查
weave/store.py      SQLite 状态存储
weave/executor.py   子进程执行任务
weave/api.py        HTTP 接口
dags/               DAG 定义
jobs/               任务脚本
```

## 动态展开（map/reduce）

给任务加 `expand` 声明，它就会在运行时展开成多个实例：

```yaml
tasks:
  - name: extract
    script: extract.py                 # 产出 /tmp/weave/data/shards_{ds}.json

  - name: transform
    script: transform_shard.py
    depends_on: [extract]
    params: {stage: transform}         # 每个实例都带的基础参数
    expand:
      path: /tmp/weave/data/shards_{ds}.json   # 分区来源：JSON 列表
      key: shard                                # 实例身份字段
      name: "transform[{key}]"                  # 展示名，必须含 {key}

  - name: aggregate
    script: aggregate.py
    depends_on: [transform]            # reduce：全部实例成功后才就绪
```

`expand` 也支持内联列表：`expand: [{shard: a}, {shard: b}]`
（等价于 `expand: {items: [...]}`）。

### 语义约定

- **实例命名与身份**：实例由 `key` 标识（默认依次取分区里的 `key`、`id`，
  都没有时用分区内容的哈希）。展示名默认 `<task>[<key>]`，可用
  `expand.name` 模板覆盖。身份与列表顺序无关，重排不会导致重跑。
- **参数快照**：实例参数 = 任务 `params` + 分区字段，落库时同时保存
  JSON 快照与摘要。同名实例参数变化时，旧尝试留档，按新参数重新执行。
- **空展开**：`expand` 来源为空列表是合法的；map 逻辑状态记为 success，
  reduce 正常就绪（空集合上没有工作可做）。
- **实例级失败与重试**：一个实例失败不会阻止同任务的兄弟实例执行
  （部分完成会完整记录），但逻辑任务失败、下游被跳过。重试以实例为单位，
  每次尝试都追加一条历史记录。
- **reduce 完成条件**：下游只依赖上游的**逻辑状态**；逻辑状态为 success
  当且仅当当前展开下的**全部活跃实例**最新一次尝试都成功（空展开视为成功）。
  下游可通过环境变量 `WEAVE_UPSTREAM_SUMMARY` 拿到每个上游任务的实例汇总
  （total/succeeded/failed/running/pending 与成功实例 key）。
- **同 run 续跑**：`weave run <dag> --ds <ds> --run-id <id>`（或 `--resume`
  取同 DAG、同 ds 最近一次 run）。已成功且参数未变的实例直接跳过；
  失败、未执行、参数变化的实例追加新的尝试。
- **展开结果变化**：续跑时新出现的实例执行；消失的实例标记为不活跃，
  历史保留但不参与汇总；上游输出变化会使下游重新执行。
- **中断与超时**：上次进程中断遗留的 `running` 尝试在续跑时标为失败并
  重新执行；单个实例超时只影响该实例，不会拖垮兄弟实例。

### 状态查询

```bash
python3 -m weave status <run_id>      # 逻辑任务、实例（最新尝试）、全部历史尝试
curl localhost:9700/status/<run_id>   # 同上
curl localhost:9700/tasks/<run_id>[/<task>]  # 实例级状态与汇总
```

存储分三层：`runs`（整体 run）、`logical_tasks`（逻辑任务聚合，含展开摘要
与实例数）、`task_instances`（每实例每次尝试）。旧版的 `task_runs` 表会在
打开数据库时自动迁移，并保留为兼容视图。

## 写一个任务

任务就是一个 Python 脚本，通过环境变量拿到上下文：

- `WEAVE_RUN_ID`：本次运行 ID
- `WEAVE_DS`：数据日期
- `WEAVE_PARAM_<NAME>`：DAG 里声明的参数（复合值以 JSON 表示）
- 展开实例额外有 `WEAVE_TASK`、`WEAVE_INSTANCE_KEY`、`WEAVE_INSTANCE_NAME`、
  `WEAVE_INSTANCE_INDEX`、`WEAVE_INSTANCE_COUNT`、`WEAVE_UPSTREAM_SUMMARY`

## 扩展方式

- 新的调度语义：改 `schedule.py` 的 `is_due` / `catchup_windows`
- 新的重试策略：改 `retry.py` 的 `delay_for`
- 任务扇出：改 `expand.py`；实例身份由 `TaskInstance.key` 决定
- 新的存储后端：`store.py` 的接口保持不变即可替换
