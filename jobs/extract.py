"""示例任务：抽取。把 ds 写成中间文件，并产出分区清单。"""

import json
import os
from pathlib import Path

ds = os.environ.get("WEAVE_DS", "unknown")
run_id = os.environ.get("WEAVE_RUN_ID", "unknown")

out = Path("/tmp/weave/data")
out.mkdir(parents=True, exist_ok=True)
rows = [{"id": i, "amount": 100 + i} for i in range(5)]
(out / f"extract_{ds}.json").write_text(json.dumps(rows), encoding="utf-8")

# 分区清单：每行 1 个 shard，id 决定实例身份。
shards = [{"shard": f"s{n}", "ids": [i for i in range(len(rows)) if i % 3 == n]}
          for n in range(3)]
(out / f"shards_{ds}.json").write_text(json.dumps(shards), encoding="utf-8")
print(f"extracted {len(rows)} rows for {ds} in {len(shards)} shards (run {run_id})")
