"""示例任务：抽取。把 ds 写成一个中间文件。"""

import json
import os
from pathlib import Path

ds = os.environ.get("WEAVE_DS", "unknown")
run_id = os.environ.get("WEAVE_RUN_ID", "unknown")

out = Path("/tmp/weave/data")
out.mkdir(parents=True, exist_ok=True)
rows = [{"id": i, "amount": 100 + i} for i in range(5)]
(out / f"extract_{ds}.json").write_text(json.dumps(rows), encoding="utf-8")
print(f"extracted {len(rows)} rows for {ds} (run {run_id})")
