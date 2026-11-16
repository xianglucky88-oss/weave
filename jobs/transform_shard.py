"""示例任务：按分区转换。每个实例只处理自己的 ids。"""

import json
import os
from pathlib import Path

ds = os.environ.get("WEAVE_DS", "unknown")
shard = os.environ["WEAVE_PARAM_SHARD"]
base = Path("/tmp/weave/data")

raw = json.loads((base / f"shards_{ds}.json").read_text(encoding="utf-8"))
ids = next(p["ids"] for p in raw if p["shard"] == shard)
rows = json.loads((base / f"extract_{ds}.json").read_text(encoding="utf-8"))
selected = [r for r in rows if r["id"] in ids]
for r in selected:
    r["amount_with_tax"] = round(r["amount"] * 1.06, 2)

out = base / "shards"
out.mkdir(parents=True, exist_ok=True)
(out / f"{ds}_{shard}.json").write_text(json.dumps(selected), encoding="utf-8")
print(f"transformed shard {shard}: {len(selected)} rows")
