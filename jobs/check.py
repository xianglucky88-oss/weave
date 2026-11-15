"""示例任务：检查。校验最终产物的行数与金额守恒。"""

import json
import os
import sys
from pathlib import Path

ds = os.environ.get("WEAVE_DS", "unknown")
base = Path("/tmp/weave/data")
rows = json.loads((base / "final" / f"{ds}.json").read_text(encoding="utf-8"))

if not rows:
    print("no rows produced", file=sys.stderr)
    sys.exit(1)

src = json.loads((base / f"extract_{ds}.json").read_text(encoding="utf-8"))
if len(src) != len(rows):
    print(f"row count mismatch: {len(src)} vs {len(rows)}", file=sys.stderr)
    sys.exit(1)

print(f"check passed: {len(rows)} rows")
