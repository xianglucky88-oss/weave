import json
import sys

d = json.load(sys.stdin)
print("run:", d["run"]["status"])
for t in d["logical_tasks"]:
    print("  logical  %-10s %-8s instances=%s" % (
        t["task"], t["status"], t["instance_count"]))
for i in d["instances"]:
    print("  instance %-20s %-8s attempt=%s" % (
        i["instance_name"], i["status"], i["attempt"]))
