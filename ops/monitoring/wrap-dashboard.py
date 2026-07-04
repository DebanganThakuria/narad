#!/usr/bin/env python3
import json
import sys


def main() -> int:
    if len(sys.argv) != 3:
        print("usage: wrap-dashboard.py <dashboard.json> <payload.json>", file=sys.stderr)
        return 2
    with open(sys.argv[1], "r", encoding="utf-8") as src:
        dashboard = json.load(src)
    payload = {
        "dashboard": dashboard,
        "overwrite": True,
        "folderId": 0,
    }
    with open(sys.argv[2], "w", encoding="utf-8") as dst:
        json.dump(payload, dst)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
