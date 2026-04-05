#!/usr/bin/env python3
import argparse
import json
import subprocess
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Dispatch due mirror workflows")
    parser.add_argument("--due-json", required=True)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    payload = json.loads(Path(args.due_json).read_text(encoding="utf-8"))
    failed = []

    for item in payload.get("due", []):
        repo = f"{item['org']}/{item['repo']}"
        cmd = [
            "gh",
            "workflow",
            "run",
            item["workflow_file"],
            "--repo",
            repo,
            "--ref",
            item["branch"],
        ]
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            failed.append({"repo": repo, "stderr": result.stderr.strip()})

    if failed:
        print("Failed dispatches:")
        for item in failed:
            print(f"- {item['repo']}: {item['stderr']}")
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
