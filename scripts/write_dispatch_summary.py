#!/usr/bin/env python3
import argparse
import json
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Write mirror dispatch summary")
    parser.add_argument("--due-json", required=True)
    parser.add_argument("--summary-path", required=True)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    data = json.loads(Path(args.due_json).read_text(encoding="utf-8"))
    due = data.get("due", [])
    skipped = data.get("skipped", [])
    summary = Path(args.summary_path)

    with summary.open("a", encoding="utf-8") as f:
        f.write("## Mirror dispatch summary\n\n")
        f.write(f"Generated at: {data.get('generated_at_utc', 'n/a')}\n\n")
        f.write(f"Due mirrors: {len(due)}\n")
        if due:
            f.write("\n")
            for item in due:
                f.write(
                    f"- {item['org']}/{item['repo']} ({item['cadence']} {item['timezone']})\n"
                )
        f.write(f"\nSkipped entries: {len(skipped)}\n")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
