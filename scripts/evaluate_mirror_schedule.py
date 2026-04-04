#!/usr/bin/env python3
import argparse
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import yaml
from croniter import croniter


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Evaluate mirror schedules")
    parser.add_argument("--global-config", required=True)
    parser.add_argument("--mirrors-config", required=True)
    parser.add_argument("--org", required=True)
    parser.add_argument("--now", default="", help="UTC timestamp, ISO-8601")
    return parser.parse_args()


def load_yaml(path: str) -> dict:
    p = Path(path)
    if not p.exists():
        return {}
    with p.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    if not isinstance(data, dict):
        raise ValueError(f"Invalid YAML object in {path}")
    return data


def parse_now(raw: str) -> datetime:
    if not raw:
        return datetime.now(timezone.utc).replace(second=0, microsecond=0)
    normalized = raw.replace("Z", "+00:00")
    dt = datetime.fromisoformat(normalized)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).replace(second=0, microsecond=0)


def effective_cfg(defaults: dict, override: dict) -> dict:
    cfg = dict(defaults)
    cfg.update(override or {})
    return cfg


def is_due(cadence: str, now_utc: datetime, tz_name: str) -> bool:
    zone = ZoneInfo(tz_name)
    local_now = now_utc.astimezone(zone)
    marker = local_now + timedelta(minutes=1)
    prev_run = croniter(cadence, marker).get_prev(datetime)
    return prev_run.replace(second=0, microsecond=0) == local_now.replace(second=0, microsecond=0)


def main() -> int:
    args = parse_args()
    global_cfg = load_yaml(args.global_config)
    mirrors_cfg = load_yaml(args.mirrors_config)

    defaults = global_cfg.get("defaults", {})
    if not isinstance(defaults, dict):
        raise ValueError("defaults must be an object")

    mirrors = mirrors_cfg.get("mirrors", {})
    if not isinstance(mirrors, dict):
        raise ValueError("mirrors must be an object")

    now_utc = parse_now(args.now)
    max_dispatch = int(defaults.get("max_dispatch_per_run", 50))

    due = []
    skipped = []

    for repo_name in sorted(mirrors.keys()):
        override = mirrors.get(repo_name) or {}
        if not isinstance(override, dict):
            skipped.append({"repo": repo_name, "reason": "invalid override object"})
            continue

        cfg = effective_cfg(defaults, override)
        enabled = bool(cfg.get("enabled", True))
        cadence = str(cfg.get("cadence", "")).strip()
        tz_name = str(cfg.get("timezone", "UTC")).strip() or "UTC"
        workflow_file = str(cfg.get("workflow_file", "sync-upstream.yml")).strip() or "sync-upstream.yml"
        branch = str(cfg.get("branch", "main")).strip() or "main"

        if not enabled:
            skipped.append({"repo": repo_name, "reason": "disabled"})
            continue
        if not cadence:
            skipped.append({"repo": repo_name, "reason": "missing cadence"})
            continue

        try:
            due_now = is_due(cadence, now_utc, tz_name)
        except Exception as exc:
            skipped.append({"repo": repo_name, "reason": f"invalid schedule: {exc}"})
            continue

        if not due_now:
            skipped.append({"repo": repo_name, "reason": "not due"})
            continue

        due.append(
            {
                "org": args.org,
                "repo": repo_name,
                "workflow_file": workflow_file,
                "branch": branch,
                "cadence": cadence,
                "timezone": tz_name,
            }
        )

    due = due[:max_dispatch]

    print(
        json.dumps(
            {
                "generated_at_utc": now_utc.isoformat().replace("+00:00", "Z"),
                "due": due,
                "skipped": skipped,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
