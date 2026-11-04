#!/usr/bin/env python3
"""Retain bounded extraction diagnostics without environment or generated source."""
from __future__ import annotations

import argparse
from datetime import datetime
import json
import os
from pathlib import Path
import re

MAX_RECORDS = 32
MAX_INPUT_BYTES = 1048576


def redact(text: str) -> str:
    for name, value in os.environ.items():
        if value and re.search(
                r"TOKEN|PASSWORD|SECRET|CREDENTIAL|AUTHORIZATION|API_KEY|PRIVATE_KEY|ACCESS_KEY", name, re.I):
            text = text.replace(value, "[REDACTED]")
    text = re.sub(r"((?:https?|ssh|git(?:\+https?|\+ssh)?|ftp)://)[^\s/@]+(?::[^\s/@]*)?@",
                  r"\1[REDACTED]@", text, flags=re.I)
    text = re.sub(r"(?i)\bBearer\s+[^\s\"'<>]+", "Bearer [REDACTED]", text)
    text = re.sub(r"\b(?:gh[pousr]_|github_pat_)[A-Za-z0-9_]+", "[REDACTED]", text)
    return re.sub(r"(?i)(\b(?:password|token|secret|api[_-]?key)\s*[=:]\s*)[^\s\"'<>]+",
                  r"\1[REDACTED]", text)


def timestamp(value: str) -> datetime:
    if not isinstance(value, str):
        raise ValueError("diagnostic timestamp must be text")
    parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("diagnostic timestamps require a timezone")
    return parsed


def collect(reports: Path, since_file: Path) -> dict:
    try:
        since = timestamp(since_file.read_text())
    except (OSError, ValueError):
        return {"measurementStarted": False, "records": [], "omittedRecords": 0}
    records = []
    unreadable = []
    stale = 0
    omitted = 0
    for path in sorted(reports.glob("*.json")):
        if path.is_symlink() or not path.is_file():
            continue
        try:
            if path.stat().st_size > MAX_INPUT_BYTES:
                raise ValueError("report exceeds diagnostic read limit")
            report = json.loads(path.read_text())
            if not isinstance(report, dict) or not isinstance(report.get("stage"), str):
                continue
            if timestamp(report.get("generated_at", "")) < since:
                stale += 1
                continue
        except (OSError, ValueError, TypeError):
            if len(unreadable) < MAX_RECORDS:
                unreadable.append(redact(path.name)[:255])
            continue
        # No toolchain environment, generated Lean/LLBC source or arbitrary
        # report fields. The report already stores the final 4000 compiler chars.
        if len(records) == MAX_RECORDS:
            omitted += 1
            continue
        record = {"file": redact(path.name)[:255]}
        for field in ("crate", "stage", "generated_at", "error", "log"):
            value = report.get(field)
            if isinstance(value, str):
                value = redact(value)
                record[field] = value[-4096:] if field == "log" else value[:1024 if field == "error" else 255]
        records.append(record)
    return {"measurementStarted": True, "records": records,
            "omittedRecords": omitted,
            "staleRecords": stale, "unreadableReports": unreadable}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("reports", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--since-file", required=True, type=Path)
    args = parser.parse_args()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(collect(args.reports, args.since_file), ensure_ascii=False) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
