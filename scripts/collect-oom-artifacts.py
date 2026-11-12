#!/usr/bin/env python3
"""Collect non-invasive Ferrosa/fmem OOM investigation artifacts.

The collector is intentionally read-only: it inspects Docker/container state,
process memory files under /proc, logs, repo identity, and compose events. It
never stops containers, restarts services, deletes data, or mutates the live
cluster.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import os
import pathlib
import re
import shutil
import subprocess
import sys
import time
from typing import Iterable, Sequence

DEFAULT_CONTAINERS = (
    "ferrosa-memory-node1-1",
    "ferrosa-memory-node2-1",
    "ferrosa-memory-node3-1",
    "ferrosa-memory-ferrosa-memory-mcp-1",
)

SECRET_NAME_RE = (
    r"(?:[A-Z0-9_]*SECRET[A-Z0-9_]*|[A-Z0-9_]*TOKEN[A-Z0-9_]*|"
    r"[A-Z0-9_]*PASSWORD[A-Z0-9_]*|[A-Z0-9_]*API[_-]?KEY[A-Z0-9_]*|password|api_token)"
)
SECRET_LINE_RE = re.compile(
    rf"(?im)^(?P<prefix>\s*[\"']?{SECRET_NAME_RE}[\"']?\s*[:=]\s*[\"']?)"
    r"(?P<value>.*?)(?P<suffix>[\"']?\s*,?\s*)$"
)
SECRET_ENV_RE = re.compile(
    rf"(?P<prefix>(?:^|[\s\"'\[,]))(?P<key>{SECRET_NAME_RE})(?P<sep>=)(?!\[REDACTED\])"
    r"(?P<value>[^\"'\s,\]]+)",
    re.IGNORECASE,
)
URL_CREDENTIAL_RE = re.compile(
    r"(?P<prefix>\b[a-z][a-z0-9+.-]*://[^/\s:@]+:)(?P<password>[^@\s/]+)(?P<suffix>@)",
    re.IGNORECASE,
)


def utc_now() -> str:
    return dt.datetime.now(dt.UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def timestamp_slug() -> str:
    return dt.datetime.now(dt.UTC).strftime("%Y%m%d-%H%M%S")


def memory_sample_header() -> list[str]:
    return [
        "timestamp_utc",
        "container",
        "status",
        "health",
        "oom_killed",
        "exit_code",
        "restart_count",
        "pid",
        "rss_kb",
        "hwm_kb",
        "vmsize_kb",
        "threads",
        "cgroup_current_bytes",
        "docker_mem_usage",
        "docker_mem_percent",
        "docker_pids",
    ]


def redact_secrets(text: str) -> str:
    """Mask common credential assignment values without changing structure."""

    redacted = URL_CREDENTIAL_RE.sub(r"\g<prefix>[REDACTED]\g<suffix>", text)
    redacted = SECRET_LINE_RE.sub(lambda m: f"{m.group('prefix')}[REDACTED]{m.group('suffix')}", redacted)
    return SECRET_ENV_RE.sub(
        lambda m: f"{m.group('prefix')}{m.group('key')}{m.group('sep')}[REDACTED]",
        redacted,
    )


def run_command(args: Sequence[str], cwd: pathlib.Path | None = None, timeout: int = 30) -> str:
    try:
        proc = subprocess.run(
            list(args),
            cwd=str(cwd) if cwd else None,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            timeout=timeout,
            check=False,
        )
        return redact_secrets(proc.stdout)
    except FileNotFoundError:
        return f"command not found: {args[0]}\n"
    except subprocess.TimeoutExpired as e:
        output = e.stdout or ""
        if isinstance(output, bytes):
            output = output.decode(errors="replace")
        return redact_secrets(output) + f"\n[TIMEOUT after {timeout}s: {' '.join(args)}]\n"


def write_text(path: pathlib.Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(redact_secrets(text), encoding="utf-8")


def docker_inspect(container: str) -> dict:
    out = run_command(["docker", "inspect", container], timeout=15)
    try:
        parsed = json.loads(out)
    except json.JSONDecodeError:
        return {"Name": container, "inspect_error": out.strip()}
    return parsed[0] if parsed else {"Name": container, "inspect_error": "empty docker inspect"}


def container_state(inspect: dict) -> dict[str, object]:
    state = inspect.get("State") or {}
    health = state.get("Health") or {}
    return {
        "status": state.get("Status", ""),
        "health": health.get("Status", ""),
        "oom_killed": state.get("OOMKilled", ""),
        "exit_code": state.get("ExitCode", ""),
        "restart_count": inspect.get("RestartCount", ""),
        "pid": state.get("Pid", 0) or 0,
    }


def proc_status(pid: int) -> dict[str, str]:
    status: dict[str, str] = {}
    if pid <= 0:
        return status
    path = pathlib.Path("/proc") / str(pid) / "status"
    try:
        for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
            key, _, value = line.partition(":")
            if key in {"VmRSS", "VmHWM", "VmSize", "Threads"}:
                status[key] = value.strip().split()[0]
    except OSError:
        pass
    return status


def cgroup_current_bytes(pid: int) -> str:
    if pid <= 0:
        return ""
    cgroup_file = pathlib.Path("/proc") / str(pid) / "cgroup"
    try:
        for line in cgroup_file.read_text(encoding="utf-8", errors="replace").splitlines():
            parts = line.split(":", 2)
            if len(parts) != 3:
                continue
            rel = parts[2].lstrip("/")
            for root in (pathlib.Path("/sys/fs/cgroup"),):
                candidate = root / rel / "memory.current"
                if candidate.exists():
                    return candidate.read_text(encoding="utf-8").strip()
    except OSError:
        return ""
    return ""


def docker_stats_by_name(containers: Sequence[str]) -> dict[str, dict[str, str]]:
    if not containers:
        return {}
    fmt = "{{json .}}"
    out = run_command(["docker", "stats", "--no-stream", "--format", fmt, *containers], timeout=20)
    stats: dict[str, dict[str, str]] = {}
    for line in out.splitlines():
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue
        name = row.get("Name") or row.get("Container")
        if name:
            stats[name] = row
    return stats


def sample_memory(containers: Sequence[str], sample_dir: pathlib.Path) -> list[list[object]]:
    sample_time = utc_now()
    stats = docker_stats_by_name(containers)
    rows: list[list[object]] = []
    for container in containers:
        inspect = docker_inspect(container)
        state = container_state(inspect)
        pid = int(state.get("pid") or 0)
        proc = proc_status(pid)
        stat = stats.get(container, {})
        if pid > 0:
            smaps = pathlib.Path("/proc") / str(pid) / "smaps_rollup"
            if smaps.exists():
                dst = sample_dir / "proc-smaps-rollup" / f"{sample_time}-{container}.txt"
                try:
                    write_text(dst, smaps.read_text(encoding="utf-8", errors="replace"))
                except OSError as e:
                    write_text(dst, f"failed to read {smaps}: {e}\n")
        rows.append(
            [
                sample_time,
                container,
                state["status"],
                state["health"],
                state["oom_killed"],
                state["exit_code"],
                state["restart_count"],
                pid,
                proc.get("VmRSS", ""),
                proc.get("VmHWM", ""),
                proc.get("VmSize", ""),
                proc.get("Threads", ""),
                cgroup_current_bytes(pid),
                stat.get("MemUsage", ""),
                stat.get("MemPerc", ""),
                stat.get("PIDs", ""),
            ]
        )
    return rows


def capture_repo_state(out_dir: pathlib.Path, repo_root: pathlib.Path, ferrosa_memory: pathlib.Path) -> None:
    sections = []
    for label, path in (("ferrosa", repo_root), ("ferrosa-memory", ferrosa_memory)):
        if not path.exists():
            sections.append(f"## {label}\nmissing: {path}\n")
            continue
        sections.append(f"## {label}\n")
        sections.append(run_command(["git", "status", "--short", "--branch"], cwd=path))
        sections.append(run_command(["git", "rev-parse", "HEAD"], cwd=path))
        sections.append(run_command(["git", "log", "--oneline", "-5", "--decorate", "--date-order"], cwd=path))
        sections.append(run_command(["git", "diff", "--stat"], cwd=path))
        sections.append("\n")
    write_text(out_dir / "repo-state.txt", "".join(sections))


def capture_static_artifacts(out_dir: pathlib.Path, containers: Sequence[str], compose_file: pathlib.Path) -> None:
    write_text(out_dir / "docker-version.txt", run_command(["docker", "version"], timeout=15))
    if compose_file.exists():
        write_text(
            out_dir / "compose-ps.txt",
            run_command(["docker", "compose", "-f", str(compose_file), "ps"], timeout=30),
        )
    inspect_all = [docker_inspect(container) for container in containers]
    write_text(out_dir / "docker-inspect.json", json.dumps(inspect_all, indent=2, sort_keys=True))
    for container in containers:
        safe = container.replace("/", "_")
        write_text(
            out_dir / "logs" / f"{safe}.log",
            run_command(["docker", "logs", "--timestamps", "--tail", "2000", container], timeout=30),
        )


def append_memory_samples(out_dir: pathlib.Path, containers: Sequence[str], samples: int, interval: float) -> None:
    csv_path = out_dir / "memory-samples.csv"
    with csv_path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(memory_sample_header())
        for index in range(samples):
            for row in sample_memory(containers, out_dir):
                writer.writerow(row)
            fh.flush()
            if index != samples - 1:
                time.sleep(interval)


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out-dir", type=pathlib.Path, default=None)
    parser.add_argument("--scenario", default="manual")
    parser.add_argument("--samples", type=int, default=1)
    parser.add_argument("--interval", type=float, default=5.0)
    parser.add_argument("--compose-file", type=pathlib.Path, default=pathlib.Path("../ferrosa-memory/docker-compose.yml"))
    parser.add_argument("--ferrosa-memory", type=pathlib.Path, default=pathlib.Path("../ferrosa-memory"))
    parser.add_argument("--container", action="append", dest="containers", default=[])
    return parser.parse_args(argv)


def main(argv: Sequence[str]) -> int:
    args = parse_args(argv)
    if args.samples < 1:
        raise SystemExit("--samples must be >= 1")
    if args.interval < 0:
        raise SystemExit("--interval must be >= 0")

    repo_root = pathlib.Path(__file__).resolve().parents[1]
    out_dir = args.out_dir or pathlib.Path("/tmp/ferrosa-oom-investigation") / f"{timestamp_slug()}-{args.scenario}"
    out_dir.mkdir(parents=True, exist_ok=True)

    containers = tuple(args.containers) if args.containers else DEFAULT_CONTAINERS
    ferrosa_memory = args.ferrosa_memory
    if not ferrosa_memory.is_absolute():
        ferrosa_memory = (repo_root / ferrosa_memory).resolve()
    compose_file = args.compose_file
    if not compose_file.is_absolute():
        compose_file = (repo_root / compose_file).resolve()

    write_text(
        out_dir / "notes.md",
        "# Ferrosa OOM artifact bundle\n\n"
        f"created_utc: {utc_now()}\n"
        f"scenario: {args.scenario}\n"
        "safety: read-only collector; no container restarts, no data deletion.\n"
        f"containers: {', '.join(containers)}\n",
    )
    capture_repo_state(out_dir, repo_root, ferrosa_memory)
    capture_static_artifacts(out_dir, containers, compose_file)
    append_memory_samples(out_dir, containers, args.samples, args.interval)

    write_text(out_dir / "DONE", f"artifact bundle complete at {utc_now()}\n")
    print(out_dir)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
