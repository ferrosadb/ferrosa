#!/usr/bin/env python3
"""Run the same fresh, locked dependency checks locally and in Security CI."""

import argparse
from pathlib import Path
import subprocess
import sys

CARGO_DENY_VERSION = "0.20.2"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tool-version", action="store_true")
    args = parser.parse_args()
    if args.tool_version:
        print(CARGO_DENY_VERSION)
        return 0

    root = Path(subprocess.check_output(
        ["git", "rev-parse", "--show-toplevel"], text=True).strip())
    tracked = subprocess.check_output(
        ["git", "ls-files", "-z", "--", "Cargo.lock", "**/Cargo.lock"], cwd=root)
    locks = sorted({Path(name.decode()) for name in tracked.split(b"\0") if name})
    if not locks and not (root / "Cargo.toml").exists():
        return 0  # No Rust dependency graph in this checkout.
    if not locks or ((root / "Cargo.toml").exists() and Path("Cargo.lock") not in locks):
        print("Dependency gate: track Cargo.lock before pushing a Rust workspace.", file=sys.stderr)
        return 1
    policy = root / "deny.toml"
    if not policy.is_file():
        print("Dependency gate: deny.toml is missing. Update this checkout to the "
              "repository's CI dependency policy before pushing.", file=sys.stderr)
        return 1
    for lock in locks:
        if not (root / lock.with_name("Cargo.toml")).is_file():
            print(f"Dependency gate: no Cargo.toml beside tracked {lock}.", file=sys.stderr)
            return 1

    try:
        version = subprocess.run(["cargo", "deny", "--version"], text=True,
                                 capture_output=True, stdin=subprocess.DEVNULL)
    except FileNotFoundError:
        version = None
    if version is None or version.returncode or version.stdout.strip() != f"cargo-deny {CARGO_DENY_VERSION}":
        print(f"Dependency gate requires cargo-deny {CARGO_DENY_VERSION}, matching CI. "
              f"Install with: cargo install cargo-deny --version {CARGO_DENY_VERSION} --locked",
              file=sys.stderr)
        return 1

    for lock in locks:
        manifest = str(lock.with_name("Cargo.toml"))
        print(f"Checking dependencies: {manifest}", flush=True)
        command = ["cargo", "deny", "--manifest-path", manifest, "--config", str(policy),
                   "--all-features", "--locked"]
        # A warm local index can predate a yank that a fresh CI runner sees.
        # Refresh both data sources for every independent dependency graph.
        subprocess.run([*command, "fetch", "db", "index"], cwd=root,
                       stdin=subprocess.DEVNULL, check=True)
        subprocess.run([*command, "check", "advisories", "bans", "licenses", "sources",
                        "--hide-inclusion-graph"], cwd=root,
                       stdin=subprocess.DEVNULL, check=True)
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except subprocess.CalledProcessError as error:
        sys.exit(error.returncode if error.returncode > 0 else 1)
