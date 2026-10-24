#!/usr/bin/env python3
"""Seed isolated Cargo targets with external dependencies only; no local objects."""
import json
import os
from pathlib import Path
import shutil
import subprocess

os.environ["PATH"] = "/usr/local/cargo/bin:" + os.environ["PATH"]
source = Path("/work/baseline")
shared = Path("/work/target")
candidate = Path("/work/candidate-target")
baseline = Path("/work/baseline-target")
assert shared.is_dir() and not candidate.exists() and not baseline.exists()
shared.rename(candidate)
metadata = json.loads(subprocess.check_output(
    ["cargo", "metadata", "--no-deps", "--format-version", "1"], cwd=source, text=True
))
names = [package["name"] for package in metadata["packages"]]
env = dict(os.environ, CARGO_TARGET_DIR=str(candidate))
args = ["cargo", "clean", "--release"]
for name in names:
    args.extend(["--package", name])
subprocess.run(args, cwd=source, env=env, check=True)
# Explicitly remove workspace artifacts too: a relocated package may not match
# Cargo's package-id lookup, which is the cache hazard this script prevents.
for profile in (candidate / "debug", candidate / "release"):
    for folder in (".fingerprint", "build", "deps"):
        parent = profile / folder
        if not parent.exists():
            continue
        for path in parent.iterdir():
            name = path.name.removeprefix("lib")
            if name.startswith(("ferrosa", "xtask")):
                if path.is_dir():
                    shutil.rmtree(path)
                else:
                    path.unlink()
    shutil.rmtree(profile / "examples", ignore_errors=True)
    for path in profile.glob("ferrosa*"):
        if path.is_file():
            path.unlink()
shutil.copytree(candidate, baseline)
print(json.dumps({"isolated": True, "workspace_crates_cleaned": names,
                  "baseline_target": str(baseline), "candidate_target": str(candidate)}))
