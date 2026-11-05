#!/usr/bin/env python3
"""Build the pinned toolchain without concurrent writers to Dune's build tree."""
from __future__ import annotations

import argparse
import os
from pathlib import Path
import shutil
import subprocess
import tempfile


def build(source: Path) -> int:
    source = source.resolve()
    make = shutil.which("make")
    if make is None:
        raise FileNotFoundError("make is required by the pinned toolchain bootstrap")
    # Charon's default ML target has parallel `format` and `build-dev`
    # prerequisites, both of which write the same Dune tree. Select its existing
    # build-only target; retain the release Rust target and make's job count.
    # Recursive $(MAKE) may bypass PATH, so choose the target at the root rather
    # than attempting to intercept a later recursive default invocation.
    adapter = f"""#!/usr/bin/env python3
import os
from pathlib import Path
import re
import sys
args = sys.argv[1:]
if (len(args) == 3 and args[0] == '-C'
        and Path(args[1]).resolve() == Path({str(source / 'charon')!r})
        and re.fullmatch(r'-j[1-9][0-9]*', args[2])):
    args.extend(['build-charon-rust', 'build-dev-charon-ml'])
os.execv({make!r}, [{make!r}, *args])
"""
    with tempfile.TemporaryDirectory(prefix="lean-gap-make-") as directory:
        wrapper = Path(directory) / "make"
        wrapper.write_text(adapter)
        wrapper.chmod(0o755)
        env = {**os.environ, "PATH": directory + os.pathsep + os.environ["PATH"]}
        return subprocess.run([str(source / "bootstrap.sh")], env=env, check=False).returncode


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", type=Path)
    return build(parser.parse_args().source)


if __name__ == "__main__":
    raise SystemExit(main())
