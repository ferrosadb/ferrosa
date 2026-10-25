"""Test builds optimize the interpreters and model checker tests run inside.

Two groups of tests spend almost all of their time inside a dependency, not in
ferrosa code. nextest's `compile-bound` group runs them one at a time, so in PR
CI they formed a serial tail of ~12 minutes after the rest of the suite had
finished (CI run 40017365568: 90% of tests done at 9.5 minutes, the last test
at 18.2):

* the AssemblyScript UDF tests run the `asc` compiler in QuickJS (rquickjs-sys,
  C built at -O0 in a dev profile) and compile its output with Cranelift. In
  CI, each paid the compiler's cold start in its own process: 73-132 s each.
* `pump_loom` runs loom's model checker over every thread interleaving: 214 s.

Optimizing only those dependencies, as Cargo.toml already does for the
password KDFs, changes their speed and nothing else. Measured locally in a
debug build: the five ferrosa-udf asc tests went from 52.1 s to 8.2 s (the cold
first one from 29.5 s to 2.9 s), and pump_loom from 45.7 s to 15.6 s, with no
measurable change in compile time. The tests still run on every PR.
"""

import tomllib
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
CARGO = tomllib.loads((REPO_ROOT / "Cargo.toml").read_text(encoding="utf-8"))

ENGINES = {
    # QuickJS, which runs the AssemblyScript compiler.
    "rquickjs-sys", "rquickjs-core",
    # Cranelift and wasmtime, which compile and run its wasm output.
    "cranelift-codegen", "cranelift-frontend", "regalloc2",
    "wasmtime", "wasmtime-internal-cranelift", "wasmtime-environ", "wasmparser",
    # Componentizing the module (the component:: asc tests).
    "wit-component", "wasm-encoder",
    # loom's model checker (pump_loom).
    "loom",
}


class DevProfileOptimizesTestEngines(unittest.TestCase):
    def test_every_engine_is_optimized_in_dev_and_test_builds(self):
        packages = CARGO["profile"]["dev"].get("package", {})
        missing = sorted(p for p in ENGINES if packages.get(p, {}).get("opt-level") != 3)
        self.assertEqual(missing, [], "dev-profile opt-level = 3 missing")

    def test_every_engine_is_a_real_dependency(self):
        # A misspelled package table is silently ignored by cargo.
        lock = (REPO_ROOT / "Cargo.lock").read_text(encoding="utf-8")
        for name in sorted(ENGINES):
            self.assertIn(f'name = "{name}"\n', lock, name)


if __name__ == "__main__":
    unittest.main()
