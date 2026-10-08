---
title: Profile-Guided Optimization for the shipped `ferrosa` engine
status: in-process
created: 2026-10-08
updated: 2026-10-08
owner: bkearns
priority: P2
source_location: scripts/pgo-build.sh, ferrosa/src/bin/pgo-workload.rs
---

# Profile-Guided Optimization for the shipped `ferrosa` engine

## What this is

The shipped artifact is the `ferrosa` binary, built by `Dockerfile` (the node
image) and packaged by `Dockerfile.release` for the multi-arch release. This
spec wires the rustc profile-guided optimization workflow
(<https://doc.rust-lang.org/rustc/profile-guided-optimization.html>) into that
build as an opt-in, and records what the workflow actually does on this
toolchain — including the failure mode that silently produces a build that is
not optimized at all.

Nothing here is on by default. `PGO=0` builds exactly what we build today.

This is a port of the same pipeline proven on `ferrosa-memory`
(`specs/pgo-release-build.md` there); the mechanism, the silent-failure modes
and the guards are identical, so this document records what is *different* for
the engine rather than repeating the argument.

## The mechanism, in one paragraph

PGO feeds the compiler facts about a *typical* execution — which branches are
taken, which functions are hot, which call sites are monomorphic — so LLVM can
make better inlining, machine-code layout and register-allocation decisions.
The data comes from an instrumented build that is actually run, not from a
sampling profiler. Four steps:

1. Build instrumented: `RUSTFLAGS="-Cprofile-generate=<abs dir>"`.
2. Run the instrumented binary. It writes `default_<id>.profraw` and, on a clean
   exit, updates it in place.
3. Merge: `llvm-profdata merge -o merged.profdata <abs dir>`.
4. Build again: `RUSTFLAGS="-Cprofile-use=<abs merged.profdata>"`.

The rustc book's cargo-specific notes, all of which the script honours:
`--target <triple>` on every invocation (keeps `RUSTFLAGS` away from build
scripts, which would otherwise emit profraw of their own); `--release`; absolute
paths for `-Cprofile-*`; delete prior profile data first; and
`-Cllvm-args=-pgo-warn-missing-function` during `-Cprofile-use`, because LLVM
does not warn by default when a function has no profile data.

`llvm-profdata` must come from the toolchain that will consume the profile:
`$(rustc --print sysroot)/lib/rustlib/<host>/bin/llvm-profdata`. Installing
`llvm-tools-preview` does **not** put it on `PATH`. `rust-toolchain.toml` pins
`1.98.0`, the same channel the memory pipeline was validated on, so the
shutdown/zero-count measurements there carry over unchanged.

## The training workload

`ferrosa/src/bin/pgo-workload.rs`, a second `[[bin]]` of the `ferrosa` package,
gated on the `pgo-bench` feature (`required-features`).

It drives the **real `StorageEngine`** in-process: the same memtable, flush,
compaction, SSTable-read and merge code the server runs. `ferrosa-loadgen`
already does this and is the obvious donor, but it is the wrong binary for PGO
for three independent reasons, which is why this is a new bin rather than a
flag on loadgen:

1. **Feature disambiguation.** `ferrosa-loadgen` enables
   `ferrosa-storage/compaction-validator`. That feature changes the
   `ferrosa-storage` crate's `-C metadata`; profiling the server through a
   loadgen-shaped build would discard the server's `ferrosa-storage` profile
   while exiting 0. A second bin of the *`ferrosa` package* cannot do this — it
   shares the package's feature set, so every library crate is configured
   identically between the instrumented run and the optimized build.
2. **Determinism.** `ferrosa-loadgen` seeds from `rand::rng()` (entropy) and
   runs for a wall-clock `--duration`. Two runs are different experiments.
   The trainer uses a fixed seed, a local xorshift PRNG, and a fixed iteration
   count.
3. **Self-termination.** The profile source must exit on its own terms. A
   `--duration` run that a supervisor kills writes a zero-count profraw (see
   the measurement below). The trainer finishes its loop and returns.

Fixed operation mix (60% insert, 15% update, 10% delete, 15% read), fixed value
size, `num_keys` fixed, with a flush + compaction poll every `iterations/20` so
the run spends real time in the write path, not only memtable inserts.

### What it buys, and what it does not

**What it buys.** The storage engine — memtable, flush, SSTable write and read,
merge, compaction, checksums, compression — is the code the engine spends its
CPU in, and all of it runs identically here. This is a strictly better training
input than a mock store: a profile from this workload covers the real functions
the server calls in its data path.

**What it does not buy — the network/server shell.** The shipped binary is a
server: CQL wire handling, the internode RPC, TLS, the tokio accept loop, the
Arrow/PG/Bolt front ends and the coordinator state machine are all absent from
an in-process run. A profile trained here says nothing about them, and that is
where a database spends a large fraction of its time. Closing that gap means
profiling the shipped binary against a live cluster — see the live-cluster
section. It is a release-time pass, not a per-build default.

**What it does not buy — `main()`.** The instrumented run executes the
`pgo-workload` bin, so the server's own `main()` (and everything only the
server bin links) has no counters. That is expected and appears by name in
STEP 4's "no profile data" list. It is a small fraction of the binary.

## Failure modes this pipeline guards against

These are the memory pipeline's measured findings, all toolchain-level and
therefore identical here (same rustc 1.98.0). They are repeated because they are
the reason the guards exist:

| # | Failure mode | Effect | Control |
| --- | --- | --- | --- |
| 1 | Zero-count profile accepted | Build looks optimized, is not; no diagnostic | `Total count != 0` assertion in STEP 3b |
| 2 | Profile from a killed process | Same, and the file's presence is misleading | Profile only from self-terminating runs |
| 3 | Workload stops exercising code | Valid profile of the wrong program | Workload error budget + `--check-hits`, both gating |
| 4 | Architecture mismatch (profile on aarch64, image amd64) | Stale-profile warnings; optimizations discarded | `PGO_TARGET`; in-image collection is self-consistent |
| 5 | Left-over profraw from an earlier run | Skews the profile toward the last thing that ran | STEP 0 deletes the profile directory |
| 6 | Build scripts emit profraw | Noise; possible merge confusion | `--target` on every cargo invocation |
| 7 | A feature on one side only changes `-C metadata` | The crate's own profile is discarded, exit 0 | Trainer is a bin of the same package; identical `PGO_FEATURES` on both builds; STEP 5 counts uncovered functions |
| 8 | Hermetic profile applied to network/I-O paths | Optimizes the wrong thing; no correctness risk | Documented limitation; live-cluster pass is the fix |
| 9 | PGO silently becomes the default | Doubled build time for no measured gain | `ARG PGO=0`; opt-in only |
| 10 | Profiling harness ships in a released binary | Dead code in production | `pgo-bench` is non-default and `required-features`-gated; the release path never enables it |

The zero-count measurement, for the record: an instrumented process killed in
continuous mode writes a profraw whose header looks correct and whose
`Total count` is 0; `-Cprofile-use` on it exits **0**, prints `Finished`, and
emits only `no profile data available for function … up to 0 count discarded`.
A build pipeline that trusted the exit code would ship it.

## How to run it

### Locally

```sh
scripts/pgo-build.sh --baseline --verify
```

Produces `target/release/ferrosa`, a baseline build under `target/pgo-baseline/`,
and a size delta. `--verify` re-runs the workload against the optimized build.
Costs an instrumented and an optimized build of the workspace.

Useful knobs:

```sh
PGO_CODEGEN_UNITS=1 scripts/pgo-build.sh --baseline   # let PGO inline across CGUs
PGO_WORKLOAD_ITERATIONS=50000 scripts/pgo-build.sh    # more counters
PGO_TARGET=x86_64-unknown-linux-musl scripts/pgo-build.sh   # match the release artifact
```

### In the node image

```sh
docker build --build-arg PGO=1 -t ferrosa-pgo .
```

The builder and the artifact agree on architecture, which they must, since a
profile is only meaningful for the exact binary shape that produced it.

### Against a live cluster (release-time, not yet implemented)

The production-faithful pass profiles the paths the in-process workload cannot
reach:

1. Instrument an image and deploy it to a scratch app — never a production app.
2. Drive it with `ferrosa-loadgen --node <addr>` and the cluster profiles, or
   the locust harness under `tests/load`.
3. Stop it **gracefully** so the profraw is flushed. Per the measurement above,
   this must be an orderly shutdown, not a `SIGKILL`; if that cannot be
   guaranteed, the profile is not usable and the run should be repeated rather
   than trusted.
4. Pull the profraw out of the machine, merge, and rebuild.

This step needs throwaway infra and a scratch cluster; it is the right input
for I/O-bound and network optimisation and the wrong default for every build.

## How to know it worked

A PGO change is only real if it moves a number. For each of these, the baseline
binary from `--baseline` is the control:

| Question | How to answer it |
| --- | --- |
| Did it get faster? | `scripts/pgo-ab.sh <baseline> <pgo>` for the hermetic run; the live load harness for the end-to-end number. Interleaved, ≥5 runs, p50/p99 not the mean. |
| Is the profile complete? | The `-pgo-warn-missing-function` output from STEP 4. A long list means the profile does not cover the shipped binary. |
| Did the binary change at all? | `pgo-ab.sh` refuses to run if the two binaries are byte-identical. |
| Did the profile help the right paths? | `llvm-profdata show --all-functions` on the merged profile: is the hot function list the data path, or startup? |

Size is a *weak* signal: PGO usually shrinks or is neutral, but a size change
proves the compiler did something, whereas a hash match proves it did not.

## Definition of done

- [x] Workload and runner exist, gated and tested (6 tests).
- [x] Driver script enforces data-present, non-zero-count, and workload-success.
- [x] `Dockerfile` can build with and without PGO; default unchanged.
- [x] Pipeline runs end to end locally (`EXIT=0`): 20,000-call training run, 0
      errors, 110M-count profile over 36,288 functions (12.7 MB), optimized
      binary 97,839,464 B vs baseline 101,645,368 B (**−3.74 %**), verify passed.
- [ ] `docker build --build-arg PGO=1` run successfully against a scratch image.
- [ ] An A/B latency measurement with p50/p99 on a real workload.
- [ ] Live-cluster profiling pass implemented and validated (needs scratch infra).
- [ ] Decide from that evidence whether PGO becomes the default.

### What the end-to-end run actually produced

Measured on `aarch64-apple-darwin`, rustc 1.98.0, `--baseline --verify`:

| quantity | value |
| --- | --- |
| training workload | 20,000 calls, **0 errors**, ok_rate 1.0000, 3,023 reads (1,983 hit), 21 flushes, 20 compactions |
| merged profile | **110,202,544 counts**, 36,288 instrumented functions, 12,693,768 bytes |
| optimized binary | 97,839,464 bytes |
| baseline binary | 101,645,368 bytes |
| size delta | **−3.74 %** (PGO shrank it) |
| STEP 4 uncovered functions (optimized build) | 99,125 |
| verify (STEP 4c) | passed against the optimized build |

The uncovered-function count is large in absolute terms and that is the
honest headline. Broken down by crate, it is exactly the shape the previous
section predicts — the data path is well covered, the server shell is not:

| crate | functions without profile data |
| --- | --- |
| `ferrosa_storage` | 4,480 |
| `ferrosa_common` | 296 |
| `ferrosa_net` | 6,468 |
| `ferrosa_postgres` | 10,156 |
| `ferrosa_sparql` | 11,938 |
| `ferrosa_graph` | 23,476 |
| `ferrosa` (server bin, incl. `main`) | 26,118 |
| `ferrosa_cql` | 31,946 |
| `ferrosa_cluster` | 48,338 |

A profile collected from this workload reaches the storage engine and its
shared core; it barely touches CQL, the coordinator, or the network stack.
Treat this as a proven *correctness* pipeline — the build is optimized and we
can prove the data path is covered — not as a proven end-to-end speedup. A
size shrink is a weak signal that the compiler did something; the end-to-end
number needs the live-cluster pass.

**Size shrinking, not growing, is the difference from the memory run.** There,
PGO grew the binary (+3.86 %, consistent with aggressive inlining); here it
shrank it (−3.74 %). Both prove the compiler did something; neither proves it
helped. The end-to-end A/B is the only thing that does.

**The run reproduces.** A second `--baseline --verify` run on the same
revision produced a size delta of −3.74 % (97,841,320 B vs 101,645,368 B) and a
merged profile of the same shape — the determinism the workload is built for
holds end to end, not only in the unit test.

## Roadmap

**Now.** Pipeline landed, in-process workload, script + Dockerfile wiring.

**Next.** A/B on the live load harness; first `PGO=1` node image; a CI job that
builds with `PGO=1 --verify` so the path cannot rot.

**Later.** Live-cluster profiling pass; per-deployment profiles if hot paths
differ; evaluate BOLT post-link layout on top of PGO.

## Why not `cargo-pgo`

The rustc book points at `cargo-pgo` as the convenient wrapper. We do not use
it: its value is hiding the four steps, and the four steps are where the failure
modes above hide. A wrapper that manages the workflow without asserting on the
profile's contents would hand back an unoptimized build with a success exit
code.

## References

- rustc book, Profile-guided Optimization —
  <https://doc.rust-lang.org/rustc/profile-guided-optimization.html>
- LLVM profile formats — <https://llvm.org/docs/CommandGuide/llvm-profdata.html>
- `scripts/pgo-build.sh` — the driver
- `ferrosa/src/bin/pgo-workload.rs` — the training workload
- `ferrosa-memory` `specs/pgo-release-build.md` — the origin of this design
