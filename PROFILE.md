# Profiling Ferrosa

This guide is for testing an optimized Ferrosa process and tuning its profiling
at runtime. The diagnostic build keeps full DWARF symbols and compiles jemalloc
with heap profiling support. Heap profiling is opt-in at process startup through
`_RJEM_MALLOC_CONF`; changing its settings does not require recompiling.
The `profiling` Cargo profile and the `ferrosa/profiling` Cargo feature are
build-time choices. They provide optimized DWARF symbols and jemalloc profiler
support; they do not force a sampling rate or begin heap dumps by themselves.

## Build

Use the curated shippable feature set plus the diagnostic allocator feature:

```bash
cargo build --locked --profile profiling \
  --target x86_64-unknown-linux-musl \
  -p ferrosa --bin ferrosa \
  --features ferrosa/full,ferrosa/profiling
```

`profiling` inherits release optimizations and retains full debug information.
`ferrosa/profiling` compiles jemalloc's heap profiler into the binary. Normal
release builds do not enable that feature and continue to use the optimized,
stripped release path. Do not use `--all-features`: it includes test-only and
unfinished features.

The installer publishes the OCI archive separately for amd64 and arm64. Choose
the architecture-specific download link from the profiling-image workflow
summary, then load it with `podman load -i oci.tar` (or the OCI tool used by your
test host).

## Test runtime heap profiling

Run a short process with `_RJEM_MALLOC_CONF` set. `prof_final:true` writes a heap
profile when the process exits:

```bash
mkdir -p /tmp/ferrosa-profiles
_RJEM_MALLOC_CONF='dirty_decay_ms:0,muzzy_decay_ms:0,prof:true,prof_active:true,prof_final:true,prof_prefix:/tmp/ferrosa-profiles/ferrosa' \
  ./target/x86_64-unknown-linux-musl/profiling/ferrosa --version
ls -l /tmp/ferrosa-profiles
```

This is also a build smoke test: a profiling binary should create a `.heap`
file. The installer image job runs this check before publishing each OCI image.

For a workload test, set the same environment on the Ferrosa process, run the
representative workload, then stop it cleanly to request the final dump. In a
container, pass the value with `podman run --env _RJEM_MALLOC_CONF='...' ...`. Mount a
writable host directory at the chosen `prof_prefix` location to retain the dump
outside the container.

The build prefixes jemalloc symbols with `_rjem_`, so its environment variable
is `_RJEM_MALLOC_CONF`. Jemalloc processes environment options after Ferrosa's
link-time defaults; include `dirty_decay_ms:0,muzzy_decay_ms:0` explicitly when
you want those values to remain obvious alongside other settings.

## Runtime tunables

The settings below can be changed for another process, request, or host run
without rebuilding the binary. Jemalloc environment options apply when a new
process starts.

| Setting | What it changes | Example or default |
|---|---|---|
| `_RJEM_MALLOC_CONF` | jemalloc options; profiling support must be compiled into this diagnostic binary | `prof:true,prof_active:true,prof_final:true,prof_prefix:/tmp/ferrosa-profiles/ferrosa` |
| `prof_active` | Turns heap sampling on or off for the process | `true` |
| `lg_prof_sample` | Heap allocation sampling interval as a base-2 exponent; smaller values collect more samples and add more overhead | jemalloc default is `19` (about 512 KiB) |
| `prof_prefix` | Prefix and directory for jemalloc `.heap` files | Use a writable mounted directory |
| `prof_final` | Writes a final heap profile when the process exits | `true` for short test runs |
| `prof_gdump` | Requests additional heap dumps as allocated memory grows | Enable for growth investigations |
| `dirty_decay_ms`, `muzzy_decay_ms` | Controls how quickly freed pages are returned to the operating system | Ferrosa's link-time defaults are both `0` |
| `FERROSA_TELEMETRY_SAMPLE_RATE` | Fraction of tracing spans sampled by Ferrosa telemetry | Default `0.01` (1%); set per process |
| `/api/debug/flamechart?seconds=N` | Duration of the authenticated tracing activity chart | Default `5` seconds, capped at `60` |
| `RUST_LOG` | Runtime tracing log filter | For example, `info` or `ferrosa=debug` |

Use a lower `lg_prof_sample` value to capture more allocation events. Start with
the default when measuring workload latency, since heavier sampling can change
the result. jemalloc accepts `_RJEM_MALLOC_CONF` at startup; changes apply to a new
process and do not need a new binary.

The debug flamechart endpoint samples Ferrosa activity and renders active query
and connection labels. It is not an instruction-level CPU profile. Use Linux
`perf` for CPU samples; the host kernel and permissions control whether the test
host can collect them:

```bash
sudo perf record -F 99 --call-graph dwarf -p "$FERROSA_PID" -- sleep 60
sudo perf report
```

`-F 99` changes the CPU sampling frequency without recompiling. The profiling
binary's DWARF information lets `perf` resolve native stack frames.

## Before sharing a result

Record the OCI architecture, Ferrosa source revision shown in the workflow
summary, exact runtime environment values, workload and duration. Compare against
the same workload on the normal release image; the profiling build is meant for
diagnostics, not as a production image.
