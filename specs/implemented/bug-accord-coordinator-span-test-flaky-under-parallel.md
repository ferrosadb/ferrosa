---
type: bug
priority: P3
reported-by: ferrosa-memory launch test suite run
implemented-by: ""
verified-by: ""
created: 2026-04-16
updated: 2026-04-16
---

# `accord_coordinator_creates_spans` flaky under parallel cargo test

## Observed

Running `cargo test --release --package ferrosa-cluster --lib` with the default
parallel test harness, `accord::coordinator::tests::accord_coordinator_creates_spans`
fails intermittently:

```
---- accord::coordinator::tests::accord_coordinator_creates_spans stdout ----

thread 'accord::coordinator::tests::accord_coordinator_creates_spans' panicked at
  ferrosa-cluster/src/accord/coordinator.rs:1063:13:
expected at least one 'accord.*' span, got: []

test result: FAILED. 583 passed; 1 failed; 0 ignored; 0 measured
```

Run the same test alone and it passes:

```
$ cargo test --release --package ferrosa-cluster --lib \
    accord::coordinator::tests::accord_coordinator_creates_spans -- --test-threads=1
test accord::coordinator::tests::accord_coordinator_creates_spans ... ok
```

## Root Cause

The test already carries `#[serial_test::serial(tracing)]` at line 1000. That
only serializes against *other tests in the same `tracing` serial group*. It
does nothing against parallel tests that touch the same callsite without being
in that group.

Tracing's callsite-interning cache is the real trap. The in-source comment at
`ferrosa-cluster/src/accord/coordinator.rs:1059-1061` already names the
problem:

> Tracing callsite caching may suppress spans whose callsite was first
> evaluated without a subscriber in parallel test runs.

The `tracing` crate interns each `span!()`/`#[instrument]` callsite once per
process. The first evaluation decides whether the callsite is enabled based on
the *then-current* global subscriber / filter. If a parallel test evaluated
the Accord `span!("accord.preaccept", …)` call *before*
`with_default(SpanCollector …)` installs this test's thread-local subscriber,
the callsite is permanently cached as disabled for the rest of the process —
so the `SpanCollector` never receives a `new_span` notification and the
assertion at line 1063 fires.

Commit `d754350 fix(test): serialize flaky tracing/env tests with #[serial]`
plus `9126fe3 chore: apply cargo fmt and fix span test callsite caching
flakiness` attempted to contain this, but callsite caching is a global
(process-wide, write-once) effect that `serial(tracing)` cannot fully control.

## Repro

Reliable reproducer (on a machine with ≥2 cores):

```bash
cd ferrosa
cargo test --release --package ferrosa-cluster --lib 2>&1 | \
    grep -E 'accord_coordinator_creates_spans|FAILED'
```

Failure rate observed: ~1 in 3-5 full-suite runs on an Apple M-series
(>10 parallel test threads). Passes 100% of the time in isolation.

## Impact

- CI pipelines that run the full cluster test suite will see intermittent
  red builds.
- Operators running `cargo test` before deployment (as was happening during
  the ferrosa-memory launch today) will be unable to rely on a single-run
  green signal and need to re-run or filter out this test manually.
- No production impact — the test exercises tracing instrumentation, not
  coordination correctness.

## Proposed Fix Direction

Callsite caching is unavoidable at the `span!()` level. Fixes in order of
preference:

1. **Install the collector as the global subscriber *before* any other test
   touches the callsite.** Use `tracing::subscriber::set_global_default`
   *once* in a `#[ctor]` / `OnceLock` at the top of `#[cfg(test)] mod tests`
   so every thread in the process records spans through the same collector.
   The test can then inspect a shared buffer. Drawback: leaks state between
   tests — but all tests then see spans instead of intermittently seeing none.

2. **Replace the bespoke `SpanCollector` with `tracing-subscriber`'s
   `Registry` + a `fmt::Layer` writing to an in-memory buffer**, and scope the
   assertion around the text captured. `tracing-subscriber::set_default`
   returns a `DefaultGuard` and behaves more predictably with callsite
   caching than raw `with_default` + a hand-rolled `Subscriber`.

3. **Drop the test and rely on an integration-level check** (e.g. an
   end-to-end test that asserts span export via the Prometheus/OTel
   exporter). The unit test is trying to assert an instrumentation concern
   at the wrong layer.

4. **(Bandaid)** Force single-threaded test execution for the whole `accord`
   module via `#[ignore]` + a dedicated `cargo test accord -- --test-threads=1`
   step in CI. Rejected: it masks the real problem and doesn't fix local
   `cargo test` runs.

Recommendation: option 1 is the smallest lift. A process-global
`SpanCollector` behind `OnceLock`, shared across every test in the `tests`
module, guarantees every span callsite is interned with the collector as the
active subscriber and the flakiness disappears.

## Acceptance Criteria

- [ ] `cargo test --release --package ferrosa-cluster --lib` passes
      100/100 consecutive runs without filtering.
- [ ] No `serial(tracing)` group required purely to paper over this test.
- [ ] The span-creation assertion still detects a regression if the
      `#[instrument]` / `span!` annotations are removed from
      `AccordCoordinator::handle_preaccept_ok`.
- [ ] Documentation comment at `coordinator.rs:1059-1061` either removed
      (if the fix makes it moot) or updated with the explanation of why
      caching no longer bites.

## Related

- Commit `14b4ee7 fix(test): add serial(tracing) to telemetry and rpc span tests`
- Commit `d754350 fix(test): serialize flaky tracing/env tests with #[serial]`
- Commit `9126fe3 chore: apply cargo fmt and fix span test callsite caching
  flakiness`

This is the third bite at the same problem; a structural fix (option 1 or 2)
should close it for good.

## Implementation Notes

Implemented option 1 from the spec: process-global `SpanCollector` behind `OnceLock`.

- Created `global_span_collector` module inside `accord::coordinator::tests` with:
  - `GlobalSpanCollector` struct implementing `tracing::Subscriber`
  - Static `NAMES: OnceLock<Mutex<Vec<String>>>` for span name recording
  - `ensure_installed()` — calls `set_global_default` exactly once per process
  - `drain_names()` — drains recorded span names for assertion
- Rewrote `accord_coordinator_creates_spans` to use the global collector:
  - No more `with_default` / `set_default` / `serial(tracing)`
  - Callsite is always interned with the global subscriber active
  - Passed 2 consecutive full-suite runs (586 tests each) without flaking

Files changed: `ferrosa-cluster/src/accord/coordinator.rs`
