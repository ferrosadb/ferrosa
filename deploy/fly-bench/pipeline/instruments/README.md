# Bounded profiling capture used by the scan experiment

`scan144_profiler.py` is the exact Linux collector used for the published
captures. It needs Python 3 and `perf`, and is separate from the timed primary
throughput probe. Use a unique label and an existing validated fixture.

The collector accepts `<target> <label>`: `postgres` and `cassandra` select native
processes by name; a Ferrosa target reads the benchmark PID from
`/tmp/pipeline_bolt_protocol_144_ferrosa.pid`. Output is
`/tmp/pipeline_bolt_protocol_144_profile_<label>/`.

1. Start the collector and wait for its `ready` file: user CPU recording is active.
2. Touch its `go` file, then release the client's timed workload barrier.
3. After the timed client operation finishes, touch the collector's `stop` file.
4. Wait for `done` before packaging. Verify the downloaded archive checksum before
   removing the remote recording and archive. Retain the local raw evidence.

CPU samples use `cpu-clock:u`, 49 Hz and callchains. Context-switch callchains
sample every 100 events for five seconds; each context data/text file is capped
at 64 MiB. CPU recording stops before either `perf script` conversion runs, so
text decoding cannot compete with the timed workload. GO and stop waits are
bounded. `capture-status.json` and `switch-capture-status.json` retain failures.

The controller used a 1 GiB free-space guard on server and client before every
measurement; `resources_low_overhead.sh` sampled free space and OS/TCP counters
at 1 Hz. Sampling overhead remains uncalibrated. Context-switch event weights
are not off-CPU time, lock wait durations or query latency. Java frames may lack
method names. Missing/failed captures must remain explicit.

The published primary observations are complete. Five PG CPU captures were
interrupted by Fly control-channel timeouts; they are not presented as completed.
