# ferrosa-sim

Deterministic simulator + TLA+ refinement check for the Ferrosa Raft
layer.  Sprint 5 (ADR-016, ADR-017).

## Determinism contract

Two `SimulatedCluster` runs constructed with the **same seed** and
the **same voter count** produce **byte-identical traces**.  The
contract holds because:

1. **No wall-clock reads.**  All time is `Tick`, an integer counter
   advanced by the event loop.  No `std::time::Instant`, no
   `tokio::time::Instant`.
2. **Single-threaded event loop.**  No `tokio::spawn`, no scheduling
   choice that depends on the OS scheduler.
3. **Seeded RNG.**  `crate::rng::SeededRng` is a splitmix64
   generator owned by the `SimulatedCluster`.  Election-timeout
   randomization is the only source of non-determinism, and it
   draws from this generator alone.
4. **Stable event ordering.**  The event queue is a
   `BinaryHeap<Scheduled>` keyed by `(deadline, monotonic seq)`.
   Two events with the same deadline fire in insertion order — never
   in pointer-address or hash order.
5. **`BTreeMap`, never `HashMap`.**  All per-node state and peer
   iteration uses ordered maps so that the spawn order of
   `RequestVote`s broadcast to peers is independent of the build's
   `RandomState` seed.

The W5.4 unit test `same_seed_produces_same_trace` pins the contract
in CI.

## Sprint 5 status

See `specs/archive/project-plans/raft-correctness-sprints/sprint-05-progress.md` for per-WI status and
the `specs/tla/raft.tla` spec the refinement check (W5.10) verifies
the simulator against.

## Why not Madsim?

ADR-017 grants the implementer authority to fall back from Madsim if
integration friction is too high.  Sprint 5 elects the in-house
fallback up-front: the headline goal of Sprint 5 is the **TLA+
refinement check**, which requires a *protocol-level* simulator, not
a full process simulator over openraft + sled.  Madsim adoption
remains an option for a future sprint if in-process integration
tests in `ferrosa-cluster/tests/` grow hard-to-reproduce flakes.
