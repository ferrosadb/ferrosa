import Ferrosa

/-!
# Transactions: strict serializability through Accord

> *"Distributed transactions with strict serializability via the Accord consensus protocol — not
> Paxos, not 2PC. Multi-partition atomic operations with all-or-nothing semantics."* — ferrosa.ai

Strict serializability is a property of histories: there is one serial order of the committed
transactions that explains every read and respects real time. The specification defines it the way
Jepsen's Elle checker does, proves what Accord's apply layer must do to produce it, and checks a
recorded history from a real cluster against the same definition.
-/

/-!
## What the guarantee means

A strictly serializable history is serializable, and a history can be serializable without being
strict: a read that returns an old value after a newer write has completed has a serial explanation,
but not one that respects the clock. A transaction always sees its own writes, and a read of a value
no transaction ever wrote has no explanation in any order.
-/

#check @Ferrosa.Accord.strict_implies_serializable
#check @Ferrosa.Accord.hStaleRead_serial_but_not_strict
#check @Ferrosa.Accord.hReadYourWrites_ok
#check @Ferrosa.Accord.hPhantom_not_serializable

/-!
## Why Accord produces it

A replica applies a transaction only after the transactions it depends on, and that dependency wait
is the only thing that pins the order: with the dependency removed, every interleaving becomes
admissible. With it, every permitted interleaving produces a serializable history. The strictness
half rests on the deadline buffer: every in-flight conflicting transaction with a lower timestamp is
still in the buffer when a transaction is processed, so it is recorded as a dependency.
-/

#check @Ferrosa.Accord.LogOk.dep_applied_before
#check @Ferrosa.Accord.no_deps_all_orders_admissible
#check @Ferrosa.Accord.all_scenarios_serializable
#check @Ferrosa.Accord.deadline_yields_dependency
#check @Ferrosa.Accord.missing_dependency_means_premise_failed

/-!
## Recovery

When a coordinator fails, recovery must propose the value a quorum actually chose. The correct
acceptor does.

**Does not hold in the protocol as written.** An acceptor that conflates the promised and the accepted
ballot moves the accepted ballot on a Prepare without an Accept. Recovery then proposes a value no
quorum chose, and a dropped dependency turns into a write-skew history that is not serializable.
-/

#check @Ferrosa.Accord.recovery_correct
#check @Ferrosa.Accord.recovery_wrong_value
#check @Ferrosa.Accord.write_skew_not_serializable
#check @Ferrosa.Accord.dep_guard_restores_serializability

/-!
## All or nothing, and write skew

A multi-key write decided once is all-or-nothing; statements executed independently are not. No
consistency level, however strong, prevents write skew on its own: that takes validating the read set
at commit, and a detected skew is returned to the last writer as a retryable error that names the
conflicting rows.

**Does not hold yet.** The write path does not yet capture a transaction's read set and validate it
before committing, so `SERIALIZABLE` is specified and not available; `SNAPSHOT` is, and it admits
write skew.
-/

#check @Ferrosa.Sql.Txn.one_decision_is_atomic
#check @Ferrosa.Sql.Txn.independent_execution_is_not_atomic
#check @Ferrosa.Sql.Consistency.no_consistency_level_prevents_write_skew
#check @Ferrosa.Sql.Consistency.read_set_validation_is_what_prevents_write_skew
#check @Ferrosa.Sql.Consistency.the_conflict_is_named_for_the_client
#check @Ferrosa.Sql.Txn.snapshot_isolation_permits_write_skew

/-!
## Checking a real run

> *"Elle-certified Accord list-append · 3-node RF=3 · 0 anomalies."* — ferrosa.ai

`Ferrosa.Accord.RunCheck` reads a recorded Elle history (2,400 committed transactions from a Fly
cluster) and checks it against the definitions above; it passes. The specification also states what
such a run can and cannot establish. A run whose reads are local to a datacenter that failed over
cannot establish strict serializability, and a checker that drops read-only transactions can miss the
only cycle.

**Does not hold yet.** The Jepsen suite does not yet assert strict serializability for multi-key
transactions, and its checker does not include read-only transactions in the dependency graph.
-/

#check @Ferrosa.Testing.Jepsen.a_local_read_run_cannot_establish_strict_serializability
#check @Ferrosa.Testing.Jepsen.a_read_only_transaction_can_carry_the_only_cycle
#check @Ferrosa.Testing.Jepsen.the_only_qualifying_run_shape
