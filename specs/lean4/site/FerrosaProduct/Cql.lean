import Ferrosa

/-!
# CQL compatibility: what a Cassandra application can rely on

> *"Drop-in CQL compatibility. DDL, DML, prepared statements, batch operations, lightweight
> transactions (IF NOT EXISTS, IF conditions) … collection types (map, set, list) with full UPDATE
> `+` and `-` operators."* — ferrosa.ai

Wire compatibility is tested against real drivers rather than proved. What the specification proves
is the *meaning* of the statements an application sends: which statements need a read before they can
be acknowledged, what a conditional write promises when two clients race, how collections merge, and
what an aggregate returns.
-/

/-!
## Each statement takes the path it needs

A plain `INSERT` reads nothing, so it can be acknowledged on the write path alone. `INSERT … IF NOT
EXISTS`, `UPDATE … IF` and `DELETE … IF EXISTS` read the partition to test their condition, and one
conditional statement makes a whole batch take the read phase.
-/

#check @Ferrosa.Cql.qInsert_classified
#check @Ferrosa.Cql.qInsertIf_classified
#check @Ferrosa.Cql.qUpdateIf_classified
#check @Ferrosa.Cql.qBatchMixed_classified
#check @Ferrosa.Cql.conditional_txn_not_blind

/-!
## Lightweight transactions are compare-and-set

When two clients send the same condition against the same key, at most one succeeds, in either order.
The one that loses is handed back the value it tested, which is the value the next transaction will
see. A rejected write changes nothing, and a conditional update on an absent row never creates it.
The guarantee holds at the level of the replicated store as well as the single cell: `lwt_refines`
connects the two.

The protection depends on the condition being read at the transaction's serial point. A conditional
statement whose read set misses the key it tests is not protected, and two of them produce a history
with no serial explanation while both clients are told they succeeded. The classifier above is what
keeps a CQL condition inside its read set.
-/

#check @Ferrosa.Cql.Lwt.at_most_one_applies_either_order
#check @Ferrosa.Cql.Lwt.failed_cas_reports_the_state_it_tested
#check @Ferrosa.Cql.Lwt.not_applied_leaves_state
#check @Ferrosa.Cql.Lwt.update_if_on_empty_fails
#check @Ferrosa.Cql.Refine.lwt_refines
#check @Ferrosa.Cql.Lwt.unprotected_cas_is_not_serializable

/-!
## Collections: sets merge, lists do not

Adding to a set commutes, so concurrent adds survive last-write-wins merging in any order. Appending
to a list does not: two concurrent appends merged by last-write-wins keep one element where the
serial execution keeps two.

**Does not hold yet.** `tags = tags + {'x'}` has no `IF` clause, so the implemented classifier treats
it as a blind write, but it cannot be executed without the old value. The tightened classifier
`blindSem` is specified and refuses it; the code still uses the looser one.
-/

#check @Ferrosa.Cql.Collections.set_lww_is_order_free
#check @Ferrosa.Cql.Collections.list_lww_loses_an_element
#check @Ferrosa.Cql.Collections.blind_classification_is_term_blind
#check @Ferrosa.Cql.Collections.blindSem_rejects_collection_append

/-!
## Aggregates

`COUNT(*)` is exact and keeps no rows in memory. `COUNT(DISTINCT)` is exact. `COUNT(ESTIMATE_DISTINCT)`
is a proved lower bound, and there is an input on which it is strictly lower, which is why the two
names can never be swapped.

**Does not hold yet.** `COUNT(DISTINCT)` keeps one entry per distinct value, and per-shard distinct
counts cannot be summed into a cluster-wide one. The estimate has no proved error bound. The CQL parser
does not accept either distinct form yet.
-/

#check @Ferrosa.Cql.AggregateObligations.count_star_is_exact
#check @Ferrosa.Cql.Aggregate.exact_distinct_is_cardinality
#check @Ferrosa.Cql.Aggregate.estimate_le_exact
#check @Ferrosa.Cql.Aggregate.exact_and_estimate_not_interchangeable
#check @Ferrosa.Cql.AggregateObligations.sum_of_shard_counts_overcounts

/-!
## Secondary indexes

Choosing which replica to read from never drops a replica: readiness is a preference, not a filter.
Index entries are a set, so concurrent inserts of different entries commute.

**Does not hold yet.** A read through an index that has not caught up can miss a committed write, and
that history is not serializable. An index write that is not atomic with its table write lets the two
diverge.
-/

#check @Ferrosa.Cql.Index.reorder_never_filters
#check @Ferrosa.Cql.Index.index_entries_commute
#check @Ferrosa.Cql.Index.indexed_read_can_be_stale
#check @Ferrosa.Sql.Index.a_non_atomic_index_write_is_inconsistent
