import Ferrosa

/-!
# Postgres, Cypher over Bolt, and SPARQL on the same data

> *"One database for AI-native workloads. Search, vectors, graph, SPARQL, transactions, and CDC —
> unified on S3."* — ferrosa.ai

Each extra front end is a second meaning for the same tables, so each has its own semantics to get
right. The specification models the parts where a front end's standard and a naive implementation
disagree, and records which side the code is on.
-/

/-!
## Postgres wire

`COUNT(*)` and `COUNT(col)` differ exactly on NULL, and a sequence needs only to be monotone, not gap
free. A single statement is atomic per partition.

**Does not hold yet.** `COMMIT` applies statements one at a time, so a multi-statement transaction is
not atomic. A naive `<>` lets NULL rows through where SQL's three-valued logic excludes them, and
`SUM` over no rows returns 0 where SQL returns NULL.
-/

#check @Ferrosa.Sql.count_star_and_count_col_disagree
#check @Ferrosa.Sql.sequence_need_only_be_monotone
#check @Ferrosa.Sql.commit_is_not_atomic_across_statements
#check @Ferrosa.Sql.null_passes_a_naive_not_equal
#check @Ferrosa.Sql.sum_of_no_rows_is_null_not_zero

/-!
## Indexes under Postgres

An equality lookup on an indexed column contacts one node and reads one record. A range lookup costs
one round trip per node, which is the accepted shape: a range cannot be served in constant time.
-/

#check @Ferrosa.Sql.Index.equality_contacts_one_node
#check @Ferrosa.Sql.Index.scatter_gather_is_the_accepted_shape

/-!
## The Bolt session

A failed Bolt session stays failed until the client sends RESET, and RESET restores the initial state
exactly. A result stream can be released without being read.

**Does not hold yet.** Replies cannot be paired to messages by position: after a failure, ignored
messages shift every later reply, so a client cannot tell which statement failed. Unconsumed result
streams queue without bound.
-/

#check @Ferrosa.Bolt.failure_is_sticky
#check @Ferrosa.Bolt.reset_clears_streams_and_failure
#check @Ferrosa.Bolt.replies_are_not_one_per_message
#check @Ferrosa.Bolt.ignored_messages_shift_every_later_reply
#check @Ferrosa.Bolt.unconsumed_runs_grow_the_queue

/-!
## SPARQL

Results are bags: a duplicated triple counts twice. Adding a triple never removes a solution, while
deleting one can, so deletions need re-evaluation rather than an incremental patch.

**Does not hold yet.** `p+` without a visited set returns a pair once per path instead of once, and a
self-loop diverges. The natural RDF-star encoding gives a quoted triple and an IRI the same store key.
-/

#check @Ferrosa.Sparql.a_duplicated_triple_double_counts
#check @Ferrosa.Sparql.adding_a_triple_never_removes_a_match
#check @Ferrosa.Sparql.closure_without_a_visited_set_duplicates_pairs
#check @Ferrosa.Sparql.encoding_is_not_injective
