---
type: bug
priority: P3
status: open
created: 2026-05-10
discovered-by: 2026-05-10 secondary-thread test run with FERROSA_TEST_CLUSTER_NODES set
---

# Bug: `cluster::tests::provision_t1_cluster` hard-codes Firecracker even when `FERROSA_TEST_CLUSTER_NODES` is set

## Symptom

The gate at `ferrosa-jepsen/src/cluster.rs:222-223` accepts EITHER
`FERROSA_TEST_CLUSTER_NODES` OR `FERROSA_TEST_FIRECRACKER` — but the
test body unconditionally invokes the Firecracker provisioner:

```
called `Result::unwrap()` on an `Err` value: configure boot source
Caused by:
    firecracker API error on PUT http://localhost/boot-source:
    {"fault_message":"Boot source error: The kernel file cannot be opened: ..."}
```

A user who sets `FERROSA_TEST_CLUSTER_NODES=localhost:49042,localhost:49043,localhost:49044`
expecting to use a pre-provisioned cluster gets a Firecracker failure instead.

## Where to fix

`ferrosa-jepsen/src/cluster.rs::provision_t1_cluster` (or the function
it calls): after the gate passes, branch on which env var is set:

- If `FERROSA_TEST_CLUSTER_NODES` is set: parse the comma-separated list
  and construct a `ClusterInfo` from those addresses, skipping any
  Firecracker provisioning.
- Else (only `FERROSA_TEST_FIRECRACKER` set): call the Firecracker
  provisioner.

## Severity

P3 — a misleading error message, not a correctness issue. Users who set
the wrong env var get a worse error than a panic with setup instructions
would have given them. Doesn't affect the 5 docker tests that work.

## Acceptance

- Setting `FERROSA_TEST_CLUSTER_NODES=...` and running
  `cargo test -p ferrosa-jepsen --lib provision_t1_cluster` passes
  against the cluster on those addresses without invoking Firecracker.
