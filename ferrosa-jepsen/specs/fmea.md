---
crate: ferrosa-jepsen
doc: fmea
last_updated: 2026-07-16
---

# ferrosa-jepsen — Failure Modes and Effects Analysis

| Failure mode | Effect | Detection | Mitigation / current control |
|---|---|---|---|
| T3 has no live CQL contact points | A multi-DC test could appear to run against `MockCqlSession`. | `Tier::MultiDc` returns an error before the combination starts. | Require exactly the topology's count of `FERROSA_TEST_CLUSTER_NODES`; do not provision a mock fallback for T3/T4. |
| Port-mapped nodes advertise their container port | The Scylla driver discovers addresses it cannot dial and loses its pool. | Real T3 bank setup fails while creating the keyspace. | Each T3 node sets `FERROSA_CQL_BROADCAST` to its distinct host-mapped port; `t3_topology` guards all six mappings. |
| Driver pins a coordinator absent from a new session's metadata | The load-balancing policy returns an empty plan before the workload begins. | CQL query reports an empty load-balancing plan. | Pin the second session to the live node object from the probe session rather than re-looking up a host ID before topology refresh completes. |
| WAN nemesis is selected but never executed | A report labels a faulted run that only tested normal workload behavior. | Orchestrator unit test records inject/heal calls; real runs fail on missing executor. | Run the selected non-`noop` action concurrently with workload and propagate injection/heal failures. |
| WAN command lacks network privilege, tools, or the right IP family | `dc-partition`/`dc-slow` fail instead of changing connectivity. | Command exit status is included in the run failure; the Fly validation applies and removes IPv6 partition and `tc` rules on a real machine. | T3 image installs `iptables` and `iproute2`; all six T3 services declare `NET_ADMIN`; topology test guards capability; WAN partition selects `ip6tables` for Fly private IPv6 addresses. |
| Stale Docker CLI masks a functioning Podman daemon | Local fault executor targets an unavailable Docker service. | Runtime selection fails or commands cannot start. | `container_runtime()` checks `info` and chooses a usable daemon; `FERROSA_CONTAINER_RUNTIME` can explicitly select one. |
| Local Apple Podman emulates AMD64 netfilter incompletely | `iptables`/`tc` cannot exercise a Linux WAN fault locally. | The command fails loudly with its kernel error. | Treat this as an unsupported local fault host; validate the compose path on native Linux CI or Fly, never turn the nemesis into a no-op. |

The controls above deliberately prefer a loud, diagnosable failure to a
successful-looking test that did not use its requested topology or fault.
