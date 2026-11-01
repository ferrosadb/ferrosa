"""Tests for the ADR-013 membership-mutation gate.

ADR-013 (specs/decisions/013-membership-change-protocol.md) puts every change
to the four membership stores behind one module. No code outside it may call
`add_learner`, `change_membership` or `register_node`, or propose a `JoinNode`,
`LeaveNode` or `UpdateNodeInfo`. The gate is the only thing that holds that
line, so it has to see every such site, whatever the receiver is called, and
nothing else.

Each case builds a throwaway git repo, so the gate runs against real
`git ls-files`. The gate does not yet pass on this repository: the sites it
still finds need an ADR-013 decision each (forge task t_bb34e7d9), and the gate
joins CI when they are migrated.
"""

import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
GATE = REPO / "scripts" / "ci-gates" / "no-raw-membership-mutation.py"

SRC = "ferrosa-cluster/src"


def run_gate(root: Path) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, str(GATE), str(root)], capture_output=True, text=True
    )


def repo_with(files: dict) -> tempfile.TemporaryDirectory:
    tmp = tempfile.TemporaryDirectory()
    root = Path(tmp.name)
    subprocess.run(["git", "-C", str(root), "init", "-q"], check=True)
    for name, body in files.items():
        path = root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(body)
        subprocess.run(["git", "-C", str(root), "add", name], check=True)
    return tmp


class GateTest(unittest.TestCase):
    def expect(self, files: dict, want: int, msg: str) -> None:
        with repo_with(files) as d:
            got = run_gate(Path(d))
            self.assertEqual(got.returncode, want, f"{msg}\n{got.stdout}{got.stderr}")

    # ── what must be refused ────────────────────────────────────────────────

    def test_a_raw_primitive_is_refused_whatever_the_receiver_is_called(self):
        # The receiver's name is not the contract. `raft_arc` and a builder
        # chain both reach the same primitive as `raft`.
        for call in (
            "raft.change_membership(m, true)",
            "raft_arc.change_membership(m, true)",
            "raft_for_membership_repair\n    .change_membership(m, true)",
            "self.raft.add_learner(id, node, true)",
            "network_factory.register_node(id, host)",
            "factory.register_node(id, host)",
        ):
            self.expect({f"{SRC}/controller/x.rs": f"fn f() {{ {call}; }}\n"}, 1, call)

    def test_proposing_a_membership_op_outside_the_module_is_refused(self):
        for op in ("JoinNode(info)", "LeaveNode { node_id }", "UpdateNodeInfo(info)"):
            body = f"fn f() {{ let c = RaftCommand {{ op: RaftOp::{op}, v }}; w(c); }}\n"
            self.expect({f"{SRC}/controller/x.rs": body}, 1, op)

    def test_a_fully_qualified_membership_op_is_refused(self):
        body = "fn f() { let op = crate::raft::RaftOp::UpdateNodeInfo(i); }\n"
        self.expect({f"{SRC}/controller/x.rs": body}, 1, "qualified path")

    def test_a_site_in_production_code_before_a_test_module_is_refused(self):
        body = (
            "fn f() { raft_arc.change_membership(m, true); }\n"
            "#[cfg(test)]\nmod tests {\n    fn g() {}\n}\n"
        )
        self.expect({f"{SRC}/x.rs": body}, 1, "production before tests")

    # ── what is not membership, and must not be refused ─────────────────────

    def test_a_ddl_proposal_is_not_a_membership_mutation(self):
        body = (
            "fn f() { let cmd = RaftCommand { op: RaftOp::CreateKeyspace(ks), v };\n"
            "    raft.client_write(cmd); }\n"
        )
        self.expect({f"{SRC}/ddl_path.rs": body}, 0, "DDL client_write")

    def test_token_and_state_ops_are_outside_adr_013(self):
        for op in ("AssignTokens { node_id, tokens }", "SetNodeState { node_id, state }"):
            body = f"fn f() {{ raft.client_write(RaftCommand {{ op: RaftOp::{op}, v }}); }}\n"
            self.expect({f"{SRC}/rebalance.rs": body}, 0, op)

    def test_matching_on_a_membership_op_is_not_proposing_one(self):
        # The state machine applies these ops and the log store replays them;
        # recognising an op is not proposing it.
        for body in (
            "fn f(op: RaftOp) { match op { RaftOp::JoinNode(n) => a(n), _ => {} } }\n",
            "fn f(op: RaftOp) { match op { RaftOp::JoinNode(n) | RaftOp::UpdateNodeInfo(n) => a(n), _ => {} } }\n",
            "fn f(op: RaftOp) { match op { RaftOp::CreateKeyspace(_)\n    | RaftOp::LeaveNode { .. }\n    | RaftOp::Noop => 1 } }\n",
            "fn f(op: RaftOp) { if let RaftOp::LeaveNode { node_id } = op { a(node_id) } }\n",
            "fn f(op: &RaftOp) -> bool { matches!(op, RaftOp::JoinNode(_) | RaftOp::AssignTokens { .. }) }\n",
        ):
            self.expect({f"{SRC}/raft/state_machine.rs": body}, 0, body)

    def test_proposing_a_membership_op_as_an_argument_is_refused(self):
        body = "fn f() { proposer.propose(RaftOp::LeaveNode { node_id }).await; }\n"
        self.expect({f"{SRC}/controller/x.rs": body}, 1, "argument")

    def test_the_membership_module_and_the_forwarding_helper_are_allowed(self):
        body = "fn f() { raft.change_membership(m, true); let o = RaftOp::JoinNode(i); }\n"
        for path in (
            f"{SRC}/membership/mod.rs",
            f"{SRC}/membership/sub/y.rs",
            f"{SRC}/raft_forward.rs",
            f"{SRC}/raft_forward_codec.rs",
        ):
            self.expect({path: body}, 0, path)

    def test_the_network_factory_may_call_its_own_register_node(self):
        body = "impl F { fn add(&self) { self.register_node(id, host); } }\n"
        self.expect({f"{SRC}/raft/network.rs": body}, 0, "factory internals")

    def test_test_code_may_build_membership_fixtures(self):
        prod = "fn f() {}\n"
        tests = "#[cfg(test)]\nmod tests {\n    fn g() { let o = RaftOp::JoinNode(i); raft.change_membership(m, true); }\n}\n"
        self.expect({f"{SRC}/x.rs": prod + tests}, 0, "inline test module")
        self.expect({f"{SRC}/controller/tests.rs": tests}, 0, "tests.rs")
        self.expect({f"{SRC}/controller/a_tests.rs": tests}, 0, "*_tests.rs")
        self.expect({"ferrosa-cluster/tests/it.rs": tests}, 0, "integration test")

    def test_comments_and_a_function_definition_are_not_calls(self):
        body = (
            "/// Proposes `RaftOp::JoinNode` through `raft.change_membership()`.\n"
            "// raft_arc.add_learner(id, n, true)\n"
            "pub fn register_node(&self, id: u64, host: Uuid) {}\n"
        )
        self.expect({f"{SRC}/controller/x.rs": body}, 0, "docs, comments, definition")

    def test_code_outside_the_cluster_crate_is_not_scanned(self):
        body = "fn f() { raft.change_membership(m, true); }\n"
        self.expect({"ferrosa/src/main.rs": body}, 0, "other crate")

    # ── the gate itself ─────────────────────────────────────────────────────

    def test_there_is_no_allowlist(self):
        # A list of tolerated sites is a waiver. The old gate's list was keyed
        # by line number, drifted, and ended up excusing nothing while CI ran
        # nothing. Sites get migrated, not listed.
        self.assertFalse(
            any((REPO / "scripts" / "ci-gates").glob("no-raw-*.allowlist")),
            "the membership gate must not carry an allowlist",
        )

    def test_a_non_checkout_cannot_be_scanned(self):
        with tempfile.TemporaryDirectory() as d:
            self.assertEqual(run_gate(Path(d)).returncode, 2)


if __name__ == "__main__":
    unittest.main()
