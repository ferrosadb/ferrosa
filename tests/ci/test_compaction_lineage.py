"""Contract for analyze_lines(iterable[str]); no live infrastructure.

Return exactly {"nodes": {machine: {table: summary}}, "errors": [diagnostic]}.
Summary is {"promoted_outputs": int, "max_generation_depth": int,
            "unknown_ancestry_outputs": int}. Only groups with accepted outputs
appear. An accepted output needs a start, exactly the declared distinct inputs,
and a /compaction/ promotion, all unambiguously on the same machine/table.
Counts deduplicate (machine, table, output generation); exact log replay is inert.

Fresh /sstables/ inputs have depth 0; an observed output has depth
1 + max(input depths) ONLY if every input has fully observed ancestry.
Unseen /compaction/ inputs, published /sstables/table/gen/gen-Data.db inputs,
and their descendants remain unknown. Publication may rename a generation;
never infer that mapping from adjacent numbers or timing. Such outputs
still count as promotions, increment unknown_ancestry_outputs, and contribute
nothing to max_generation_depth (0 if no known depth). Unknown ancestry is not
a malformed task. Generation IDs are scoped to machine AND table.

Diagnostics have exactly code, node, table, line; line is a 1-based input line
number, with EOF issues attached to the task start. Unrecoverable node/table
identities are None. Codes used here: missing_start, input_count_mismatch,
incomplete_task, overlapping_tasks, malformed_event, missing_node.
Malformed/incomplete/ambiguous tasks contribute no output or ancestry. A second
start on the same node/table before promotion quarantines that pair for the rest
of the capture: these logs have no task IDs to attribute later events safely.
Other pairs continue independently. Diagnostics may include cascading issues;
their ordering and prose are deliberately not part of the contract.

Blank/unrelated lines and regular /sstables/ flush promotions are ignored.
Bad relevant evidence is diagnosed, never silently reported as success.
Non-string input raises TypeError; iterator failures propagate.
Promotion is local evidence only: no completed/S3-manifest metric is returned.

"""

import importlib.util
from pathlib import Path
import sys
import unittest


# Test list:
# - Real ANSI/Fly sample; empty/noise; regular flush versus compaction output.
# - True generation 2, sibling generation 1, unknown ancestry propagation.
# - Interleaved workers/tables/nodes; generation IDs scoped to both identities.
# - Replay deduplication; missing start/inputs/promotion; overlapping starts.
# - Malformed fields/paths; explicit diagnostics and input-stream failures.

ROOT = Path(__file__).resolve().parents[2]
MODULE_NAME = "ferrosa_compaction_lineage"
SPEC = importlib.util.spec_from_file_location(
    MODULE_NAME, ROOT / "deploy/fly-compaction-explore/lineage.py"
)
lineage = importlib.util.module_from_spec(SPEC)
sys.modules[MODULE_NAME] = lineage
SPEC.loader.exec_module(lineage)

NODE = "80d3edf6574168"
OTHER_NODE = "9080e12a34bc56"
TABLE = "cx_tables_w1.t1"
OTHER_TABLE = "cx_tables_w1.t2"


def log(message, node=NODE, tick=0):
    """Synthetic events retain the sampled Fly/ANSI envelope and Rust target."""
    target = "flush" if message.startswith("flush:") else "compaction::executor"
    return (
        f"\x1b[2m2026-10-23T23:03:43Z\x1b[0m app[{node}] "
        "\x1b[32mlax\x1b[0m [\x1b[34minfo\x1b[0m]"
        f"2026-10-23T23:03:43.{tick:06d}Z  INFO "
        f"ferrosa_storage::{target}: {message}\n"
    )


def start(count=2, table=TABLE, node=NODE, tick=0):
    return log(
        f"compaction: starting streaming task table_id={table} inputs={count}",
        node, tick,
    )


def opened(gen, directory="sstables", table=TABLE, node=NODE, tick=1):
    return log(
        f"compaction: opening input SSTable gen={gen} data_file_size=4186954 "
        f'path="/var/lib/ferrosa/data/{directory}/{table}/{gen}-Data.db"',
        node, tick,
    )


def promoted(gen, directory="compaction", table=TABLE, node=NODE, tick=2):
    return log(
        "flush: staged SSTable verified, promoted, and fsynced "
        f"gen={gen} data_bytes=123 "
        f"path=/var/lib/ferrosa/data/{directory}/{table}/{gen}-Data.db",
        node, tick,
    )


def task(inputs=(123, 124), output=456, table=TABLE, node=NODE, tick=0):
    return [start(len(inputs), table, node, tick)] + [
        opened(gen, table=table, node=node, tick=tick + i + 1)
        for i, gen in enumerate(inputs)
    ] + [promoted(output, table=table, node=node, tick=tick + len(inputs) + 1)]


def summary(count=1, depth=1, unknown=0):
    return {"promoted_outputs": count, "max_generation_depth": depth,
            "unknown_ancestry_outputs": unknown}


class CompactionLineageTests(unittest.TestCase):
    def test_published_generation_directory_has_unknown_renamed_ancestry(self):
        nested = opened(123).replace('/123-Data.db', '/123/123-Data.db')
        self.assert_clean([start(), nested, opened(124), promoted(456)],
                          {NODE: {TABLE: summary(depth=0, unknown=1)}})

    def test_known_generation_keeps_ancestry_in_published_directory(self):
        nested = opened(456).replace('/456-Data.db', '/456/456-Data.db')
        self.assert_clean(task() + [start(tick=3), nested, opened(125), promoted(789)],
                          {NODE: {TABLE: summary(count=2, depth=2)}})

    def test_published_directory_generation_must_match_filename(self):
        nested = opened(123).replace('/123-Data.db', '/999/123-Data.db')
        result = self.analyze([start(), nested, opened(124), promoted(456)])
        self.assertEqual(result['nodes'], {})
        self.assertIn('malformed_event', [error['code'] for error in result['errors']])

    def analyze(self, lines):
        # Exercise a one-pass iterable, not a sequence-only implementation.
        result = lineage.analyze_lines(iter(lines))
        self.assertEqual(set(result), {"nodes", "errors"})
        self.assertIsInstance(result["nodes"], dict)
        self.assertIsInstance(result["errors"], list)
        for tables in result["nodes"].values():
            for actual in tables.values():
                self.assertEqual(set(actual), set(summary()))
                for value in actual.values():
                    self.assertIs(type(value), int)
                    self.assertGreaterEqual(value, 0)
        for error in result["errors"]:
            self.assertEqual(set(error), {"code", "node", "table", "line"})
            self.assertIsInstance(error["code"], str)
            self.assertTrue(error["code"])
            self.assertIs(type(error["line"]), int)
            self.assertGreaterEqual(error["line"], 1)
            self.assertLessEqual(error["line"], len(lines))
        return result

    def assert_clean(self, lines, nodes):
        self.assertEqual(self.analyze(lines), {"nodes": nodes, "errors": []})

    def assert_excluded(self, lines, code, node=NODE, table=TABLE):
        result = self.analyze(lines)
        self.assertEqual(result["nodes"], {})
        self.assertTrue(any(
            e["code"] == code and e["node"] == node and e["table"] == table
            for e in result["errors"]
        ), result["errors"])
        return result

    def test_empty_and_unrelated_logs_are_explicitly_empty(self):
        for lines in ([], ["\n", log("node ready"), "unrelated diagnostic\n"]):
            with self.subTest(lines=lines):
                self.assert_clean(lines, {})

    def test_small_real_sample_with_ansi_fly_prefix_is_generation_one(self):
        # Six targeted events from cluster.log, 2026-10-23T23:03:43.
        # Values/timestamps are copied; envelope construction avoids embedding
        # the large external capture or making tests depend on its availability.
        lines = [
            start(4, tick=881398),
            opened(1792796583861554, tick=881446),
            opened(1792796502561803, tick=881764),
            opened(1792796543187204, tick=881949),
            opened(1792796623860068, tick=882119),
            promoted(1792796623937837, tick=974997).replace(
                "data_bytes=123", "data_bytes=16615977"
            ),
        ]
        self.assert_clean(lines, {NODE: {TABLE: summary()}})

    def test_later_task_consuming_observed_output_is_generation_two(self):
        lines = task() + [
            start(tick=10), opened(456, "compaction", tick=11),
            opened(125, tick=12), promoted(789, tick=13),
        ]
        self.assert_clean(lines, {NODE: {TABLE: summary(2, 2)}})

    def test_two_fresh_tasks_are_two_promotions_but_only_generation_one(self):
        self.assert_clean(task() + task((125, 126), 789, tick=10),
                          {NODE: {TABLE: summary(2, 1)}})

    def test_depth_uses_deepest_input_not_sum_or_input_order(self):
        lines = task() + task((125, 126), 457, tick=10) + [
            start(tick=20), opened(456, "compaction", tick=21),
            opened(457, "compaction", tick=22), promoted(789, tick=23),
            start(tick=30), opened(127, tick=31),
            opened(789, "compaction", tick=32), promoted(790, tick=33),
        ]
        self.assert_clean(lines, {NODE: {TABLE: summary(4, 3)}})

    def test_unseen_compaction_input_is_unknown_not_fresh(self):
        lines = [start(), opened(123, "compaction"), opened(124), promoted(456)]
        self.assert_clean(lines, {NODE: {TABLE: summary(1, 0, 1)}})

    def test_unknown_ancestry_propagates_and_cannot_raise_known_max_depth(self):
        lines = task() + [
            start(tick=10), opened(900, "compaction", tick=11),
            opened(901, tick=12), promoted(902, tick=13),
            start(tick=20), opened(456, "compaction", tick=21),
            opened(902, "compaction", tick=22), promoted(903, tick=23),
        ]
        self.assert_clean(lines, {NODE: {TABLE: summary(3, 1, 2)}})

    def test_two_workers_can_interleave_tables_and_nodes(self):
        a = task()
        b = task(table=OTHER_TABLE)
        c = task(node=OTHER_NODE)
        lines = [event for triple in zip(a, b, c) for event in triple]
        self.assert_clean(lines, {
            NODE: {TABLE: summary(), OTHER_TABLE: summary()},
            OTHER_NODE: {TABLE: summary()},
        })

    def test_same_generation_on_another_node_cannot_supply_ancestry(self):
        lines = task() + [
            start(node=OTHER_NODE), opened(456, "compaction", node=OTHER_NODE),
            opened(125, node=OTHER_NODE), promoted(789, node=OTHER_NODE),
        ]
        self.assert_clean(lines, {
            NODE: {TABLE: summary()},
            OTHER_NODE: {TABLE: summary(1, 0, 1)},
        })

    def test_same_generation_in_another_table_cannot_supply_ancestry(self):
        lines = task() + [
            start(table=OTHER_TABLE), opened(456, "compaction", table=OTHER_TABLE),
            opened(125, table=OTHER_TABLE), promoted(789, table=OTHER_TABLE),
        ]
        self.assert_clean(lines, {
            NODE: {TABLE: summary(), OTHER_TABLE: summary(1, 0, 1)},
        })

    def test_exact_replay_of_lines_or_entire_capture_is_inert(self):
        lines = task() + [
            start(tick=10), opened(456, "compaction", tick=11),
            opened(125, tick=12), promoted(789, tick=13),
        ]
        for replay in (lines + lines, [line for line in lines for _ in range(2)]):
            with self.subTest(replay=replay):
                self.assert_clean(replay, {NODE: {TABLE: summary(2, 2)}})

    def test_repeated_promotion_with_new_timestamp_does_not_inflate_count(self):
        self.assert_clean(task() + [promoted(456, tick=99)],
                          {NODE: {TABLE: summary()}})

    def test_regular_flush_does_not_count_or_end_an_active_task(self):
        self.assert_clean([promoted(777, "sstables")], {})
        lines = [start(), opened(123), promoted(777, "sstables"),
                 opened(124), promoted(456)]
        self.assert_clean(lines, {NODE: {TABLE: summary()}})

    def test_missing_start_excludes_orphan_inputs_and_promotion(self):
        for lines in ([promoted(456)], [opened(123), opened(124), promoted(456)]):
            with self.subTest(lines=lines):
                self.assert_excluded(lines, "missing_start")

    def test_missing_declared_inputs_excludes_promotion(self):
        for inputs in ([], [opened(123)], [opened(123), opened(124)]):
            with self.subTest(inputs=inputs):
                self.assert_excluded([start(4), *inputs, promoted(456)],
                                     "input_count_mismatch")

    def test_duplicate_input_generation_cannot_satisfy_declared_count(self):
        self.assert_excluded([
            start(), opened(123, tick=1), opened(123, tick=2), promoted(456),
        ], "input_count_mismatch")

    def test_extra_inputs_are_not_silently_dropped(self):
        self.assert_excluded([
            start(), opened(123), opened(124), opened(125), promoted(456),
        ], "input_count_mismatch")

    def test_missing_promotion_is_incomplete_at_eof(self):
        result = self.assert_excluded(
            [log("noise"), start(), opened(123), opened(124)], "incomplete_task"
        )
        self.assertIn({"code": "incomplete_task", "node": NODE,
                       "table": TABLE, "line": 2}, result["errors"])

    def test_later_start_before_promotion_cannot_steal_previous_output(self):
        lines = [
            start(), opened(123), opened(124),
            start(tick=10), opened(125, tick=11), opened(126, tick=12),
            promoted(456, tick=13), promoted(789, tick=14),
        ]
        result = self.assert_excluded(lines, "overlapping_tasks")
        self.assertIn({"code": "overlapping_tasks", "node": NODE,
                       "table": TABLE, "line": 4}, result["errors"])

    def test_ambiguous_pair_does_not_poison_independent_worker(self):
        lines = [start(), opened(123), start(tick=10)] + task(table=OTHER_TABLE)
        lines += [opened(124), promoted(456)]
        result = self.analyze(lines)
        self.assertEqual(result["nodes"], {NODE: {OTHER_TABLE: summary()}})
        self.assertTrue(any(e["code"] == "overlapping_tasks"
                            for e in result["errors"]))

    def test_excluded_output_cannot_become_known_ancestry(self):
        lines = [start(4), opened(123), promoted(456)] + [
            start(tick=10), opened(456, "compaction", tick=11),
            opened(125, tick=12), promoted(789, tick=13),
        ]
        result = self.analyze(lines)
        self.assertEqual(result["nodes"], {NODE: {TABLE: summary(1, 0, 1)}})
        self.assertTrue(any(e["code"] == "input_count_mismatch"
                            for e in result["errors"]))

    def test_malformed_start_input_count_excludes_task(self):
        for count in ("", "nope", "-1", "0", "1.5"):
            with self.subTest(count=count):
                self.assert_excluded([
                    start(count), opened(123), opened(124), promoted(456),
                ], "malformed_event")

    def test_malformed_input_or_promotion_cannot_create_success(self):
        bad_inputs = [
            opened(123).replace("gen=123", "gen=oops"),
            opened(123).replace("/123-Data.db", "/999-Data.db"),
            opened(123).replace("/sstables/", "/unrecognized/"),
        ]
        bad_outputs = [
            promoted(456).replace("gen=456", "gen=oops"),
            promoted(456).replace("/456-Data.db", "/999-Data.db"),
        ]
        for bad in bad_inputs:
            with self.subTest(input=bad):
                self.assert_excluded([start(), bad, opened(124), promoted(456)],
                                     "malformed_event")
        for bad in bad_outputs:
            with self.subTest(output=bad):
                self.assert_excluded([start(), opened(123), opened(124), bad],
                                     "malformed_event")

    def test_missing_path_is_diagnosed_without_inventing_table_identity(self):
        self.assert_excluded([
            log("compaction: opening input SSTable gen=123 data_file_size=4186954"),
        ], "malformed_event", table=None)

    def test_input_for_wrong_table_cannot_fill_another_tasks_input_count(self):
        lines = [start(), opened(123), opened(124, table=OTHER_TABLE), promoted(456)]
        self.assert_excluded(lines, "input_count_mismatch")

    def test_missing_machine_identity_is_explicit_not_a_shared_default_node(self):
        lines = [line.replace(f"app[{NODE}] ", "") for line in task()]
        self.assert_excluded(lines, "missing_node", node=None)

    def test_non_string_elements_raise_instead_of_becoming_empty_evidence(self):
        for value in (None, 123, b"log bytes", {}):
            with self.subTest(value=value):
                with self.assertRaises(TypeError):
                    lineage.analyze_lines(iter([*task(), value]))

    def test_input_read_errors_propagate_even_after_valid_promotions(self):
        def failing_stream():
            yield from task()
            raise OSError("capture read failed")

        with self.assertRaisesRegex(OSError, "capture read failed"):
            lineage.analyze_lines(failing_stream())


if __name__ == "__main__":
    unittest.main()
