#!/usr/bin/env python3
"""Conservative, capture-order lineage from Fly compaction log evidence.

Local promotion is not evidence of S3 upload or manifest completion. No task
IDs exist in these events, so overlapping tasks cannot be reconstructed.
"""

import argparse
from dataclasses import dataclass, field
import json
import re
import shlex


ANSI = re.compile(r"\x1b\[[0-?]*[ -/]*[@-~]")
NODE = re.compile(r"\bapp\[([^\]\s]+)\]")
EVENTS = {
    "start": "compaction: starting streaming task",
    "input": "compaction: opening input SSTable",
    "promotion": "flush: staged SSTable verified, promoted, and fsynced",
}


@dataclass
class Task:
    line: int
    count: int | None
    inputs: dict = field(default_factory=dict)
    invalid: bool = False


def _fields(text):
    """Reject duplicated fields and broken quoting instead of guessing."""
    fields = {}
    valid = True
    try:
        tokens = shlex.split(text)
    except ValueError:
        return {}, False
    for token in tokens:
        key, separator, value = token.partition("=")
        if not separator or not key or key in fields:
            valid = False
        if key in fields:
            fields[key] = None
        else:
            fields[key] = value
    return fields, valid


def _number(value):
    return int(value) if value and re.fullmatch(r"[0-9]+", value) else None


def _path(value):
    """Recover the table even when the directory or filename is malformed."""
    parts = (value or "").split("/")
    if len(parts) < 4 or parts[0] != "":
        return None, None, None
    directory, table, filename = parts[-3:]
    published_generation = None
    if len(parts) >= 5 and parts[-4] == "sstables":
        directory, table = "published_compaction", parts[-3]
        published_generation = _number(parts[-2])
    if not table or table in (".", ".."):
        table = None
    match = re.fullmatch(r"([0-9]+)-Data\.db", filename)
    valid = directory in ("sstables", "compaction", "published_compaction") and all(
        part and part not in (".", "..") for part in parts[1:]
    )
    if directory == "published_compaction":
        valid = valid and match is not None and published_generation == int(match[1])
    generation = int(match[1]) if match and valid else None
    return directory, table, generation


@dataclass
class _Event:
    line: int
    kind: str | None
    node: str | None
    table: str | None = None
    generation: int | None = None
    directory: str | None = None
    count: int | None = None
    valid: bool = False

    @property
    def pair(self):
        return self.node, self.table

    @property
    def identity(self):
        # Publication can change the directory without changing the identity.
        return self.node, self.table, self.generation


def _parse_event(raw, line_number, seen):
    """Parse relevant evidence, retaining partial identities for diagnostics."""
    if not isinstance(raw, str):
        raise TypeError("log lines must be strings")
    text = ANSI.sub("", raw).strip()
    matches = [(kind, marker) for kind, marker in EVENTS.items() if marker in text]
    if not matches or text in seen:
        return None
    seen.add(text)
    nodes = NODE.findall(text)
    node = nodes[0] if len(nodes) == 1 else None
    if len(matches) != 1:
        return _Event(line_number, None, node)

    kind, marker = matches[0]
    fields, valid = _fields(text.split(marker, 1)[1])
    event = _Event(line_number, kind, node, generation=_number(fields.get("gen")))
    if kind == "start":
        event.table = fields.get("table_id") or None
        event.count = _number(fields.get("inputs"))
        event.valid = (
            valid and event.table is not None
            and event.count is not None and event.count > 0
        )
    else:
        event.directory, event.table, path_generation = _path(fields.get("path"))
        event.valid = valid and event.table is not None and event.generation is not None
        event.valid = event.valid and path_generation == event.generation
        # A regular flush neither supplies compaction lineage nor closes a task.
        if kind == "promotion" and event.directory != "compaction" and event.valid:
            return None
    return event


@dataclass
class _LineageState:
    errors: list = field(default_factory=list)
    active: dict = field(default_factory=dict)
    quarantined: set = field(default_factory=set)
    # Directory is deliberately absent from generation identity.
    ancestry: dict = field(default_factory=dict)
    outputs: dict = field(default_factory=dict)

    def diagnose(self, code, node, table, line):
        self.errors.append(dict(code=code, node=node, table=table, line=line))

    def invalidate(self, node, table):
        # Missing identity cannot safely be assigned to just one active task.
        for (task_node, task_table), task in self.active.items():
            if (node is None or node == task_node) and (
                table is None or table == task_table
            ):
                task.invalid = True

    def apply(self, event):
        node, table = event.pair
        if event.kind is None:
            self.diagnose("malformed_event", node, None, event.line)
            self.invalidate(node, None)
            return
        if node is None:
            self.diagnose("missing_node", None, table, event.line)
            self.invalidate(None, table)
            return
        if not event.valid:
            self.diagnose("malformed_event", node, table, event.line)
            self.invalidate(node, table)
        if table is None or event.pair in self.quarantined:
            return
        if event.kind == "start":
            self.start(event)
            return
        if not event.valid:
            if event.kind == "promotion" and event.generation is not None:
                self.ancestry.setdefault(event.identity, None)
            return
        task = self.active.get(event.pair)
        if task is None:
            if event.kind == "promotion" and event.identity in self.outputs:
                return
            self.diagnose("missing_start", node, table, event.line)
            # An orphan compaction generation must not later look like a flush.
            if event.directory != "sstables":
                self.ancestry.setdefault(event.identity, None)
            return
        if event.kind == "input":
            self.add_input(event, task)
        else:
            self.complete(event, task)

    def start(self, event):
        if event.pair in self.active:
            self.diagnose("overlapping_tasks", event.node, event.table, event.line)
            self.quarantined.add(event.pair)
            del self.active[event.pair]
        else:
            self.active[event.pair] = Task(event.line, event.count, invalid=not event.valid)

    def add_input(self, event, task):
        depth = self.ancestry.get(
            event.identity, 0 if event.directory == "sstables" else None
        )
        if event.directory != "sstables":
            self.ancestry.setdefault(event.identity, depth)
        if event.generation in task.inputs and task.inputs[event.generation] != depth:
            self.diagnose("malformed_event", event.node, event.table, event.line)
            task.invalid = True
        task.inputs[event.generation] = depth

    def complete(self, event, task):
        """Close a valid promotion event; only a fully attributed task adds output."""
        del self.active[event.pair]
        if len(task.inputs) != task.count:
            self.diagnose("input_count_mismatch", event.node, event.table, event.line)
            task.invalid = True
        if event.generation in task.inputs or event.identity in self.outputs:
            # A second task claiming an existing output is not an exact replay.
            self.diagnose("malformed_event", event.node, event.table, event.line)
            task.invalid = True
        if task.invalid:
            self.ancestry.setdefault(event.identity, None)
            return
        depths = task.inputs.values()
        depth = None if any(d is None for d in depths) else 1 + max(depths)
        self.ancestry[event.identity] = depth
        self.outputs[event.identity] = depth


def _summarize_outputs(outputs):
    nodes = {}
    for (node, table, _), depth in outputs.items():
        summary = nodes.setdefault(node, {}).setdefault(table, {
            "promoted_outputs": 0,
            "max_generation_depth": 0,
            "unknown_ancestry_outputs": 0,
        })
        summary["promoted_outputs"] += 1
        if depth is None:
            summary["unknown_ancestry_outputs"] += 1
        else:
            summary["max_generation_depth"] = max(summary["max_generation_depth"], depth)
    return nodes


def analyze_lines(lines):
    """Return node/table promotion counts, known depth, and explicit diagnostics.

    Exact event replay is inert. Only fully attributed tasks supply ancestry;
    unknown ancestry propagates, including after publication into /sstables/.
    Input must be an iterable of strings; read and type errors propagate.
    """
    state = _LineageState()
    seen = set()
    for line_number, raw in enumerate(lines, 1):
        event = _parse_event(raw, line_number, seen)
        if event is not None:
            state.apply(event)
    for (node, table), task in state.active.items():
        state.diagnose("incomplete_task", node, table, task.line)
    return {"nodes": _summarize_outputs(state.outputs), "errors": state.errors}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("log_path", help="Fly log capture to read as UTF-8")
    args = parser.parse_args()
    with open(args.log_path, encoding="utf-8") as capture:
        result = analyze_lines(capture)
    print(json.dumps(result, indent=2, sort_keys=True))
    return 1 if result["errors"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
