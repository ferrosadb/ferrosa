---
title: "O_DIRECT compaction/rebuild reader — FMEA"
status: proposed
component: storage-io
task: t_bed7ed0f
executive_summary: >
  Failure-mode analysis for the opt-in Linux O_DIRECT compaction/rebuild reader.
  Highest risks (RPN 18): silent data corruption from mixing direct/buffered access
  to an overlapping Data.db range, a wrong bounce-buffer copy dropping the unaligned
  tail, and checksum verification over partial (aligned-superset) rather than
  reassembled data. Each maps to an explicit test. RPN = Severity x Occurrence x
  Detection (1-3 each; higher = worse). The read-only nature of the path bounds
  durability risk; correctness and residency dominate.
last_revised: 2026-07-21
---

# `O_DIRECT` compaction/rebuild reader — FMEA

Severity/Occurrence/Detection scored 1 (best) to 3 (worst). RPN = S x O x D.
Every RPN >= 50-equivalent (here RPN >= 8 on the 1-3 scale) maps to a named test.

| # | Failure mode | Effect | S | O | D | RPN | Control + test |
|---|---|---|---|---|---|---|---|
| F1 | **Mixed access on an overlapping range** — same Data.db read both direct and buffered (or mmap) → stale/incoherent bytes | Silent corruption in compacted output | 3 | 2 | 3 | 18 | Direct reader used ONLY by the separate compaction/rebuild pool; foreground stays buffered; assert no fd is opened both ways. Test: `direct_and_buffered_never_share_a_datadb_range` (fail-loud guard) + a compaction whose input is direct produces byte-identical output to buffered. |
| F2 | **Unaligned tail dropped** — bounce-buffer copies only the aligned superset, loses the requested head/tail slice | Wrong bytes returned; corrupt merge | 3 | 2 | 3 | 18 | Explicit head/tail slice math; property test over random (offset, len) vs `FileReadAt` oracle. Test: `direct_read_matches_buffered_for_arbitrary_offset_len` (proptest). |
| F3 | **Checksum over partial data** — verify runs on the aligned superset, not the reassembled logical range | Accepts corruption / rejects good data | 3 | 1 | 2 | 6 | Checksum only on reassembled logical bytes. Test: `checksum_verified_on_reassembled_range`. |
| F4 | **Short read at EOF mis-handled** — aligned read past EOF returns < requested; treated as error or over-copied | Compaction fails or truncates | 3 | 2 | 2 | 12 | EOF-aware superset clamp; return exact byte count. Test: `direct_short_read_at_eof_matches_buffered`. |
| F5 | **Alignment mis-probe** — STATX_DIOALIGN absent/wrong, direct read issued anyway → EINVAL / partial | Scan errors instead of falling back | 3 | 2 | 2 | 12 | Require proven mem+offset+len alignment before any direct read; else buffered fallback. Test: `unproven_alignment_falls_back_to_buffered`. |
| F6 | **Unsupported filesystem** — O_DIRECT open returns EINVAL (tmpfs/overlay/some network fs) | Compaction crash on that fs | 3 | 2 | 1 | 6 | Detect at open; fall back per-file; count fallbacks. Test: `unsupported_fs_open_falls_back` (mock EINVAL). |
| F7 | **Non-Linux / old-kernel build** — direct path compiled/enabled where unavailable | Build break or runtime panic | 3 | 1 | 1 | 3 | `#[cfg(target_os="linux")]` adapter + always-buffered stub elsewhere; runtime kernel check. Test: `non_linux_uses_buffered` (cfg-gated). |
| F8 | **Unbounded aligned buffers** — per-reader bounce/readahead buffers grow with request size or reader count | OOM under the 2 GiB cgroup | 3 | 2 | 2 | 12 | Bounded `max_buffer_bytes` per reader + bounded compaction pool size; fail-loud on exceed. Test: `aligned_buffer_residency_is_bounded`. |
| F9 | **Foreground pool contamination** — direct reader accidentally handed to a foreground read | Foreground latency/alignment cost, cache bypass on hot reads | 3 | 1 | 2 | 6 | Separate pool + type/flag preventing foreground use. Test: `foreground_reads_never_use_direct`. |
| F10 | **Cancellation leak** — compaction cancelled mid-read leaves fd/aligned buffer or a blocked pread | fd/mem leak, stuck worker | 2 | 2 | 2 | 8 | RAII fd + buffer; cancellation checked between bounded reads (never mid-syscall on a huge buffer). Test: `cancel_mid_scan_releases_resources`. |
| F11 | **Readahead too deep** — large explicit readahead re-pollutes memory or stalls | Defeats the purpose; latency spike | 2 | 2 | 2 | 8 | Bounded `readahead_bytes`; metric `readahead_depth`. Test: `readahead_within_config_bound`. |
| F12 | **Silent perf regression** — direct path slower (extra copies/syscalls) but shipped on | Compaction throughput drop | 2 | 2 | 3 | 12 | Default-off + measurement promotion gate; emit direct vs buffered bytes + device latency. Gate: NVMe cgroup comparison must show no throughput regression before enabling. |
| F13 | **Crash/restart mid-compaction** — partial output while inputs read direct | (No new risk — read-only) | 2 | 1 | 1 | 2 | Direct reads add no durability surface; existing staging→rename atomic-commit unchanged. Test: existing compaction crash-recovery suite runs with direct inputs. |

## Priority summary

- **RPN 18 (critical), gate before enabling:** F1 (mixed access), F2 (tail drop) —
  both are silent-corruption paths and must have the byte-identical-output proof +
  proptest green.
- **RPN 12 (high):** F4 EOF, F5 alignment probe, F8 buffer residency, F12 perf
  regression — the correctness + OOM + promotion surface.
- **RPN <= 8:** checksum scope, fs/platform fallback, contamination, cancellation,
  readahead — controlled by the fallback-first design + bounded buffers.

The read-only nature (F13) keeps durability out of scope; the dominant surface is
copy/alignment correctness (F1-F5) and memory residency (F8, F11), all gated behind
default-off + the measurement criterion (F12).
