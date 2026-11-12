//! Production WAL workloads with explicit warmup, identity and integrity checks.
//! Correctness: replay is byte-exact; allocations and timing cover named operations.
//! Last revised: 2026-10-29
//! Last changed: Added recovery, damaged-tail, shared-owner and release cost probes.

use std::hint::black_box;
use std::time::Instant;

use ferrosa_common::{CellBytes, CellValue, DecoratedKey, PartitionKey};
use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};
use ferrosa_storage::commitlog::{Mutation, ProbeSegment as Segment, SegmentReader};

use super::allocation::{measure, Counts};
use super::capture::{boundary, Capture};

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

fn mutation(id: u8, size: usize) -> Mutation {
    let payload: Vec<_> = (0..size).map(|i| (i as u8).wrapping_add(id)).collect();
    Mutation {
        mutation_id: [id; 16],
        keyspace: "probe".into(),
        table: "wal".into(),
        key: DecoratedKey::new(PartitionKey::new(vec![id])),
        rows: vec![Row {
            clustering: vec![id],
            cells: vec![(0, CellValue::live(payload, 1_000_000 + i64::from(id)))],
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::with_timestamp(1_000_000),
        }],
        timestamp: 1_000_000,
    }
}

fn write(segment: &mut Segment, mutation: &Mutation) -> u64 {
    segment
        .append(mutation)
        .expect("probe segment capacity must cover every entry")
        .offset
}

fn write_single(segment: &mut Segment, mutation: &Mutation) -> u64 {
    segment
        .append_single(mutation)
        .expect("single-row fixture must fit the segment")
        .offset
}

fn encoded(mutation: &Mutation) -> Vec<u8> {
    let mut bytes = vec![0; mutation.serialized_size()];
    mutation.serialize_into(&mut bytes);
    bytes
}

/// A process-crash simulation: the synced prefix remains; one buffered write is lost.
/// This exercises segment encoding/sync/reader, not StorageEngine replay admission.
fn recovery() -> Result<Capture> {
    if !cfg!(feature = "correctness-tracing") {
        return Err("capture requires --features correctness-tracing".into());
    }
    let capture = Capture::new(128);
    tracing::subscriber::with_default(capture.clone(), || -> Result {
        let directory = tempfile::tempdir()?;
        let mut segment = Segment::new(1, 512 * 1024, directory.path());
        let mutations: Vec<_> = (1..=16).map(|id| mutation(id, 4096)).collect();
        let mut positions = Vec::new();
        for group in mutations.chunks(4) {
            for mutation in group {
                let writer = if positions.len() % 2 == 0 {
                    write
                } else {
                    write_single
                };
                positions.push(writer(&mut segment, mutation));
            }
            segment.flush_to_disk()?;
            for offset in &positions[positions.len() - group.len()..] {
                // Harness acknowledgment, deliberately after the real sync returns.
                boundary(3, 1, *offset);
            }
        }
        write(&mut segment, &mutation(99, 4096)); // never flushed or acknowledged
        let path = segment.path().to_path_buf();
        drop(segment); // Segment has no implicit flush on drop.
        boundary(4, 0, 0);
        let mut reader = SegmentReader::open(&path)?;
        for (expected, offset) in mutations.iter().zip(positions) {
            let (position, actual) = reader.next_entry()?.ok_or("missing durable mutation")?;
            if position.offset != offset || encoded(expected) != encoded(&actual) {
                return Err("replay position or complete serialized payload changed".into());
            }
        }
        if reader.next_entry()?.is_some() {
            return Err("unsynced mutation survived buffer loss".into());
        }
        Ok(())
    })?;
    let events = capture.events()?;
    for (kind, expected) in [(1, 17), (2, 4), (3, 16), (4, 1), (5, 16)] {
        if events.iter().filter(|event| event[0] == kind).count() != expected {
            return Err(format!("missing boundary events of kind {kind}").into());
        }
    }
    Ok(capture)
}

#[cfg(all(test, feature = "correctness-tracing"))]
pub fn verify_recovery() -> Result {
    recovery().map(|_| ())
}

pub fn capture(path: &str) -> Result {
    verify_allocations()?;
    verify_faults()?;
    recovery()?.export(path)?;
    println!("captured real WAL boundaries; exact replay, allocation and fault checks passed");
    Ok(())
}

/// Zero allocations are required only for prepared shared owners, borrowed slicing,
/// serialization into caller storage, and reserved segment entry writes.
/// Mutation construction, row-metadata clones, decode and file sync are excluded.
pub fn verify_allocations() -> Result {
    let capture = Capture::new(256);
    tracing::subscriber::with_default(capture, || -> Result {
        for size in [0, 1, 64, 65, 256, 4096, 65536] {
            let mutation = mutation(7, size);
            let owner: &CellBytes = mutation.rows[0].cells[0].1.value.as_ref().unwrap();
            let before = owner.as_slice().to_vec();
            let (_, sharing) = measure(|| {
                let alias = black_box(owner.clone());
                assert_eq!(alias.as_ptr(), owner.as_ptr());
                let range = size / 4..size - size / 4;
                let view = black_box(&alias[range.clone()]);
                assert_eq!(view.as_ptr(), owner.as_ptr().wrapping_add(range.start));
                assert_eq!(view, &before[range]);
                let moved = CellValue::live_bytes(alias, 1_000_000);
                assert_eq!(moved.value.as_ref().unwrap().as_ptr(), owner.as_ptr());
                black_box(moved);
            });
            let mut output = vec![0; mutation.serialized_size()];
            mutation.serialize_into(&mut output); // warm codec before accounting
            let (_, encoding) = measure(|| mutation.serialize_into(black_box(&mut output)));
            let directory = tempfile::tempdir()?;
            let mut segment = Segment::new(
                1,
                4 * Segment::entry_total_size(&mutation) + 1024,
                directory.path(),
            );
            write(&mut segment, &mutation); // register tracing callsite before accounting
            write_single(&mut segment, &mutation);
            let (_, writing) = measure(|| black_box(write(&mut segment, black_box(&mutation))));
            let (_, single_writing) =
                measure(|| black_box(write_single(&mut segment, black_box(&mutation))));
            for (operation, counts) in [
                ("shared clone / borrowed slice / move", sharing),
                ("serialize_into", encoding),
                ("reserved segment write", writing),
                ("reserved single-row write", single_writing),
            ] {
                if counts != Counts::default() {
                    return Err(format!(
                        "{operation} allocated at payload size {size}: {counts:?}"
                    )
                    .into());
                }
            }
            if owner.as_slice() != before {
                return Err("source payload mutated in flight".into());
            }
            let decoded = Mutation::deserialize_from(&output)?;
            if encoded(&decoded) != output {
                return Err("codec round trip changed bytes".into());
            }
        }
        Ok(())
    })
}

pub fn verify_faults() -> Result {
    use std::io::{Seek, SeekFrom, Write};
    for corrupt in [false, true] {
        let directory = tempfile::tempdir()?;
        let mut segment = Segment::new(2, 16384, directory.path());
        let offset = write(&mut segment, &mutation(3, 4096));
        segment.flush_to_disk()?;
        let path = segment.path().to_path_buf();
        drop(segment);
        let mut file = std::fs::OpenOptions::new().write(true).open(&path)?;
        if corrupt {
            // Alter a payload byte without updating its checksum.
            file.seek(SeekFrom::Start(offset + 8 + 32))?;
            file.write_all(&[0xff])?;
        } else {
            file.set_len(file.metadata()?.len() - 1)?;
        }
        file.sync_all()?;
        drop(file);
        let mut reader = SegmentReader::open(&path)?;
        if reader.next_entry()?.is_some() {
            return Err("corrupt or torn payload admitted".into());
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wrapper_refuses_invalid_shape_and_full_segment_without_writing() {
        let directory = tempfile::tempdir().unwrap();
        let mut segment = Segment::new(3, 4096, directory.path());
        let mut mutation = mutation(7, 256);
        mutation.rows.push(mutation.rows[0].clone());
        assert!(segment.append_single(&mutation).is_none());
        mutation.rows.pop();
        let first = segment.append_single(&mutation).unwrap();
        assert_eq!(
            first.offset, 25,
            "rejected shape must not reserve buffer bytes"
        );
        let mut written = 1;
        while segment.append(&mutation).is_some() {
            written += 1;
        }
        assert!(segment.append(&mutation).is_none());
        segment.flush_to_disk().unwrap();
        let mut reader = SegmentReader::open(segment.path()).unwrap();
        let mut recovered = 0;
        while reader.next_entry().unwrap().is_some() {
            recovered += 1;
        }
        assert_eq!(recovered, written);
    }
}

#[derive(serde::Serialize)]
struct Sample {
    enabled: bool,
    ns_per_op: f64,
    p99_ns: u128,
    allocations: Counts,
}

fn sample(enabled: bool, operations: usize, payload: usize, single_row: bool) -> Result<Sample> {
    let directory = tempfile::tempdir()?;
    let mutation = mutation(7, payload);
    let mut segment = Segment::new(
        1,
        (operations + 1) * Segment::entry_total_size(&mutation) + 1024,
        directory.path(),
    );
    let capture = Capture::new(operations + 1);
    let dispatcher = if enabled {
        tracing::Dispatch::new(capture.clone())
    } else {
        tracing::Dispatch::new(tracing::subscriber::NoSubscriber::default())
    };
    let mut latencies = vec![0_u128; operations];
    let writer = if single_row { write_single } else { write };
    let (elapsed, allocations) = tracing::dispatcher::with_default(&dispatcher, || {
        writer(&mut segment, &mutation);
        let (elapsed, counts) = measure(|| {
            let start = Instant::now();
            for latency in &mut latencies {
                let operation = Instant::now();
                black_box(writer(&mut segment, black_box(&mutation)));
                *latency = operation.elapsed().as_nanos();
            }
            start.elapsed()
        });
        (elapsed, counts)
    });
    let events = capture.events()?;
    let expected = if enabled && cfg!(feature = "correctness-tracing") {
        operations + 1
    } else {
        0
    };
    if events.len() != expected {
        return Err("benchmark did not capture every write".into());
    }
    if allocations != Counts::default() {
        return Err("benchmark hot path allocated".into());
    }
    latencies.sort_unstable();
    Ok(Sample {
        enabled,
        ns_per_op: elapsed.as_nanos() as f64 / operations as f64,
        p99_ns: latencies[(operations - 1) * 99 / 100],
        allocations,
    })
}

/// Alternating paired trials; output is evidence, the external gate compares builds.
pub fn bench() -> Result {
    if cfg!(debug_assertions) {
        return Err("benchmark requires --release".into());
    }
    let mut results = Vec::new();
    for single_row in [false, true] {
        for payload in [256, 4096, 65536] {
            let operations = (8 * 1024 * 1024 / payload).clamp(128, 4096);
            let mut samples = Vec::new();
            for round in 0..9 {
                for enabled in if round % 2 == 0 {
                    [false, true]
                } else {
                    [true, false]
                } {
                    samples.push(sample(enabled, operations, payload, single_row)?);
                }
            }
            results.push(
                serde_json::json!({ "path": if single_row { "single_row" } else { "mutation" },
            "payload_bytes": payload, "operations": operations, "samples": samples }),
            );
        }
    }
    println!(
        "{}",
        serde_json::json!({ "instrumented_build": cfg!(feature = "correctness-tracing"),
        "scope": "single-thread reserved segment encoding and CRC; excludes fsync, decode, rotation and CQL",
        "results": results })
    );
    Ok(())
}
