//! Disk-backed state for the SPARQL operators that cannot stream.
//!
//! ORDER BY, DISTINCT, grouped COUNT, property-path closure and the WHERE half
//! of a DELETE/INSERT all have to see more than one solution before they can
//! answer. None of them may hold "every solution" in memory, so each keeps a
//! bounded working set and spills the rest:
//!
//! - ordering reuses ferrosa-storage's [`ExternalSorter`] (sorted runs plus a
//!   bounded k-way merge), with [`SpillSolution`] as its row type;
//! - membership reuses ferrosa-storage's [`SpillingDedup`], keyed by
//!   [`solution_key`] / [`term_key`];
//! - unordered "remember these for later" state uses [`SpillBag`], an
//!   arrival-order bag that stays in memory below the spill threshold and moves
//!   to an [`ExternalSorter`] above it.
//!
//! Every spill directory is a [`TempSortTableReservation`], so a dropped or
//! cancelled query removes its files the same way a completed one does.
//! Correctness: round-trips every RDF term exactly; spill I/O errors propagate.
//! Last revised: 2026-11-16
//! Last changed: Introduce spillable solutions, keys and bags for streaming SPARQL.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::Arc;

use ferrosa_storage::engine::TempSortTableReservation;
use ferrosa_storage::external_sort::{ExternalSorter, SortedRows, SpillOrder, SpillRow};
use ferrosa_storage::spilling_dedup::{SpillingDedup, DEFAULT_RESIDENT_KEYS};
use serde::{Deserialize, Serialize};

use crate::error::SparqlError;
use crate::results::{Binding, TextDirection, TripleBinding};

/// One solution mapping.
pub type Solution = HashMap<String, Binding>;

/// What the executor's spilling operators did, for tests and diagnostics.
///
/// Counters only; nothing here bounds a query.
#[derive(Debug, Default)]
pub struct ExecutionStats {
    storage_rows_read: AtomicU64,
    sort_spills: AtomicU64,
    dedup_spills: AtomicU64,
    bag_spills: AtomicU64,
}

impl ExecutionStats {
    /// Storage rows decoded by every scan and lookup of the query.
    pub fn storage_rows_read(&self) -> u64 {
        self.storage_rows_read.load(AtomicOrdering::Relaxed)
    }
    /// ORDER BY / GROUP BY sorts whose input went to disk.
    pub fn sort_spills(&self) -> u64 {
        self.sort_spills.load(AtomicOrdering::Relaxed)
    }
    /// DISTINCT / visited sets whose keys went to disk.
    pub fn dedup_spills(&self) -> u64 {
        self.dedup_spills.load(AtomicOrdering::Relaxed)
    }
    /// Arrival-order bags (closure frontiers, update staging) that went to disk.
    pub fn bag_spills(&self) -> u64 {
        self.bag_spills.load(AtomicOrdering::Relaxed)
    }
    pub(crate) fn add_rows_read(&self, n: u64) {
        self.storage_rows_read.fetch_add(n, AtomicOrdering::Relaxed);
    }
    pub(crate) fn note_sort_spill(&self) {
        self.sort_spills.fetch_add(1, AtomicOrdering::Relaxed);
    }
}

/// Where and when the executor spills. There is no row bound: a query reads
/// and returns as many rows as its pattern matches, and only its WORKING SET is
/// bounded — by `spill_threshold_bytes` per spilling operator and by the
/// node-wide spill pool ferrosa-storage's sorter charges.
#[derive(Debug, Clone)]
pub struct ExecutionContext {
    spill_root: PathBuf,
    spill_threshold_bytes: u64,
    dedup_resident_keys: usize,
    stats: Arc<ExecutionStats>,
}

impl ExecutionContext {
    /// Spill beneath `spill_root` once an operator buffers
    /// `spill_threshold_bytes` (at least 1).
    pub fn new(spill_root: impl Into<PathBuf>, spill_threshold_bytes: u64) -> Self {
        Self {
            spill_root: spill_root.into(),
            spill_threshold_bytes: spill_threshold_bytes.max(1),
            dedup_resident_keys: DEFAULT_RESIDENT_KEYS,
            stats: Arc::new(ExecutionStats::default()),
        }
    }

    /// Keep at most `keys` DISTINCT / visited keys resident before spilling.
    pub fn with_dedup_resident_keys(mut self, keys: usize) -> Self {
        self.dedup_resident_keys = keys.max(1);
        self
    }

    /// The same settings with fresh counters, for one query.
    pub fn for_query(&self) -> Self {
        Self {
            stats: Arc::new(ExecutionStats::default()),
            ..self.clone()
        }
    }

    /// This query's counters.
    pub fn stats(&self) -> &Arc<ExecutionStats> {
        &self.stats
    }

    pub(crate) fn spill_threshold_bytes(&self) -> u64 {
        self.spill_threshold_bytes
    }

    /// Create a fresh, empty spill directory that is deleted on drop.
    pub(crate) fn reserve_dir(&self, label: &str) -> Result<TempSortTableReservation, SparqlError> {
        let path = self.unique_dir(label);
        std::fs::create_dir_all(&path).map_err(|e| {
            SparqlError::Execution(format!(
                "SPARQL {label}: cannot create spill directory {}: {e}",
                path.display()
            ))
        })?;
        Ok(TempSortTableReservation::claim_dir(path))
    }

    /// A membership set that spills past the resident key count.
    pub(crate) fn dedup(&self, label: &'static str) -> SpillDedup {
        let path = self.unique_dir(label);
        SpillDedup {
            set: SpillingDedup::new(&path, self.dedup_resident_keys),
            counted: false,
            label,
            stats: Arc::clone(&self.stats),
            _dir: TempSortTableReservation::claim_dir(path),
        }
    }

    /// An arrival-order bag that spills past the threshold.
    pub(crate) fn bag<T: SpillRow>(&self, label: &'static str) -> SpillBag<T> {
        SpillBag {
            memory: Vec::new(),
            bytes: 0,
            disk: None,
            label,
            cx: self.clone(),
        }
    }

    fn unique_dir(&self, label: &str) -> PathBuf {
        self.spill_root
            .join(format!("{label}-{}", uuid::Uuid::new_v4().simple()))
    }
}

impl Default for ExecutionContext {
    /// Spill under the system temp directory at the process spill threshold.
    /// The served engine roots its spills under the storage data directory
    /// instead (see `SparqlEngine`).
    fn default() -> Self {
        Self::new(
            std::env::temp_dir().join("ferrosa-sparql-spill"),
            ferrosa_storage::process_spill_threshold_bytes(),
        )
    }
}

// ---------------------------------------------------------------------------
// Term encoding
// ---------------------------------------------------------------------------

/// Owned spill form of a [`Binding`]. The SPARQL JSON form of a binding is not
/// round-trippable (it strips the blank-node marker), so spills use this.
#[derive(Debug, Deserialize)]
pub(crate) enum Term {
    U(String),
    B(String),
    L(
        String,
        Option<String>,
        Option<String>,
        Option<TextDirection>,
    ),
    T(Box<Term>, Box<Term>, Box<Term>),
}

/// Borrowed spill form; serializes byte-identically to [`Term`].
#[derive(Serialize)]
pub(crate) enum TermRef<'a> {
    U(&'a str),
    B(&'a str),
    L(
        &'a str,
        Option<&'a str>,
        Option<&'a str>,
        Option<TextDirection>,
    ),
    T(Box<TermRef<'a>>, Box<TermRef<'a>>, Box<TermRef<'a>>),
}

impl<'a> From<&'a Binding> for TermRef<'a> {
    fn from(binding: &'a Binding) -> Self {
        match binding {
            Binding::Uri { value } => Self::U(value),
            Binding::BlankNode { value } => Self::B(value),
            Binding::Literal {
                value,
                datatype,
                lang,
                direction,
            } => Self::L(value, datatype.as_deref(), lang.as_deref(), *direction),
            Binding::Triple { value } => Self::T(
                Box::new((&value.subject).into()),
                Box::new((&value.predicate).into()),
                Box::new((&value.object).into()),
            ),
        }
    }
}

impl Term {
    /// Move the decoded strings into a binding; nothing is copied.
    pub(crate) fn into_binding(self) -> Binding {
        match self {
            Self::U(value) => Binding::Uri { value },
            Self::B(value) => Binding::BlankNode { value },
            Self::L(value, datatype, lang, direction) => Binding::Literal {
                value,
                datatype,
                lang,
                direction,
            },
            Self::T(s, p, o) => Binding::Triple {
                value: Box::new(TripleBinding {
                    subject: s.into_binding(),
                    predicate: p.into_binding(),
                    object: o.into_binding(),
                }),
            },
        }
    }
}

fn encode_error(what: &str, e: serde_json::Error) -> SparqlError {
    SparqlError::Execution(format!("SPARQL spill: cannot encode {what}: {e}"))
}

/// Canonical bytes of one term, for membership sets.
pub(crate) fn term_key(binding: &Binding) -> Result<Vec<u8>, SparqlError> {
    serde_json::to_vec(&TermRef::from(binding)).map_err(|e| encode_error("term", e))
}

/// Canonical bytes of the given variables' values (unbound included), for
/// DISTINCT and grouping.
pub(crate) fn solution_key(row: &Solution, vars: &[String]) -> Result<Vec<u8>, SparqlError> {
    let values: Vec<Option<TermRef<'_>>> = vars
        .iter()
        .map(|var| row.get(var).map(TermRef::from))
        .collect();
    serde_json::to_vec(&values).map_err(|e| encode_error("solution key", e))
}

/// Decode a [`solution_key`] back into its terms, in variable order.
pub(crate) fn decode_solution_key(key: &[u8]) -> Result<Vec<Option<Binding>>, SparqlError> {
    let terms: Vec<Option<Term>> = serde_json::from_slice(key)
        .map_err(|e| SparqlError::Execution(format!("SPARQL spill: corrupt solution key: {e}")))?;
    Ok(terms
        .into_iter()
        .map(|t| t.map(Term::into_binding))
        .collect())
}

/// Bytes a binding keeps on the heap while buffered.
pub(crate) fn binding_heap_bytes(binding: &Binding) -> usize {
    let opt = |s: &Option<String>| s.as_ref().map_or(0, String::capacity);
    match binding {
        Binding::Uri { value } | Binding::BlankNode { value } => value.capacity(),
        Binding::Literal {
            value,
            datatype,
            lang,
            ..
        } => value.capacity() + opt(datatype) + opt(lang),
        Binding::Triple { value } => {
            std::mem::size_of::<TripleBinding>()
                + binding_heap_bytes(&value.subject)
                + binding_heap_bytes(&value.predicate)
                + binding_heap_bytes(&value.object)
        }
    }
}

// ---------------------------------------------------------------------------
// Spillable rows
// ---------------------------------------------------------------------------

/// A solution as an external-sort row. Moved in, moved out; never cloned.
#[derive(Debug)]
pub(crate) struct SpillSolution(pub(crate) Solution);

impl Serialize for SpillSolution {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_seq(
            self.0
                .iter()
                .map(|(name, value)| (name.as_str(), TermRef::from(value))),
        )
    }
}

impl<'de> Deserialize<'de> for SpillSolution {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let entries: Vec<(String, Term)> = Vec::deserialize(deserializer)?;
        Ok(Self(
            entries
                .into_iter()
                .map(|(name, term)| (name, term.into_binding()))
                .collect(),
        ))
    }
}

/// Per-entry overhead of a `HashMap<String, Binding>` slot, approximately.
const SOLUTION_SLOT_BYTES: usize =
    std::mem::size_of::<(String, Binding)>() + std::mem::size_of::<u64>();

impl SpillRow for SpillSolution {
    fn retained_bytes(&self) -> usize {
        std::mem::size_of::<Solution>()
            + self.0.capacity() * SOLUTION_SLOT_BYTES
            + self
                .0
                .iter()
                .map(|(name, value)| name.capacity() + binding_heap_bytes(value))
                .sum::<usize>()
    }
}

/// The comparator for arrival-order state: every row compares equal, so the
/// stable run sort keeps arrival order within a run and the bag never pays for
/// an ordering nobody reads.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Arrival;

impl<T> SpillOrder<T> for Arrival {
    fn compare(&self, _: &T, _: &T) -> std::cmp::Ordering {
        std::cmp::Ordering::Equal
    }
}

/// Map a storage spill error into the SPARQL error space.
pub(crate) fn spill_error(what: &str, e: impl std::fmt::Display) -> SparqlError {
    SparqlError::Execution(format!("SPARQL {what}: spill I/O failed: {e}"))
}

// ---------------------------------------------------------------------------
// Membership
// ---------------------------------------------------------------------------

/// [`SpillingDedup`] plus its directory reservation and spill accounting.
pub(crate) struct SpillDedup {
    // Field order is drop order: the set removes its file before the
    // reservation removes the directory.
    set: SpillingDedup,
    counted: bool,
    label: &'static str,
    stats: Arc<ExecutionStats>,
    _dir: TempSortTableReservation,
}

impl SpillDedup {
    /// `true` when `key` had not been seen. A spill I/O error is an error, not
    /// a "new" or "seen" answer.
    pub(crate) fn insert(&mut self, key: &[u8]) -> Result<bool, SparqlError> {
        let fresh = self
            .set
            .insert(key)
            .map_err(|e| spill_error(self.label, e))?;
        if !self.counted && self.set.spilled_to_disk() {
            self.counted = true;
            self.stats
                .dedup_spills
                .fetch_add(1, AtomicOrdering::Relaxed);
        }
        Ok(fresh)
    }
}

// ---------------------------------------------------------------------------
// Arrival-order bag
// ---------------------------------------------------------------------------

/// An unordered spillable collection: push now, iterate once later.
///
/// Below the spill threshold it is a `Vec` (no directory is created — most
/// closure frontiers are tiny). At the threshold it reserves a directory and
/// hands everything to an [`ExternalSorter`] with the [`Arrival`] order, whose
/// buffer and the node-wide spill pool bound it from then on.
pub(crate) struct SpillBag<T: SpillRow> {
    memory: Vec<T>,
    bytes: usize,
    disk: Option<(ExternalSorter<T, Arrival>, TempSortTableReservation)>,
    label: &'static str,
    cx: ExecutionContext,
}

impl<T: SpillRow> SpillBag<T> {
    pub(crate) fn push(&mut self, row: T) -> Result<(), SparqlError> {
        if let Some((sorter, _)) = self.disk.as_mut() {
            return sorter.push(row).map_err(|e| spill_error(self.label, e));
        }
        self.bytes += row.retained_bytes();
        self.memory.push(row);
        if self.bytes as u64 >= self.cx.spill_threshold_bytes() {
            let dir = self.cx.reserve_dir(self.label)?;
            let mut sorter =
                ExternalSorter::new(dir.path(), Arrival, self.cx.spill_threshold_bytes());
            for row in self.memory.drain(..) {
                sorter.push(row).map_err(|e| spill_error(self.label, e))?;
            }
            self.bytes = 0;
            self.cx
                .stats
                .bag_spills
                .fetch_add(1, AtomicOrdering::Relaxed);
            self.disk = Some((sorter, dir));
        }
        Ok(())
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.disk.is_none() && self.memory.is_empty()
    }

    /// Consume the bag. Disk-backed contents are read back run by run.
    pub(crate) fn drain(self) -> Result<BagIter<T>, SparqlError> {
        match self.disk {
            None => Ok(BagIter::Memory(self.memory.into_iter())),
            Some((sorter, dir)) => {
                let rows = sorter.finish().map_err(|e| spill_error(self.label, e))?;
                Ok(BagIter::Disk {
                    rows,
                    label: self.label,
                    _dir: dir,
                })
            }
        }
    }
}

/// Iterator over a drained [`SpillBag`]. Keeps the spill directory alive until
/// the last row is read.
pub(crate) enum BagIter<T: SpillRow> {
    Memory(std::vec::IntoIter<T>),
    Disk {
        rows: SortedRows<T, Arrival>,
        label: &'static str,
        _dir: TempSortTableReservation,
    },
}

impl<T: SpillRow> Iterator for BagIter<T> {
    type Item = Result<T, SparqlError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Memory(rows) => rows.next().map(Ok),
            Self::Disk { rows, label, .. } => {
                let label = *label;
                rows.next().map(|r| r.map_err(|e| spill_error(label, e)))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn every_kind_of_term() -> Solution {
        let mut row = Solution::new();
        row.insert("u".into(), Binding::uri("http://ex/a"));
        row.insert("b".into(), Binding::blank_node("_:b0"));
        row.insert(
            "l".into(),
            Binding::literal(
                "42",
                Some("http://www.w3.org/2001/XMLSchema#integer".into()),
                None,
            ),
        );
        row.insert("g".into(), Binding::literal("hi", None, Some("en".into())));
        row.insert(
            "d".into(),
            Binding::Literal {
                value: "x".into(),
                datatype: None,
                lang: Some("ar".into()),
                direction: Some(TextDirection::Rtl),
            },
        );
        row.insert(
            "t".into(),
            Binding::triple(
                Binding::uri("urn:s"),
                Binding::uri("urn:p"),
                Binding::blank_node("_:o"),
            )
            .unwrap(),
        );
        row
    }

    #[test]
    fn a_spilled_solution_round_trips_every_term_exactly() {
        let row = every_kind_of_term();
        let bytes = serde_json::to_vec(&SpillSolution(row.clone())).unwrap();
        let back: SpillSolution = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(back.0, row);
    }

    #[test]
    fn solution_keys_distinguish_kind_datatype_and_unbound() {
        let vars = vec!["x".to_string()];
        let mut uri = Solution::new();
        uri.insert("x".into(), Binding::uri("a"));
        let mut lit = Solution::new();
        lit.insert("x".into(), Binding::literal("a", None, None));
        let unbound = Solution::new();
        let keys = [
            solution_key(&uri, &vars).unwrap(),
            solution_key(&lit, &vars).unwrap(),
            solution_key(&unbound, &vars).unwrap(),
        ];
        assert_ne!(keys[0], keys[1]);
        assert_ne!(keys[0], keys[2]);
        assert_ne!(keys[1], keys[2]);
        let decoded = decode_solution_key(&keys[0]).unwrap();
        assert_eq!(decoded, vec![Some(Binding::uri("a"))]);
    }

    #[test]
    fn a_bag_past_its_threshold_spills_and_returns_every_row() {
        let dir = tempfile::tempdir().unwrap();
        let cx = ExecutionContext::new(dir.path(), 256);
        let mut bag = cx.bag::<SpillSolution>("test-bag");
        for i in 0..500 {
            let mut row = Solution::new();
            row.insert("i".into(), Binding::literal(i.to_string(), None, None));
            bag.push(SpillSolution(row)).unwrap();
        }
        assert_eq!(cx.stats().bag_spills(), 1);
        let mut seen: Vec<u32> = bag
            .drain()
            .unwrap()
            .map(|r| r.unwrap().0["i"].scalar_value().unwrap().parse().unwrap())
            .collect();
        seen.sort_unstable();
        assert_eq!(seen, (0..500).collect::<Vec<_>>());
    }
}
