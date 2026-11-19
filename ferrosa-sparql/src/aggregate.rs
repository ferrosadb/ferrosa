//! COUNT accumulators over a stream of solution mappings, with spilled state.
//!
//! COUNT never retains its input solutions. What it does retain is spilled:
//!
//! - `COUNT(DISTINCT …)` admits each (group, aggregate, value) key once through
//!   a spilling de-duplication set;
//! - grouped counts pre-aggregate in a hash map until it reaches the spill
//!   threshold, then flush partial groups into ferrosa-storage's external
//!   sorter keyed by the group's canonical bytes; the final merge sums the
//!   partials of each group as it streams out.
//!
//! Correctness: preserve RDF term identity, unbound values and empty-input groups.
//! Last revised: 2026-11-16
//! Last changed: Spill DISTINCT keys and grouped partials instead of capping them.

use std::borrow::Cow;
use std::collections::HashMap;
use std::future::ready;
use std::sync::Arc;

use ferrosa_storage::engine::TempSortTableReservation;
use ferrosa_storage::external_sort::{ExternalSorter, SortedRows, SpillOrder, SpillRow};
use futures::stream;
use futures::{StreamExt, TryStreamExt};
use serde::{Deserialize, Serialize};

use crate::error::SparqlError;
use crate::executor::SolutionStream;
use crate::planner::CountAggregate;
use crate::results::Binding;
use crate::spill::{
    decode_solution_key, solution_key, spill_error, term_key, ExecutionContext, Solution,
    SpillDedup, TermRef,
};

const XSD_INTEGER: &str = "http://www.w3.org/2001/XMLSchema#integer";

/// Fold `rows` into COUNT results: one row for a global count, one row per
/// group otherwise.
pub(crate) fn count_stream(
    rows: SolutionStream,
    variables: Arc<[String]>,
    specs: Arc<[(String, CountAggregate)]>,
    cx: ExecutionContext,
) -> SolutionStream {
    stream::once(async move {
        let mut counts = Counts::new(variables, specs, cx);
        rows.try_for_each(|row| ready(counts.feed(row))).await?;
        counts.finish()
    })
    .try_flatten()
    .boxed()
}

/// One group's partial counts, as an external-sort row.
#[derive(Serialize, Deserialize)]
struct Partial {
    key: Vec<u8>,
    counts: Vec<u64>,
}

impl SpillRow for Partial {
    fn retained_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.key.capacity()
            + self.counts.capacity() * std::mem::size_of::<u64>()
    }
}

#[derive(Clone, Copy)]
struct ByKey;

impl SpillOrder<Partial> for ByKey {
    fn compare(&self, a: &Partial, b: &Partial) -> std::cmp::Ordering {
        a.key.cmp(&b.key)
    }
}

struct Counts {
    variables: Arc<[String]>,
    specs: Arc<[(String, CountAggregate)]>,
    cx: ExecutionContext,
    /// The global counters (no GROUP BY variables).
    single: Option<Vec<u64>>,
    /// Resident partial groups, keyed by canonical group bytes.
    groups: HashMap<Vec<u8>, Vec<u64>>,
    group_bytes: usize,
    spilled: Option<(ExternalSorter<Partial, ByKey>, TempSortTableReservation)>,
    /// Admitted DISTINCT keys, shared by every DISTINCT aggregate and group.
    distinct: Option<SpillDedup>,
}

impl Counts {
    fn new(
        variables: Arc<[String]>,
        specs: Arc<[(String, CountAggregate)]>,
        cx: ExecutionContext,
    ) -> Self {
        let distinct = specs
            .iter()
            .any(|(_, spec)| spec.distinct)
            .then(|| cx.dedup("sparql-count-distinct"));
        Self {
            single: variables.is_empty().then(|| vec![0; specs.len()]),
            variables,
            specs,
            cx,
            groups: HashMap::new(),
            group_bytes: 0,
            spilled: None,
            distinct,
        }
    }

    fn feed(&mut self, row: Solution) -> Result<(), SparqlError> {
        let key = if self.single.is_some() {
            Vec::new()
        } else {
            solution_key(&row, &self.variables)?
        };
        let increments = self.increments(&key, &row)?;
        if let Some(single) = self.single.as_mut() {
            return add(single, &increments);
        }
        if let Some(counts) = self.groups.get_mut(&key) {
            return add(counts, &increments);
        }
        self.group_bytes += key.capacity() + increments.capacity() * 8 + 64;
        self.groups.insert(key, increments);
        if self.group_bytes as u64 >= self.cx.spill_threshold_bytes() {
            self.flush_groups()?;
        }
        Ok(())
    }

    /// What `row` adds to each aggregate of its group.
    fn increments(&mut self, group: &[u8], row: &Solution) -> Result<Vec<u64>, SparqlError> {
        let specs = Arc::clone(&self.specs);
        let mut increments = Vec::with_capacity(specs.len());
        for (index, (_, spec)) in specs.iter().enumerate() {
            let counted = match &spec.expression {
                Some(expression) => match count_expression(expression, row)? {
                    None => false,
                    Some(_) if !spec.distinct => true,
                    Some(value) => self.admit(group, index, &term_key(&value)?)?,
                },
                None if !spec.distinct => true,
                None => self.admit(group, index, &whole_solution_key(row)?)?,
            };
            increments.push(u64::from(counted));
        }
        Ok(increments)
    }

    /// Admit a DISTINCT value of aggregate `index` in `group` once.
    fn admit(&mut self, group: &[u8], index: usize, value: &[u8]) -> Result<bool, SparqlError> {
        let set = self
            .distinct
            .as_mut()
            .expect("a DISTINCT aggregate always has a set");
        let mut key = Vec::with_capacity(16 + group.len() + value.len());
        key.extend_from_slice(&(group.len() as u64).to_le_bytes());
        key.extend_from_slice(group);
        key.extend_from_slice(&(index as u64).to_le_bytes());
        key.extend_from_slice(value);
        set.insert(&key)
    }

    /// Move the resident partial groups into the spilling sorter.
    fn flush_groups(&mut self) -> Result<(), SparqlError> {
        if self.spilled.is_none() {
            let dir = self.cx.reserve_dir("sparql-group-by")?;
            let sorter = ExternalSorter::new(dir.path(), ByKey, self.cx.spill_threshold_bytes());
            self.spilled = Some((sorter, dir));
        }
        let (sorter, _) = self.spilled.as_mut().expect("sorter was just created");
        for (key, counts) in self.groups.drain() {
            sorter
                .push(Partial { key, counts })
                .map_err(|e| spill_error("GROUP BY", e))?;
        }
        self.group_bytes = 0;
        Ok(())
    }

    fn finish(mut self) -> Result<SolutionStream, SparqlError> {
        let variables = Arc::clone(&self.variables);
        let specs = Arc::clone(&self.specs);
        if let Some(single) = self.single.take() {
            let row = finish_group(&variables, &specs, Vec::new(), single);
            return Ok(stream::iter([Ok(row)]).boxed());
        }
        if self.spilled.is_none() {
            let rows: Vec<_> = self
                .groups
                .drain()
                .map(|(key, counts)| {
                    let terms = decode_solution_key(&key)?;
                    Ok(finish_group(&variables, &specs, terms, counts))
                })
                .collect();
            return Ok(stream::iter(rows).boxed());
        }
        self.flush_groups()?;
        let (sorter, dir) = self.spilled.take().expect("spilled state checked above");
        self.cx.stats().note_sort_spill();
        let sorted = sorter.finish().map_err(|e| spill_error("GROUP BY", e))?;
        Ok(stream::iter(MergedGroups {
            sorted: sorted.peekable(),
            variables,
            specs,
            _dir: dir,
        })
        .boxed())
    }
}

/// Sums the sorted partials of each group as they stream out.
struct MergedGroups {
    sorted: std::iter::Peekable<SortedRows<Partial, ByKey>>,
    variables: Arc<[String]>,
    specs: Arc<[(String, CountAggregate)]>,
    _dir: TempSortTableReservation,
}

impl MergedGroups {
    fn next_group(&mut self) -> Result<Option<Solution>, SparqlError> {
        let Some(first) = self.sorted.next() else {
            return Ok(None);
        };
        let Partial { key, mut counts } = first.map_err(|e| spill_error("GROUP BY", e))?;
        while let Some(Ok(next)) = self.sorted.peek() {
            if next.key != key {
                break;
            }
            let Some(Ok(next)) = self.sorted.next() else {
                unreachable!("peeked an Ok partial");
            };
            add(&mut counts, &next.counts)?;
        }
        let terms = decode_solution_key(&key)?;
        Ok(Some(finish_group(
            &self.variables,
            &self.specs,
            terms,
            counts,
        )))
    }
}

impl Iterator for MergedGroups {
    type Item = Result<Solution, SparqlError>;
    fn next(&mut self) -> Option<Self::Item> {
        self.next_group().transpose()
    }
}

fn add(counts: &mut [u64], increments: &[u64]) -> Result<(), SparqlError> {
    for (count, inc) in counts.iter_mut().zip(increments) {
        *count = count
            .checked_add(*inc)
            .ok_or_else(|| SparqlError::Execution("COUNT overflow".into()))?;
    }
    Ok(())
}

/// Canonical bytes of a whole solution, for COUNT(DISTINCT *).
fn whole_solution_key(row: &Solution) -> Result<Vec<u8>, SparqlError> {
    let mut entries: Vec<(&str, TermRef<'_>)> = row
        .iter()
        .map(|(name, value)| (name.as_str(), TermRef::from(value)))
        .collect();
    entries.sort_unstable_by(|a, b| a.0.cmp(b.0));
    serde_json::to_vec(&entries)
        .map_err(|e| SparqlError::Execution(format!("COUNT(DISTINCT *) key: {e}")))
}

fn finish_group(
    variables: &[String],
    specs: &[(String, CountAggregate)],
    key: Vec<Option<Binding>>,
    counts: Vec<u64>,
) -> Solution {
    let mut row: Solution = variables
        .iter()
        .zip(key)
        .filter_map(|(name, value)| value.map(|v| (name.clone(), v)))
        .collect();
    for ((name, _), count) in specs.iter().zip(counts) {
        row.insert(
            name.clone(),
            Binding::literal(count.to_string(), Some(XSD_INTEGER.into()), None),
        );
    }
    row
}

/// Borrow a variable term; only computed expressions need an owned binding.
fn count_expression<'a>(
    expression: &spargebra::algebra::Expression,
    row: &'a Solution,
) -> Result<Option<Cow<'a, Binding>>, SparqlError> {
    if let spargebra::algebra::Expression::Variable(variable) = expression {
        Ok(row.get(variable.as_str()).map(Cow::Borrowed))
    } else {
        Ok(crate::bind::evaluate(expression, row)?.map(Cow::Owned))
    }
}
