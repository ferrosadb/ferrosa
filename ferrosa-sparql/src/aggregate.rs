//! Bounded COUNT accumulators over a stream of solution mappings.
//! Correctness: preserve RDF term identity, unbound values and empty-input groups.
//! Last revised: 2026-10-27
//! Last changed: Add scalar/grouped COUNT without retaining input solutions.

use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::error::SparqlError;
use crate::executor::{check_solution_bound, ExecutionLimits};
use crate::planner::CountAggregate;
use crate::results::Binding;

type Solution = HashMap<String, Binding>;
type GroupKey = Vec<Option<Binding>>;

enum Seen {
    Terms(HashSet<Binding>),
    Solutions(HashSet<Arc<Vec<(String, Binding)>>>),
}

struct Counter {
    value: u64,
    seen: Option<Seen>,
}

/// Retain only group keys, counters and explicitly requested DISTINCT state.
pub(crate) struct Counts<'a> {
    variables: &'a [String],
    specs: &'a [(String, CountAggregate)],
    limits: &'a ExecutionLimits,
    single: Option<Vec<Counter>>,
    groups: HashMap<GroupKey, Vec<Counter>>,
    distinct_values: usize,
}

impl<'a> Counts<'a> {
    pub(crate) fn new(
        variables: &'a [String],
        specs: &'a [(String, CountAggregate)],
        limits: &'a ExecutionLimits,
    ) -> Self {
        Self {
            variables,
            specs,
            limits,
            single: variables.is_empty().then(|| counters(specs)),
            groups: HashMap::new(),
            distinct_values: 0,
        }
    }

    pub(crate) fn feed(&mut self, row: Solution) -> Result<(), SparqlError> {
        let states = if let Some(single) = &mut self.single {
            single
        } else {
            let key: GroupKey = self
                .variables
                .iter()
                .map(|name| row.get(name).cloned())
                .collect();
            if !self.groups.contains_key(&key) {
                check_solution_bound(self.groups.len().saturating_add(1), self.limits)?;
            }
            self.groups
                .entry(key)
                .or_insert_with(|| counters(self.specs))
        };
        for ((_, spec), state) in self.specs.iter().zip(states.iter_mut()) {
            self.distinct_values +=
                usize::from(state.feed(spec, &row, self.distinct_values, self.limits)?);
        }
        if states
            .iter()
            .any(|state| matches!(state.seen, Some(Seen::Solutions(_))))
        {
            // COUNT(DISTINCT *) owns exactly the retained solution key. Move its
            // terms once, then share it across any repeated DISTINCT * aggregates.
            let mut key: Vec<_> = row.into_iter().collect();
            key.sort_unstable_by(|a, b| a.0.cmp(&b.0));
            let key = Arc::new(key);
            for state in states {
                self.distinct_values +=
                    usize::from(state.feed_solution(&key, self.distinct_values, self.limits)?);
            }
        }

        Ok(())
    }

    pub(crate) fn finish(self) -> Vec<Solution> {
        if let Some(single) = self.single {
            vec![finish_group(self.variables, self.specs, vec![], single)]
        } else {
            self.groups
                .into_iter()
                .map(|(key, states)| finish_group(self.variables, self.specs, key, states))
                .collect()
        }
    }
}

fn counters(specs: &[(String, CountAggregate)]) -> Vec<Counter> {
    specs
        .iter()
        .map(|(_, spec)| Counter {
            value: 0,
            seen: if !spec.distinct {
                None
            } else if spec.expression.is_some() {
                Some(Seen::Terms(HashSet::new()))
            } else {
                Some(Seen::Solutions(HashSet::new()))
            },
        })
        .collect()
}

fn admit_distinct(retained: usize, limits: &ExecutionLimits) -> Result<(), SparqlError> {
    check_solution_bound(retained.saturating_add(1), limits)
}

fn finish_group(
    variables: &[String],
    specs: &[(String, CountAggregate)],
    key: GroupKey,
    states: Vec<Counter>,
) -> Solution {
    let mut row: Solution = variables
        .iter()
        .zip(key)
        .filter_map(|(name, value)| value.map(|v| (name.clone(), v)))
        .collect();
    for ((name, _), counter) in specs.iter().zip(states) {
        row.insert(
            name.clone(),
            Binding::literal(
                counter.value.to_string(),
                Some("http://www.w3.org/2001/XMLSchema#integer".into()),
                None,
            ),
        );
    }
    row
}

impl Counter {
    /// Count one bound expression/solution, retaining only explicit DISTINCT keys.
    /// The parent supplies one shared retention budget across groups/aggregates.
    fn feed(
        &mut self,
        spec: &CountAggregate,
        row: &Solution,
        retained: usize,
        limits: &ExecutionLimits,
    ) -> Result<bool, SparqlError> {
        let value = spec
            .expression
            .as_ref()
            .map(|expression| count_expression(expression, row))
            .transpose()?
            .flatten();
        if spec.expression.is_some() && value.is_none() {
            return Ok(false);
        }
        let inserted = match &mut self.seen {
            None => false,
            Some(Seen::Terms(terms)) => {
                let value = value.as_ref().expect("COUNT expression has a bound term");
                if terms.contains(value.as_ref()) {
                    return Ok(false);
                }
                admit_distinct(retained, limits)?;
                terms.insert(value.as_ref().clone())
            }
            Some(Seen::Solutions(_)) => return Ok(false),
        };

        self.value = self
            .value
            .checked_add(1)
            .ok_or_else(|| SparqlError::Execution("COUNT overflow".into()))?;
        Ok(inserted)
    }
    fn feed_solution(
        &mut self,
        key: &Arc<Vec<(String, Binding)>>,
        retained: usize,
        limits: &ExecutionLimits,
    ) -> Result<bool, SparqlError> {
        let Some(Seen::Solutions(solutions)) = &mut self.seen else {
            return Ok(false);
        };
        if solutions.contains(key) {
            return Ok(false);
        }
        admit_distinct(retained, limits)?;
        solutions.insert(Arc::clone(key));
        self.value = self
            .value
            .checked_add(1)
            .ok_or_else(|| SparqlError::Execution("COUNT overflow".into()))?;
        Ok(true)
    }
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
