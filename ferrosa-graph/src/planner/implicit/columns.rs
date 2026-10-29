//! Infer only missing scalar columns from a Bolt write's bound expressions.
use super::{column, column_type};
use crate::error::{GraphError, Result};
use crate::parser::{Expr, Pattern, Statement};
use ferrosa_schema::metadata::column::ColumnMetadata;
use ferrosa_schema::registry::SchemaSnapshot;
use ferrosa_schema::ColumnKind;
use std::collections::BTreeMap;

/// Empty on the common path; values/column metadata are cloned only for new columns.
pub fn columns_needing_declarations(
    snapshot: &SchemaSnapshot,
    keyspace: &str,
    statement: &Statement,
) -> Result<BTreeMap<String, Vec<ColumnMetadata>>> {
    let (patterns, assignments, include_inline) = match statement {
        Statement::Create { patterns, .. } => (patterns.as_slice(), &[][..], true),
        Statement::Merge {
            patterns,
            set_clause,
            ..
        } => (patterns.as_slice(), set_clause.as_slice(), true),
        Statement::Set {
            pattern,
            assignments,
            ..
        } => (pattern.as_slice(), assignments.as_slice(), false),
        _ => return Ok(BTreeMap::new()),
    };
    let mut additions = BTreeMap::new();
    for pattern in patterns {
        visit(pattern, &mut |var, label, props| {
            let written = props
                .iter()
                .filter(|_| include_inline)
                .map(|(name, value)| (name.as_str(), value))
                .chain(
                    assignments
                        .iter()
                        .filter(|a| var == Some(a.var.as_str()))
                        .map(|a| (a.property.as_str(), &a.value)),
                );
            add_properties(snapshot, keyspace, label, written, &mut additions)
        })?;
    }
    Ok(additions)
}

fn visit<'a>(
    pattern: &'a Pattern,
    f: &mut impl FnMut(Option<&'a str>, &'a str, &'a [(String, Expr)]) -> Result<()>,
) -> Result<()> {
    match pattern {
        Pattern::Node {
            var,
            label: Some(label),
            props,
        } => f(var.as_deref(), label, props),
        Pattern::Rel {
            var,
            rel_type: Some(label),
            props,
            ..
        } if !label.contains('|') => f(var.as_deref(), label, props),
        Pattern::Path(elements) => {
            for element in elements {
                visit(element, f)?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

fn add_properties<'a>(
    snapshot: &SchemaSnapshot,
    keyspace: &str,
    label: &str,
    written: impl Iterator<Item = (&'a str, &'a Expr)>,
    additions: &mut BTreeMap<String, Vec<ColumnMetadata>>,
) -> Result<()> {
    // Resolve once per written entity, with the same label rules as planning.
    let mut written = written.peekable();
    if written.peek().is_none() {
        return Ok(());
    }
    let resolved = crate::planner::logical::resolve_label(snapshot, keyspace, label)?;
    let table = &snapshot.tables[&(resolved.keyspace, resolved.table)];
    for (name, value) in written {
        if table.columns.contains_key(name) {
            continue;
        }
        let inferred = column_type(label, name, value)?;
        let columns = additions.entry(table.name.clone()).or_default();
        if let Some(existing) = columns.iter().find(|column| column.name == name) {
            if existing.column_type != inferred {
                return Err(GraphError::Validation(format!(
                    "conflicting first-write types for {keyspace}.{}.{name}",
                    table.name
                )));
            }
        } else {
            columns.push(column(name, ColumnKind::Regular, 0, &inferred));
        }
    }
    Ok(())
}
