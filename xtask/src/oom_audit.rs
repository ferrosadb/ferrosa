//! AST-based static audit guarding against read-path materialization / OOM
//! regressions (P0 OOM Guard, `specs/p0-oom-guard/blueprint.md`).
//!
//! D2 (decision record): this audit is **AST-based** (`syn`). It must never
//! match source text with `.contains()` — a source-grep test is precisely what
//! certified the original bug. Every rule below inspects parsed syntax.

use std::path::{Path, PathBuf};

use serde::Deserialize;
use syn::spanned::Spanned;
use syn::visit::Visit;

/// Default "today" used by pure lib code. The CLI may override via `--today`.
/// Lib code never reads the system clock (deterministic, testable).
pub const DEFAULT_TODAY: &str = "2026-06-29";

// ---------------------------------------------------------------------------
// Findings + allowlist
// ---------------------------------------------------------------------------

/// One audit hit: a place a materializing read shape was detected, or an
/// expired allowlist entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Finding {
    pub path: String,
    pub line: usize,
    pub rule: &'static str,
    pub message: String,
    /// The fn / method symbol the finding attaches to, used for whitelist
    /// `symbol` matching. Empty when not applicable.
    pub symbol: String,
}

/// Rule ids. Kept as constants so tests and allowlist entries can't typo them.
pub mod rule {
    pub const STREAM_RETURNS_VEC: &str = "stream-returns-vec";
    pub const RETURNS_VEC_PARTITION_OR_ROW: &str = "returns-vec-partition-or-row";
    pub const WITH_CAPACITY_LIMIT: &str = "with-capacity-limit";
    pub const STREAM_PUSH_ACCUMULATION: &str = "stream-push-accumulation";
    pub const LIMITED_ROWS_CALL_SITE: &str = "limited-rows-call-site";
    pub const COLLECT_VEC_PARTITION_OR_ROW: &str = "collect-vec-partition-or-row";
    pub const UNBOUNDED_RANGE_READ: &str = "unbounded-range-read";
    pub const MATERIALIZING_RANGE_READ_CALL_SITE: &str = "materializing-range-read-call-site";
    pub const CQLVALUE_ROW_ACCUMULATION: &str = "cqlvalue-row-accumulation";
    pub const CLONE_ON_ROW_DATA: &str = "clone-on-row-data";
    pub const CLONED_STREAM_ELEMENTS: &str = "cloned-stream-elements";
    pub const CLONE_IN_SCAN_CLOSURE: &str = "clone-in-scan-closure";
    pub const COPIES_ROW_DATA_ARG: &str = "copies-row-data-arg";
    pub const COPY_DERIVE_LARGE_TYPE: &str = "copy-derive-large-type";
    pub const SERVER_SIDE_RESULT_CAP: &str = "server-side-result-cap";
    pub const EXPIRED_ALLOW: &str = "expired-allow-entry";
}

#[derive(Debug, Clone, Deserialize)]
pub struct AllowEntry {
    /// Substring matched against the finding's file path.
    pub path: String,
    /// Rule id this entry suppresses.
    pub rule: String,
    /// Optional substring matched against the finding's symbol.
    #[serde(default)]
    pub symbol: Option<String>,
    // `reason` is required by the schema for human review / audit trail; it is
    // not consumed programmatically, hence the explicit allow.
    #[allow(dead_code)]
    pub reason: String,
    pub owner: String,
    /// `YYYY-MM-DD`. An entry whose `expires < today` is itself a finding.
    pub expires: String,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct Allowlist {
    #[serde(default, rename = "allow")]
    pub entries: Vec<AllowEntry>,
}

impl Allowlist {
    /// Parse the TOML allow file body.
    pub fn from_str(src: &str) -> anyhow::Result<Self> {
        Ok(toml::from_str(src)?)
    }

    /// Load from disk; a missing file is an empty allowlist (no suppressions).
    pub fn load(path: &Path) -> anyhow::Result<Self> {
        match std::fs::read_to_string(path) {
            Ok(src) => Self::from_str(&src),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Self::default()),
            Err(e) => Err(e.into()),
        }
    }

    /// True if `entry` suppresses `finding` (path + rule + optional symbol match).
    fn suppresses(entry: &AllowEntry, finding: &Finding) -> bool {
        finding.rule == entry.rule
            && finding.path.contains(&entry.path)
            && entry
                .symbol
                .as_deref()
                .is_none_or(|s| finding.symbol.contains(s))
    }

    /// True if any entry suppresses this finding.
    pub fn allows(&self, finding: &Finding) -> bool {
        self.entries.iter().any(|e| Self::suppresses(e, finding))
    }
}

/// Findings for any allow entry whose `expires` is strictly before `today`.
/// An expired exemption is a guard failure — it must be renewed or removed.
pub fn expired_allow_findings(allow: &Allowlist, today: &str) -> Vec<Finding> {
    allow
        .entries
        .iter()
        .filter(|e| e.expires.as_str() < today)
        .map(|e| Finding {
            path: e.path.clone(),
            line: 0,
            rule: rule::EXPIRED_ALLOW,
            message: format!(
                "allow entry for rule `{}` expired {} (owner {}); renew or remove",
                e.rule, e.expires, e.owner
            ),
            symbol: e.symbol.clone().unwrap_or_default(),
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Type / call shape helpers (pure)
// ---------------------------------------------------------------------------

/// Last path segment ident of a type path, e.g. `a::b::Vec` -> `Vec`.
fn last_segment_ident(ty: &syn::Type) -> Option<String> {
    if let syn::Type::Path(tp) = ty {
        return tp.path.segments.last().map(|s| s.ident.to_string());
    }
    None
}

/// If `ty` is `Vec<Inner>` (by last segment), return `Inner`.
fn vec_inner(ty: &syn::Type) -> Option<&syn::Type> {
    let syn::Type::Path(tp) = ty else { return None };
    let seg = tp.path.segments.last()?;
    if seg.ident != "Vec" {
        return None;
    }
    let syn::PathArguments::AngleBracketed(args) = &seg.arguments else {
        return None;
    };
    args.args.iter().find_map(|a| match a {
        syn::GenericArgument::Type(t) => Some(t),
        _ => None,
    })
}

/// If `ty` is `Result<Ok, ..>` (by last segment), return `Ok`.
fn result_ok(ty: &syn::Type) -> Option<&syn::Type> {
    let syn::Type::Path(tp) = ty else { return None };
    let seg = tp.path.segments.last()?;
    if seg.ident != "Result" {
        return None;
    }
    let syn::PathArguments::AngleBracketed(args) = &seg.arguments else {
        return None;
    };
    args.args.iter().find_map(|a| match a {
        syn::GenericArgument::Type(t) => Some(t),
        _ => None,
    })
}

/// Does this return type ultimately yield a `Vec<_>` (directly or as the Ok arm
/// of a `Result`)? Best-effort: also treats a type alias whose ident contains
/// `Vec` as a wrapper.
fn yields_vec(ty: &syn::Type) -> bool {
    if vec_inner(ty).is_some() {
        return true;
    }
    if let Some(ok) = result_ok(ty) {
        if vec_inner(ok).is_some() {
            return true;
        }
        if let Some(id) = last_segment_ident(ok) {
            return id.contains("Vec");
        }
    }
    matches!(last_segment_ident(ty), Some(id) if id != "Vec" && id.contains("Vec"))
}

/// The inner element ident of a `Vec<Inner>` return (through `Result`), e.g.
/// `Result<Vec<Partition>>` -> `Partition`.
fn vec_element_ident(ty: &syn::Type) -> Option<String> {
    let direct = vec_inner(ty);
    let via_result = result_ok(ty).and_then(vec_inner);
    direct.or(via_result).and_then(last_segment_ident)
}

/// Return type of a fn signature, if it has one (not `()`).
fn return_type(sig: &syn::Signature) -> Option<&syn::Type> {
    match &sig.output {
        syn::ReturnType::Type(_, ty) => Some(ty),
        syn::ReturnType::Default => None,
    }
}

/// Bare-identifier names that signal a paging / user-derived capacity argument.
const CAP_IDENTS: &[&str] = &["limit", "cap", "n", "count"];

/// True if a `with_capacity` argument expression is a capacity heuristic hit:
/// a bare ident in `CAP_IDENTS`, or any ident path containing `limit`.
fn is_paging_capacity_arg(expr: &syn::Expr) -> bool {
    if let syn::Expr::Path(p) = expr {
        if let Some(id) = p.path.segments.last() {
            let name = id.ident.to_string();
            return CAP_IDENTS.contains(&name.as_str()) || name.contains("limit");
        }
    }
    false
}

/// Method names whose call sites trigger the broad-scan rule (e).
fn is_limited_rows_call(method: &str) -> bool {
    method == "range_read_limited_rows"
        || method == "read_local_range_limited_rows"
        || (method.starts_with("coordinate_") && method.ends_with("_limited_rows"))
}

/// `Partition`/`Row` element idents that signal broad-scan row materialization.
fn is_partition_or_row_ident(id: &str) -> bool {
    id == "Partition" || id == "Row"
}

/// If `ty` (through an outer `Vec`/`Result`) is a `Vec<Partition>` / `Vec<Row>`,
/// return the element ident.
fn vec_partition_or_row_elem(ty: &syn::Type) -> Option<String> {
    let elem = vec_element_ident(ty)?;
    is_partition_or_row_ident(&elem).then_some(elem)
}

/// True if `ty` is `Vec<Vec<Option<CqlValue>>>` or `Vec<Vec<CqlValue>>`
/// (broad-scan CQL row materialization). Best-effort on last-segment idents.
fn is_cqlvalue_row_matrix(ty: &syn::Type) -> bool {
    // Outer must be Vec<inner>.
    let Some(inner) = vec_inner(ty) else {
        return false;
    };
    // inner must be Vec<cell>.
    let Some(cell) = vec_inner(inner) else {
        return false;
    };
    // cell is either `CqlValue` directly or `Option<CqlValue>`.
    if last_segment_ident(cell).as_deref() == Some("CqlValue") {
        return true;
    }
    if last_segment_ident(cell).as_deref() == Some("Option") {
        if let syn::Type::Path(tp) = cell {
            if let Some(seg) = tp.path.segments.last() {
                if let syn::PathArguments::AngleBracketed(args) = &seg.arguments {
                    return args.args.iter().any(|a| {
                        matches!(a, syn::GenericArgument::Type(t)
                            if last_segment_ident(t).as_deref() == Some("CqlValue"))
                    });
                }
            }
        }
    }
    false
}

/// Turbofish element of a `.collect::<Vec<Partition>>()` call, if it is a
/// `Vec<Partition>` / `Vec<Row>`.
fn collect_turbofish_partition_or_row(node: &syn::ExprMethodCall) -> Option<String> {
    if node.method != "collect" {
        return None;
    }
    let args = node.turbofish.as_ref()?;
    args.args.iter().find_map(|a| match a {
        syn::GenericArgument::Type(t) => vec_partition_or_row_elem(t),
        _ => None,
    })
}

/// Receiver idents whose per-element `.clone()`/`.to_vec()`/`.to_owned()` we flag
/// as a row-data copy on a scan path (rule: clone-on-row-data). Case-insensitive.
/// `chunk`/`fragment` are the shapes row data takes in the streaming pipeline
/// (net frames, coordinator merge) — copying them defeats move streaming the
/// same way copying a partition does.
const ROW_DATA_IDENTS: &[&str] = &[
    "partition",
    "partitions",
    "row",
    "rows",
    "cell",
    "cells",
    "chunk",
    "chunks",
    "frag",
    "frags",
    "fragment",
    "fragments",
];

/// Type idents that carry row data. A local/param declared with one of these
/// (possibly behind `&`/`Vec`/`Option`/`Box`/`Arc`/slice) is row data no matter
/// what the binding is called — this closes the renamed-binding blind spot
/// (`let part = partition; part.clone()`).
const ROW_DATA_TYPE_IDENTS: &[&str] = &["Partition", "Row", "Cell", "CellValue"];

/// Copying method names that materialize a clone of row data.
fn is_copying_method(method: &str) -> bool {
    method == "clone" || method == "to_vec" || method == "to_owned"
}

/// Iterator adapters that take a closure over the stream's elements. A clone of
/// the closure param inside one of these is a per-element copy of the stream.
fn is_element_closure_adapter(method: &str) -> bool {
    matches!(
        method,
        "map"
            | "filter"
            | "filter_map"
            | "flat_map"
            | "for_each"
            | "inspect"
            | "fold"
            | "scan"
            | "take_while"
            | "skip_while"
            | "and_then"
    )
}

/// Trailing ident of a receiver expression, looking through `&`/`*`/parens:
/// a bare ident, the field name of a field access (`self.rows` -> `rows`), or
/// the method name of an accessor call (`holder.rows()` -> `rows`).
fn receiver_trailing_ident(expr: &syn::Expr) -> Option<String> {
    match expr {
        syn::Expr::Path(p) => p.path.segments.last().map(|s| s.ident.to_string()),
        syn::Expr::Field(f) => match &f.member {
            syn::Member::Named(id) => Some(id.to_string()),
            syn::Member::Unnamed(_) => None,
        },
        syn::Expr::MethodCall(mc) => Some(mc.method.to_string()),
        syn::Expr::Reference(r) => receiver_trailing_ident(&r.expr),
        syn::Expr::Paren(p) => receiver_trailing_ident(&p.expr),
        syn::Expr::Group(g) => receiver_trailing_ident(&g.expr),
        syn::Expr::Unary(u) => receiver_trailing_ident(&u.expr),
        syn::Expr::Await(a) => receiver_trailing_ident(&a.base),
        syn::Expr::Try(t) => receiver_trailing_ident(&t.expr),
        _ => None,
    }
}

/// Every named link in a method-call chain: root idents, field names, and
/// method names, looking through `&`/`*`/parens/`.await`/`?`.
/// `holder.rows().iter()` -> ["holder", "rows", "iter"].
fn chain_idents(expr: &syn::Expr) -> Vec<String> {
    let mut idents = Vec::new();
    collect_chain_idents(expr, &mut idents);
    idents
}

fn collect_chain_idents(expr: &syn::Expr, out: &mut Vec<String>) {
    match expr {
        syn::Expr::MethodCall(mc) => {
            collect_chain_idents(&mc.receiver, out);
            out.push(mc.method.to_string());
        }
        syn::Expr::Path(p) => {
            if let Some(seg) = p.path.segments.last() {
                out.push(seg.ident.to_string());
            }
        }
        syn::Expr::Field(f) => {
            collect_chain_idents(&f.base, out);
            if let syn::Member::Named(id) = &f.member {
                out.push(id.to_string());
            }
        }
        syn::Expr::Call(c) => {
            if let syn::Expr::Path(p) = &*c.func {
                if let Some(seg) = p.path.segments.last() {
                    out.push(seg.ident.to_string());
                }
            }
        }
        syn::Expr::Reference(r) => collect_chain_idents(&r.expr, out),
        syn::Expr::Paren(p) => collect_chain_idents(&p.expr, out),
        syn::Expr::Group(g) => collect_chain_idents(&g.expr, out),
        syn::Expr::Unary(u) => collect_chain_idents(&u.expr, out),
        syn::Expr::Await(a) => collect_chain_idents(&a.base, out),
        syn::Expr::Try(t) => collect_chain_idents(&t.expr, out),
        _ => {}
    }
}

/// True if `ident` (case-insensitive exact match) is in the row-data set.
fn is_row_data_ident(ident: &str) -> bool {
    let lower = ident.to_ascii_lowercase();
    ROW_DATA_IDENTS.contains(&lower.as_str())
}

/// True if `ty` is (or wraps) a row-data type: `Partition`, `&[Row]`,
/// `Vec<Cell>`, `Option<Box<Partition>>`, …
fn is_row_data_type(ty: &syn::Type) -> bool {
    match ty {
        syn::Type::Reference(r) => is_row_data_type(&r.elem),
        syn::Type::Paren(p) => is_row_data_type(&p.elem),
        syn::Type::Slice(s) => is_row_data_type(&s.elem),
        syn::Type::Array(a) => is_row_data_type(&a.elem),
        syn::Type::Path(tp) => {
            let Some(seg) = tp.path.segments.last() else {
                return false;
            };
            let name = seg.ident.to_string();
            if ROW_DATA_TYPE_IDENTS.contains(&name.as_str()) {
                return true;
            }
            // Wrapper types: recurse into the single meaningful generic arg.
            if matches!(
                name.as_str(),
                "Vec" | "Option" | "Box" | "Arc" | "Rc" | "Cow"
            ) {
                if let syn::PathArguments::AngleBracketed(args) = &seg.arguments {
                    return args.args.iter().any(
                        |a| matches!(a, syn::GenericArgument::Type(t) if is_row_data_type(t)),
                    );
                }
            }
            false
        }
        _ => false,
    }
}

/// All idents bound by a pattern (`|r|`, `|(k, v)|`, `|Foo { a, .. }|`).
fn pat_idents(pat: &syn::Pat, out: &mut Vec<String>) {
    match pat {
        syn::Pat::Ident(pi) => out.push(pi.ident.to_string()),
        syn::Pat::Type(pt) => pat_idents(&pt.pat, out),
        syn::Pat::Tuple(t) => t.elems.iter().for_each(|p| pat_idents(p, out)),
        syn::Pat::TupleStruct(ts) => ts.elems.iter().for_each(|p| pat_idents(p, out)),
        syn::Pat::Struct(ps) => ps.fields.iter().for_each(|f| pat_idents(&f.pat, out)),
        syn::Pat::Reference(r) => pat_idents(&r.pat, out),
        syn::Pat::Paren(p) => pat_idents(&p.pat, out),
        _ => {}
    }
}

/// True if `expr` (or any subexpression) references a streaming source: a call
/// to `range_iter`, an ident/method containing `stream`, or a `.next()` call.
fn contains_stream_source(expr: &syn::Expr) -> bool {
    struct StreamFinder {
        found: bool,
    }
    impl<'ast> Visit<'ast> for StreamFinder {
        fn visit_expr_method_call(&mut self, node: &'ast syn::ExprMethodCall) {
            let m = node.method.to_string();
            if m == "range_iter" || m == "next" || m.contains("stream") {
                self.found = true;
            }
            syn::visit::visit_expr_method_call(self, node);
        }
        fn visit_path(&mut self, node: &'ast syn::Path) {
            if node
                .segments
                .iter()
                .any(|s| s.ident == "range_iter" || s.ident.to_string().contains("stream"))
            {
                self.found = true;
            }
            syn::visit::visit_path(self, node);
        }
    }
    let mut f = StreamFinder { found: false };
    f.visit_expr(expr);
    f.found
}

/// Call sites we flag as materializing range reads (rule:
/// materializing-range-read-call-site): the plain/projected variants, excluding
/// stream and `_from`/`_limited_rows` siblings.
fn is_materializing_range_read_call(method: &str) -> bool {
    method == "range_read" || method == "range_read_projected"
}

/// True if two CONSECUTIVE arguments are both the literal `None` — the
/// range-bound `(.., None, None, ..)` shape of an unbounded full-table read.
/// Mirrors the retired `scripts/check-unbounded-reads.py` regex in the AST.
fn has_consecutive_none_pair<'a, I>(args: I) -> bool
where
    I: IntoIterator<Item = &'a syn::Expr>,
{
    let nones: Vec<bool> = args.into_iter().map(is_none_literal).collect();
    nones.windows(2).any(|w| w[0] && w[1])
}

/// True if `expr` is the literal `None` (path whose last segment is `None`).
fn is_none_literal(expr: &syn::Expr) -> bool {
    matches!(expr, syn::Expr::Path(p)
        if p.path.segments.last().map(|s| s.ident == "None") == Some(true))
}

// ---------------------------------------------------------------------------
// Visitor
// ---------------------------------------------------------------------------

struct Auditor<'a> {
    path: &'a str,
    findings: Vec<Finding>,
    /// Name of the fn currently being visited (for symbol attribution).
    current_fn: String,
    /// Idents bound to a row-data TYPE in the current fn (params + typed
    /// locals) — row data regardless of what the binding is called.
    row_typed: std::collections::HashSet<String>,
}

impl<'a> Auditor<'a> {
    fn new(path: &'a str) -> Self {
        Self {
            path,
            findings: Vec::new(),
            current_fn: String::new(),
            row_typed: std::collections::HashSet::new(),
        }
    }

    /// True if this receiver/arg ident is row data: by name, or by the type it
    /// was bound with in the current fn.
    fn is_row_data(&self, ident: &str) -> bool {
        is_row_data_ident(ident) || self.row_typed.contains(ident)
    }

    /// True if any named link of a method-call chain is row data, or the chain
    /// draws from a streaming source. Used for the per-ELEMENT rules (6/7), so
    /// a chain ending in `.next()` is excluded: `.cloned()`/closures after
    /// `.next()` touch ONE `Option` element, not every element of the stream.
    fn chain_is_row_data(&self, expr: &syn::Expr) -> bool {
        let ids = chain_idents(expr);
        if ids.last().map(String::as_str) == Some("next") {
            return false;
        }
        ids.iter().any(|id| self.is_row_data(id))
            || ids
                .iter()
                .any(|id| id.contains("stream") || id == "range_iter")
    }

    /// Collect row-typed param idents from a fn signature into `row_typed`.
    fn collect_row_typed_params(&mut self, sig: &syn::Signature) {
        for input in &sig.inputs {
            if let syn::FnArg::Typed(pt) = input {
                if is_row_data_type(&pt.ty) {
                    let mut ids = Vec::new();
                    pat_idents(&pt.pat, &mut ids);
                    self.row_typed.extend(ids);
                }
            }
        }
    }

    fn push(&mut self, line: usize, rule: &'static str, message: String) {
        let symbol = self.current_fn.clone();
        self.push_with_symbol(line, rule, message, &symbol);
    }

    /// Push a finding with an explicit symbol (used by call-site rules whose
    /// allowlist key is the called method / receiver, not the enclosing fn).
    fn push_with_symbol(&mut self, line: usize, rule: &'static str, message: String, symbol: &str) {
        self.findings.push(Finding {
            path: self.path.to_string(),
            line,
            rule,
            message,
            symbol: symbol.to_string(),
        });
    }

    /// Apply the signature-shape rules (a) and (b) to a fn.
    fn check_signature(&mut self, sig: &syn::Signature) {
        let name = sig.ident.to_string();
        let Some(ret) = return_type(sig) else { return };

        // Rule (a): a `stream`-named fn returning a Vec.
        if name.contains("stream") && yields_vec(ret) {
            let line = sig.ident.span().start().line;
            self.push(
                line,
                rule::STREAM_RETURNS_VEC,
                format!("fn `{name}` is named *stream* but returns a materialized Vec"),
            );
        }

        // Rule (b): production fn returning Vec<Partition> / Vec<Row>.
        if let Some(elem) = vec_element_ident(ret) {
            if is_partition_or_row_ident(&elem) {
                let line = sig.ident.span().start().line;
                self.push(
                    line,
                    rule::RETURNS_VEC_PARTITION_OR_ROW,
                    format!("fn `{name}` returns Vec<{elem}> (materializes the read path)"),
                );
            }
        }

        // Rule 4: fn returning Vec<Vec<(Option<)CqlValue(>)>>.
        if is_cqlvalue_row_matrix(ret) {
            let line = sig.ident.span().start().line;
            self.push(
                line,
                rule::CQLVALUE_ROW_ACCUMULATION,
                format!("fn `{name}` returns a Vec<Vec<CqlValue>> row matrix (broad-scan rows)"),
            );
        }
    }

    /// Free-function call rules (rule (c) with_capacity, rule 2 free-fn
    /// `read_range(None, None, ..)`).
    fn check_expr_call(&mut self, node: &syn::ExprCall) {
        let syn::Expr::Path(p) = &*node.func else {
            return;
        };
        let segs: Vec<String> = p
            .path
            .segments
            .iter()
            .map(|s| s.ident.to_string())
            .collect();
        let last = segs.last().map(String::as_str);

        // Rule (c): Vec::with_capacity(<paging cap>).
        if last == Some("with_capacity") && segs.iter().any(|s| s == "Vec") {
            if let Some(arg) = node.args.first() {
                if is_paging_capacity_arg(arg) {
                    self.push(
                        node.span().start().line,
                        rule::WITH_CAPACITY_LIMIT,
                        "Vec::with_capacity from a paging/limit-derived size pre-allocates an \
                         unbounded buffer"
                            .to_string(),
                    );
                }
            }
        }

        // Rule 2: free-fn `read_range(None, None, ..)`.
        if last == Some("read_range") && has_consecutive_none_pair(node.args.iter()) {
            self.push(
                node.span().start().line,
                rule::UNBOUNDED_RANGE_READ,
                "read_range(None, None, ..) is an unbounded full-table range read".to_string(),
            );
        }

        // Rule 8 (UFCS form): `Partition::clone(&p)` / `Clone::clone(&partition)`
        // — the explicit-path spelling of a row-data clone.
        if last == Some("clone") {
            let type_seg_is_row = segs
                .iter()
                .any(|s| ROW_DATA_TYPE_IDENTS.contains(&s.as_str()));
            let arg_ident = node.args.first().and_then(receiver_trailing_ident);
            let arg_is_row = arg_ident.as_deref().is_some_and(|id| self.is_row_data(id));
            if type_seg_is_row || arg_is_row {
                let sym = arg_ident.unwrap_or_else(|| segs.join("::"));
                self.push_with_symbol(
                    node.span().start().line,
                    rule::COPIES_ROW_DATA_ARG,
                    "UFCS `::clone(..)` of row data copies the whole value (prefer move)"
                        .to_string(),
                    &sym,
                );
            }
        }

        // Rule 8 (Vec::from form): `Vec::from(&rows)` clones every element.
        if last == Some("from") && segs.iter().any(|s| s == "Vec") {
            if let Some(id) = node.args.first().and_then(receiver_trailing_ident) {
                if self.is_row_data(&id) {
                    self.push_with_symbol(
                        node.span().start().line,
                        rule::COPIES_ROW_DATA_ARG,
                        format!("`Vec::from({id})` clones every row-data element (prefer move)"),
                        &id,
                    );
                }
            }
        }
    }

    /// Typed-binding rules (rule 1 collect-to-typed-Vec, rule 4 CqlValue matrix).
    fn check_typed_binding(&mut self, ty: &syn::Type, init: Option<&syn::Expr>, line: usize) {
        // Rule 1 (collect via typed binding): only when the initializer is a
        // `.collect()` call, so this doesn't double-flag plain Vec<Partition>
        // bindings already covered by the return-type rule.
        if init.is_some_and(expr_is_collect_call) {
            if let Some(elem) = vec_partition_or_row_elem(ty) {
                self.push(
                    line,
                    rule::COLLECT_VEC_PARTITION_OR_ROW,
                    format!("collect into a Vec<{elem}>-typed binding materializes the scan"),
                );
            }
        }
        if is_cqlvalue_row_matrix(ty) {
            self.push(
                line,
                rule::CQLVALUE_ROW_ACCUMULATION,
                "binding typed Vec<Vec<CqlValue>> accumulates a broad-scan row matrix".to_string(),
            );
        }
    }
}

/// True if `expr` is (or ends in) a `.collect()` method call.
fn expr_is_collect_call(expr: &syn::Expr) -> bool {
    matches!(expr, syn::Expr::MethodCall(mc) if mc.method == "collect")
}

/// Copy sites of a closure param inside a closure body: `(line, param, method)`
/// for every `.clone()/.to_vec()/.to_owned()` whose receiver is one of `params`.
fn closure_param_copy_sites(body: &syn::Expr, params: &[String]) -> Vec<(usize, String, String)> {
    struct ParamCloneFinder<'p> {
        params: &'p [String],
        hits: Vec<(usize, String, String)>,
    }
    impl<'ast> Visit<'ast> for ParamCloneFinder<'_> {
        fn visit_expr_method_call(&mut self, node: &'ast syn::ExprMethodCall) {
            let method = node.method.to_string();
            if is_copying_method(&method) {
                if let Some(recv) = receiver_trailing_ident(&node.receiver) {
                    if self.params.contains(&recv) {
                        self.hits
                            .push((node.method.span().start().line, recv, method));
                    }
                }
            }
            syn::visit::visit_expr_method_call(self, node);
        }
    }
    let mut f = ParamCloneFinder {
        params,
        hits: Vec::new(),
    };
    f.visit_expr(body);
    f.hits
}

impl<'ast> Visit<'ast> for Auditor<'_> {
    fn visit_item_fn(&mut self, node: &'ast syn::ItemFn) {
        let prev = std::mem::replace(&mut self.current_fn, node.sig.ident.to_string());
        let prev_typed = std::mem::take(&mut self.row_typed);
        self.collect_row_typed_params(&node.sig);
        self.check_signature(&node.sig);
        syn::visit::visit_item_fn(self, node);
        self.current_fn = prev;
        self.row_typed = prev_typed;
    }

    fn visit_impl_item_fn(&mut self, node: &'ast syn::ImplItemFn) {
        let prev = std::mem::replace(&mut self.current_fn, node.sig.ident.to_string());
        let prev_typed = std::mem::take(&mut self.row_typed);
        self.collect_row_typed_params(&node.sig);
        self.check_signature(&node.sig);
        syn::visit::visit_impl_item_fn(self, node);
        self.current_fn = prev;
        self.row_typed = prev_typed;
    }

    fn visit_expr_method_call(&mut self, node: &'ast syn::ExprMethodCall) {
        let method = node.method.to_string();
        let line = node.method.span().start().line;

        // Rule (e): broad-scan limited-rows call site. The call-site method name
        // is the symbol (so allowlist `symbol` matches the called method).
        if is_limited_rows_call(&method) {
            self.push_with_symbol(
                line,
                rule::LIMITED_ROWS_CALL_SITE,
                format!("call to broad-scan `{method}` materializes the read path"),
                &method,
            );
        }

        // Rule 1 (collect): `.collect::<Vec<Partition>>()` turbofish form.
        if let Some(elem) = collect_turbofish_partition_or_row(node) {
            self.push(
                line,
                rule::COLLECT_VEC_PARTITION_OR_ROW,
                format!(".collect::<Vec<{elem}>>() materializes the scan into a Vec"),
            );
        }

        // Rule 1 (extend): `.extend(<stream-source expr>)`.
        if method == "extend" {
            if let Some(arg) = node.args.first() {
                if contains_stream_source(arg) {
                    self.push(
                        line,
                        rule::COLLECT_VEC_PARTITION_OR_ROW,
                        ".extend() from a streaming source (range_iter/stream/next) materializes \
                         the stream into a Vec"
                            .to_string(),
                    );
                }
            }
        }

        // Rule 2: `read_range` whose first two args are both `None` (unbounded).
        if method == "read_range" && has_consecutive_none_pair(node.args.iter()) {
            self.push(
                line,
                rule::UNBOUNDED_RANGE_READ,
                "read_range(None, None, ..) is an unbounded full-table range read".to_string(),
            );
        }

        // Rule 3: materializing range-read call site (plain / projected).
        if is_materializing_range_read_call(&method) {
            self.push_with_symbol(
                line,
                rule::MATERIALIZING_RANGE_READ_CALL_SITE,
                format!("call to materializing `{method}` (non-stream) reads the whole range"),
                &method,
            );
        }

        // Rule 5: per-element clone of row data (move-based guard). The receiver
        // ident is the symbol so site-specific clones can be allowlisted.
        // Matches by NAME (partition/rows/chunk/…) or by TYPE (any ident the
        // current fn bound to a row-data type — closes the renamed-binding gap).
        if is_copying_method(&method) {
            if let Some(recv) = receiver_trailing_ident(&node.receiver) {
                if self.is_row_data(&recv) {
                    self.push_with_symbol(
                        line,
                        rule::CLONE_ON_ROW_DATA,
                        format!("`{recv}.{method}()` copies row data on a scan path (prefer move)"),
                        &recv,
                    );
                }
            }
        }

        // Rule 6: `.cloned()` / `.copied()` iterator adapters over row data or
        // a streaming source — clones EVERY element of the stream.
        if (method == "cloned" || method == "copied") && self.chain_is_row_data(&node.receiver) {
            let sym = chain_idents(&node.receiver)
                .first()
                .cloned()
                .unwrap_or_default();
            self.push_with_symbol(
                line,
                rule::CLONED_STREAM_ELEMENTS,
                format!(
                    "`.{method}()` copies every element of a row-data/stream chain (prefer \
                     into_iter()/move)"
                ),
                &sym,
            );
        }

        // Rule 7: clone of the closure param inside an iterator adapter over
        // row data (`rows.iter().map(|r| r.clone())`) — a per-element copy.
        if is_element_closure_adapter(&method) && self.chain_is_row_data(&node.receiver) {
            for arg in &node.args {
                let syn::Expr::Closure(cl) = arg else {
                    continue;
                };
                let mut params = Vec::new();
                for p in &cl.inputs {
                    pat_idents(p, &mut params);
                }
                let clones = closure_param_copy_sites(&cl.body, &params);
                for (cline, param, cmethod) in clones {
                    self.push_with_symbol(
                        cline,
                        rule::CLONE_IN_SCAN_CLOSURE,
                        format!(
                            "`|{param}| .. {param}.{cmethod}()` inside `.{method}()` copies each \
                             streamed element (prefer move)"
                        ),
                        &param,
                    );
                }
            }
        }

        // Rule 10: server-side result caps at call sites. The only legitimate
        // result bound derives from the inbound query (limit/page/fetch);
        // hardcoded literals (any magnitude) and consts cap results and mask
        // materialization that should stream. `take(1)` (single-element
        // accessor) is exempt.
        if method == "take" || method == "truncate" {
            if let Some(cap) = node.args.first().and_then(result_cap_arg) {
                self.push_with_symbol(
                    line,
                    rule::SERVER_SIDE_RESULT_CAP,
                    format!(
                        "`.{method}({cap})` imposes a server-side result bound not derived from \
                         the query's LIMIT/paging (caps mask materialization that should stream)"
                    ),
                    &cap,
                );
            }
        }
        if method == "clamp" || method == "min" {
            let bound_arg = if method == "clamp" {
                node.args.iter().nth(1)
            } else {
                node.args.first()
            };
            let recv_is_bound = receiver_trailing_ident(&node.receiver)
                .is_some_and(|r| is_query_bound_receiver(&r));
            if recv_is_bound {
                if let Some(cap) = bound_arg.and_then(result_cap_arg) {
                    self.push_with_symbol(
                        line,
                        rule::SERVER_SIDE_RESULT_CAP,
                        format!(
                            "`.{method}(.., {cap})` clamps a query-derived bound to a server-side \
                             cap (results must be bounded only by the query's LIMIT/paging)"
                        ),
                        &cap,
                    );
                }
            }
        }

        // Rule 8 (method form): `buf.extend_from_slice(&rows)` clones every
        // element of a row-data slice into the buffer.
        if method == "extend_from_slice" {
            if let Some(arg) = node.args.first() {
                if let Some(id) = receiver_trailing_ident(arg) {
                    if self.is_row_data(&id) {
                        self.push_with_symbol(
                            line,
                            rule::COPIES_ROW_DATA_ARG,
                            format!(
                                "`extend_from_slice({id})` clones every row-data element (prefer \
                                 extend(drain)/append/move)"
                            ),
                            &id,
                        );
                    }
                }
            }
        }

        syn::visit::visit_expr_method_call(self, node);
    }

    fn visit_expr_call(&mut self, node: &'ast syn::ExprCall) {
        self.check_expr_call(node);
        syn::visit::visit_expr_call(self, node);
    }

    fn visit_local(&mut self, node: &'ast syn::Local) {
        // Typed-binding rules: `let x: <Ty> = ..;`.
        if let syn::Pat::Type(pt) = &node.pat {
            let init = node.init.as_ref().map(|i| &*i.expr);
            self.check_typed_binding(&pt.ty, init, node.let_token.span().start().line);
            // Track row-typed locals so later clones of them are flagged even
            // under a non-row name (`let renamed: Partition = ..`).
            if is_row_data_type(&pt.ty) {
                let mut ids = Vec::new();
                pat_idents(&pt.pat, &mut ids);
                self.row_typed.extend(ids);
            }
        }
        syn::visit::visit_local(self, node);
    }

    fn visit_item_const(&mut self, node: &'ast syn::ItemConst) {
        // Rule 10: a result-count cap CONSTANT is a materialization confession
        // — the code needs a ceiling precisely because something accumulates.
        if int_literal_value(&node.expr).is_some() && is_result_cap_name(&node.ident.to_string()) {
            let name = node.ident.to_string();
            self.push_with_symbol(
                node.ident.span().start().line,
                rule::SERVER_SIDE_RESULT_CAP,
                format!(
                    "const `{name}` is a server-side result-count cap; results must be bounded \
                     only by the inbound query's LIMIT/paging (stream instead of capping)"
                ),
                &name,
            );
        }
        syn::visit::visit_item_const(self, node);
    }

    fn visit_item_struct(&mut self, node: &'ast syn::ItemStruct) {
        // Rule 9: `derive(Copy)` on a large type makes every implicit copy a
        // hidden memmove — large payloads should move or be borrowed.
        if derives_copy(&node.attrs) && struct_is_large(&node.fields) {
            self.push_with_symbol(
                node.ident.span().start().line,
                rule::COPY_DERIVE_LARGE_TYPE,
                format!(
                    "`derive(Copy)` on large struct `{}` makes every use an implicit bulk copy",
                    node.ident
                ),
                &node.ident.to_string(),
            );
        }
        syn::visit::visit_item_struct(self, node);
    }

    fn visit_field(&mut self, node: &'ast syn::Field) {
        // Rule 4: struct field typed as a CqlValue row matrix.
        if is_cqlvalue_row_matrix(&node.ty) {
            let name = node
                .ident
                .as_ref()
                .map(ToString::to_string)
                .unwrap_or_default();
            self.push(
                node.ty.span().start().line,
                rule::CQLVALUE_ROW_ACCUMULATION,
                format!("field `{name}` is a Vec<Vec<CqlValue>> row matrix (broad-scan rows)"),
            );
        }
        syn::visit::visit_field(self, node);
    }

    fn visit_expr_while(&mut self, node: &'ast syn::ExprWhile) {
        // Rule (d): `while let Some(..) = *.next().await { ...; *.push(..) }`.
        if is_stream_next_while(node) && block_has_push(&node.body) {
            self.push(
                node.while_token.span().start().line,
                rule::STREAM_PUSH_ACCUMULATION,
                "stream-draining `while let Some(..) = next().await` loop accumulates via .push() \
                 (materializes the stream)"
                    .to_string(),
            );
        }
        syn::visit::visit_expr_while(self, node);
    }
}

/// True if the while loop is `while let Some(..) = <expr>.next().await`.
fn is_stream_next_while(node: &syn::ExprWhile) -> bool {
    let syn::Expr::Let(let_expr) = &*node.cond else {
        return false;
    };
    if !pat_is_some(&let_expr.pat) {
        return false;
    }
    // RHS must be `<recv>.next().await` (Await wrapping a `.next()` method call).
    let syn::Expr::Await(await_expr) = &*let_expr.expr else {
        return false;
    };
    matches!(&*await_expr.base, syn::Expr::MethodCall(mc) if mc.method == "next")
}

/// True if the pattern is `Some(..)` (possibly via a `TupleStruct` path ending Some).
fn pat_is_some(pat: &syn::Pat) -> bool {
    if let syn::Pat::TupleStruct(ts) = pat {
        return ts.path.segments.last().map(|s| s.ident == "Some") == Some(true);
    }
    false
}

/// True if any statement in the block contains a `.push(..)` method call.
fn block_has_push(block: &syn::Block) -> bool {
    struct PushFinder {
        found: bool,
    }
    impl<'ast> Visit<'ast> for PushFinder {
        fn visit_expr_method_call(&mut self, node: &'ast syn::ExprMethodCall) {
            if node.method == "push" {
                self.found = true;
            }
            syn::visit::visit_expr_method_call(self, node);
        }
    }
    let mut f = PushFinder { found: false };
    f.visit_block(block);
    f.found
}

/// Name fragments identifying MEMORY/shape/time bounds — the DESIRED kind of
/// bound (bytes, frames, chunks, retries, timeouts…) — never result caps.
const MEMORY_SHAPE_EXEMPT: &[&str] = &[
    "BYTE",
    "SIZE",
    "FANIN",
    "FRAME",
    "CHUNK",
    "BUFFER",
    "CAPACITY",
    "RETR",
    "TIMEOUT",
    "_MS",
    "SECS",
    "DEPTH",
    "CONN",
    "POOL",
    "THREAD",
    "LANE",
    "INFLIGHT",
    "WINDOW",
    "CONCURRENCY",
    "BACKOFF",
    "INTERVAL",
    "ATTEMPT",
    // cache sizes are bounded-memory design, not result caps
    "CACHE",
    // *_LEN bounds the length of one value (bytes/chars), not a result count
    "LEN",
];

fn is_memory_shape_name(name: &str) -> bool {
    let up = name.to_ascii_uppercase();
    MEMORY_SHAPE_EXEMPT.iter().any(|e| up.contains(e))
}

/// True if a const NAME denotes a server-side RESULT-COUNT cap: cap-ish
/// (`LIMIT`/`CAP`/`MAX`) + a result unit (rows/partitions/…), and not a
/// memory/shape/time bound.
fn is_result_cap_name(name: &str) -> bool {
    let up = name.to_ascii_uppercase();
    if !(up.contains("LIMIT") || up.contains("CAP") || up.contains("MAX")) {
        return false;
    }
    if is_memory_shape_name(name) {
        return false;
    }
    const RESULT_UNITS: &[&str] = &[
        "ROW",
        "PARTITION",
        "RESULT",
        "READ",
        "SCAN",
        "RANGE",
        "MATCH",
        "KEY",
        "CANDIDATE",
        "ENTRY",
        "DOC",
        "HIT",
        "RECORD",
    ];
    RESULT_UNITS.iter().any(|u| up.contains(u))
}

/// True for a `SCREAMING_SNAKE_CASE` ident (a const reference).
fn is_screaming_const_name(name: &str) -> bool {
    name.chars()
        .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == '_')
        && name.chars().any(|c| c.is_ascii_uppercase())
}

/// Integer value of a literal expression (`10_000` → 10000), if it is one.
fn int_literal_value(expr: &syn::Expr) -> Option<u64> {
    if let syn::Expr::Lit(l) = expr {
        if let syn::Lit::Int(i) = &l.lit {
            return i.base10_parse().ok();
        }
    }
    None
}

/// If a bound argument is NOT derived from the inbound query, return a display
/// name — that's a server-side result cap, whatever its magnitude.
///
/// Flags: ANY integer literal except `1` (`take(1)` is the single-element
/// accessor idiom, equivalent to `.next()`), and any `SCREAMING_CASE` const
/// that is not a memory/shape bound. Lowercase idents (`limit`, `page_size`,
/// `fetch_size`, a local) are treated as query-derived and exempt — hardcoded
/// caps hiding in locals are caught at their const declaration instead.
fn result_cap_arg(expr: &syn::Expr) -> Option<String> {
    if let Some(v) = int_literal_value(expr) {
        return (v != 1).then(|| v.to_string());
    }
    if let syn::Expr::Path(p) = expr {
        if let Some(seg) = p.path.segments.last() {
            let name = seg.ident.to_string();
            if is_screaming_const_name(&name) && !is_memory_shape_name(&name) {
                return Some(name);
            }
        }
    }
    None
}

/// True if a `clamp`/`min` receiver looks like a query-derived result bound
/// (`limit.min(CAP)`) — the shape that silently caps user results.
fn is_query_bound_receiver(name: &str) -> bool {
    let lower = name.to_ascii_lowercase();
    lower.contains("limit") || lower.contains("count") || lower.contains("rows") || lower == "n"
}

/// True if `attrs` contains `#[derive(.., Copy, ..)]`.
fn derives_copy(attrs: &[syn::Attribute]) -> bool {
    attrs.iter().any(|a| {
        a.path().is_ident("derive") && {
            let mut hit = false;
            let _ = a.parse_nested_meta(|m| {
                if m.path.is_ident("Copy") {
                    hit = true;
                }
                Ok(())
            });
            hit
        }
    })
}

/// Array length above which a Copy type's implicit copies count as bulk moves.
const COPY_LARGE_ARRAY_LEN: u64 = 64;
/// Field count above which a Copy struct is considered large.
const COPY_LARGE_FIELD_COUNT: usize = 12;

/// True if the struct is "large" for Copy purposes: an array field with a
/// literal length >= `COPY_LARGE_ARRAY_LEN`, or >= `COPY_LARGE_FIELD_COUNT`
/// fields. Best-effort: non-literal array lengths are not evaluated.
fn struct_is_large(fields: &syn::Fields) -> bool {
    if fields.len() >= COPY_LARGE_FIELD_COUNT {
        return true;
    }
    fields.iter().any(|f| type_has_large_array(&f.ty))
}

fn type_has_large_array(ty: &syn::Type) -> bool {
    match ty {
        syn::Type::Array(a) => {
            if let syn::Expr::Lit(l) = &a.len {
                if let syn::Lit::Int(i) = &l.lit {
                    if i.base10_parse::<u64>()
                        .is_ok_and(|n| n >= COPY_LARGE_ARRAY_LEN)
                    {
                        return true;
                    }
                }
            }
            type_has_large_array(&a.elem)
        }
        syn::Type::Paren(p) => type_has_large_array(&p.elem),
        _ => false,
    }
}

// ---------------------------------------------------------------------------
// `#[cfg(test)]` stripping
// ---------------------------------------------------------------------------

/// True if `attrs` carries `#[cfg(test)]`.
fn has_cfg_test(attrs: &[syn::Attribute]) -> bool {
    attrs.iter().any(|a| {
        a.path().is_ident("cfg") && {
            let mut hit = false;
            // `cfg(test)` => the meta token stream contains the ident `test`.
            let _ = a.parse_nested_meta(|m| {
                if m.path.is_ident("test") {
                    hit = true;
                }
                Ok(())
            });
            hit
        }
    })
}

/// Remove `#[cfg(test)]` modules and test fns from a parsed file in place so the
/// audit never inspects test code (per task scope: skip `#[cfg(test)]` modules).
fn strip_test_items(file: &mut syn::File) {
    file.items.retain(|item| !item_is_test(item));
    for item in &mut file.items {
        if let syn::Item::Mod(m) = item {
            if let Some((_, items)) = &mut m.content {
                items.retain(|it| !item_is_test(it));
            }
        }
    }
}

fn item_is_test(item: &syn::Item) -> bool {
    match item {
        syn::Item::Mod(m) => has_cfg_test(&m.attrs),
        syn::Item::Fn(f) => has_cfg_test(&f.attrs) || has_test_attr(&f.attrs),
        _ => false,
    }
}

fn has_test_attr(attrs: &[syn::Attribute]) -> bool {
    attrs
        .iter()
        .any(|a| a.path().segments.last().map(|s| s.ident == "test") == Some(true))
}

// ---------------------------------------------------------------------------
// Public entry points
// ---------------------------------------------------------------------------

/// Audit a single source string. Pure: no IO, no clock. Returns un-suppressed
/// findings (the caller applies the allowlist + expired-entry findings).
pub fn audit_source(path: &str, src: &str, allow: &Allowlist) -> Vec<Finding> {
    let mut file = match syn::parse_file(src) {
        Ok(f) => f,
        // A file that doesn't parse is surfaced, not silently skipped.
        Err(e) => {
            return vec![Finding {
                path: path.to_string(),
                line: e.span().start().line,
                rule: "parse-error",
                message: format!("could not parse file as Rust: {e}"),
                symbol: String::new(),
            }]
        }
    };
    strip_test_items(&mut file);

    let mut auditor = Auditor::new(path);
    auditor.visit_file(&file);

    auditor
        .findings
        .into_iter()
        .filter(|f| !allow.allows(f))
        .collect()
}

/// True if a path should be scanned: a `.rs` file under a `src/` dir, not in
/// `tests/` or `benches/`.
fn is_scannable(path: &Path) -> bool {
    if path.extension().and_then(|e| e.to_str()) != Some("rs") {
        return false;
    }
    let comps: Vec<String> = path
        .components()
        .map(|c| c.as_os_str().to_string_lossy().into_owned())
        .collect();
    let in_src = comps.iter().any(|c| c == "src");
    let in_excluded = comps.iter().any(|c| c == "tests" || c == "benches");
    in_src && !in_excluded
}

/// Audit every scannable file under `roots`. IO happens here; detection is
/// delegated to `audit_source`. Does not apply expired-entry findings (the CLI
/// adds those once, with its `--today`).
pub fn audit_paths(roots: &[PathBuf], allow: &Allowlist) -> Vec<Finding> {
    let mut findings = Vec::new();
    for root in roots {
        for entry in walkdir::WalkDir::new(root)
            .into_iter()
            .filter_map(std::result::Result::ok)
        {
            let p = entry.path();
            if !is_scannable(p) {
                continue;
            }
            match std::fs::read_to_string(p) {
                Ok(src) => {
                    let rel = p.to_string_lossy().into_owned();
                    findings.extend(audit_source(&rel, &src, allow));
                }
                Err(e) => findings.push(Finding {
                    path: p.to_string_lossy().into_owned(),
                    line: 0,
                    rule: "read-error",
                    message: format!("could not read file: {e}"),
                    symbol: String::new(),
                }),
            }
        }
    }
    findings
}

#[cfg(test)]
mod tests {
    use super::*;

    fn no_allow() -> Allowlist {
        Allowlist::default()
    }

    // -- Rule (a): stream-named fn returning Vec ---------------------------
    #[test]
    fn rule_a_fires_on_stream_fn_returning_vec() {
        let src = r#"
            fn read_range_stream(limit: usize) -> Vec<Partition> { Vec::new() }
            fn other_stream_thing() -> Result<Vec<Row>, E> { Ok(Vec::new()) }
        "#;
        let f = audit_source("x.rs", src, &no_allow());
        let count = f
            .iter()
            .filter(|x| x.rule == rule::STREAM_RETURNS_VEC)
            .count();
        assert_eq!(count, 2, "both stream fns should fire rule (a)");
        assert!(f.iter().any(|x| x.symbol == "read_range_stream"));
    }

    #[test]
    fn rule_a_does_not_fire_on_real_streaming_fn() {
        // A genuinely streaming fn: returns impl Stream / a Stream alias / yields.
        let src = r#"
            fn range_iter_stream() -> impl Stream<Item = Partition> { todo!() }
            fn coordinate_range_read_stream() -> PartitionResultStream { todo!() }
            fn make_stream() -> ClusterPartitionStream { todo!() }
        "#;
        let f = audit_source("x.rs", src, &no_allow());
        assert!(
            !f.iter().any(|x| x.rule == rule::STREAM_RETURNS_VEC),
            "streaming return types must not trip rule (a): {f:?}"
        );
        assert_eq!(f.len(), 0, "no findings at all expected: {f:?}");
    }

    // -- Rule (b): returns Vec<Partition>/Vec<Row> -------------------------
    #[test]
    fn rule_b_fires_on_vec_partition_and_vec_row() {
        let src = r#"
            fn a() -> Vec<Partition> { Vec::new() }
            fn b() -> crate::error::Result<Vec<ferrosa_sstable::types::Row>> { todo!() }
            fn c() -> Vec<u8> { Vec::new() }
        "#;
        let f = audit_source("x.rs", src, &no_allow());
        let hits: Vec<_> = f
            .iter()
            .filter(|x| x.rule == rule::RETURNS_VEC_PARTITION_OR_ROW)
            .collect();
        assert_eq!(hits.len(), 2, "Partition + Row fire, Vec<u8> does not");
        assert!(hits.iter().all(|h| h.symbol == "a" || h.symbol == "b"));
    }

    // -- Rule (c): Vec::with_capacity(limit) -------------------------------
    #[test]
    fn rule_c_fires_on_paging_capacity() {
        let src = r#"
            fn f(limit: usize) {
                let v: Vec<u8> = Vec::with_capacity(limit);
                let w: Vec<u8> = Vec::with_capacity(row_limit);
            }
        "#;
        let f = audit_source("x.rs", src, &no_allow());
        let count = f
            .iter()
            .filter(|x| x.rule == rule::WITH_CAPACITY_LIMIT)
            .count();
        assert_eq!(count, 2, "`limit` and `row_limit` both trip rule (c)");
    }

    #[test]
    fn rule_c_does_not_fire_on_bounded_capacity() {
        let src = r#"
            fn f(items: &[u8]) {
                let v: Vec<u8> = Vec::with_capacity(items.len());
                let w: Vec<u8> = Vec::with_capacity(8);
            }
        "#;
        let f = audit_source("x.rs", src, &no_allow());
        assert!(
            !f.iter().any(|x| x.rule == rule::WITH_CAPACITY_LIMIT),
            "{f:?}"
        );
    }

    // -- Rule (d): while-let push accumulation -----------------------------
    #[test]
    fn rule_d_fires_on_stream_push_loop() {
        let src = r#"
            async fn collect_all(stream: S, vec: &mut Vec<u8>) {
                while let Some(x) = stream.next().await {
                    let item = x;
                    vec.push(item);
                }
            }
        "#;
        let f = audit_source("x.rs", src, &no_allow());
        let count = f
            .iter()
            .filter(|x| x.rule == rule::STREAM_PUSH_ACCUMULATION)
            .count();
        assert_eq!(count, 1, "the accumulation loop should fire once: {f:?}");
        assert!(f.iter().any(|x| x.rule == rule::STREAM_PUSH_ACCUMULATION));
    }

    #[test]
    fn rule_d_does_not_fire_without_push() {
        let src = r#"
            async fn count_all(stream: S) -> usize {
                let mut n = 0;
                while let Some(_x) = stream.next().await {
                    n += 1;
                }
                n
            }
        "#;
        let f = audit_source("x.rs", src, &no_allow());
        assert!(
            !f.iter().any(|x| x.rule == rule::STREAM_PUSH_ACCUMULATION),
            "{f:?}"
        );
    }

    // -- Rule (e): limited-rows call sites ---------------------------------
    #[test]
    fn rule_e_fires_on_limited_rows_calls() {
        let src = r#"
            async fn router_arm(state: S, table_id: T) {
                let a = state.range_read_limited_rows(&table_id, bound, row_limit).await;
                let b = coord.coordinate_range_read_limited_rows(&table_id, limit, 0).await;
                let c = read_local_thing.read_local_range_limited_rows(&table_id, 1, 1);
            }
        "#;
        let f = audit_source("x.rs", src, &no_allow());
        let count = f
            .iter()
            .filter(|x| x.rule == rule::LIMITED_ROWS_CALL_SITE)
            .count();
        assert_eq!(count, 3, "all three limited-rows method calls fire: {f:?}");
        assert!(f.iter().any(|x| x.symbol == "range_read_limited_rows"));
    }

    // -- #[cfg(test)] and test fns are skipped -----------------------------
    #[test]
    fn cfg_test_modules_are_skipped() {
        let src = r#"
            #[cfg(test)]
            mod tests {
                fn bad_stream() -> Vec<Partition> { Vec::new() }
            }
            #[test]
            fn a_test() -> Vec<Row> { Vec::new() }
        "#;
        let f = audit_source("x.rs", src, &no_allow());
        assert!(f.is_empty(), "test code must be skipped entirely: {f:?}");
    }

    // -- Whitelist suppression --------------------------------------------
    #[test]
    fn whitelist_suppresses_matching_finding() {
        let src = r#"fn range_read_limited_rows() -> Vec<Partition> { Vec::new() }"#;
        let allow = Allowlist::from_str(
            r#"
            [[allow]]
            path = "x.rs"
            rule = "returns-vec-partition-or-row"
            symbol = "range_read_limited_rows"
            reason = "t_15417b35 SELECT-arm fix pending"
            owner = "storage"
            expires = "2099-01-01"
        "#,
        )
        .unwrap();
        let suppressed = audit_source("x.rs", src, &allow);
        assert!(
            !suppressed
                .iter()
                .any(|x| x.rule == rule::RETURNS_VEC_PARTITION_OR_ROW),
            "whitelisted finding must be suppressed: {suppressed:?}"
        );
        // And without the allowlist it would fire.
        let raw = audit_source("x.rs", src, &no_allow());
        assert!(raw
            .iter()
            .any(|x| x.rule == rule::RETURNS_VEC_PARTITION_OR_ROW));
    }

    #[test]
    fn whitelist_symbol_mismatch_does_not_suppress() {
        let src = r#"fn other_fn() -> Vec<Row> { Vec::new() }"#;
        let allow = Allowlist::from_str(
            r#"
            [[allow]]
            path = "x.rs"
            rule = "returns-vec-partition-or-row"
            symbol = "range_read_limited_rows"
            reason = "r"
            owner = "o"
            expires = "2099-01-01"
        "#,
        )
        .unwrap();
        let f = audit_source("x.rs", src, &allow);
        assert!(
            f.iter()
                .any(|x| x.rule == rule::RETURNS_VEC_PARTITION_OR_ROW),
            "symbol mismatch must NOT suppress: {f:?}"
        );
    }

    // -- Expired allow entry produces a finding ----------------------------
    #[test]
    fn expired_allow_entry_is_a_finding() {
        let allow = Allowlist::from_str(
            r#"
            [[allow]]
            path = "write_path.rs"
            rule = "returns-vec-partition-or-row"
            reason = "t_15417b35"
            owner = "storage"
            expires = "2026-01-01"
        "#,
        )
        .unwrap();
        let expired = expired_allow_findings(&allow, "2026-06-29");
        assert_eq!(expired.len(), 1, "the past-dated entry should be flagged");
        assert_eq!(expired[0].rule, rule::EXPIRED_ALLOW);
    }

    #[test]
    fn unexpired_allow_entry_is_not_a_finding() {
        let allow = Allowlist::from_str(
            r#"
            [[allow]]
            path = "write_path.rs"
            rule = "returns-vec-partition-or-row"
            reason = "t_15417b35"
            owner = "storage"
            expires = "2026-09-30"
        "#,
        )
        .unwrap();
        let expired = expired_allow_findings(&allow, "2026-06-29");
        assert!(
            expired.is_empty(),
            "future expiry must not flag: {expired:?}"
        );
    }

    // -- Rule 1: collect-vec-partition-or-row ------------------------------
    #[test]
    fn rule_collect_fires_on_turbofish_and_typed_binding() {
        let src = r#"
            fn a(it: I) -> () {
                let _v = it.map(|x| x).collect::<Vec<Partition>>();
                let typed: Vec<Row> = it.map(|x| x).collect();
            }
        "#;
        let f = audit_source("x.rs", src, &no_allow());
        let count = f
            .iter()
            .filter(|x| x.rule == rule::COLLECT_VEC_PARTITION_OR_ROW)
            .count();
        assert_eq!(
            count, 2,
            "turbofish + typed-binding collect both fire: {f:?}"
        );
        assert!(f.iter().any(
            |x| x.rule == rule::COLLECT_VEC_PARTITION_OR_ROW && x.message.contains("Partition")
        ));
    }

    #[test]
    fn rule_collect_fires_on_extend_from_stream() {
        let src = r#"
            fn a(out: &mut Vec<u8>, storage: S) {
                out.extend(storage.range_iter(t, None, None));
            }
        "#;
        let f = audit_source("x.rs", src, &no_allow());
        let count = f
            .iter()
            .filter(|x| x.rule == rule::COLLECT_VEC_PARTITION_OR_ROW)
            .count();
        assert_eq!(count, 1, "extend from a stream source fires: {f:?}");
    }

    #[test]
    fn rule_collect_does_not_fire_on_plain_collect_or_extend() {
        let src = r#"
            fn a(it: I, out: &mut Vec<u8>, more: Vec<u8>) {
                let _v: Vec<u8> = it.collect();
                let _w = it.collect::<Vec<u8>>();
                out.extend(more.iter().copied());
            }
        "#;
        let f = audit_source("x.rs", src, &no_allow());
        assert!(
            !f.iter()
                .any(|x| x.rule == rule::COLLECT_VEC_PARTITION_OR_ROW),
            "non-row collect/extend must not fire: {f:?}"
        );
    }

    // -- Rule 2: unbounded-range-read --------------------------------------
    #[test]
    fn rule_unbounded_range_read_fires_on_none_none() {
        // Real shape: `read_range(&table_id, None, None, limit)` — the two
        // consecutive `None` range bounds make it a full-table scan.
        let src = r#"
            fn a(storage: S, table_id: T) {
                let _x = storage.read_range(&table_id, None, None, 100);
                let _y = read_range(&table_id, None, None, 10_000);
            }
        "#;
        let f = audit_source("x.rs", src, &no_allow());
        let count = f
            .iter()
            .filter(|x| x.rule == rule::UNBOUNDED_RANGE_READ)
            .count();
        assert_eq!(
            count, 2,
            "method + free-fn read_range(.,None,None,.) fire: {f:?}"
        );
    }

    #[test]
    fn rule_unbounded_range_read_does_not_fire_with_bounds() {
        let src = r#"
            fn a(storage: S, table_id: T, lo: K, hi: K) {
                let _x = storage.read_range(&table_id, Some(lo), Some(hi), 100);
                let _y = storage.read_range(&table_id, Some(lo), None, 100);
            }
        "#;
        let f = audit_source("x.rs", src, &no_allow());
        assert!(
            !f.iter().any(|x| x.rule == rule::UNBOUNDED_RANGE_READ),
            "bounded reads must not fire: {f:?}"
        );
    }

    // -- Rule 3: materializing-range-read-call-site ------------------------
    #[test]
    fn rule_materializing_call_site_fires_on_range_read_and_projected() {
        let src = r#"
            fn a(wp: W, table_id: T) {
                let _x = wp.range_read(&table_id, bound);
                let _y = wp.range_read_projected(&table_id, wanted, bound);
            }
        "#;
        let f = audit_source("x.rs", src, &no_allow());
        let count = f
            .iter()
            .filter(|x| x.rule == rule::MATERIALIZING_RANGE_READ_CALL_SITE)
            .count();
        assert_eq!(count, 2, "range_read + range_read_projected fire: {f:?}");
    }

    #[test]
    fn rule_materializing_call_site_does_not_fire_on_stream_or_from_variants() {
        let src = r#"
            fn a(wp: W, table_id: T) {
                let _x = wp.range_read_stream(&table_id, bound);
                let _y = wp.range_read_from(&table_id, bound);
                let _z = wp.range_read_limited_rows(&table_id, bound, 0);
            }
        "#;
        let f = audit_source("x.rs", src, &no_allow());
        assert!(
            !f.iter()
                .any(|x| x.rule == rule::MATERIALIZING_RANGE_READ_CALL_SITE),
            "stream/_from/_limited_rows variants must not fire rule 3: {f:?}"
        );
    }

    // -- Rule 4: cqlvalue-row-accumulation ---------------------------------
    #[test]
    fn rule_cqlvalue_matrix_fires_on_let_field_and_return() {
        let src = r#"
            struct Holder {
                rows: Vec<Vec<Option<CqlValue>>>,
            }
            fn build() -> Vec<Vec<CqlValue>> {
                let acc: Vec<Vec<Option<CqlValue>>> = Vec::new();
                acc
            }
        "#;
        let f = audit_source("x.rs", src, &no_allow());
        let count = f
            .iter()
            .filter(|x| x.rule == rule::CQLVALUE_ROW_ACCUMULATION)
            .count();
        assert_eq!(count, 3, "field + let + return all fire: {f:?}");
    }

    #[test]
    fn rule_cqlvalue_matrix_does_not_fire_on_single_row() {
        let src = r#"
            fn build() -> Vec<Option<CqlValue>> {
                Vec::new()
            }
            struct Holder { cells: Vec<CqlValue> }
        "#;
        let f = audit_source("x.rs", src, &no_allow());
        assert!(
            !f.iter().any(|x| x.rule == rule::CQLVALUE_ROW_ACCUMULATION),
            "a single Vec<CqlValue> row must not fire (needs the outer Vec): {f:?}"
        );
    }

    // -- Rule 5: clone-on-row-data -----------------------------------------
    #[test]
    fn rule_clone_on_row_data_fires_on_row_idents() {
        let src = r#"
            fn a(partition: P, rows: R, self_holder: H) {
                let _x = partition.clone();
                let _y = rows.to_vec();
                let _z = self_holder.cells.to_owned();
            }
        "#;
        let f = audit_source("x.rs", src, &no_allow());
        let count = f
            .iter()
            .filter(|x| x.rule == rule::CLONE_ON_ROW_DATA)
            .count();
        assert_eq!(count, 3, "partition/rows/cells copies fire: {f:?}");
        assert!(f
            .iter()
            .any(|x| x.rule == rule::CLONE_ON_ROW_DATA && x.symbol == "partition"));
    }

    #[test]
    fn rule_clone_on_row_data_does_not_fire_on_other_idents() {
        let src = r#"
            fn a(config: C, name: N, key: K) {
                let _x = config.clone();
                let _y = name.to_owned();
                let _z = key.to_vec();
            }
        "#;
        let f = audit_source("x.rs", src, &no_allow());
        assert!(
            !f.iter().any(|x| x.rule == rule::CLONE_ON_ROW_DATA),
            "non-row-data receivers must not fire: {f:?}"
        );
    }

    // -- Rule 5 hardening: receiver shapes the ident rule used to miss ------
    #[test]
    fn rule_clone_on_row_data_fires_through_ref_deref_paren_receivers() {
        let src = r#"
            fn a(rows: R, partition: P) {
                let _x = (&rows).to_vec();
                let _y = (*partition).clone();
                let _z = ((rows)).to_owned();
            }
        "#;
        let f = audit_source("x.rs", src, &no_allow());
        let count = f
            .iter()
            .filter(|x| x.rule == rule::CLONE_ON_ROW_DATA)
            .count();
        assert_eq!(count, 3, "&/*/paren-wrapped row receivers fire: {f:?}");
    }

    #[test]
    fn rule_clone_on_row_data_fires_on_method_call_receiver() {
        let src = r#"
            fn a(holder: H) {
                let _x = holder.rows().to_vec();
                let _y = holder.partitions().clone();
            }
        "#;
        let f = audit_source("x.rs", src, &no_allow());
        let count = f
            .iter()
            .filter(|x| x.rule == rule::CLONE_ON_ROW_DATA)
            .count();
        assert_eq!(count, 2, "accessor-method row receivers fire: {f:?}");
    }

    #[test]
    fn rule_clone_on_row_data_fires_on_row_typed_renamed_binding() {
        // The renamed-binding blind spot: receiver ident is NOT in the row-data
        // set, but its declared type is a row type (param or typed local).
        let src = r#"
            fn a(p: &Partition, source: Row) {
                let q = p.clone();
                let renamed: Partition = make();
                let r = renamed.clone();
                let s = source.to_owned();
            }
        "#;
        let f = audit_source("x.rs", src, &no_allow());
        let count = f
            .iter()
            .filter(|x| x.rule == rule::CLONE_ON_ROW_DATA)
            .count();
        assert_eq!(count, 3, "row-typed p/renamed/source all fire: {f:?}");
    }

    #[test]
    fn rule_clone_on_row_data_typed_tracking_is_per_fn() {
        // `p` is Partition-typed in `a` but not in `b` — no bleed-over.
        let src = r#"
            fn a(p: &Partition) { let _ = p.clone(); }
            fn b(p: &Config) { let _ = p.clone(); }
        "#;
        let f = audit_source("x.rs", src, &no_allow());
        let count = f
            .iter()
            .filter(|x| x.rule == rule::CLONE_ON_ROW_DATA)
            .count();
        assert_eq!(count, 1, "only the Partition-typed fn fires: {f:?}");
    }

    #[test]
    fn rule_clone_on_row_data_fires_on_scan_shape_idents() {
        // chunk/fragment move through the streaming pipeline; copying them
        // defeats move-based streaming just like copying a partition.
        let src = r#"
            fn a(chunk: C, fragment: F) {
                let _x = chunk.clone();
                let _y = fragment.to_owned();
            }
        "#;
        let f = audit_source("x.rs", src, &no_allow());
        let count = f
            .iter()
            .filter(|x| x.rule == rule::CLONE_ON_ROW_DATA)
            .count();
        assert_eq!(count, 2, "chunk/fragment copies fire: {f:?}");
    }

    // -- Rule 6: cloned-stream-elements (.cloned()/.copied() adapters) ------
    #[test]
    fn rule_cloned_stream_elements_fires_on_iter_cloned_over_row_data() {
        let src = r#"
            fn a(rows: Vec<R>, partitions: Vec<P>) {
                let v = rows.iter().cloned();
                let w = partitions.iter().copied();
            }
        "#;
        let f = audit_source("x.rs", src, &no_allow());
        let count = f
            .iter()
            .filter(|x| x.rule == rule::CLONED_STREAM_ELEMENTS)
            .count();
        assert_eq!(
            count, 2,
            "iter().cloned()/copied() over row data fire: {f:?}"
        );
    }

    #[test]
    fn rule_cloned_stream_elements_fires_on_stream_source_chain() {
        let src = r#"
            fn a(s: S) {
                let v = partition_stream(s).cloned();
            }
        "#;
        let f = audit_source("x.rs", src, &no_allow());
        assert!(
            f.iter().any(|x| x.rule == rule::CLONED_STREAM_ELEMENTS),
            ".cloned() on a stream-source chain fires: {f:?}"
        );
    }

    #[test]
    fn rule_cloned_stream_elements_does_not_fire_on_option_next_chains() {
        // `.next().cloned()` copies ONE element out of an Option — that is an
        // accessor, not a per-element stream copy.
        let src = r#"
            fn a(rows: Vec<R>) {
                let one = rows.iter().next().cloned();
                let key = self.entries.keys().next().copied();
                let g = groups.values().next().cloned();
            }
        "#;
        let f = audit_source("x.rs", src, &no_allow());
        assert!(
            !f.iter().any(|x| x.rule == rule::CLONED_STREAM_ELEMENTS),
            "Option-after-next chains must not fire: {f:?}"
        );
    }

    #[test]
    fn rule_cloned_stream_elements_does_not_fire_on_small_config_data() {
        let src = r#"
            fn a(names: Vec<String>, ports: Vec<u16>) {
                let v = names.iter().cloned();
                let w = ports.iter().copied();
            }
        "#;
        let f = audit_source("x.rs", src, &no_allow());
        assert!(
            !f.iter().any(|x| x.rule == rule::CLONED_STREAM_ELEMENTS),
            "non-row, non-stream receivers must not fire: {f:?}"
        );
    }

    // -- Rule 7: clone-in-scan-closure (`.map(|r| r.clone())`) --------------
    #[test]
    fn rule_clone_in_scan_closure_fires_on_map_clone_over_row_data() {
        let src = r#"
            fn a(rows: Vec<R>) {
                let v = rows.iter().map(|r| r.clone());
                let w = rows.iter().filter_map(|x| Some(x.to_vec()));
            }
        "#;
        let f = audit_source("x.rs", src, &no_allow());
        let count = f
            .iter()
            .filter(|x| x.rule == rule::CLONE_IN_SCAN_CLOSURE)
            .count();
        assert_eq!(count, 2, "closure-param clones over row data fire: {f:?}");
    }

    #[test]
    fn rule_clone_in_scan_closure_does_not_fire_off_row_chains() {
        let src = r#"
            fn a(configs: Vec<C>) {
                let v = configs.iter().map(|c| c.clone());
            }
        "#;
        let f = audit_source("x.rs", src, &no_allow());
        assert!(
            !f.iter().any(|x| x.rule == rule::CLONE_IN_SCAN_CLOSURE),
            "closures over non-row chains must not fire: {f:?}"
        );
    }

    #[test]
    fn rule_clone_in_scan_closure_only_flags_the_closure_param() {
        // `other.clone()` inside the closure is NOT the streamed element.
        let src = r#"
            fn a(rows: Vec<R>, other: O) {
                let v = rows.iter().map(|r| (r.id, other.clone()));
            }
        "#;
        let f = audit_source("x.rs", src, &no_allow());
        assert!(
            !f.iter().any(|x| x.rule == rule::CLONE_IN_SCAN_CLOSURE),
            "captured non-param clones must not fire this rule: {f:?}"
        );
    }

    // -- Rule 8: copies-row-data-arg (arg-position copies) -------------------
    #[test]
    fn rule_copies_row_data_arg_fires_on_extend_from_slice() {
        let src = r#"
            fn a(buf: &mut Vec<R>, rows: &[R]) {
                buf.extend_from_slice(rows);
                buf.extend_from_slice(&rows);
            }
        "#;
        let f = audit_source("x.rs", src, &no_allow());
        let count = f
            .iter()
            .filter(|x| x.rule == rule::COPIES_ROW_DATA_ARG)
            .count();
        assert_eq!(count, 2, "extend_from_slice(rows) clones every row: {f:?}");
    }

    #[test]
    fn rule_copies_row_data_arg_fires_on_ufcs_clone() {
        let src = r#"
            fn a(p: &P, partition: &Q) {
                let x = Partition::clone(p);
                let y = Clone::clone(partition);
            }
        "#;
        let f = audit_source("x.rs", src, &no_allow());
        let count = f
            .iter()
            .filter(|x| x.rule == rule::COPIES_ROW_DATA_ARG)
            .count();
        assert_eq!(count, 2, "UFCS clones of row data fire: {f:?}");
    }

    #[test]
    fn rule_copies_row_data_arg_does_not_fire_on_byte_buffers() {
        let src = r#"
            fn a(out: &mut Vec<u8>, header: &[u8]) {
                out.extend_from_slice(header);
            }
        "#;
        let f = audit_source("x.rs", src, &no_allow());
        assert!(
            !f.iter().any(|x| x.rule == rule::COPIES_ROW_DATA_ARG),
            "byte-buffer extends must not fire: {f:?}"
        );
    }

    // -- Rule 10: server-side-result-cap --------------------------------------
    #[test]
    fn rule_result_cap_fires_on_cap_named_int_consts() {
        // A result-count cap constant is a materialization confession: results
        // must be bounded only by the inbound CQL LIMIT / paging.
        let src = r#"
            const DEFAULT_RANGE_READ_LIMIT: usize = 10_000;
            pub const MAX_SCAN_PARTITIONS: usize = 5000;
            const FTS_MATCH_CAP: usize = 512;
        "#;
        let f = audit_source("x.rs", src, &no_allow());
        let count = f
            .iter()
            .filter(|x| x.rule == rule::SERVER_SIDE_RESULT_CAP)
            .count();
        assert_eq!(count, 3, "row/partition/match count caps fire: {f:?}");
        assert!(f.iter().any(|x| x.symbol == "DEFAULT_RANGE_READ_LIMIT"));
    }

    #[test]
    fn rule_result_cap_does_not_fire_on_memory_or_shape_bounds() {
        // Memory bounds (bytes/frames/chunks/buffers) and structural constants
        // are the DESIRED kind of bound — never flag them.
        let src = r#"
            const MAX_FRAME_BYTES: usize = 16777216;
            const CHUNK_SIZE_LIMIT: usize = 4096;
            const MERGE_FANIN: usize = 64;
            const MAX_RETRIES: usize = 5;
            const WRITE_TIMEOUT_MS: u64 = 30000;
            const LANE_CAPACITY: usize = 256;
            const MAX_INFLIGHT_BYTES: usize = 33554432;
            const DEFAULT_READER_CACHE_CAP: usize = 128;
            const MAX_KEY_LEN: usize = 65535;
        "#;
        let f = audit_source("x.rs", src, &no_allow());
        assert!(
            !f.iter().any(|x| x.rule == rule::SERVER_SIDE_RESULT_CAP),
            "memory/shape/retry bounds must not fire: {f:?}"
        );
    }

    #[test]
    fn rule_result_cap_fires_on_clamp_min_to_cap() {
        // Magnitude is irrelevant — min(500) caps results exactly like a
        // 10k const. Only query-derived bounds are legitimate.
        let src = r#"
            fn f(limit: usize) -> usize {
                let effective_limit = limit.clamp(1, DEFAULT_RANGE_READ_LIMIT);
                let capped = limit.min(MAX_SCAN_ROWS);
                let small = limit.min(500);
                effective_limit + capped + small
            }
        "#;
        let f = audit_source("x.rs", src, &no_allow());
        let count = f
            .iter()
            .filter(|x| x.rule == rule::SERVER_SIDE_RESULT_CAP)
            .count();
        assert_eq!(
            count, 3,
            "clamping a user limit to a server cap fires: {f:?}"
        );
    }

    #[test]
    fn rule_result_cap_does_not_fire_on_legit_clamp_min() {
        let src = r#"
            fn f(limit: usize, items: &[u8], concurrency: usize) -> usize {
                let a = limit.min(items.len());
                let b = concurrency.clamp(1, MAX_RETRIES);
                a + b
            }
        "#;
        let f = audit_source("x.rs", src, &no_allow());
        assert!(
            !f.iter().any(|x| x.rule == rule::SERVER_SIDE_RESULT_CAP),
            "min(len) and non-result clamps must not fire: {f:?}"
        );
    }

    #[test]
    fn rule_result_cap_fires_on_any_hardcoded_take_truncate() {
        // Generic: ANY hardcoded count caps results — 50 and 2 are as
        // illegitimate as 10_000. Cap-named consts fire too.
        let src = r#"
            fn f(rows: Vec<R>) {
                let v: Vec<R> = rows.into_iter().take(10000).collect();
                let s: Vec<R> = rows.into_iter().take(50).collect();
                let mut w = rows;
                w.truncate(2);
                let c = rows.iter().take(SOME_SERVER_CAP).count();
            }
        "#;
        let f = audit_source("x.rs", src, &no_allow());
        let count = f
            .iter()
            .filter(|x| x.rule == rule::SERVER_SIDE_RESULT_CAP)
            .count();
        assert_eq!(
            count, 4,
            "hardcoded take/truncate of any magnitude fire: {f:?}"
        );
    }

    #[test]
    fn rule_result_cap_does_not_fire_on_small_take_or_query_limit() {
        let src = r#"
            fn f(rows: Vec<R>, limit: usize) {
                let first = rows.iter().take(1).count();
                let user: Vec<R> = rows.into_iter().take(limit).collect();
                let mut w = user;
                w.truncate(limit);
            }
        "#;
        let f = audit_source("x.rs", src, &no_allow());
        assert!(
            !f.iter().any(|x| x.rule == rule::SERVER_SIDE_RESULT_CAP),
            "take(1)/take(limit)/truncate(limit) must not fire — the query's own \
             limit is the ONLY legitimate result bound: {f:?}"
        );
    }

    // -- Rule 9: copy-derive-large-type --------------------------------------
    #[test]
    fn rule_copy_derive_large_type_fires_on_big_array_struct() {
        let src = r#"
            #[derive(Clone, Copy)]
            struct Big { data: [u8; 4096] }
        "#;
        let f = audit_source("x.rs", src, &no_allow());
        assert!(
            f.iter().any(|x| x.rule == rule::COPY_DERIVE_LARGE_TYPE),
            "derive(Copy) on a 4096-byte array struct fires: {f:?}"
        );
    }

    #[test]
    fn rule_copy_derive_large_type_does_not_fire_on_small_types() {
        let src = r#"
            #[derive(Clone, Copy)]
            struct Token(u64);
            #[derive(Copy, Clone)]
            struct Pair { a: u64, b: u64 }
            #[derive(Clone)]
            struct BigButNotCopy { data: [u8; 4096] }
        "#;
        let f = audit_source("x.rs", src, &no_allow());
        assert!(
            !f.iter().any(|x| x.rule == rule::COPY_DERIVE_LARGE_TYPE),
            "small Copy types and non-Copy big types must not fire: {f:?}"
        );
    }
}
