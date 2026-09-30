//! Module: the canonical Parquet Variant v1 writer plus the `ferrosa.bigdecimal`
//! primitive (T-102, D4, D2a, architecture 3.2 rules C1-C11).
//! Correctness: correct when one value (with its decimal scales) has exactly one
//! byte string: sorted unique dictionary, minimal offset and id widths, the
//! smallest numeric kind that holds the value at its scale, no Float/Double, and
//! decimals keep their scale. The walk is iterative (M6) and the output size is
//! checked before the output buffer is allocated.
//! Last revised: 2026-09-28
//! Last changed: T-102 initial encoder.

use crate::builder::{Kind, Node};
use crate::error::JsonbError;
use crate::limits::{HardCeilings, Limits};
use crate::number::{Number, NumberKind};

/// Cell envelope byte: ferrosa jsonb storage format 1 (C1).
pub const ENVELOPE: u8 = 0xF1;
/// The one ferrosa extension: `ferrosa.bigdecimal`, a Variant primitive id that
/// upstream leaves unassigned (D4). Pinned by `variant_primitive_table_does_not_assign_63`.
pub const BIGDECIMAL_PRIMITIVE_ID: u8 = 63;

const HDR_NULL: u8 = 0x00;
const HDR_TRUE: u8 = 0x04;
const HDR_FALSE: u8 = 0x08;
const HDR_INT8: u8 = 3 << 2;
const HDR_INT16: u8 = 4 << 2;
const HDR_INT32: u8 = 5 << 2;
const HDR_INT64: u8 = 6 << 2;
const HDR_DEC4: u8 = 8 << 2;
const HDR_DEC8: u8 = 9 << 2;
const HDR_DEC16: u8 = 10 << 2;
const HDR_STRING: u8 = 16 << 2;
const HDR_BIGDECIMAL: u8 = BIGDECIMAL_PRIMITIVE_ID << 2;
const BASIC_SHORT_STRING: u8 = 1;
const BASIC_OBJECT: u8 = 2;
const BASIC_ARRAY: u8 = 3;
const SHORT_STRING_MAX: usize = 63;
const LARGE_COUNT: usize = 255;
/// Variant metadata header: version 1 with `sorted_strings` set (C2).
const META_HEADER_BASE: u8 = 0x01 | 0x10;
/// `offset_size_minus_one` sits in bits 6-7 of the metadata header and bit 5 is
/// reserved (Parquet VariantEncoding.md). T-102 shifted by 5, which the T-104
/// validator exposed.
const META_OFFSET_SHIFT: u8 = 6;

fn misuse(reason: &'static str) -> JsonbError {
    JsonbError::BuilderMisuse { reason }
}

/// The smallest of 1..=4 bytes that holds `v` (C4, C5).
fn width(v: usize) -> usize {
    match v {
        0..=0xFF => 1,
        0x100..=0xFFFF => 2,
        0x1_0000..=0xFF_FFFF => 3,
        _ => 4,
    }
}

fn write_le(out: &mut Vec<u8>, v: usize, w: usize) {
    out.extend((v as u64).to_le_bytes().iter().take(w));
}

fn write_uvarint(out: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        out.push((v & 0x7F) as u8 | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

fn zigzag(v: i64) -> u64 {
    ((v << 1) ^ (v >> 63)) as u64
}

pub(crate) fn write_null(out: &mut Vec<u8>) {
    out.push(HDR_NULL);
}

pub(crate) fn write_bool(out: &mut Vec<u8>, v: bool) {
    out.push(if v { HDR_TRUE } else { HDR_FALSE });
}

/// C7: up to 63 UTF-8 bytes use the short-string basic type, more use id 16.
pub(crate) fn write_string(out: &mut Vec<u8>, s: &str) {
    let len = s.len();
    if len <= SHORT_STRING_MAX {
        out.push(((len as u8) << 2) | BASIC_SHORT_STRING);
    } else {
        out.push(HDR_STRING);
        out.extend((len as u32).to_le_bytes());
    }
    out.extend_from_slice(s.as_bytes());
}

fn write_fixed(
    out: &mut Vec<u8>,
    header: u8,
    scale: Option<u8>,
    n: &Number,
    bytes: usize,
) -> Result<(), JsonbError> {
    let unscaled = n
        .unscaled_i128()
        .ok_or(misuse("number wider than i128 in a fixed-width kind"))?;
    out.push(header);
    out.extend(scale);
    out.extend(unscaled.to_le_bytes().iter().take(bytes));
    Ok(())
}

fn write_bigdecimal(out: &mut Vec<u8>, n: &Number) {
    let bytes = n.to_bigint().to_signed_bytes_be();
    out.push(HDR_BIGDECIMAL);
    write_uvarint(out, zigzag(i64::from(n.scale())));
    write_uvarint(out, bytes.len() as u64);
    out.extend_from_slice(&bytes);
}

/// C9: the smallest kind that holds the unscaled value at its stored scale.
pub(crate) fn write_number(out: &mut Vec<u8>, n: &Number) -> Result<(), JsonbError> {
    let scale = u8::try_from(n.scale()).ok();
    match n.kind() {
        NumberKind::Int8 => write_fixed(out, HDR_INT8, None, n, 1),
        NumberKind::Int16 => write_fixed(out, HDR_INT16, None, n, 2),
        NumberKind::Int32 => write_fixed(out, HDR_INT32, None, n, 4),
        NumberKind::Int64 => write_fixed(out, HDR_INT64, None, n, 8),
        NumberKind::Decimal4 => write_fixed(out, HDR_DEC4, scale, n, 4),
        NumberKind::Decimal8 => write_fixed(out, HDR_DEC8, scale, n, 8),
        NumberKind::Decimal16 => write_fixed(out, HDR_DEC16, scale, n, 16),
        NumberKind::BigDecimal => {
            write_bigdecimal(out, n);
            Ok(())
        }
    }
}

/// The sorted, unique key dictionary of a value (C3).
/// Which arena nodes the value rooted at `root` reaches. A duplicate key that
/// lost (D6b) leaves its subtree in the arena, unreachable: its keys must not
/// enter the dictionary (C3) nor its size the total.
fn reachable(nodes: &[Node], root: usize) -> Result<Vec<bool>, JsonbError> {
    let mut seen = vec![false; nodes.len()];
    let mut stack = vec![root];
    while let Some(idx) = stack.pop() {
        let slot = seen.get_mut(idx).ok_or(misuse("node index out of range"))?;
        *slot = true;
        match &nodes
            .get(idx)
            .ok_or(misuse("node index out of range"))?
            .kind
        {
            Kind::Object(entries) => stack.extend(entries.iter().map(|e| e.child)),
            Kind::Array(items) => stack.extend(items.iter().copied()),
            Kind::Scalar { .. } => {}
        }
    }
    Ok(seen)
}

fn dictionary<'a>(nodes: &'a [Node], live: &[bool]) -> Vec<&'a str> {
    let mut keys: Vec<&str> = nodes
        .iter()
        .zip(live)
        .filter_map(|(n, live)| match &n.kind {
            Kind::Object(entries) if *live => Some(entries.iter().map(|e| e.key.as_str())),
            Kind::Object(_) | Kind::Scalar { .. } | Kind::Array(_) => None,
        })
        .flatten()
        .collect();
    keys.sort_unstable();
    keys.dedup();
    keys
}

fn metadata_shape(dict: &[&str]) -> (usize, usize) {
    let key_bytes: usize = dict.iter().map(|k| k.len()).sum();
    (key_bytes, width(dict.len().max(key_bytes)))
}

fn write_metadata(out: &mut Vec<u8>, dict: &[&str]) {
    let (key_bytes, w) = metadata_shape(dict);
    out.push(META_HEADER_BASE | (((w - 1) as u8) << META_OFFSET_SHIFT));
    write_le(out, dict.len(), w);
    let mut offset = 0usize;
    write_le(out, offset, w);
    for key in dict {
        offset += key.len();
        write_le(out, offset, w);
    }
    debug_assert_eq!(offset, key_bytes);
    for key in dict {
        out.extend_from_slice(key.as_bytes());
    }
}

/// Everything the header of an object or array needs (C5, C6).
struct Layout {
    count: usize,
    children_total: usize,
    off_w: usize,
    id_w: usize,
}

impl Layout {
    fn large(&self) -> bool {
        self.count > LARGE_COUNT
    }

    fn count_w(&self) -> usize {
        if self.large() {
            4
        } else {
            1
        }
    }

    fn table_bytes(&self, ids: bool) -> usize {
        let id_bytes = if ids { self.count * self.id_w } else { 0 };
        self.count_w() + id_bytes + (self.count + 1) * self.off_w
    }
}

fn child_at(node: &Node, i: usize) -> Option<usize> {
    match &node.kind {
        Kind::Object(entries) => entries.get(i).map(|e| e.child),
        Kind::Array(items) => items.get(i).copied(),
        Kind::Scalar { .. } => None,
    }
}

fn key_id(dict: &[&str], key: &str) -> Result<usize, JsonbError> {
    dict.binary_search(&key)
        .map_err(|_| misuse("object key missing from the dictionary"))
}

fn layout(node: &Node, sizes: &[usize], dict: &[&str]) -> Result<Layout, JsonbError> {
    let count = match &node.kind {
        Kind::Object(entries) => entries.len(),
        Kind::Array(items) => items.len(),
        Kind::Scalar { .. } => return Err(misuse("layout of a scalar")),
    };
    let mut children_total = 0usize;
    for i in 0..count {
        let child = child_at(node, i).ok_or(misuse("child index out of range"))?;
        let size = sizes.get(child).ok_or(misuse("child size missing"))?;
        children_total = children_total.saturating_add(*size);
    }
    let max_id = match &node.kind {
        Kind::Object(entries) => match entries.last() {
            Some(last) => key_id(dict, &last.key)?,
            None => 0,
        },
        Kind::Array(_) | Kind::Scalar { .. } => 0,
    };
    Ok(Layout {
        count,
        children_total,
        off_w: width(children_total),
        id_w: width(max_id),
    })
}

/// Encoded size of every node, computed children-first: a child always has a
/// higher arena index than its parent, so a reverse scan needs no stack.
fn node_sizes(nodes: &[Node], dict: &[&str], live: &[bool]) -> Result<Vec<usize>, JsonbError> {
    let mut sizes = vec![0usize; nodes.len()];
    for (idx, node) in nodes.iter().enumerate().rev() {
        if !live.get(idx).copied().unwrap_or(false) {
            continue;
        }
        let size = match &node.kind {
            Kind::Scalar { len, .. } => *len,
            Kind::Object(_) => {
                let l = layout(node, &sizes, dict)?;
                1 + l.table_bytes(true) + l.children_total
            }
            Kind::Array(_) => {
                let l = layout(node, &sizes, dict)?;
                1 + l.table_bytes(false) + l.children_total
            }
        };
        *sizes.get_mut(idx).ok_or(misuse("size slot missing"))? = size;
    }
    Ok(sizes)
}

struct Writer<'a> {
    nodes: &'a [Node],
    pool: &'a [u8],
    sizes: &'a [usize],
    dict: &'a [&'a str],
    out: Vec<u8>,
}

impl<'a> Writer<'a> {
    fn node(&self, idx: usize) -> Result<&'a Node, JsonbError> {
        let nodes: &'a [Node] = self.nodes;
        nodes.get(idx).ok_or(misuse("node index out of range"))
    }

    fn write_offsets(&mut self, node: &Node, l: &Layout) -> Result<(), JsonbError> {
        let mut offset = 0usize;
        write_le(&mut self.out, offset, l.off_w);
        for i in 0..l.count {
            let child = child_at(node, i).ok_or(misuse("child index out of range"))?;
            offset += self.sizes.get(child).ok_or(misuse("child size missing"))?;
            write_le(&mut self.out, offset, l.off_w);
        }
        Ok(())
    }

    /// Header and tables of a container; its children follow in order.
    fn write_container_head(&mut self, node: &Node) -> Result<(), JsonbError> {
        let l = layout(node, self.sizes, self.dict)?;
        let large = u8::from(l.large());
        let off_bits = ((l.off_w - 1) as u8) << 2;
        match &node.kind {
            Kind::Object(entries) => {
                let id_bits = ((l.id_w - 1) as u8) << 4;
                self.out
                    .push(BASIC_OBJECT | off_bits | id_bits | (large << 6));
                write_le(&mut self.out, l.count, l.count_w());
                for e in entries {
                    write_le(&mut self.out, key_id(self.dict, &e.key)?, l.id_w);
                }
            }
            Kind::Array(_) => {
                self.out.push(BASIC_ARRAY | off_bits | (large << 4));
                write_le(&mut self.out, l.count, l.count_w());
            }
            Kind::Scalar { .. } => return Err(misuse("container head of a scalar")),
        }
        self.write_offsets(node, &l)
    }

    fn write_scalar(&mut self, start: usize, len: usize) -> Result<(), JsonbError> {
        let end = start
            .checked_add(len)
            .ok_or(misuse("scalar range overflow"))?;
        let bytes = self
            .pool
            .get(start..end)
            .ok_or(misuse("scalar range out of pool"))?;
        self.out.extend_from_slice(bytes);
        Ok(())
    }

    /// Emit one node: a scalar in full, a container's head. Returns whether a
    /// container was opened.
    fn emit(&mut self, idx: usize) -> Result<bool, JsonbError> {
        let node = self.node(idx)?;
        match &node.kind {
            Kind::Scalar { start, len } => self.write_scalar(*start, *len).map(|()| false),
            Kind::Object(_) | Kind::Array(_) => self.write_container_head(node).map(|()| true),
        }
    }

    /// Depth-first write with an explicit stack of (container, next child).
    fn write_tree(&mut self, root: usize, max_depth: usize) -> Result<(), JsonbError> {
        let mut stack: Vec<(usize, usize)> = Vec::new();
        if self.emit(root)? {
            stack.push((root, 0));
        }
        while let Some((idx, next)) = stack.last_mut() {
            let node = self
                .nodes
                .get(*idx)
                .ok_or(misuse("node index out of range"))?;
            let Some(child) = child_at(node, *next) else {
                stack.pop();
                continue;
            };
            *next += 1;
            if self.emit(child)? {
                if stack.len() >= max_depth {
                    return Err(misuse("nesting deeper than the hard ceiling"));
                }
                stack.push((child, 0));
            }
        }
        Ok(())
    }
}

/// Encode the arena rooted at `root` into a complete cell (envelope, metadata,
/// value). The size is checked against `limits` and the hard ceiling before the
/// output is allocated (D14).
pub(crate) fn encode_tree(
    nodes: &[Node],
    pool: &[u8],
    root: usize,
    limits: &Limits,
) -> Result<Vec<u8>, JsonbError> {
    let live = reachable(nodes, root)?;
    let dict = dictionary(nodes, &live);
    let sizes = node_sizes(nodes, &dict, &live)?;
    let (key_bytes, w) = metadata_shape(&dict);
    let meta_len = 1 + w * (dict.len() + 2) + key_bytes;
    let root_size = *sizes.get(root).ok_or(misuse("root size missing"))?;
    let total = 1 + meta_len + root_size;
    limits.check_encoded_len(total)?;
    HardCeilings::CURRENT.check_encoded_len(total)?;
    let mut out = Vec::with_capacity(total);
    out.push(ENVELOPE);
    write_metadata(&mut out, &dict);
    let mut writer = Writer {
        nodes,
        pool,
        sizes: &sizes,
        dict: &dict,
        out,
    };
    writer.write_tree(root, crate::limits::HARD_MAX_DEPTH as usize + 1)?;
    if writer.out.len() != total {
        return Err(misuse("encoded length differs from the computed size"));
    }
    Ok(writer.out)
}
