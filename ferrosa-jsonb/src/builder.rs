//! Module: `JsonbBuilder`, the event-driven front end of the canonical encoder
//! (T-102, D4, D6b, D14, M6).
//! Correctness: correct when misordered events fail with a typed error, nesting
//! is bounded by `max_depth`, a running lower bound of the encoded size is
//! checked as values arrive (so an oversized document is refused while it is
//! still being built, not after), duplicate object keys resolve last-wins (or
//! fail under `DuplicateKeyPolicy::Error`) and are counted, and the finished
//! bytes are the canonical form of `encode`. There is no recursion: the value is
//! an arena of nodes and an explicit frame stack.
//! Last revised: 2026-09-28
//! Last changed: T-102 initial builder.

use crate::encode::{encode_tree, write_bool, write_null, write_number, write_string};
use crate::error::JsonbError;
use crate::limits::{DuplicateKeyPolicy, HardCeilings, Limits};
use crate::number::Number;

/// Fixed bytes every cell has: envelope plus the minimal metadata (`11 00 00`).
const CELL_FLOOR: usize = 4;
/// Least bytes a container's own header takes (header, count, one offset).
const CONTAINER_FLOOR: usize = 3;
/// Least bytes an object entry adds beyond its value: one id and one offset.
const OBJECT_ENTRY_FLOOR: usize = 2;
/// Least bytes an array element adds beyond its value: one offset.
const ARRAY_ITEM_FLOOR: usize = 1;

/// A finished build: the canonical cell bytes and the duplicate-key count.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Encoded {
    /// Envelope, Variant metadata and value.
    pub bytes: Vec<u8>,
    /// Object keys dropped by last-wins resolution (D6b). The caller reports it
    /// on `jsonb_duplicate_keys_dropped_total{edge}`.
    pub duplicate_keys_dropped: u64,
}

#[derive(Debug)]
pub(crate) struct Entry {
    pub(crate) key: String,
    pub(crate) child: usize,
}

#[derive(Debug)]
pub(crate) enum Kind {
    /// Already-encoded bytes at `pool[start..start + len]`.
    Scalar {
        start: usize,
        len: usize,
    },
    Object(Vec<Entry>),
    Array(Vec<usize>),
}

#[derive(Debug)]
pub(crate) struct Node {
    pub(crate) kind: Kind,
    /// Lower bound of the encoded size of this subtree, set when it closes.
    sub: usize,
}

struct Frame {
    node: usize,
    pending_key: Option<String>,
}

/// Builds one jsonb value from begin/end/scalar events.
pub struct JsonbBuilder {
    limits: Limits,
    nodes: Vec<Node>,
    pool: Vec<u8>,
    stack: Vec<Frame>,
    root: Option<usize>,
    floor: usize,
    dropped: u64,
    failed: bool,
}

fn misuse(reason: &'static str) -> JsonbError {
    JsonbError::BuilderMisuse { reason }
}

impl JsonbBuilder {
    /// A builder that enforces `limits` as events arrive.
    pub fn new(limits: Limits) -> JsonbBuilder {
        JsonbBuilder {
            limits,
            nodes: Vec::new(),
            pool: Vec::new(),
            stack: Vec::new(),
            root: None,
            floor: CELL_FLOOR,
            dropped: 0,
            failed: false,
        }
    }

    /// Run one event; any error poisons the builder so `finish` cannot succeed
    /// on a half-applied event sequence.
    fn run(
        &mut self,
        event: impl FnOnce(&mut JsonbBuilder) -> Result<(), JsonbError>,
    ) -> Result<(), JsonbError> {
        if self.failed {
            return Err(misuse("builder already failed"));
        }
        let result = event(self);
        self.failed = result.is_err();
        result
    }

    fn add_floor(&mut self, bytes: usize) -> Result<(), JsonbError> {
        self.floor = self.floor.saturating_add(bytes);
        self.limits.check_encoded_len(self.floor)?;
        HardCeilings::CURRENT.check_encoded_len(self.floor)
    }

    fn node_mut(&mut self, idx: usize) -> Result<&mut Node, JsonbError> {
        self.nodes
            .get_mut(idx)
            .ok_or(misuse("node index out of range"))
    }

    /// Attach a new node to its parent (or make it the root) and count its
    /// smallest possible encoding against the size limit.
    fn attach(&mut self, kind: Kind, floor: usize) -> Result<usize, JsonbError> {
        let idx = self.nodes.len();
        let overhead = match self.stack.last_mut() {
            None if self.root.is_some() => return Err(misuse("second top-level value")),
            None => {
                self.root = Some(idx);
                0
            }
            Some(frame) => {
                let key = frame.pending_key.take();
                let parent = self
                    .nodes
                    .get_mut(frame.node)
                    .ok_or(misuse("frame node missing"))?;
                match (&mut parent.kind, key) {
                    (Kind::Object(entries), Some(key)) => {
                        entries.push(Entry { key, child: idx });
                        OBJECT_ENTRY_FLOOR
                    }
                    (Kind::Object(_), None) => return Err(misuse("object value without a key")),
                    (Kind::Array(items), None) => {
                        items.push(idx);
                        ARRAY_ITEM_FLOOR
                    }
                    (Kind::Array(_), Some(_)) => return Err(misuse("key inside an array")),
                    (Kind::Scalar { .. }, _) => return Err(misuse("frame on a scalar")),
                }
            }
        };
        self.nodes.push(Node { kind, sub: floor });
        self.add_floor(overhead + floor)?;
        Ok(idx)
    }

    fn scalar(
        &mut self,
        write: impl FnOnce(&mut Vec<u8>) -> Result<(), JsonbError>,
    ) -> Result<(), JsonbError> {
        let start = self.pool.len();
        write(&mut self.pool)?;
        let len = self.pool.len() - start;
        self.attach(Kind::Scalar { start, len }, len).map(|_| ())
    }

    fn infallible_scalar(&mut self, write: impl FnOnce(&mut Vec<u8>)) -> Result<(), JsonbError> {
        self.scalar(|out| {
            write(out);
            Ok(())
        })
    }

    fn begin(&mut self, object: bool) -> Result<(), JsonbError> {
        let depth = u32::try_from(self.stack.len() + 1).unwrap_or(u32::MAX);
        self.limits.check_depth(depth)?;
        HardCeilings::CURRENT.check_depth(depth)?;
        let kind = if object {
            Kind::Object(Vec::new())
        } else {
            Kind::Array(Vec::new())
        };
        let node = self.attach(kind, CONTAINER_FLOOR)?;
        self.stack.push(Frame {
            node,
            pending_key: None,
        });
        Ok(())
    }

    /// Pop the open container, checking it is the kind the caller closes.
    fn pop_container(&mut self, object: bool) -> Result<usize, JsonbError> {
        let frame = self
            .stack
            .pop()
            .ok_or(misuse("end without a matching begin"))?;
        if frame.pending_key.is_some() {
            return Err(misuse("object key without a value"));
        }
        let is_object = matches!(
            self.nodes.get(frame.node).map(|n| &n.kind),
            Some(Kind::Object(_))
        );
        if is_object != object {
            return Err(misuse("end does not match the open container kind"));
        }
        Ok(frame.node)
    }

    fn subtree_floor(&self, idx: usize) -> usize {
        self.nodes.get(idx).map_or(0, |n| n.sub)
    }

    fn end(&mut self, object: bool) -> Result<(), JsonbError> {
        let idx = self.pop_container(object)?;
        let node = self.node_mut(idx)?;
        let sub = if object {
            let Kind::Object(entries) = std::mem::replace(&mut node.kind, Kind::Array(Vec::new()))
            else {
                return Err(misuse("object node changed kind"));
            };
            let entries = self.dedupe(entries)?;
            let sub = CONTAINER_FLOOR
                + entries
                    .iter()
                    .map(|e| OBJECT_ENTRY_FLOOR + self.subtree_floor(e.child))
                    .sum::<usize>();
            self.node_mut(idx)?.kind = Kind::Object(entries);
            sub
        } else {
            let Kind::Array(items) = &node.kind else {
                return Err(misuse("array node changed kind"));
            };
            let items = items.clone();
            CONTAINER_FLOOR
                + items
                    .iter()
                    .map(|c| ARRAY_ITEM_FLOOR + self.subtree_floor(*c))
                    .sum::<usize>()
        };
        self.node_mut(idx)?.sub = sub;
        Ok(())
    }

    /// Sort entries by key bytes and resolve duplicates: the last occurrence wins
    /// (D6b). A dropped entry gives its size back to the running lower bound.
    fn dedupe(&mut self, mut entries: Vec<Entry>) -> Result<Vec<Entry>, JsonbError> {
        entries.sort_by(|a, b| a.key.cmp(&b.key));
        let mut out: Vec<Entry> = Vec::with_capacity(entries.len());
        for entry in entries {
            match out.last_mut() {
                Some(prev) if prev.key == entry.key => {
                    if self.limits.duplicate_keys == DuplicateKeyPolicy::Error {
                        return Err(JsonbError::DuplicateKey);
                    }
                    let give_back = OBJECT_ENTRY_FLOOR + self.subtree_floor(prev.child);
                    self.floor = self.floor.saturating_sub(give_back);
                    self.dropped += 1;
                    *prev = entry;
                }
                _ => out.push(entry),
            }
        }
        Ok(out)
    }

    /// Open an object.
    pub fn begin_object(&mut self) -> Result<(), JsonbError> {
        self.run(|b| b.begin(true))
    }

    /// Close the innermost object.
    pub fn end_object(&mut self) -> Result<(), JsonbError> {
        self.run(|b| b.end(true))
    }

    /// Open an array.
    pub fn begin_array(&mut self) -> Result<(), JsonbError> {
        self.run(|b| b.begin(false))
    }

    /// Close the innermost array.
    pub fn end_array(&mut self) -> Result<(), JsonbError> {
        self.run(|b| b.end(false))
    }

    /// The key of the next object value.
    pub fn key(&mut self, key: &str) -> Result<(), JsonbError> {
        self.run(|b| {
            let frame = b.stack.last_mut().ok_or(misuse("key outside an object"))?;
            if frame.pending_key.is_some() {
                return Err(misuse("two keys in a row"));
            }
            let in_object = matches!(
                b.nodes.get(frame.node).map(|n| &n.kind),
                Some(Kind::Object(_))
            );
            if !in_object {
                return Err(misuse("key inside an array"));
            }
            frame.pending_key = Some(key.to_string());
            Ok(())
        })
    }

    /// A JSON null.
    pub fn null(&mut self) -> Result<(), JsonbError> {
        self.run(|b| b.infallible_scalar(write_null))
    }

    /// A JSON boolean.
    pub fn boolean(&mut self, v: bool) -> Result<(), JsonbError> {
        self.run(|b| b.infallible_scalar(|out| write_bool(out, v)))
    }

    /// A JSON string (already valid UTF-8).
    pub fn string(&mut self, v: &str) -> Result<(), JsonbError> {
        self.run(|b| b.infallible_scalar(|out| write_string(out, v)))
    }

    /// An exact number, stored at its scale (D2a).
    pub fn number(&mut self, v: Number) -> Result<(), JsonbError> {
        self.run(|b| b.scalar(|out| write_number(out, &v)))
    }

    /// Finish: every container must be closed and a value present.
    pub fn finish(self) -> Result<Encoded, JsonbError> {
        if self.failed {
            return Err(misuse("builder already failed"));
        }
        if !self.stack.is_empty() {
            return Err(misuse("finish with an unclosed container"));
        }
        let root = self.root.ok_or(misuse("finish with no value"))?;
        let bytes = encode_tree(&self.nodes, &self.pool, root, &self.limits)?;
        Ok(Encoded {
            bytes,
            duplicate_keys_dropped: self.dropped,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::limits::LimitsConfig;

    fn limits_with(max_encoded: u64, max_depth: u64) -> Limits {
        let no_env = |_: &str| None;
        let cfg = LimitsConfig {
            max_encoded_bytes: Some(max_encoded),
            max_nesting_depth: Some(max_depth),
            ..LimitsConfig::default()
        };
        Limits::from_config_with_env(&cfg, &no_env, 64 * 1024 * 1024).expect("limits")
    }

    fn default_limits() -> Limits {
        limits_with(1024 * 1024, 1000)
    }

    fn reason(e: JsonbError) -> &'static str {
        match e {
            JsonbError::BuilderMisuse { reason } => reason,
            other => panic!("expected BuilderMisuse, got {other:?}"),
        }
    }

    #[test]
    fn jsonb_builder_rejects_unbalanced_and_oversize() {
        let mut b = JsonbBuilder::new(default_limits());
        assert_eq!(
            reason(b.end_array().unwrap_err()),
            "end without a matching begin"
        );

        let mut b = JsonbBuilder::new(default_limits());
        b.begin_array().unwrap();
        assert_eq!(
            reason(b.end_object().unwrap_err()),
            "end does not match the open container kind"
        );

        let mut b = JsonbBuilder::new(default_limits());
        b.begin_array().unwrap();
        assert_eq!(
            reason(b.finish().unwrap_err()),
            "finish with an unclosed container"
        );

        let b = JsonbBuilder::new(default_limits());
        assert_eq!(reason(b.finish().unwrap_err()), "finish with no value");

        let mut b = JsonbBuilder::new(default_limits());
        b.null().unwrap();
        assert_eq!(reason(b.null().unwrap_err()), "second top-level value");

        let mut b = JsonbBuilder::new(default_limits());
        b.begin_object().unwrap();
        assert_eq!(reason(b.null().unwrap_err()), "object value without a key");

        let mut b = JsonbBuilder::new(default_limits());
        b.begin_object().unwrap();
        b.key("a").unwrap();
        assert_eq!(
            reason(b.end_object().unwrap_err()),
            "object key without a value"
        );

        let mut b = JsonbBuilder::new(default_limits());
        b.begin_array().unwrap();
        assert_eq!(reason(b.key("a").unwrap_err()), "key inside an array");

        // A poisoned builder cannot finish.
        let mut b = JsonbBuilder::new(default_limits());
        assert!(b.end_array().is_err());
        assert_eq!(reason(b.finish().unwrap_err()), "builder already failed");
    }

    #[test]
    fn jsonb_builder_oversize_is_refused_while_building() {
        let mut b = JsonbBuilder::new(limits_with(64, 1000));
        b.begin_array().unwrap();
        let mut failed_at = None;
        for i in 0..64 {
            if let Err(e) = b.string("0123456789") {
                assert!(matches!(e, JsonbError::EncodedTooLarge { max: 64, .. }));
                failed_at = Some(i);
                break;
            }
        }
        let at = failed_at.expect("a 64-byte limit must trip before 64 ten-byte strings");
        assert!(
            at < 8,
            "tripped at element {at}, expected within the first few"
        );
    }

    #[test]
    fn jsonb_builder_exact_size_at_limit_passes_and_one_over_fails() {
        let mut b = JsonbBuilder::new(default_limits());
        b.null().unwrap();
        let exact = b.finish().unwrap().bytes.len();
        let mut ok = JsonbBuilder::new(limits_with(exact as u64, 1000));
        ok.null().unwrap();
        assert!(ok.finish().is_ok());
        let mut over = JsonbBuilder::new(limits_with(exact as u64 - 1, 1000));
        assert!(matches!(
            over.null().err(),
            Some(JsonbError::EncodedTooLarge { .. })
        ));
    }

    #[test]
    fn jsonb_builder_dropped_duplicates_give_their_size_back() {
        // The size bound counts a duplicate until the object closes (dedupe runs
        // at `end_object`), then returns it: afterwards the running bound equals
        // that of the same object written once.
        let text = "z".repeat(40);
        let mut twice = JsonbBuilder::new(default_limits());
        twice.begin_object().unwrap();
        for _ in 0..2 {
            twice.key("a").unwrap();
            twice.string(&text).unwrap();
        }
        twice.end_object().unwrap();
        let mut once = JsonbBuilder::new(default_limits());
        once.begin_object().unwrap();
        once.key("a").unwrap();
        once.string(&text).unwrap();
        once.end_object().unwrap();
        assert_eq!(twice.floor, once.floor);
        assert_eq!(twice.finish().unwrap().duplicate_keys_dropped, 1);
    }

    #[test]
    fn jsonb_builder_depth_limit_and_no_recursion_at_1000() {
        let out = std::thread::Builder::new()
            .stack_size(256 * 1024)
            .spawn(|| {
                let mut b = JsonbBuilder::new(limits_with(1024 * 1024, 1000));
                for _ in 0..1000 {
                    b.begin_array().unwrap();
                }
                let over = b.begin_array().unwrap_err();
                assert_eq!(
                    over,
                    JsonbError::DepthExceeded {
                        depth: 1001,
                        max: 1000
                    }
                );
                let mut b = JsonbBuilder::new(limits_with(1024 * 1024, 1000));
                for _ in 0..1000 {
                    b.begin_array().unwrap();
                }
                b.null().unwrap();
                for _ in 0..1000 {
                    b.end_array().unwrap();
                }
                b.finish().unwrap().bytes.len()
            })
            .unwrap()
            .join()
            .unwrap();
        assert!(out > 1000);
    }

    #[test]
    fn jsonb_builder_duplicate_policy_error_is_typed() {
        let mut limits = default_limits();
        limits.duplicate_keys = DuplicateKeyPolicy::Error;
        let mut b = JsonbBuilder::new(limits);
        b.begin_object().unwrap();
        for _ in 0..2 {
            b.key("a").unwrap();
            b.null().unwrap();
        }
        assert_eq!(b.end_object().unwrap_err(), JsonbError::DuplicateKey);
    }
}
