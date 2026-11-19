//! Incremental response bodies for streamed SPARQL results.
//!
//! The document head is written first, then each solution (or triple) as it is
//! pulled from the executor, then the tail. Rows are coalesced into chunks of
//! about [`RESPONSE_CHUNK_BYTES`] so a large result is not one write per row.
//! The HTTP body pulls the next chunk only when the client has taken the last
//! one, so a slow client back-pressures the storage scan.
//!
//! An error after the head has been sent cannot become an error status. It
//! ends the body with an I/O error, which aborts the response: the client sees
//! a truncated, failed transfer, never a well-formed document that silently
//! lacks rows.
//! Correctness: byte-compatible with the collected serializers; mid-stream errors abort.
//! Last revised: 2026-11-16
//! Last changed: Stream SELECT and graph results into the HTTP body.

use axum::body::Bytes;
use futures::stream::{self, BoxStream};
use futures::StreamExt;

use crate::engine::{ConstructedTriple, GraphStream};
use crate::error::SparqlError;
use crate::executor::{Solution, SolutionStream};
use crate::results::{ntriples_row, xml_head, xml_result, Binding, ResultFormat, XML_TAIL};

/// Target size of one body chunk. A buffer size, not a result bound.
pub(crate) const RESPONSE_CHUNK_BYTES: usize = 64 * 1024;

/// A response body as a stream of chunks.
pub(crate) type BodyStream = BoxStream<'static, Result<Bytes, std::io::Error>>;

/// How each item of a stream is written into the body.
enum Encoder {
    /// SPARQL Results JSON. `allow_rdf12` is false when the head declared no
    /// 1.2 version, so a 1.2 term arriving later is an error, not a lie.
    Json {
        allow_rdf12: bool,
        first: bool,
    },
    Xml {
        vars: Vec<String>,
    },
    NTriples {
        vars: Vec<String>,
    },
}

impl Encoder {
    fn row(&mut self, row: &Solution, out: &mut Vec<u8>) -> Result<(), String> {
        match self {
            Self::Json { allow_rdf12, first } => {
                if !*allow_rdf12 && row.values().any(Binding::requires_json12) {
                    return Err("a solution carries an RDF 1.2 term, which the negotiated \
                                SPARQL Results JSON version cannot represent"
                        .into());
                }
                if !std::mem::take(first) {
                    out.push(b',');
                }
                serde_json::to_writer(&mut *out, row).map_err(|e| e.to_string())
            }
            Self::Xml { vars } => {
                out.extend_from_slice(xml_result(vars, row)?.as_bytes());
                Ok(())
            }
            Self::NTriples { vars } => {
                out.extend_from_slice(ntriples_row(vars, row)?.as_bytes());
                Ok(())
            }
        }
    }
}

/// The SELECT document head for `format`.
fn select_head(
    format: ResultFormat,
    vars: &[String],
    may_bind_rdf12: bool,
) -> Result<(Vec<u8>, Encoder, &'static [u8]), String> {
    let json = |version: Option<&str>, allow_rdf12: bool| {
        #[derive(serde::Serialize)]
        struct Head<'a> {
            vars: &'a [String],
            #[serde(skip_serializing_if = "Option::is_none")]
            version: Option<&'a str>,
        }
        let mut head = b"{\"head\":".to_vec();
        serde_json::to_writer(&mut head, &Head { vars, version }).map_err(|e| e.to_string())?;
        head.extend_from_slice(b",\"results\":{\"bindings\":[");
        Ok::<_, String>((
            head,
            Encoder::Json {
                allow_rdf12,
                first: true,
            },
            &b"]}}"[..],
        ))
    };
    match format {
        ResultFormat::Json => json(may_bind_rdf12.then_some("1.2"), may_bind_rdf12),
        ResultFormat::Json12 => json(Some("1.2"), true),
        ResultFormat::Json11 => json(None, false),
        ResultFormat::Xml => Ok((
            xml_head(vars).into_bytes(),
            Encoder::Xml {
                vars: vars.to_vec(),
            },
            XML_TAIL.as_bytes(),
        )),
        ResultFormat::Turtle | ResultFormat::NTriples => Ok((
            Vec::new(),
            Encoder::NTriples {
                vars: vars.to_vec(),
            },
            &b""[..],
        )),
    }
}

/// Stream a SELECT result in `format`.
pub(crate) fn select_body(
    format: ResultFormat,
    vars: &[String],
    may_bind_rdf12: bool,
    rows: SolutionStream,
) -> BodyStream {
    let (head, mut encoder, tail) = match select_head(format, vars, may_bind_rdf12) {
        Ok(parts) => parts,
        Err(e) => return failed(e),
    };
    chunked(head, rows, move |row, out| encoder.row(&row, out), tail)
}

/// Stream a CONSTRUCT/DESCRIBE result as N-Triples (Turtle is the same subset).
pub(crate) fn graph_body(triples: GraphStream) -> BodyStream {
    chunked(
        Vec::new(),
        triples,
        |t: ConstructedTriple, out: &mut Vec<u8>| {
            out.extend_from_slice(t.to_ntriples_line().as_bytes());
            Ok(())
        },
        &b""[..],
    )
}

fn body_error(message: String) -> std::io::Error {
    tracing::error!(error = %message, "SPARQL response aborted mid-stream");
    std::io::Error::other(message)
}

fn failed(message: String) -> BodyStream {
    stream::iter([Err(body_error(message))]).boxed()
}

/// State of a chunked body between pulls.
struct Chunking<T, F> {
    items: BoxStream<'static, Result<T, SparqlError>>,
    encode: F,
    buf: Vec<u8>,
    tail: &'static [u8],
    done: bool,
}

/// Head, then encoded items coalesced into chunks, then tail.
fn chunked<T, F>(
    head: Vec<u8>,
    items: BoxStream<'static, Result<T, SparqlError>>,
    encode: F,
    tail: &'static [u8],
) -> BodyStream
where
    T: Send + 'static,
    F: FnMut(T, &mut Vec<u8>) -> Result<(), String> + Send + 'static,
{
    let state = Chunking {
        items,
        encode,
        buf: head,
        tail,
        done: false,
    };
    stream::unfold(state, |mut state| async move {
        if state.done {
            return None;
        }
        while state.buf.len() < RESPONSE_CHUNK_BYTES {
            match state.items.next().await {
                Some(Ok(item)) => {
                    if let Err(e) = (state.encode)(item, &mut state.buf) {
                        state.done = true;
                        return Some((Err(body_error(e)), state));
                    }
                }
                Some(Err(e)) => {
                    state.done = true;
                    return Some((Err(body_error(e.to_string())), state));
                }
                None => {
                    state.buf.extend_from_slice(state.tail);
                    state.done = true;
                    break;
                }
            }
        }
        let chunk = Bytes::from(std::mem::take(&mut state.buf));
        Some((Ok(chunk), state))
    })
    .boxed()
}
