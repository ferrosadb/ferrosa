//! Module: Read admin-API response bodies without holding them.
//! Correctness: A body is streamed to its destination (stdout, stderr or a
//! streaming JSON parser) chunk by chunk with no size cap, and a failed read is
//! an error, never an empty body that reads as an empty result.
//! Last revised: 2026-10-28
//! Last changed: Replaced the 4 MiB-capped `response_text` buffer with
//! streaming copy, streamed error bodies, and a bounded-channel JSON parser.

use std::io::Write;

use serde::de::DeserializeOwned;

use super::WebError;

/// Chunks in flight between the network and a blocking JSON parser. Bounds
/// memory and makes a slow parser apply backpressure to the download.
const PARSE_CHUNKS_IN_FLIGHT: usize = 8;

/// Copy a body to `out` as it arrives and end it with a newline if it did not
/// end with one. Returns the number of body bytes written; an empty body
/// writes nothing.
///
/// # Errors
///
/// A failed read or write, with what failed.
pub(crate) async fn copy_body(
    resp: &mut reqwest::Response,
    out: &mut impl Write,
) -> Result<u64, WebError> {
    let mut written = 0u64;
    let mut last = b'\n';
    while let Some(chunk) = resp
        .chunk()
        .await
        .map_err(|e| format!("reading the response body after {written} bytes: {e}"))?
    {
        out.write_all(&chunk)?;
        written += chunk.len() as u64;
        if let Some(&b) = chunk.last() {
            last = b;
        }
    }
    if last != b'\n' {
        out.write_all(b"\n")?;
    }
    out.flush()?;
    Ok(written)
}

/// Print `prefix`, then the body, to stdout. The common success shape of the
/// admin commands: a one-line verdict followed by whatever the node said.
///
/// # Errors
///
/// As [`copy_body`].
pub(crate) async fn print_with_body(
    resp: &mut reqwest::Response,
    prefix: &str,
) -> Result<(), WebError> {
    let mut out = std::io::stdout().lock();
    writeln!(out, "{prefix}")?;
    copy_body(resp, &mut out).await?;
    Ok(())
}

/// Turn an error response into an error naming `what` and the status, with
/// the node's explanation.
///
/// A JSON body carrying an `error` string is read through the streaming
/// parser and quoted; anything else is streamed to stderr under a marker and
/// the error points at it. Either way nothing is buffered, and a body that
/// cannot be read is said to be unreadable rather than dropped.
pub(crate) async fn error_from_body(mut resp: reqwest::Response, what: &str) -> WebError {
    let status = resp.status();
    if is_json(&resp) {
        #[derive(serde::Deserialize)]
        struct ErrorBody {
            error: Option<String>,
        }
        return match parse_json_body::<ErrorBody>(resp).await {
            Ok(ErrorBody { error: Some(msg) }) => format!("{what} (HTTP {status}): {msg}").into(),
            Ok(ErrorBody { error: None }) => {
                format!("{what} (HTTP {status}); the JSON error body has no `error` field").into()
            }
            Err(e) => format!("{what} (HTTP {status}); its error body did not parse: {e}").into(),
        };
    }
    let mut err = std::io::stderr().lock();
    let copied = match writeln!(err, "--- {what}: HTTP {status} response body ---") {
        Ok(()) => copy_body(&mut resp, &mut err).await,
        Err(e) => Err(e.into()),
    };
    match copied {
        Ok(0) => format!("{what} (HTTP {status}) with an empty body").into(),
        Ok(_) => format!("{what} (HTTP {status}); the response body is printed above").into(),
        Err(e) => format!("{what} (HTTP {status}); the response body was unreadable: {e}").into(),
    }
}

fn is_json(resp: &reqwest::Response) -> bool {
    resp.headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|ct| ct.starts_with("application/json"))
}

/// Deserialize a body into `T` as it downloads. Memory is bounded by `T` and
/// the chunks in flight, not by the body: unknown fields are skipped by the
/// parser without being stored.
///
/// # Errors
///
/// A failed read, or a body that is not a `T`.
pub(crate) async fn parse_json_body<T>(resp: reqwest::Response) -> Result<T, WebError>
where
    T: DeserializeOwned + Send + 'static,
{
    parse_body_with(resp, |reader| Ok(serde_json::from_reader(reader)?)).await
}

/// Run `parse` on a blocking thread over the body as a `std::io::Read`, fed
/// chunk by chunk from the network through a bounded channel. If `parse`
/// stops early, the download stops with it.
///
/// # Errors
///
/// A failed read (surfaced to `parse` as an I/O error), or `parse`'s own.
pub(crate) async fn parse_body_with<T, F>(
    mut resp: reqwest::Response,
    parse: F,
) -> Result<T, WebError>
where
    T: Send + 'static,
    F: FnOnce(&mut dyn std::io::Read) -> Result<T, WebError> + Send + 'static,
{
    let (tx, rx) = tokio::sync::mpsc::channel(PARSE_CHUNKS_IN_FLIGHT);
    let parser = tokio::task::spawn_blocking(move || {
        let mut reader = ChunkReader {
            rx,
            current: None,
            offset: 0,
        };
        parse(&mut reader)
    });
    loop {
        let item = match resp.chunk().await {
            Ok(Some(chunk)) => Ok(chunk),
            Ok(None) => break,
            Err(e) => Err(format!("reading the response body: {e}")),
        };
        let failed = item.is_err();
        // A closed channel means the parser already finished or failed; its
        // result below says which.
        if tx.send(item).await.is_err() || failed {
            break;
        }
    }
    drop(tx);
    parser
        .await
        .map_err(|e| format!("response parser task failed: {e}"))?
}

/// `std::io::Read` over chunks received from the network task.
struct ChunkReader<B> {
    rx: tokio::sync::mpsc::Receiver<Result<B, String>>,
    current: Option<B>,
    offset: usize,
}

impl<B: AsRef<[u8]>> std::io::Read for ChunkReader<B> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        loop {
            if let Some(chunk) = &self.current {
                let rest = &chunk.as_ref()[self.offset..];
                if !rest.is_empty() {
                    let n = rest.len().min(buf.len());
                    buf[..n].copy_from_slice(&rest[..n]);
                    self.offset += n;
                    return Ok(n);
                }
            }
            match self.rx.blocking_recv() {
                Some(Ok(chunk)) => {
                    self.current = Some(chunk);
                    self.offset = 0;
                }
                Some(Err(e)) => return Err(std::io::Error::other(e)),
                None => return Ok(0),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Serve one raw HTTP response (headers + body exactly as given) and close.
    async fn serve_raw(raw: Vec<u8>) -> String {
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut seen = Vec::new();
            let mut buf = [0u8; 1024];
            while !seen.windows(4).any(|w| w == b"\r\n\r\n") {
                let n = socket.read(&mut buf).await.unwrap();
                assert!(
                    n > 0 && seen.len() < 16 * 1024,
                    "request headers never ended"
                );
                seen.extend_from_slice(&buf[..n]);
            }
            // The client may stop reading early (a parser that failed); a
            // write error then is expected and is not what these tests check.
            if let Err(e) = socket.write_all(&raw).await {
                eprintln!("test server: client closed early: {e}");
            }
        });
        format!("http://{addr}/")
    }

    fn http(status: &str, content_type: &str, content_length: usize, body: &[u8]) -> Vec<u8> {
        let mut raw = format!(
            "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\n\
             Content-Length: {content_length}\r\nConnection: close\r\n\r\n"
        )
        .into_bytes();
        raw.extend_from_slice(body);
        raw
    }

    #[tokio::test]
    async fn copy_body_streams_the_whole_body_and_ends_the_line() {
        let url = serve_raw(http("200 OK", "text/plain", 5, b"hello")).await;
        let mut resp = reqwest::get(&url).await.unwrap();
        let mut out = Vec::new();
        let n = copy_body(&mut resp, &mut out).await.unwrap();
        assert_eq!((n, out.as_slice()), (5, b"hello\n".as_slice()));
    }

    /// No size cap: a body far past the old 4 MiB limit is copied in full.
    #[tokio::test]
    async fn copy_body_has_no_size_cap() {
        let body = vec![b'x'; (4 << 20) + 1];
        let url = serve_raw(http("200 OK", "text/plain", body.len(), &body)).await;
        let mut resp = reqwest::get(&url).await.unwrap();
        let mut out = Vec::new();
        let n = copy_body(&mut resp, &mut out).await.unwrap();
        assert_eq!(n, body.len() as u64);
    }

    /// A body cut short is a failure, not a short or empty result.
    #[tokio::test]
    async fn copy_body_refuses_a_truncated_body() {
        let url = serve_raw(http("200 OK", "text/plain", 100, b"partial")).await;
        let mut resp = reqwest::get(&url).await.unwrap();
        let err = copy_body(&mut resp, &mut Vec::new())
            .await
            .expect_err("a truncated body is an error");
        assert!(err.to_string().contains("after 7 bytes"), "{err}");
    }

    #[tokio::test]
    async fn parse_json_body_reads_only_the_fields_asked_for() {
        #[derive(serde::Deserialize)]
        struct Wanted {
            schema_version: String,
        }
        let body = br#"{"schema_version":"v1","state_members":{"a":[1,2,3]},"x":"y"}"#;
        let url = serve_raw(http("200 OK", "application/json", body.len(), body)).await;
        let got: Wanted = parse_json_body(reqwest::get(&url).await.unwrap())
            .await
            .unwrap();
        assert_eq!(got.schema_version, "v1");
    }

    #[tokio::test]
    async fn parse_json_body_refuses_a_truncated_body() {
        let url = serve_raw(http("200 OK", "application/json", 100, br#"{"schema_v"#)).await;
        let err = parse_json_body::<std::collections::BTreeMap<String, String>>(
            reqwest::get(&url).await.unwrap(),
        )
        .await
        .expect_err("a truncated body does not parse");
        assert!(!err.to_string().is_empty());
    }

    #[tokio::test]
    async fn error_from_body_quotes_a_json_error_field() {
        let body = br#"{"error":"table not registered"}"#;
        let url = serve_raw(http("404 Not Found", "application/json", body.len(), body)).await;
        let err = error_from_body(reqwest::get(&url).await.unwrap(), "rebuild").await;
        assert_eq!(
            err.to_string(),
            "rebuild (HTTP 404 Not Found): table not registered"
        );
    }

    #[tokio::test]
    async fn error_from_body_streams_a_non_json_body_and_says_so() {
        let url = serve_raw(http(
            "502 Bad Gateway",
            "text/html",
            9,
            b"<h1>x</h1>"[..9].as_ref(),
        ))
        .await;
        let err = error_from_body(reqwest::get(&url).await.unwrap(), "ring").await;
        assert_eq!(
            err.to_string(),
            "ring (HTTP 502 Bad Gateway); the response body is printed above"
        );
    }
}
