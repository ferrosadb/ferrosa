//! A minimal PostgreSQL v3 frontend client that speaks raw frames.
//!
//! The driver-level tests (`tokio-postgres`) cannot name a prepared statement:
//! the driver assigns `s0`, `s1`, … from a process-global counter and exposes no
//! way to read or set that name. Anything that must address a prepared statement
//! BY NAME over the wire (and assert the exact SQLSTATE / command tag the server
//! answers) needs the frames themselves, so this module drives the protocol
//! directly: startup + SCRAM-SHA-256, then `Query` / `Parse` / `Bind` /
//! `Execute` / `Sync` / `Terminate`.
//!
//! Included with `#[path]` by the test binaries that need it; not every binary
//! uses every helper.
#![allow(dead_code)]

use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

type HmacSha256 = Hmac<Sha256>;

fn hmac256(key: &[u8], msg: &[u8]) -> [u8; 32] {
    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC accepts any key length");
    mac.update(msg);
    mac.finalize().into_bytes().into()
}

/// One backend frame: the tag byte and its body (length word stripped).
#[derive(Debug, Clone)]
pub struct Frame {
    pub tag: u8,
    pub body: Vec<u8>,
}

impl Frame {
    /// The `(field code, value)` pairs of an `ErrorResponse` / `NoticeResponse`.
    fn fields(&self) -> Vec<(u8, String)> {
        let mut fields = Vec::new();
        let mut i = 0;
        while i < self.body.len() && self.body[i] != 0 {
            let code = self.body[i];
            let rest = &self.body[i + 1..];
            let end = rest.iter().position(|b| *b == 0).unwrap_or(rest.len());
            fields.push((code, String::from_utf8_lossy(&rest[..end]).into_owned()));
            i += 1 + end + 1;
        }
        fields
    }

    /// The SQLSTATE of an `ErrorResponse`, or `None` for any other frame.
    pub fn sqlstate(&self) -> Option<String> {
        (self.tag == b'E')
            .then(|| self.fields().into_iter().find(|(code, _)| *code == b'C'))
            .flatten()
            .map(|(_, value)| value)
    }

    /// The primary message of an `ErrorResponse`, or `None` for any other frame.
    pub fn error_message(&self) -> Option<String> {
        (self.tag == b'E')
            .then(|| self.fields().into_iter().find(|(code, _)| *code == b'M'))
            .flatten()
            .map(|(_, value)| value)
    }

    /// The completion tag of a `CommandComplete` (`DEALLOCATE`, `DEALLOCATE ALL`,
    /// `SELECT 3`, …), or `None` for any other frame.
    pub fn command_tag(&self) -> Option<String> {
        if self.tag != b'C' {
            return None;
        }
        let end = self
            .body
            .iter()
            .position(|b| *b == 0)
            .unwrap_or(self.body.len());
        Some(String::from_utf8_lossy(&self.body[..end]).into_owned())
    }

    /// A one-line rendering for assertion messages.
    pub fn describe(&self) -> String {
        match self.tag {
            b'E' => format!(
                "ErrorResponse {{ sqlstate: {:?}, message: {:?} }}",
                self.sqlstate(),
                self.error_message()
            ),
            b'C' => format!("CommandComplete({:?})", self.command_tag()),
            tag => format!("Frame({:?})", tag as char),
        }
    }
}

/// A connection that has completed startup. Sends raw frontend frames and
/// collects backend frames.
pub struct WireClient {
    stream: TcpStream,
}

impl WireClient {
    /// Connect, run the SCRAM-SHA-256 handshake as `user`, and stop at the first
    /// `ReadyForQuery`.
    pub async fn connect(port: u16, user: &str, password: &str) -> WireClient {
        let stream = TcpStream::connect(("127.0.0.1", port))
            .await
            .expect("connect to the listener");
        let mut client = WireClient { stream };
        client.startup(user, "ferrosa", password).await;
        client
    }

    async fn startup(&mut self, user: &str, database: &str, password: &str) {
        let mut body = Vec::new();
        body.extend_from_slice(&196_608i32.to_be_bytes()); // protocol 3.0
        for (key, value) in [("user", user), ("database", database)] {
            body.extend_from_slice(key.as_bytes());
            body.push(0);
            body.extend_from_slice(value.as_bytes());
            body.push(0);
        }
        body.push(0);
        let mut frame = (body.len() as i32 + 4).to_be_bytes().to_vec();
        frame.extend_from_slice(&body);
        self.stream
            .write_all(&frame)
            .await
            .expect("send StartupMessage");

        let nonce = nonce(24);
        let client_first_bare = format!("n=,r={nonce}");
        let client_first = format!("n,,{client_first_bare}");
        let mut server_signature = None;

        loop {
            let frame = self.read_frame().await;
            match frame.tag {
                b'R' => {
                    let code = i32::from_be_bytes(frame.body[..4].try_into().unwrap());
                    let payload = &frame.body[4..];
                    match code {
                        0 => {} // AuthenticationOk
                        10 => {
                            // AuthenticationSASL: pick SCRAM-SHA-256.
                            let mechanisms: Vec<&[u8]> = payload
                                .split(|b| *b == 0)
                                .filter(|m| !m.is_empty())
                                .collect();
                            assert!(
                                mechanisms.contains(&b"SCRAM-SHA-256".as_slice()),
                                "server must offer SCRAM-SHA-256, offered {mechanisms:?}"
                            );
                            let mut body = b"SCRAM-SHA-256\0".to_vec();
                            body.extend_from_slice(&(client_first.len() as i32).to_be_bytes());
                            body.extend_from_slice(client_first.as_bytes());
                            self.send_password(&body).await;
                        }
                        11 => {
                            // AuthenticationSASLContinue: server-first.
                            let server_first = String::from_utf8_lossy(payload).into_owned();
                            let combined_nonce = field(&server_first, "r")
                                .expect("server-first carries the combined nonce");
                            assert!(
                                combined_nonce.starts_with(&nonce),
                                "server-first nonce must extend the client nonce"
                            );
                            let salt = BASE64
                                .decode(field(&server_first, "s").expect("salt"))
                                .expect("salt is base64");
                            let iterations: u32 = field(&server_first, "i")
                                .expect("iteration count")
                                .parse()
                                .expect("integer");

                            let mut salted = [0u8; 32];
                            pbkdf2::pbkdf2_hmac::<Sha256>(
                                password.as_bytes(),
                                &salt,
                                iterations,
                                &mut salted,
                            );
                            let client_key = hmac256(&salted, b"Client Key");
                            let stored_key: [u8; 32] = Sha256::digest(client_key).into();
                            let server_key = hmac256(&salted, b"Server Key");

                            let without_proof = format!("c=biws,r={combined_nonce}");
                            let auth_message =
                                format!("{client_first_bare},{server_first},{without_proof}");
                            let client_signature = hmac256(&stored_key, auth_message.as_bytes());
                            let proof: Vec<u8> = client_key
                                .iter()
                                .zip(client_signature.iter())
                                .map(|(key, sig)| key ^ sig)
                                .collect();
                            let client_final =
                                format!("{without_proof},p={}", BASE64.encode(&proof));
                            self.send_password(client_final.as_bytes()).await;

                            server_signature = Some(hmac256(&server_key, auth_message.as_bytes()));
                        }
                        12 => {
                            // AuthenticationSASLFinal: verify the server proof.
                            let final_msg = String::from_utf8_lossy(payload);
                            let v = field(&final_msg, "v").expect("server-final carries v=");
                            let got = BASE64.decode(v).expect("server proof is base64");
                            assert_eq!(
                                got,
                                server_signature.expect("a proof was computed"),
                                "server signature must verify"
                            );
                        }
                        other => panic!("unexpected authentication request {other}"),
                    }
                }
                b'E' => panic!("startup failed: {}", frame.describe()),
                b'Z' => return,
                _ => {} // ParameterStatus, BackendKeyData, …
            }
        }
    }

    async fn send_password(&mut self, payload: &[u8]) {
        self.send_frame(b'p', payload).await;
    }

    /// Send a raw frontend frame, for a shape this helper does not model.
    pub async fn send_raw(&mut self, tag: u8, body: &[u8]) {
        self.send_frame(tag, body).await;
    }

    /// Read backend frames through the next `ReadyForQuery`.
    pub async fn read_ready(&mut self) -> Vec<Frame> {
        self.read_until_ready().await
    }

    /// Read one backend frame.
    async fn read_frame(&mut self) -> Frame {
        let mut head = [0u8; 5];
        self.stream
            .read_exact(&mut head)
            .await
            .expect("read a backend frame header");
        let len = i32::from_be_bytes(head[1..5].try_into().unwrap());
        assert!(len >= 4, "frame length word must count itself");
        let mut body = vec![0u8; (len - 4) as usize];
        self.stream
            .read_exact(&mut body)
            .await
            .expect("read a backend frame body");
        Frame { tag: head[0], body }
    }

    async fn send_frame(&mut self, tag: u8, body: &[u8]) {
        let mut frame = Vec::with_capacity(body.len() + 5);
        frame.push(tag);
        frame.extend_from_slice(&(body.len() as i32 + 4).to_be_bytes());
        frame.extend_from_slice(body);
        self.stream
            .write_all(&frame)
            .await
            .expect("send a frontend frame");
    }

    /// Send a `Query` (simple protocol) and collect every reply through the
    /// final `ReadyForQuery`.
    pub async fn simple(&mut self, sql: &str) -> Vec<Frame> {
        let mut body = sql.as_bytes().to_vec();
        body.push(0);
        self.send_frame(b'Q', &body).await;
        self.read_until_ready().await
    }

    /// Send `Parse` + `Sync` and collect the replies (`ParseComplete`, or the
    /// `ErrorResponse`).
    pub async fn parse(&mut self, name: &str, sql: &str) -> Vec<Frame> {
        let mut body = Vec::new();
        body.extend_from_slice(name.as_bytes());
        body.push(0);
        body.extend_from_slice(sql.as_bytes());
        body.push(0);
        body.extend_from_slice(&0i16.to_be_bytes()); // no declared parameter OIDs
        self.send_frame(b'P', &body).await;
        self.send_frame(b'S', &[]).await;
        self.read_until_ready().await
    }

    /// Send `Bind` + `Execute` + `Sync` for a portal over a prepared statement
    /// with no parameters, and collect the replies.
    pub async fn bind_execute(&mut self, portal: &str, statement: &str) -> Vec<Frame> {
        let mut bind = Vec::new();
        bind.extend_from_slice(portal.as_bytes());
        bind.push(0);
        bind.extend_from_slice(statement.as_bytes());
        bind.push(0);
        bind.extend_from_slice(&0i16.to_be_bytes()); // no parameter formats
        bind.extend_from_slice(&0i16.to_be_bytes()); // no parameter values
        bind.extend_from_slice(&0i16.to_be_bytes()); // no result formats
        self.send_frame(b'B', &bind).await;

        let mut execute = Vec::new();
        execute.extend_from_slice(portal.as_bytes());
        execute.push(0);
        execute.extend_from_slice(&0i32.to_be_bytes()); // max_rows 0 = all
        self.send_frame(b'E', &execute).await;

        self.send_frame(b'S', &[]).await;
        self.read_until_ready().await
    }

    /// Send `Sync` alone, collecting the (usually empty) replies through
    /// `ReadyForQuery`.
    pub async fn sync(&mut self) -> Vec<Frame> {
        self.send_frame(b'S', &[]).await;
        self.read_until_ready().await
    }

    /// Send `Terminate` and wait for the server to close the connection,
    /// returning any frames it sent before the close (a clean close sends
    /// none).
    pub async fn terminate(mut self) -> Vec<Frame> {
        self.send_frame(b'X', &[]).await;
        let frames = Vec::new();
        let mut buf = [0u8; 64];
        match self.stream.read(&mut buf).await {
            Ok(0) => frames,
            Ok(_) => panic!("the server must not send bytes after Terminate"),
            Err(error) => panic!("the server must close cleanly after Terminate: {error}"),
        }
    }

    async fn read_until_ready(&mut self) -> Vec<Frame> {
        let mut frames = Vec::new();
        loop {
            let frame = self.read_frame().await;
            let ready = frame.tag == b'Z';
            frames.push(frame);
            if ready {
                return frames;
            }
        }
    }
}

/// The value of a comma-separated `key=value` SCRAM attribute.
fn field<'a>(message: &'a str, key: &str) -> Option<&'a str> {
    message
        .split(',')
        .find_map(|part| part.strip_prefix(&format!("{key}=")))
}

/// A random alphanumeric nonce (24 characters).
fn nonce(len: usize) -> String {
    const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    (0..len)
        .map(|_| ALPHABET[rand::random::<usize>() % ALPHABET.len()] as char)
        .collect()
}

/// Assert `frames` carries exactly one `ErrorResponse` with `sqlstate`, and
/// return its message.
pub fn assert_sqlstate(frames: &[Frame], sqlstate: &str) -> String {
    let errors: Vec<&Frame> = frames.iter().filter(|f| f.tag == b'E').collect();
    assert_eq!(
        errors.len(),
        1,
        "expected exactly one ErrorResponse, got {:?}",
        frames.iter().map(Frame::describe).collect::<Vec<_>>()
    );
    let error = errors[0];
    assert_eq!(
        error.sqlstate().as_deref(),
        Some(sqlstate),
        "wrong SQLSTATE: {}",
        error.describe()
    );
    error.error_message().unwrap_or_default()
}

/// The single `CommandComplete` tag among `frames`, asserting there is no
/// `ErrorResponse` at all.
pub fn command_tag(frames: &[Frame]) -> String {
    for frame in frames {
        if frame.tag == b'E' {
            panic!("unexpected error reply: {}", frame.describe());
        }
    }
    let tags: Vec<String> = frames.iter().filter_map(Frame::command_tag).collect();
    assert_eq!(
        tags.len(),
        1,
        "expected exactly one CommandComplete, got {:?}",
        frames.iter().map(Frame::describe).collect::<Vec<_>>()
    );
    tags.into_iter().next().unwrap()
}
