//! Sans-IO Postgres connection state machine.
//!
//! [`Connection`] owns the inbound byte buffer and drives the protocol phases —
//! startup (incl. `SSLRequest` negotiation), SCRAM authentication (delegated to
//! [`crate::handshake::Handshake`]), and the ready/query phase — turning received
//! bytes into bytes to send. It performs **no** I/O, so the whole connection
//! lifecycle is unit-testable with in-memory buffers; a thin tokio wrapper feeds
//! it from a `TcpStream`.

use bytes::{BufMut, BytesMut};

use crate::codec::{self, CodecError};
use crate::handshake::{Handshake, HandshakeError, VerifierStore};
use crate::messages::{BackendMessage, FrontendMessage, StartupFrame, TransactionStatus};

/// A connection-level failure (fail loud — caller should close the socket).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConnError {
    Codec(CodecError),
    Handshake(HandshakeError),
    /// A message arrived that is not valid for the current phase.
    Unexpected(&'static str),
    /// The server requires TLS (`[postgres] require_tls`) and the client sent
    /// its StartupMessage without first negotiating TLS via `SSLRequest`.
    TlsRequired,
}

/// How the connection answers `SSLRequest`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TlsPolicy {
    /// Answer `SSLRequest` with `S` (a TLS acceptor is configured). When
    /// false the server answers `N` and the session stays plaintext.
    pub offer: bool,
    /// Refuse a StartupMessage that was not preceded by a TLS handshake.
    pub require: bool,
}

impl TlsPolicy {
    /// No TLS: decline `SSLRequest`, accept plaintext startups.
    pub const PLAINTEXT: TlsPolicy = TlsPolicy {
        offer: false,
        require: false,
    };
}

impl From<CodecError> for ConnError {
    fn from(e: CodecError) -> Self {
        ConnError::Codec(e)
    }
}
impl From<HandshakeError> for ConnError {
    fn from(e: HandshakeError) -> Self {
        ConnError::Handshake(e)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    AwaitingStartup,
    /// `S` was sent in answer to `SSLRequest`; the caller must now run the TLS
    /// handshake and call [`Connection::on_tls_established`].
    AwaitingTls,
    Authenticating,
    Ready,
    Closed,
}

/// Sans-IO Postgres connection driver.
pub struct Connection<'a, S: VerifierStore> {
    inbuf: BytesMut,
    handshake: Handshake<'a, S>,
    phase: Phase,
    tls: TlsPolicy,
    tls_established: bool,
    /// `(process_id, secret_key)` advertised in BackendKeyData for cancellation.
    /// Placeholder `(0, 0)` until the cancel protocol is wired (follow-up).
    backend_key: (i32, i32),
}

impl<'a, S: VerifierStore> Connection<'a, S> {
    pub fn new(store: &'a S, server_nonce: impl Into<String>, tls: TlsPolicy) -> Self {
        Self {
            inbuf: BytesMut::new(),
            handshake: Handshake::new(store, server_nonce),
            phase: Phase::AwaitingStartup,
            tls,
            tls_established: false,
            backend_key: (0, 0),
        }
    }

    /// True after `S` was sent: the caller must run the server-side TLS
    /// handshake on the socket before feeding any more bytes.
    pub fn tls_upgrade_pending(&self) -> bool {
        self.phase == Phase::AwaitingTls
    }

    /// The caller completed the TLS handshake; subsequent bytes are the
    /// decrypted stream, starting with the client's StartupMessage.
    pub fn on_tls_established(&mut self) -> Result<(), ConnError> {
        if self.phase != Phase::AwaitingTls {
            return Err(ConnError::Unexpected(
                "TLS established without an SSLRequest",
            ));
        }
        self.tls_established = true;
        self.phase = Phase::AwaitingStartup;
        Ok(())
    }

    /// The authenticated session's authorization context (role + superuser
    /// flag), available once [`Connection::is_ready`].
    pub fn auth_context(&self) -> Option<&ferrosa_schema::AuthContext> {
        self.handshake.auth_context()
    }

    /// True once the client has authenticated and the server sent the first
    /// `ReadyForQuery`.
    pub fn is_ready(&self) -> bool {
        self.phase == Phase::Ready
    }

    /// True once the connection has terminated (client `Terminate`, cancel, or
    /// a fatal protocol error the caller should close on).
    pub fn is_closed(&self) -> bool {
        self.phase == Phase::Closed
    }

    /// Take ownership of any bytes still buffered after the handshake reached
    /// `Ready`, leaving the connection's buffer empty.
    ///
    /// A client may pipeline its first `Query` in the same TCP segment as the
    /// SASL final response; those bytes land in `inbuf` while the handshake
    /// completes. The server's post-auth loop seeds its own read buffer with
    /// this so a pipelined first query is not lost.
    pub fn take_inbuf(&mut self) -> BytesMut {
        std::mem::take(&mut self.inbuf)
    }

    /// Feed received bytes; return the bytes to write back (possibly empty when
    /// more input is needed). Drives as many complete frames as are buffered.
    pub fn on_bytes(&mut self, input: &[u8]) -> Result<Vec<u8>, ConnError> {
        self.inbuf.extend_from_slice(input);
        let mut out = BytesMut::new();

        loop {
            match self.phase {
                Phase::AwaitingStartup => match codec::read_startup(&mut self.inbuf)? {
                    None => break,
                    Some(StartupFrame::SslRequest) => {
                        if self.tls_established {
                            return Err(ConnError::Unexpected("SSLRequest inside a TLS session"));
                        }
                        if !self.tls.offer {
                            out.put_u8(b'N'); // no certificate configured: stay plaintext
                            continue;
                        }
                        // Anything the client sent after SSLRequest arrived
                        // in plaintext; processing it as if it were inside
                        // TLS would let a man-in-the-middle inject commands
                        // (the CVE-2021-23214 shape). Refuse it.
                        if !self.inbuf.is_empty() {
                            return Err(ConnError::Unexpected(
                                "data pipelined after SSLRequest before the TLS handshake",
                            ));
                        }
                        out.put_u8(b'S');
                        self.phase = Phase::AwaitingTls;
                        break;
                    }
                    Some(StartupFrame::CancelRequest { .. }) => {
                        self.phase = Phase::Closed;
                        break;
                    }
                    Some(StartupFrame::Startup(_)) if self.tls.require && !self.tls_established => {
                        self.phase = Phase::Closed;
                        return Err(ConnError::TlsRequired);
                    }
                    Some(StartupFrame::Startup(msg)) => {
                        for m in self.handshake.on_startup(&msg)? {
                            m.encode(&mut out);
                        }
                        self.phase = Phase::Authenticating;
                    }
                },
                // The caller must run the TLS handshake before feeding bytes.
                Phase::AwaitingTls => {
                    return Err(ConnError::Unexpected(
                        "bytes received before the TLS handshake completed",
                    ))
                }
                Phase::Authenticating => match codec::read_frontend(&mut self.inbuf)? {
                    None => break,
                    Some(FrontendMessage::SaslResponse { data }) => {
                        for m in self.handshake.on_sasl(&data)? {
                            m.encode(&mut out);
                        }
                        if self.handshake.is_authenticated() {
                            append_startup_complete(&mut out, self.backend_key);
                            self.phase = Phase::Ready;
                        }
                    }
                    Some(FrontendMessage::Terminate) => {
                        self.phase = Phase::Closed;
                        break;
                    }
                    Some(_) => return Err(ConnError::Unexpected("expected SASL response")),
                },
                Phase::Ready => match codec::read_frontend(&mut self.inbuf)? {
                    None => break,
                    Some(FrontendMessage::Query(_)) => {
                        // No relational engine yet (M1 pending). Fail loud with
                        // SQLSTATE 0A000 (feature_not_supported) rather than
                        // returning a fake empty result.
                        BackendMessage::ErrorResponse {
                            fields: vec![
                                (b'S', "ERROR".into()),
                                (b'C', "0A000".into()),
                                (b'M', "query execution not yet implemented".into()),
                            ],
                        }
                        .encode(&mut out);
                        BackendMessage::ReadyForQuery(TransactionStatus::Idle).encode(&mut out);
                    }
                    Some(FrontendMessage::Sync) => {
                        BackendMessage::ReadyForQuery(TransactionStatus::Idle).encode(&mut out);
                    }
                    Some(FrontendMessage::Terminate) => {
                        self.phase = Phase::Closed;
                        break;
                    }
                    Some(_) => { /* ignore unknown post-auth messages for now */ }
                },
                Phase::Closed => break,
            }
        }

        Ok(out.to_vec())
    }
}

/// Append the post-authentication startup-completion messages: the run-time
/// parameters drivers read on connect, the cancellation key, and the first
/// ReadyForQuery that signals the session is usable.
fn append_startup_complete(out: &mut BytesMut, backend_key: (i32, i32)) {
    const PARAMS: &[(&str, &str)] = &[
        ("server_version", "16.0 (ferrosa)"),
        ("server_encoding", "UTF8"),
        ("client_encoding", "UTF8"),
        ("DateStyle", "ISO, MDY"),
        ("TimeZone", "UTC"),
        ("integer_datetimes", "on"),
        ("standard_conforming_strings", "on"),
    ];
    for (name, value) in PARAMS {
        BackendMessage::ParameterStatus {
            name: (*name).to_string(),
            value: (*value).to_string(),
        }
        .encode(out);
    }
    BackendMessage::BackendKeyData {
        process_id: backend_key.0,
        secret_key: backend_key.1,
    }
    .encode(out);
    BackendMessage::ReadyForQuery(TransactionStatus::Idle).encode(out);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::messages::{StartupFrame, StartupMessage};
    use crate::scram::ScramVerifier;
    use base64::{engine::general_purpose::STANDARD, Engine as _};

    const SALT_B64: &str = "W22ZaJ0SNY7soEsUEjb6gQ==";
    const SERVER_NONCE: &str = "%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0";
    const CLIENT_FIRST: &str = "n,,n=user,r=rOprNGfwEbeRWgbNEkqO";
    const CLIENT_FINAL: &str = "c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,p=dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ=";

    struct MockStore(ScramVerifier);
    impl VerifierStore for MockStore {
        fn verifier(&self, user: &str) -> Option<ScramVerifier> {
            (user == "user").then(|| self.0.clone())
        }
        fn admit(&self, _user: &str) -> Result<(), String> {
            Ok(())
        }
        fn record_failure(&self, _user: &str) {}
        fn record_success(&self, user: &str) -> Result<ferrosa_schema::AuthContext, String> {
            Ok(ferrosa_schema::AuthContext {
                role: user.to_string(),
                is_superuser: false,
                must_change_password: false,
            })
        }
    }

    fn store() -> MockStore {
        let salt = STANDARD.decode(SALT_B64).unwrap();
        MockStore(ScramVerifier::from_password("pencil", &salt, 4096))
    }

    fn startup_frame(params: &[(&str, &str)]) -> Vec<u8> {
        let mut body = Vec::new();
        body.extend_from_slice(&196608i32.to_be_bytes());
        for (k, v) in params {
            body.extend_from_slice(k.as_bytes());
            body.push(0);
            body.extend_from_slice(v.as_bytes());
            body.push(0);
        }
        body.push(0);
        let mut frame = Vec::new();
        frame.extend_from_slice(&((body.len() + 4) as i32).to_be_bytes());
        frame.extend_from_slice(&body);
        frame
    }

    fn p_frame(body: &[u8]) -> Vec<u8> {
        let mut v = vec![b'p'];
        v.extend_from_slice(&((body.len() + 4) as i32).to_be_bytes());
        v.extend_from_slice(body);
        v
    }

    fn sasl_initial(client_first: &str) -> Vec<u8> {
        let mut body = Vec::new();
        body.extend_from_slice(b"SCRAM-SHA-256");
        body.push(0);
        body.extend_from_slice(&(client_first.len() as i32).to_be_bytes());
        body.extend_from_slice(client_first.as_bytes());
        p_frame(&body)
    }

    #[test]
    fn full_byte_level_handshake_reaches_ready() {
        let s = store();
        let mut conn = Connection::new(&s, SERVER_NONCE, TlsPolicy::PLAINTEXT);

        // startup -> AuthenticationSASL ('R')
        let out1 = conn
            .on_bytes(&startup_frame(&[("user", "user"), ("database", "ferrosa")]))
            .unwrap();
        assert_eq!(out1[0], b'R');
        assert_eq!(i32::from_be_bytes(out1[5..9].try_into().unwrap()), 10);
        assert!(!conn.is_ready());

        // SASLInitialResponse -> AuthenticationSASLContinue ('R', subtype 11)
        let out2 = conn.on_bytes(&sasl_initial(CLIENT_FIRST)).unwrap();
        assert_eq!(out2[0], b'R');
        assert_eq!(i32::from_be_bytes(out2[5..9].try_into().unwrap()), 11);

        // SASLResponse(client-final) -> SASLFinal + Ok + ReadyForQuery
        let out3 = conn.on_bytes(&p_frame(CLIENT_FINAL.as_bytes())).unwrap();
        // ends with ReadyForQuery: 'Z', len=5, 'I'
        assert_eq!(&out3[out3.len() - 6..], &[b'Z', 0, 0, 0, 5, b'I']);
        assert!(conn.is_ready());
    }

    fn ssl_request() -> Vec<u8> {
        let mut ssl = Vec::new();
        ssl.extend_from_slice(&8i32.to_be_bytes());
        ssl.extend_from_slice(&80877103i32.to_be_bytes());
        ssl
    }

    const OFFER: TlsPolicy = TlsPolicy {
        offer: true,
        require: false,
    };
    const REQUIRE: TlsPolicy = TlsPolicy {
        offer: true,
        require: true,
    };

    #[test]
    fn ssl_request_is_declined_with_single_byte() {
        let s = store();
        let mut conn = Connection::new(&s, SERVER_NONCE, TlsPolicy::PLAINTEXT);
        assert_eq!(conn.on_bytes(&ssl_request()).unwrap(), vec![b'N']);
        assert!(!conn.is_ready() && !conn.is_closed());
        // then a real startup still works
        let out = conn.on_bytes(&startup_frame(&[("user", "user")])).unwrap();
        assert_eq!(out[0], b'R');
    }

    #[test]
    fn ssl_request_is_accepted_with_s_when_tls_is_offered() {
        let s = store();
        let mut conn = Connection::new(&s, SERVER_NONCE, OFFER);
        assert_eq!(conn.on_bytes(&ssl_request()).unwrap(), vec![b'S']);
        assert!(
            conn.tls_upgrade_pending(),
            "caller must now run the TLS handshake"
        );
        conn.on_tls_established().unwrap();
        assert!(!conn.tls_upgrade_pending());
        let out = conn.on_bytes(&startup_frame(&[("user", "user")])).unwrap();
        assert_eq!(out[0], b'R', "startup proceeds inside TLS");
    }

    #[test]
    fn bytes_pipelined_after_ssl_request_are_refused() {
        // Plaintext bytes sent before the TLS handshake would otherwise be
        // processed as if they arrived inside TLS (CVE-2021-23214 shape).
        let s = store();
        let mut conn = Connection::new(&s, SERVER_NONCE, OFFER);
        let mut bytes = ssl_request();
        bytes.extend_from_slice(&startup_frame(&[("user", "user")]));
        assert!(matches!(
            conn.on_bytes(&bytes),
            Err(ConnError::Unexpected(_))
        ));
    }

    #[test]
    fn plaintext_startup_is_refused_when_tls_is_required() {
        let s = store();
        let mut conn = Connection::new(&s, SERVER_NONCE, REQUIRE);
        assert_eq!(
            conn.on_bytes(&startup_frame(&[("user", "user")])),
            Err(ConnError::TlsRequired)
        );
        assert!(!conn.is_ready());
    }

    #[test]
    fn tls_startup_is_accepted_when_tls_is_required() {
        let s = store();
        let mut conn = Connection::new(&s, SERVER_NONCE, REQUIRE);
        assert_eq!(conn.on_bytes(&ssl_request()).unwrap(), vec![b'S']);
        conn.on_tls_established().unwrap();
        let out = conn.on_bytes(&startup_frame(&[("user", "user")])).unwrap();
        assert_eq!(out[0], b'R');
    }

    #[test]
    fn a_second_ssl_request_inside_tls_is_refused() {
        let s = store();
        let mut conn = Connection::new(&s, SERVER_NONCE, OFFER);
        conn.on_bytes(&ssl_request()).unwrap();
        conn.on_tls_established().unwrap();
        assert!(matches!(
            conn.on_bytes(&ssl_request()),
            Err(ConnError::Unexpected(_))
        ));
    }

    #[test]
    fn partial_startup_frame_buffers_until_complete() {
        let s = store();
        let mut conn = Connection::new(&s, SERVER_NONCE, TlsPolicy::PLAINTEXT);
        let frame = startup_frame(&[("user", "user")]);
        let (head, tail) = frame.split_at(frame.len() - 3);
        assert_eq!(conn.on_bytes(head).unwrap(), Vec::<u8>::new()); // nothing yet
        let out = conn.on_bytes(tail).unwrap();
        assert_eq!(out[0], b'R'); // AuthenticationSASL once complete
    }

    #[test]
    fn query_before_engine_fails_loud_then_ready() {
        let s = store();
        let mut conn = Connection::new(&s, SERVER_NONCE, TlsPolicy::PLAINTEXT);
        conn.on_bytes(&startup_frame(&[("user", "user")])).unwrap();
        conn.on_bytes(&sasl_initial(CLIENT_FIRST)).unwrap();
        conn.on_bytes(&p_frame(CLIENT_FINAL.as_bytes())).unwrap();
        assert!(conn.is_ready());

        // simple Query 'Q'
        let mut q = vec![b'Q'];
        let body = b"SELECT 1\x00";
        q.extend_from_slice(&((body.len() + 4) as i32).to_be_bytes());
        q.extend_from_slice(body);
        let out = conn.on_bytes(&q).unwrap();
        assert_eq!(out[0], b'E'); // ErrorResponse (feature not supported)
        assert_eq!(&out[out.len() - 6..], &[b'Z', 0, 0, 0, 5, b'I']); // then ReadyForQuery
    }

    #[test]
    fn bad_password_surfaces_handshake_error() {
        let salt = STANDARD.decode(SALT_B64).unwrap();
        let s = MockStore(ScramVerifier::from_password("wrong", &salt, 4096));
        let mut conn = Connection::new(&s, SERVER_NONCE, TlsPolicy::PLAINTEXT);
        conn.on_bytes(&startup_frame(&[("user", "user")])).unwrap();
        conn.on_bytes(&sasl_initial(CLIENT_FIRST)).unwrap();
        let err = conn
            .on_bytes(&p_frame(CLIENT_FINAL.as_bytes()))
            .unwrap_err();
        assert!(matches!(err, ConnError::Handshake(_)));
        assert!(!conn.is_ready());
    }

    #[test]
    fn startup_frame_parses_via_codec() {
        // guard: our test frame builder matches the codec's parser
        let mut buf = BytesMut::from(&startup_frame(&[("user", "u")])[..]);
        match codec::read_startup(&mut buf).unwrap().unwrap() {
            StartupFrame::Startup(StartupMessage { parameters, .. }) => {
                assert_eq!(parameters[0], ("user".into(), "u".into()));
            }
            other => panic!("unexpected {other:?}"),
        }
    }
}
