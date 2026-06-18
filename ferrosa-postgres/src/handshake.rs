//! Postgres SCRAM handshake state machine (sans-IO).
//!
//! Drives `StartupMessage → AuthenticationSASL → (SASLInitialResponse) →
//! AuthenticationSASLContinue → (SASLResponse) → AuthenticationSASLFinal +
//! AuthenticationOk + ReadyForQuery` using the SCRAM-SHA-256 exchange (D4).
//!
//! No I/O happens here: the caller feeds parsed frontend payloads and emits the
//! returned backend messages, so the whole auth flow is unit-testable without a
//! socket (harness layer H1). The connection/transport layer wires this to the
//! codec and the real `ferrosa-schema` role store later.

use crate::messages::{BackendMessage, StartupMessage};
use crate::scram::{self, ScramServerFirst, ScramVerifier};

/// The only mechanism offered/accepted in v1 (channel binding is Q4-deferred).
const MECHANISM: &str = "SCRAM-SHA-256";

/// Supplies the stored SCRAM verifier for a role (D4). Backed later by
/// `ferrosa-schema`'s role store; abstracted so the handshake stays pure.
pub trait VerifierStore {
    fn verifier(&self, user: &str) -> Option<ScramVerifier>;
}

/// A handshake failure (fail loud — never authenticate on doubt).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HandshakeError {
    /// No `user` parameter in the StartupMessage.
    MissingUser,
    /// The named role has no SCRAM verifier (cannot authenticate over Postgres).
    UnknownRole,
    /// Client offered a mechanism we do not support.
    UnsupportedMechanism,
    /// A message arrived out of order for the current phase.
    UnexpectedMessage,
    /// Underlying SCRAM failure (bad proof, malformed, channel binding).
    Scram(scram::ScramError),
}

enum Phase {
    Start,
    AwaitingInitial {
        verifier: ScramVerifier,
    },
    AwaitingFinal {
        verifier: ScramVerifier,
        ctx: ScramServerFirst,
    },
    Authenticated,
    Failed,
}

/// Sans-IO SCRAM handshake driver.
pub struct Handshake<'a, S: VerifierStore> {
    store: &'a S,
    server_nonce: String,
    phase: Phase,
}

impl<'a, S: VerifierStore> Handshake<'a, S> {
    /// Create a handshake. `server_nonce` is this server's nonce contribution
    /// (randomly generated per connection in production; injected in tests).
    pub fn new(store: &'a S, server_nonce: impl Into<String>) -> Self {
        Self {
            store,
            server_nonce: server_nonce.into(),
            phase: Phase::Start,
        }
    }

    /// Whether the client has successfully authenticated.
    pub fn is_authenticated(&self) -> bool {
        matches!(self.phase, Phase::Authenticated)
    }

    /// Handle the StartupMessage: resolve the role and offer SASL.
    pub fn on_startup(
        &mut self,
        startup: &StartupMessage,
    ) -> Result<Vec<BackendMessage>, HandshakeError> {
        if !matches!(self.phase, Phase::Start) {
            self.phase = Phase::Failed;
            return Err(HandshakeError::UnexpectedMessage);
        }
        let user = startup.get("user").ok_or(HandshakeError::MissingUser)?;
        // NOTE: returning UnknownRole here is a user-enumeration oracle; a
        // hardened version runs the exchange against a dummy verifier. Tracked
        // as a follow-up (threat-model).
        let verifier = self
            .store
            .verifier(user)
            .ok_or(HandshakeError::UnknownRole)?;
        self.phase = Phase::AwaitingInitial { verifier };
        Ok(vec![BackendMessage::AuthenticationSasl {
            mechanisms: vec![MECHANISM.to_string()],
        }])
    }

    /// Handle a SASL payload (SASLInitialResponse first, then SASLResponse).
    pub fn on_sasl(&mut self, data: &[u8]) -> Result<Vec<BackendMessage>, HandshakeError> {
        match std::mem::replace(&mut self.phase, Phase::Failed) {
            Phase::AwaitingInitial { verifier } => {
                let (mechanism, client_first) = parse_sasl_initial(data)?;
                if mechanism != MECHANISM {
                    return Err(HandshakeError::UnsupportedMechanism);
                }
                let ctx = scram::server_first(&client_first, &self.server_nonce, &verifier)
                    .map_err(HandshakeError::Scram)?;
                let cont = BackendMessage::AuthenticationSaslContinue {
                    data: ctx.server_first.clone().into_bytes(),
                };
                self.phase = Phase::AwaitingFinal { verifier, ctx };
                Ok(vec![cont])
            }
            Phase::AwaitingFinal { verifier, ctx } => {
                let client_final = std::str::from_utf8(data).map_err(|_| {
                    HandshakeError::Scram(scram::ScramError::Malformed("client-final not UTF-8"))
                })?;
                let server_final = scram::verify_client_final(&ctx, client_final, &verifier)
                    .map_err(HandshakeError::Scram)?;
                self.phase = Phase::Authenticated;
                // The connection layer appends ParameterStatus + BackendKeyData
                // + ReadyForQuery once authentication completes.
                Ok(vec![
                    BackendMessage::AuthenticationSaslFinal {
                        data: server_final.into_bytes(),
                    },
                    BackendMessage::AuthenticationOk,
                ])
            }
            _ => Err(HandshakeError::UnexpectedMessage),
        }
    }
}

/// Parse a SASLInitialResponse body: `mechanism\0` + i32(len) + client-first[len].
fn parse_sasl_initial(data: &[u8]) -> Result<(String, String), HandshakeError> {
    let malformed = |why: &'static str| HandshakeError::Scram(scram::ScramError::Malformed(why));
    let nul = data
        .iter()
        .position(|&b| b == 0)
        .ok_or_else(|| malformed("no mechanism terminator"))?;
    let mechanism = std::str::from_utf8(&data[..nul])
        .map_err(|_| malformed("mechanism not UTF-8"))?
        .to_string();
    let rest = &data[nul + 1..];
    if rest.len() < 4 {
        return Err(malformed("missing SASL length"));
    }
    let len = i32::from_be_bytes(rest[0..4].try_into().unwrap());
    let payload = &rest[4..];
    // len == -1 means "no initial data"; otherwise it must match exactly.
    if len >= 0 && len as usize != payload.len() {
        return Err(malformed("SASL length mismatch"));
    }
    let client_first = std::str::from_utf8(payload)
        .map_err(|_| malformed("client-first not UTF-8"))?
        .to_string();
    Ok((mechanism, client_first))
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::{engine::general_purpose::STANDARD, Engine as _};

    const SALT_B64: &str = "W22ZaJ0SNY7soEsUEjb6gQ==";
    const SERVER_NONCE: &str = "%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0";
    const CLIENT_FIRST: &str = "n,,n=user,r=rOprNGfwEbeRWgbNEkqO";
    const SERVER_FIRST: &str =
        "r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096";
    const CLIENT_FINAL: &str = "c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,p=dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ=";
    const SERVER_FINAL: &str = "v=6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4=";

    struct MockStore {
        user: String,
        verifier: ScramVerifier,
    }
    impl VerifierStore for MockStore {
        fn verifier(&self, user: &str) -> Option<ScramVerifier> {
            (user == self.user).then(|| self.verifier.clone())
        }
    }

    fn store(password: &str) -> MockStore {
        let salt = STANDARD.decode(SALT_B64).unwrap();
        MockStore {
            user: "user".into(),
            verifier: ScramVerifier::from_password(password, &salt, 4096),
        }
    }

    fn startup(user: &str) -> StartupMessage {
        StartupMessage {
            protocol_version: 196608,
            parameters: vec![
                ("user".into(), user.into()),
                ("database".into(), "ferrosa".into()),
            ],
        }
    }

    fn sasl_initial(mechanism: &str, client_first: &str) -> Vec<u8> {
        let mut v = Vec::new();
        v.extend_from_slice(mechanism.as_bytes());
        v.push(0);
        v.extend_from_slice(&(client_first.len() as i32).to_be_bytes());
        v.extend_from_slice(client_first.as_bytes());
        v
    }

    #[test]
    fn full_handshake_succeeds() {
        let s = store("pencil");
        let mut hs = Handshake::new(&s, SERVER_NONCE);

        let r1 = hs.on_startup(&startup("user")).unwrap();
        assert_eq!(
            r1,
            vec![BackendMessage::AuthenticationSasl {
                mechanisms: vec![MECHANISM.to_string()]
            }]
        );

        let r2 = hs.on_sasl(&sasl_initial(MECHANISM, CLIENT_FIRST)).unwrap();
        assert_eq!(
            r2,
            vec![BackendMessage::AuthenticationSaslContinue {
                data: SERVER_FIRST.as_bytes().to_vec()
            }]
        );

        let r3 = hs.on_sasl(CLIENT_FINAL.as_bytes()).unwrap();
        assert_eq!(
            r3,
            vec![
                BackendMessage::AuthenticationSaslFinal {
                    data: SERVER_FINAL.as_bytes().to_vec()
                },
                BackendMessage::AuthenticationOk,
            ]
        );
        assert!(hs.is_authenticated());
    }

    #[test]
    fn unknown_role_is_rejected() {
        let s = store("pencil");
        let mut hs = Handshake::new(&s, SERVER_NONCE);
        assert_eq!(
            hs.on_startup(&startup("nobody")),
            Err(HandshakeError::UnknownRole)
        );
    }

    #[test]
    fn missing_user_is_rejected() {
        let s = store("pencil");
        let mut hs = Handshake::new(&s, SERVER_NONCE);
        let su = StartupMessage {
            protocol_version: 196608,
            parameters: vec![],
        };
        assert_eq!(hs.on_startup(&su), Err(HandshakeError::MissingUser));
    }

    #[test]
    fn wrong_password_fails_auth_and_stays_unauthenticated() {
        let s = store("not-pencil");
        let mut hs = Handshake::new(&s, SERVER_NONCE);
        hs.on_startup(&startup("user")).unwrap();
        hs.on_sasl(&sasl_initial(MECHANISM, CLIENT_FIRST)).unwrap();
        assert_eq!(
            hs.on_sasl(CLIENT_FINAL.as_bytes()),
            Err(HandshakeError::Scram(scram::ScramError::ProofMismatch))
        );
        assert!(!hs.is_authenticated());
    }

    #[test]
    fn unsupported_mechanism_is_rejected() {
        let s = store("pencil");
        let mut hs = Handshake::new(&s, SERVER_NONCE);
        hs.on_startup(&startup("user")).unwrap();
        assert_eq!(
            hs.on_sasl(&sasl_initial("SCRAM-SHA-256-PLUS", CLIENT_FIRST)),
            Err(HandshakeError::UnsupportedMechanism)
        );
    }

    #[test]
    fn sasl_before_startup_is_rejected() {
        let s = store("pencil");
        let mut hs = Handshake::new(&s, SERVER_NONCE);
        assert_eq!(
            hs.on_sasl(&sasl_initial(MECHANISM, CLIENT_FIRST)),
            Err(HandshakeError::UnexpectedMessage)
        );
    }
}
