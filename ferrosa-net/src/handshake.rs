use bytes::BytesMut;
use hmac::{Hmac, Mac};
use sha2::Sha256;
use tokio_util::codec::Framed;
use uuid::Uuid;

use crate::codec::{Frame, FrameHeader, InternodeCodec, Lane, MsgType};
use crate::config::NetConfig;
use crate::error::{NetError, Result};
use crate::message::Message;

type HmacSha256 = Hmac<Sha256>;

/// Current protocol version.
pub const PROTOCOL_VERSION: u8 = 1;

/// Compute PSK auth token: HMAC-SHA256(key=psk, data=cluster_name|host_id|nonce).
pub fn compute_auth_token(cluster_name: &str, host_id: &Uuid, nonce: u64, psk: &str) -> Vec<u8> {
    let mut mac = HmacSha256::new_from_slice(psk.as_bytes()).expect("HMAC accepts any key length");
    mac.update(cluster_name.as_bytes());
    mac.update(host_id.as_bytes());
    mac.update(&nonce.to_be_bytes());
    mac.finalize().into_bytes().to_vec()
}

/// Verify a received auth token using the HMAC crate's constant-time verification.
pub fn verify_auth_token(
    cluster_name: &str,
    host_id: &Uuid,
    nonce: u64,
    psk: &str,
    token: &[u8],
) -> bool {
    let mut mac = HmacSha256::new_from_slice(psk.as_bytes()).expect("HMAC accepts any key length");
    mac.update(cluster_name.as_bytes());
    mac.update(host_id.as_bytes());
    mac.update(&nonce.to_be_bytes());
    mac.verify_slice(token).is_ok()
}

/// Run initiator side of handshake over a framed connection.
pub async fn initiate_handshake<T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin>(
    framed: &mut Framed<T, InternodeCodec>,
    config: &NetConfig,
    local_host_id: Uuid,
) -> Result<Uuid> {
    use futures::{SinkExt, StreamExt};

    let nonce: u64 = rand::random();
    let auth_token = match &config.psk {
        Some(psk) => {
            let hmac = compute_auth_token(&config.cluster_name, &local_host_id, nonce, psk);
            let mut token = nonce.to_be_bytes().to_vec();
            token.extend_from_slice(&hmac);
            token
        }
        None => vec![],
    };

    let handshake = Message::Handshake {
        cluster_name: config.cluster_name.clone(),
        host_id: local_host_id,
        protocol_version: PROTOCOL_VERSION,
        supported_compression: vec![0],
        auth_token,
    };
    let mut body = BytesMut::new();
    handshake.encode(&mut body)?;
    let frame = Frame {
        header: FrameHeader::new(MsgType::Handshake, Lane::Raft, 0, body.len() as u32),
        body: body.freeze(),
    };
    framed.send(frame).await?;

    let ack_frame = framed
        .next()
        .await
        .ok_or_else(|| NetError::HandshakeFailed("connection closed".into()))??;
    let ack = Message::decode(ack_frame.header.msg_type, &mut ack_frame.body.clone())?;

    match ack {
        Message::HandshakeAck {
            host_id,
            accepted,
            reason,
            ..
        } => {
            if accepted {
                Ok(host_id)
            } else {
                Err(NetError::HandshakeFailed(reason))
            }
        }
        _ => Err(NetError::Protocol("expected HandshakeAck".into())),
    }
}

/// Run acceptor side of handshake over a framed connection.
pub async fn accept_handshake<T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin>(
    framed: &mut Framed<T, InternodeCodec>,
    config: &NetConfig,
    local_host_id: Uuid,
) -> Result<Uuid> {
    use futures::StreamExt;

    let hs_frame = framed
        .next()
        .await
        .ok_or_else(|| NetError::HandshakeFailed("connection closed".into()))??;
    let hs = Message::decode(hs_frame.header.msg_type, &mut hs_frame.body.clone())?;

    let (peer_host_id, peer_cluster, peer_version, peer_token) = match hs {
        Message::Handshake {
            cluster_name,
            host_id,
            protocol_version,
            auth_token,
            ..
        } => (host_id, cluster_name, protocol_version, auth_token),
        _ => return Err(NetError::Protocol("expected Handshake".into())),
    };

    if peer_cluster != config.cluster_name {
        let reason = format!(
            "cluster mismatch: expected '{}', got '{}'",
            config.cluster_name, peer_cluster
        );
        send_handshake_ack(framed, local_host_id, false, &reason).await?;
        return Err(NetError::HandshakeFailed(reason));
    }

    if peer_version != PROTOCOL_VERSION {
        let reason = format!("unsupported protocol version: {}", peer_version);
        send_handshake_ack(framed, local_host_id, false, &reason).await?;
        return Err(NetError::HandshakeFailed(reason));
    }

    if let Some(psk) = &config.psk {
        if peer_token.len() < 40 {
            let reason = "auth token too short".to_string();
            send_handshake_ack(framed, local_host_id, false, &reason).await?;
            return Err(NetError::HandshakeFailed(reason));
        }
        let nonce = u64::from_be_bytes(peer_token[..8].try_into().unwrap());
        if !verify_auth_token(
            &config.cluster_name,
            &peer_host_id,
            nonce,
            psk,
            &peer_token[8..],
        ) {
            let reason = "PSK authentication failed".to_string();
            send_handshake_ack(framed, local_host_id, false, &reason).await?;
            return Err(NetError::HandshakeFailed(reason));
        }
    }

    send_handshake_ack(framed, local_host_id, true, "").await?;
    Ok(peer_host_id)
}

async fn send_handshake_ack<T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin>(
    framed: &mut Framed<T, InternodeCodec>,
    host_id: Uuid,
    accepted: bool,
    reason: &str,
) -> Result<()> {
    use futures::SinkExt;
    let ack = Message::HandshakeAck {
        host_id,
        protocol_version: PROTOCOL_VERSION,
        chosen_compression: 0,
        accepted,
        reason: reason.to_string(),
    };
    let mut body = BytesMut::new();
    ack.encode(&mut body)?;
    let frame = Frame {
        header: FrameHeader::new(MsgType::HandshakeAck, Lane::Raft, 0, body.len() as u32),
        body: body.freeze(),
    };
    framed.send(frame).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::duplex;
    use tokio_util::codec::Framed;

    use crate::codec::InternodeCodec;
    use crate::config::NetConfig;

    fn test_config(cluster: &str, psk: Option<&str>) -> NetConfig {
        NetConfig {
            cluster_name: cluster.to_string(),
            psk: psk.map(|s| s.to_string()),
            ..NetConfig::default()
        }
    }

    #[tokio::test]
    async fn handshake_success_with_psk() {
        let (client_io, server_io) = duplex(8192);
        let config = test_config("ferrosa", Some("secret"));
        let client_id = Uuid::new_v4();
        let server_id = Uuid::new_v4();

        let mut client_framed =
            Framed::new(client_io, InternodeCodec::new(config.max_frame_body_size));
        let mut server_framed =
            Framed::new(server_io, InternodeCodec::new(config.max_frame_body_size));

        let client_fut = initiate_handshake(&mut client_framed, &config, client_id);
        let server_fut = accept_handshake(&mut server_framed, &config, server_id);

        let (client_res, server_res) = tokio::join!(client_fut, server_fut);
        assert_eq!(client_res.unwrap(), server_id);
        assert_eq!(server_res.unwrap(), client_id);
    }

    #[tokio::test]
    async fn handshake_rejects_cluster_name_mismatch() {
        let (client_io, server_io) = duplex(8192);
        let client_config = test_config("ferrosa", None);
        let server_config = test_config("other", None);

        let mut client_framed = Framed::new(
            client_io,
            InternodeCodec::new(client_config.max_frame_body_size),
        );
        let mut server_framed = Framed::new(
            server_io,
            InternodeCodec::new(server_config.max_frame_body_size),
        );

        let client_fut = initiate_handshake(&mut client_framed, &client_config, Uuid::new_v4());
        let server_fut = accept_handshake(&mut server_framed, &server_config, Uuid::new_v4());

        let (client_res, _server_res) = tokio::join!(client_fut, server_fut);
        assert!(matches!(client_res, Err(NetError::HandshakeFailed(_))));
    }

    #[tokio::test]
    async fn handshake_rejects_bad_psk() {
        let (client_io, server_io) = duplex(8192);
        let client_config = test_config("ferrosa", Some("secret1"));
        let server_config = test_config("ferrosa", Some("secret2"));

        let mut client_framed = Framed::new(
            client_io,
            InternodeCodec::new(client_config.max_frame_body_size),
        );
        let mut server_framed = Framed::new(
            server_io,
            InternodeCodec::new(server_config.max_frame_body_size),
        );

        let client_fut = initiate_handshake(&mut client_framed, &client_config, Uuid::new_v4());
        let server_fut = accept_handshake(&mut server_framed, &server_config, Uuid::new_v4());

        let (client_res, _server_res) = tokio::join!(client_fut, server_fut);
        assert!(matches!(client_res, Err(NetError::HandshakeFailed(_))));
    }

    #[tokio::test]
    async fn handshake_succeeds_without_psk() {
        let (client_io, server_io) = duplex(8192);
        let config = test_config("ferrosa", None);
        let client_id = Uuid::new_v4();
        let server_id = Uuid::new_v4();

        let mut client_framed =
            Framed::new(client_io, InternodeCodec::new(config.max_frame_body_size));
        let mut server_framed =
            Framed::new(server_io, InternodeCodec::new(config.max_frame_body_size));

        let client_fut = initiate_handshake(&mut client_framed, &config, client_id);
        let server_fut = accept_handshake(&mut server_framed, &config, server_id);

        let (client_res, server_res) = tokio::join!(client_fut, server_fut);
        assert_eq!(client_res.unwrap(), server_id);
        assert_eq!(server_res.unwrap(), client_id);
    }

    #[tokio::test]
    async fn handshake_rejects_protocol_version_mismatch() {
        let (client_io, server_io) = duplex(8192);
        let config = test_config("ferrosa", None);

        let mut client_framed =
            Framed::new(client_io, InternodeCodec::new(config.max_frame_body_size));
        let bad_handshake = Message::Handshake {
            cluster_name: "ferrosa".to_string(),
            host_id: Uuid::new_v4(),
            protocol_version: 0,
            supported_compression: vec![0],
            auth_token: vec![],
        };
        use futures::SinkExt;
        let mut body = BytesMut::new();
        bad_handshake.encode(&mut body).unwrap();
        let frame = Frame {
            header: FrameHeader::new(MsgType::Handshake, Lane::Raft, 0, body.len() as u32),
            body: body.freeze(),
        };
        client_framed.send(frame).await.unwrap();

        let mut server_framed =
            Framed::new(server_io, InternodeCodec::new(config.max_frame_body_size));
        let server_fut = accept_handshake(&mut server_framed, &config, Uuid::new_v4());
        assert!(matches!(
            server_fut.await,
            Err(NetError::HandshakeFailed(_))
        ));
    }

    #[test]
    fn compute_auth_token_deterministic() {
        let host_id = Uuid::new_v4();
        let token1 = compute_auth_token("ferrosa", &host_id, 42, "secret");
        let token2 = compute_auth_token("ferrosa", &host_id, 42, "secret");
        assert_eq!(token1, token2);
    }

    #[test]
    fn compute_auth_token_differs_with_different_nonce() {
        let host_id = Uuid::new_v4();
        let token1 = compute_auth_token("ferrosa", &host_id, 1, "secret");
        let token2 = compute_auth_token("ferrosa", &host_id, 2, "secret");
        assert_ne!(token1, token2);
    }
}
