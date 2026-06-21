use std::net::SocketAddr;
use std::time::Duration;

use bytes::{BufMut, BytesMut};
use ferrosa_cql::frame::{FrameHeader, Opcode, HEADER_SIZE};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};
use uuid::Uuid;

const FRAME_TIMEOUT: Duration = Duration::from_secs(5);

struct RawFrame {
    opcode: Opcode,
    body: Vec<u8>,
}

fn encode_startup_frame() -> BytesMut {
    let mut body = BytesMut::new();
    body.put_u16(1);
    body.put_u16(b"CQL_VERSION".len() as u16);
    body.put_slice(b"CQL_VERSION");
    body.put_u16(b"3.0.0".len() as u16);
    body.put_slice(b"3.0.0");

    let header = FrameHeader {
        version: 0x04,
        flags: 0,
        stream_id: 0,
        opcode: Opcode::Startup,
        length: body.len() as u32,
    };
    let mut buf = BytesMut::new();
    header.encode(&mut buf);
    buf.extend_from_slice(&body);
    buf
}

async fn read_frame(stream: &mut TcpStream) -> RawFrame {
    let mut hdr_buf = vec![0u8; HEADER_SIZE];
    timeout(FRAME_TIMEOUT, stream.read_exact(&mut hdr_buf))
        .await
        .expect("timed out waiting for CQL frame header")
        .expect("failed to read CQL frame header");
    let header = FrameHeader::decode(&hdr_buf).expect("decode CQL frame header");
    let mut body = vec![0u8; header.length as usize];
    if !body.is_empty() {
        timeout(FRAME_TIMEOUT, stream.read_exact(&mut body))
            .await
            .expect("timed out waiting for CQL frame body")
            .expect("failed to read CQL frame body");
    }
    RawFrame {
        opcode: header.opcode,
        body,
    }
}

async fn connect_auth_disabled(addr: SocketAddr) -> TcpStream {
    let mut stream = TcpStream::connect(addr).await.expect("connect to CQL node");
    let startup = encode_startup_frame();
    stream.write_all(&startup).await.expect("send STARTUP");
    let resp = read_frame(&mut stream).await;
    assert_eq!(resp.opcode, Opcode::Ready, "expected READY from {addr:?}");
    stream
}

fn encode_query_body(query: &str) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&(query.len() as i32).to_be_bytes());
    body.extend_from_slice(query.as_bytes());
    body.extend_from_slice(&1u16.to_be_bytes()); // Consistency ONE.
    body.push(0); // no flags
    body
}

async fn execute_query(stream: &mut TcpStream, query: &str) -> RawFrame {
    let body = encode_query_body(query);
    let header = FrameHeader {
        version: 0x04,
        flags: 0,
        stream_id: 0,
        opcode: Opcode::Query,
        length: body.len() as u32,
    };
    let mut buf = BytesMut::new();
    header.encode(&mut buf);
    buf.extend_from_slice(&body);
    stream.write_all(&buf).await.expect("send QUERY");
    let resp = read_frame(stream).await;
    if resp.opcode == Opcode::Error {
        panic!("query failed: {query}: {}", decode_error(&resp.body));
    }
    assert_eq!(resp.opcode, Opcode::Result, "expected RESULT for {query}");
    resp
}

fn decode_error(body: &[u8]) -> String {
    if body.len() < 6 {
        return format!("malformed error body: {body:?}");
    }
    let code = i32::from_be_bytes(body[0..4].try_into().unwrap());
    let len = u16::from_be_bytes(body[4..6].try_into().unwrap()) as usize;
    let msg = body
        .get(6..6 + len)
        .and_then(|b| std::str::from_utf8(b).ok())
        .unwrap_or("<invalid utf8>");
    format!("0x{code:04x}: {msg}")
}

fn read_u16(body: &[u8], pos: &mut usize) -> u16 {
    let value = u16::from_be_bytes(body[*pos..*pos + 2].try_into().unwrap());
    *pos += 2;
    value
}

fn read_i32(body: &[u8], pos: &mut usize) -> i32 {
    let value = i32::from_be_bytes(body[*pos..*pos + 4].try_into().unwrap());
    *pos += 4;
    value
}

fn skip_string(body: &[u8], pos: &mut usize) {
    let len = read_u16(body, pos) as usize;
    *pos += len;
}

fn skip_bytes(body: &[u8], pos: &mut usize) {
    let len = read_i32(body, pos);
    if len >= 0 {
        *pos += len as usize;
    }
}

fn result_row_count(body: &[u8]) -> usize {
    let mut pos = 0;
    let kind = read_i32(body, &mut pos);
    if kind == 0x0001 {
        return 0; // Void.
    }
    assert_eq!(kind, 0x0002, "expected Rows result kind, got {kind:#x}");

    let flags = read_i32(body, &mut pos);
    let column_count = read_i32(body, &mut pos) as usize;
    if flags & 0x0002 != 0 {
        skip_bytes(body, &mut pos); // has_more_pages paging_state
    }
    let global_table_spec = flags & 0x0001 != 0;
    if global_table_spec {
        skip_string(body, &mut pos); // keyspace
        skip_string(body, &mut pos); // table
    }
    for _ in 0..column_count {
        if !global_table_spec {
            skip_string(body, &mut pos);
            skip_string(body, &mut pos);
        }
        skip_string(body, &mut pos); // column name
        let _ty = read_u16(body, &mut pos); // simple type id; test queries use uuid/text/int only.
    }
    read_i32(body, &mut pos) as usize
}

fn cluster_ports() -> Vec<u16> {
    std::env::var("FERROSA_TEST_CQL_PORTS")
        .ok()
        .map(|ports| {
            ports
                .split(',')
                .map(|p| p.trim().parse().expect("valid CQL port"))
                .collect()
        })
        .unwrap_or_else(|| vec![9042, 9043, 9044])
}

#[tokio::test]
#[ignore = "requires scripts/test-cluster-up-ci.sh 3-node cluster; CI runs ignored cluster tests"]
async fn fts_match_returns_flushed_row_from_each_live_cluster_node() {
    if std::env::var("FERROSA_TEST_CONTAINERS").is_err() {
        panic!("set FERROSA_TEST_CONTAINERS=1 and start the 3-node test cluster first");
    }

    let ports = cluster_ports();
    assert!(ports.len() >= 3, "test requires at least three CQL ports");
    let addrs: Vec<SocketAddr> = ports
        .iter()
        .map(|port| SocketAddr::from(([127, 0, 0, 1], *port)))
        .collect();

    let token = format!("ftslive{}", Uuid::new_v4().simple());
    let keyspace = format!("ftsprobe_{}", Uuid::new_v4().simple());
    let tenant = Uuid::new_v4();
    let row_id = Uuid::new_v4();

    let mut setup = connect_auth_disabled(addrs[0]).await;
    execute_query(
        &mut setup,
        &format!(
            "CREATE KEYSPACE {keyspace} WITH replication = {{'class':'NetworkTopologyStrategy','datacenter1':'3'}}"
        ),
    )
    .await;
    execute_query(
        &mut setup,
        &format!(
            "CREATE TABLE {keyspace}.t (tenant_id uuid, id uuid, body text, PRIMARY KEY ((tenant_id), id))"
        ),
    )
    .await;
    execute_query(
        &mut setup,
        &format!("CREATE INDEX ON {keyspace}.t (body) USING 'fulltext'"),
    )
    .await;
    execute_query(
        &mut setup,
        &format!(
            "INSERT INTO {keyspace}.t (tenant_id, id, body) VALUES ({tenant}, {row_id}, '{token} native fts probe body')"
        ),
    )
    .await;

    // Let DDL/write replication and per-SSTable FTI sidecars settle. The bug this
    // protects against flapped after flush/rebuild; repeated checks across every
    // coordinator catch coordinator-local and transient-empty behavior.
    sleep(Duration::from_secs(8)).await;

    for round in 0..5 {
        for addr in &addrs {
            let mut node = connect_auth_disabled(*addr).await;
            let resp = execute_query(
                &mut node,
                &format!(
                    "SELECT id FROM {keyspace}.t WHERE tenant_id = {tenant} AND body = fts_match('{token}') LIMIT 5 ALLOW FILTERING"
                ),
            )
            .await;
            let rows = result_row_count(&resp.body);
            assert_eq!(
                rows, 1,
                "round {round}: coordinator {addr} returned {rows} rows for stable fts_match token {token}"
            );
        }
        sleep(Duration::from_secs(2)).await;
    }
}
