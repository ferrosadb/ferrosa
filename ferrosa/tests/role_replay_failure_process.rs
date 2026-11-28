//! A node whose persisted roles cannot be read must refuse to start, and must
//! never seed over the auth store it could not read (t_95557f4c).
//!
//! Before the fix, `main` logged "failed to replay persisted roles" at WARN and
//! ran the seed-role bootstrap anyway. With the persisted roles missing from the
//! registry, `ferrosa_admin` looked absent, so the bootstrap created it again
//! with `FERROSA_SEED_ADMIN_PASSWORD` or, when that was unset, the PUBLISHED
//! default. One unreadable role row made the node serve a well-known superuser
//! credential and refuse the operator's. Seeded roles were also never written
//! to `system_auth.roles`, so a crash before the next `schema.json` write left
//! nothing and the next start seeded again from whatever env it carried.
//!
//! These tests run the real binary (`CARGO_BIN_EXE_ferrosa`) with CQL auth on,
//! on a throwaway data dir and loopback ports, and log in over the PostgreSQL
//! wire (SCRAM against the same role registry CQL uses). The replay failure is
//! a real one: a `system_auth.roles` row whose `is_superuser` cell is not a
//! valid boolean, written into the stopped node's data dir through the storage
//! engine, which the loader's fail-loud decode rejects at startup.

use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

use ferrosa_common::CellValue;
use ferrosa_schema::system::persistence::{role_to_row, ROLES_COL_IS_SUPERUSER};
use ferrosa_schema::{PasswordHasher, RoleMetadata};
use ferrosa_storage::engine::StorageEngineConfig;
use ferrosa_storage::{CommitLogConfig, CompactionConfig, StorageEngine, TableId};
use tempfile::TempDir;
use tokio_postgres::config::SslMode;
use tokio_postgres::{Config, NoTls};

mod common;
use common::ports::PortAllocator;

/// How long a node may take to boot or to exit.
const DEADLINE: Duration = Duration::from_secs(120);

/// The log line a node writes once every listener is up.
const READY: &str = "ferrosa ready; waiting for SIGINT or SIGTERM";

/// The exit code a node uses when its persisted roles cannot be read.
const EXIT_AUTH_STORE_UNREADABLE: i32 = 65;

/// The operator's admin password for these throwaway nodes. Test-only.
const OPERATOR_ADMIN_PASSWORD: &str = "operator-chosen-admin-95557f4c"; // pragma: allowlist secret

/// The published default `ferrosa_admin` password.
const PUBLISHED_DEFAULT: &str = ferrosa_schema::auth::bootstrap::SEED_ADMIN_PASSWORD;

const ADMIN: &str = ferrosa_schema::auth::bootstrap::SEED_ADMIN_USER;

/// The role whose persisted row is made undecodable.
const CORRUPT_ROLE: &str = "corrupt_role_95557f4c";

struct Node {
    child: Child,
    log_dir: PathBuf,
    pg_port: u16,
}

impl Node {
    /// Start the binary on `data_dir` with auth on. `seed_admin` is
    /// `FERROSA_SEED_ADMIN_PASSWORD`; `None` leaves it unset.
    fn start(root: &Path, data_dir: &Path, name: &str, seed_admin: Option<&str>) -> Self {
        let config = root.join("ferrosa.toml");
        if !config.exists() {
            std::fs::write(&config, "").expect("write empty config");
        }
        let log_dir = root.join(format!("logs-{name}"));
        let pg_port = PortAllocator::new().get();
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_ferrosa"));
        cmd.env_clear()
            .env("PATH", std::env::var_os("PATH").unwrap_or_default())
            .env("HOME", root)
            .env("RUST_LOG", "info")
            .env("RUST_MIN_STACK", "33554432")
            .env("FERROSA_AUTH_ENABLED", "true")
            .env("FERROSA_CONFIG", &config)
            .env("FERROSA_DATA_DIR", data_dir)
            .env("FERROSA_LOG_DIR", &log_dir)
            .env("FERROSA_WEB_BIND", "127.0.0.1:0")
            .env("FERROSA_CQL_BIND", "127.0.0.1:0")
            .env("FERROSA_GRAPH_BIND", "127.0.0.1:0")
            .env("FERROSA_BOLT_PORT", "0")
            .env("FERROSA_SPARQL_BIND", "127.0.0.1:0")
            .env("FERROSA_POSTGRES_BIND", format!("127.0.0.1:{pg_port}"))
            .env("FERROSA_FLIGHT_BIND", "127.0.0.1:0")
            .env("FERROSA_INTERNODE_BIND", "127.0.0.1:0")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped());
        if let Some(password) = seed_admin {
            cmd.env("FERROSA_SEED_ADMIN_PASSWORD", password);
        }
        let child = cmd.spawn().expect("spawn the ferrosa binary");
        Self {
            child,
            log_dir,
            pg_port,
        }
    }

    fn log(&self) -> String {
        let path = self.log_dir.join("ferrosa.log");
        match std::fs::read_to_string(&path) {
            Ok(text) => text,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => String::new(),
            Err(e) => panic!("read {}: {e}", path.display()),
        }
    }

    fn stderr(&mut self) -> String {
        use std::io::Read;
        let mut out = String::new();
        if let Some(mut pipe) = self.child.stderr.take() {
            pipe.read_to_string(&mut out).expect("read stderr");
        }
        out
    }

    /// Wait for the process to exit; panic past the deadline.
    fn wait(&mut self) -> ExitStatus {
        let started = Instant::now();
        loop {
            if let Some(status) = self.child.try_wait().expect("try_wait") {
                return status;
            }
            assert!(
                started.elapsed() < DEADLINE,
                "ferrosa did not exit within {DEADLINE:?}; log:\n{}",
                self.log()
            );
            std::thread::sleep(Duration::from_millis(50));
        }
    }

    /// Wait until the node is ready (`Ok`) or exits first (`Err(status)`).
    fn ready_or_exit(&mut self) -> Result<(), ExitStatus> {
        let started = Instant::now();
        while !self.log().contains(READY) {
            if let Some(status) = self.child.try_wait().expect("try_wait") {
                return Err(status);
            }
            assert!(
                started.elapsed() < DEADLINE,
                "neither ready nor exited within {DEADLINE:?}; log:\n{}",
                self.log()
            );
            std::thread::sleep(Duration::from_millis(100));
        }
        Ok(())
    }

    fn wait_ready(&mut self) {
        if let Err(status) = self.ready_or_exit() {
            panic!(
                "ferrosa exited ({status}) before it was ready; stderr:\n{}\nlog:\n{}",
                self.stderr(),
                self.log()
            );
        }
    }

    /// SIGKILL: the crash shape. No shutdown path runs, so nothing is written
    /// on the way down (no `schema.json`, no memtable flush).
    fn crash(&mut self) {
        self.child.kill().expect("SIGKILL ferrosa");
        self.child.wait().expect("reap ferrosa");
    }

    /// SIGTERM and require a clean exit.
    fn stop(&mut self) {
        let pid = self.child.id().to_string();
        let sent = Command::new("kill")
            .args(["-TERM", &pid])
            .status()
            .expect("run kill");
        assert!(sent.success(), "kill -TERM {pid} failed");
        let status = self.wait();
        assert_eq!(
            status.code(),
            Some(0),
            "clean shutdown; log:\n{}",
            self.log()
        );
    }

    /// Whether `role` can log in with `password` over the PostgreSQL wire.
    fn can_login(&self, role: &str, password: &str) -> bool {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("client runtime");
        runtime.block_on(async {
            let attempt = Config::new()
                .host("127.0.0.1")
                .port(self.pg_port)
                .user(role)
                .password(password)
                .dbname("ferrosa")
                .ssl_mode(SslMode::Disable)
                .connect_timeout(Duration::from_secs(10))
                .connect(NoTls)
                .await;
            match attempt {
                Ok((_client, connection)) => {
                    drop(connection);
                    true
                }
                Err(e) => {
                    eprintln!("login {role} refused: {e:?}");
                    false
                }
            }
        })
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        // Never leave a node running after a failed assertion. Best-effort: a
        // node that already exited makes kill fail, which is the outcome wanted.
        if let Ok(None) = self.child.try_wait() {
            if let Err(e) = self.child.kill() {
                eprintln!("teardown: kill ferrosa pid {}: {e}", self.child.id());
            }
            if let Err(e) = self.child.wait() {
                eprintln!("teardown: wait ferrosa pid {}: {e}", self.child.id());
            }
        }
    }
}

/// The storage layout the binary uses for `FERROSA_DATA_DIR=data_dir`
/// (`StorageEngineConfig::from_env`).
fn offline_config(data_dir: &Path) -> StorageEngineConfig {
    StorageEngineConfig {
        commit_log: CommitLogConfig {
            log_dir: data_dir.join("commitlog"),
            checkpoint_dir: data_dir.join("commitlog"),
            archive: None,
            ..CommitLogConfig::default()
        },
        compaction: CompactionConfig::from_env(data_dir.join("compaction")),
        object_store: None,
        local_cache_max_bytes: 1024 * 1024,
        local_disk_free_reserve_bytes: 0,
        flush_threshold_bytes: 64 * 1024 * 1024,
        memtable_backpressure_bytes: u64::MAX,
        flush_max_age_secs: 5,
        data_dir: data_dir.to_path_buf(),
        index_backend: ferrosa_storage::index::IndexBackendConfig::Local,
        auth_enabled: true,
        auth_warn: false,
        write_verify: false,
        max_pending_replay_mutations_without_schema: 1_000_000,
        memtable_num_shards: 64,
        cache_hot_window_secs: 900,
    }
}

/// Open the STOPPED node's storage, run `edit`, then shut it down so every
/// write is flushed before the node starts again.
fn with_offline_engine<T>(data_dir: &Path, edit: impl FnOnce(&Arc<StorageEngine>) -> T) -> T {
    let (engine, pending) =
        StorageEngine::open(offline_config(data_dir), None).expect("open the stopped node's data");
    engine
        .register_system_tables()
        .expect("register system tables");
    engine.replay_mutations(pending).expect("replay commit log");
    let engine = Arc::new(engine);
    let out = edit(&engine);
    engine.shutdown().expect("shut the offline engine down");
    out
}

fn roles_table() -> TableId {
    TableId::new("system_auth", "roles")
}

fn corrupt_role() -> RoleMetadata {
    RoleMetadata {
        name: CORRUPT_ROLE.to_string(),
        is_superuser: false,
        can_login: true,
        salted_hash: None,
        member_of: Default::default(),
        scram: None,
    }
}

/// Persist a `system_auth.roles` row whose `is_superuser` cell is not a valid
/// boolean encoding, so the loader's decode refuses it.
fn write_undecodable_role(engine: &Arc<StorageEngine>) {
    let (key, mut row, ts) = role_to_row(&corrupt_role());
    let cell = row
        .cells
        .iter_mut()
        .find(|(col, _)| *col == ROLES_COL_IS_SUPERUSER)
        .expect("role rows carry is_superuser");
    cell.1 = CellValue::live(vec![7], ts);
    engine
        .write(&roles_table(), &key, Arc::new(row), ts)
        .expect("write the undecodable role row");
}

/// Overwrite the undecodable row with a valid one (a later write wins).
fn repair_undecodable_role(engine: &Arc<StorageEngine>) {
    let (key, row, ts) = role_to_row(&corrupt_role());
    engine
        .write(&roles_table(), &key, Arc::new(row), ts)
        .expect("repair the undecodable role row");
}

/// The stored `ferrosa_admin` salted hash in `system_auth.roles`, read straight
/// from storage; `None` when no admin row is persisted.
fn stored_admin_hash(engine: &Arc<StorageEngine>) -> Option<String> {
    ferrosa_cluster::system_table_loader::SystemTableLoader::new(Arc::clone(engine))
        .load_roles()
        .expect("persisted roles decode")
        .into_iter()
        .find(|role| role.name == ADMIN)
        .map(|role| role.salted_hash.expect("ferrosa_admin has a stored hash"))
}

/// Persist `ferrosa_admin` with `password`, the row an operator's
/// `ALTER ROLE ferrosa_admin WITH PASSWORD = ...` leaves in system_auth.roles.
fn write_rotated_admin(engine: &Arc<StorageEngine>, password: &str) -> String {
    let hash = PasswordHasher::Bcrypt { cost: 4 }
        .hash_password(password)
        .expect("hash the rotated password");
    let (key, row, ts) = role_to_row(&RoleMetadata {
        name: ADMIN.to_string(),
        is_superuser: true,
        can_login: true,
        salted_hash: Some(hash.clone()),
        member_of: Default::default(),
        scram: Some(ferrosa_schema::auth::scram::derive_with_random_salt(
            password,
        )),
    });
    engine
        .write(&roles_table(), &key, Arc::new(row), ts)
        .expect("write the rotated admin row");
    hash
}

/// The operator's rotated admin password. Test-only.
const ROTATED_ADMIN_PASSWORD: &str = "operator-rotated-admin-95557f4c"; // pragma: allowlist secret

#[derive(Clone, Copy, Debug)]
enum Exit {
    Clean,
    Crash,
}

/// A fresh node seeded with the operator's admin password, then ended by
/// `exit`.
fn provision(root: &Path, data_dir: &Path, exit: Exit) {
    let mut first = Node::start(root, data_dir, "provision", Some(OPERATOR_ADMIN_PASSWORD));
    first.wait_ready();
    assert!(
        first.can_login(ADMIN, OPERATOR_ADMIN_PASSWORD),
        "a fresh node seeds ferrosa_admin with FERROSA_SEED_ADMIN_PASSWORD"
    );
    assert!(
        !first.can_login(ADMIN, PUBLISHED_DEFAULT),
        "the published default must not log in when a password was configured"
    );
    match exit {
        Exit::Clean => first.stop(),
        Exit::Crash => first.crash(),
    }
}

/// The node must refuse to start on an auth store it cannot read, name the
/// table and the cause, and leave the stored admin row exactly as it was.
///
/// Shape: a node is provisioned, its admin rotated (persisted to
/// system_auth.roles), and it crashes before any clean shutdown writes
/// `schema.json`; one role row is then undecodable.
///
/// RED before the fix: the node becomes ready and `ferrosa_admin` logs in with
/// the PUBLISHED default while the rotated password is refused, because the
/// failed replay fell through to seeding a fresh admin.
#[test]
fn unreadable_role_store_refuses_to_start_and_keeps_the_stored_admin() {
    let root = TempDir::new().expect("temp dir");
    let data_dir = root.path().join("data");
    provision(root.path(), &data_dir, Exit::Crash);

    let provisioned_hash = with_offline_engine(&data_dir, |engine| {
        let hash = write_rotated_admin(engine, ROTATED_ADMIN_PASSWORD);
        write_undecodable_role(engine);
        hash
    });

    // No FERROSA_SEED_ADMIN_PASSWORD: the published default is what a fall
    // through to seeding would write.
    let mut broken = Node::start(root.path(), &data_dir, "broken", None);
    let outcome = broken.ready_or_exit();
    if outcome.is_ok() {
        let default_logs_in = broken.can_login(ADMIN, PUBLISHED_DEFAULT);
        let rotated_logs_in = broken.can_login(ADMIN, ROTATED_ADMIN_PASSWORD);
        panic!(
            "a node whose system_auth.roles cannot be read must refuse to start; it became \
             ready (published default logs in: {default_logs_in}, the operator's rotated \
             password logs in: {rotated_logs_in})"
        );
    }
    let status = outcome.unwrap_err();
    let stderr = broken.stderr();
    let evidence = format!("{stderr}\n{}", broken.log());
    assert_eq!(
        status.code(),
        Some(EXIT_AUTH_STORE_UNREADABLE),
        "refusal exit code; output:\n{evidence}"
    );
    assert!(
        evidence.contains("system_auth.roles"),
        "the refusal names the table; output:\n{evidence}"
    );
    assert!(
        evidence.contains(&format!(
            "role {CORRUPT_ROLE} has invalid is_superuser encoding"
        )),
        "the refusal names the cause; output:\n{evidence}"
    );
    assert!(
        !evidence.contains("seed role created"),
        "nothing may be seeded over an unreadable auth store; output:\n{evidence}"
    );

    let after = with_offline_engine(&data_dir, |engine| {
        repair_undecodable_role(engine);
        stored_admin_hash(engine)
    });
    assert_eq!(
        after,
        Some(provisioned_hash),
        "the stored ferrosa_admin row must be untouched by the refused start"
    );
}

/// After a clean shutdown, restarts seed nothing and keep the operator's
/// admin credential, with and without `FERROSA_SEED_ADMIN_PASSWORD`.
#[test]
fn restarts_after_a_clean_shutdown_keep_the_seeded_admin() {
    restarts_keep_the_seeded_admin(Exit::Clean);
}

/// The same after a crash. The seeded admin must be durable from the moment
/// it is created, not only once a clean shutdown writes `schema.json`.
#[test]
fn restarts_after_a_crash_keep_the_seeded_admin() {
    restarts_keep_the_seeded_admin(Exit::Crash);
}

fn restarts_keep_the_seeded_admin(exit: Exit) {
    let root = TempDir::new().expect("temp dir");
    let data_dir = root.path().join("data");
    provision(root.path(), &data_dir, exit);
    let provisioned_hash = with_offline_engine(&data_dir, stored_admin_hash);

    // Without the seed env first: the shape of a restart by a supervisor that
    // does not carry FERROSA_SEED_ADMIN_PASSWORD (t_5e873fe6).
    for (name, seed_env) in [
        ("restart-without-seed-env", None),
        ("restart-with-seed-env", Some(OPERATOR_ADMIN_PASSWORD)),
    ] {
        let mut node = Node::start(root.path(), &data_dir, name, seed_env);
        node.wait_ready();
        assert!(
            node.can_login(ADMIN, OPERATOR_ADMIN_PASSWORD),
            "{name}: the operator's admin password still logs in; log:\n{}",
            node.log()
        );
        assert!(
            !node.can_login(ADMIN, PUBLISHED_DEFAULT),
            "{name}: the published default must not log in; log:\n{}",
            node.log()
        );
        assert!(
            !node.log().contains("seed role created"),
            "{name}: a restart on persisted roles seeds nothing; log:\n{}",
            node.log()
        );
        match exit {
            Exit::Clean => node.stop(),
            Exit::Crash => node.crash(),
        }
        let hash = with_offline_engine(&data_dir, stored_admin_hash);
        assert_eq!(
            hash, provisioned_hash,
            "{exit:?} {name}: the stored ferrosa_admin row is unchanged"
        );
    }
    assert!(
        provisioned_hash.is_some(),
        "{exit:?}: the seeded ferrosa_admin must be persisted to system_auth.roles when it is \
         created"
    );
}
