//! The one place ferrosa makes a file durable.
//!
//! Every durability sync in the workspace (commit log, SSTable components,
//! schema snapshots, compaction intents, Raft and Accord logs, directory
//! entries) goes through [`sync_all`], [`sync_data`] or [`sync_dir`]. Calling
//! `File::sync_all` / `File::sync_data` directly is refused by `clippy.toml`
//! (`disallowed-methods`), because on Apple targets both of those are
//! `fcntl(F_FULLFSYNC)` whatever the operator configured.
//!
//! # The mode, and what it costs
//!
//! [`SyncMode`] only changes anything on macOS. There, `fsync(2)` hands the
//! data to the drive but does not flush the drive's volatile cache. On Linux
//! `fsync(2)` / `fdatasync(2)` already flush the device cache, so every mode
//! issues exactly the call it always did: a mode never makes Linux weaker.
//!
//! | Mode | macOS call | Process crash | OS crash | Power loss |
//! |------|------------|---------------|----------|------------|
//! | `Full` | `fcntl(F_FULLFSYNC)` | survives | survives | survives |
//! | `Barrier` | `fcntl(F_BARRIERFSYNC)` | survives | survives | may lose the tail since the drive last flushed its cache; never reorders it |
//! | `Fsync` (default) | `fsync(2)` | survives | survives | may lose or reorder recent writes |
//!
//! The default is `Fsync`. A macOS install is a laptop or desktop, not a
//! storage server: `F_FULLFSYNC` costs a full drive-cache flush on every call
//! (milliseconds each on Apple SSDs), and the cases it protects against beyond
//! `fsync(2)` are sudden power loss or a drive reset. A laptop on battery does
//! not lose power suddenly, and durable state is also written behind to the
//! object store when one is configured. An operator who needs power-loss
//! durability on a Mac sets `full` (`[storage] fsync_mode = "full"` or
//! `FERROSA_FSYNC_MODE=full`). The choice is logged at startup and every call
//! issued is counted in `ferrosa_durable_syncs_total{call=...}`.

use std::fs::File;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::OnceLock;

/// Which system call makes a file durable on macOS. See the module docs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SyncMode {
    /// `F_FULLFSYNC`: flush the drive cache on every sync. Power-loss durable.
    Full,
    /// `F_BARRIERFSYNC`: ordered, not power-loss durable.
    Barrier,
    /// `fsync(2)`: neither ordered nor durable across power loss on macOS.
    #[default]
    Fsync,
}

/// The environment variable that selects the process-wide mode. The TOML key
/// `[storage] fsync_mode` wins over it, as for every other setting.
pub const ENV: &str = "FERROSA_FSYNC_MODE";

/// Whether a sync must also make metadata durable (`sync_all`, a new file's
/// size, a directory entry) or only the data (`sync_data`). Off Apple targets
/// this picks `fsync(2)` or `fdatasync(2)`, exactly as `std` did before.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Scope {
    All,
    Data,
}

/// The system call a sync issues.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Call {
    FullFsync,
    BarrierFsync,
    Fsync,
    Fdatasync,
}

impl Call {
    const ALL: [Self; 4] = [
        Self::FullFsync,
        Self::BarrierFsync,
        Self::Fsync,
        Self::Fdatasync,
    ];

    /// The label used in metrics.
    pub fn name(self) -> &'static str {
        match self {
            Self::FullFsync => "f_fullfsync",
            Self::BarrierFsync => "f_barrierfsync",
            Self::Fsync => "fsync",
            Self::Fdatasync => "fdatasync",
        }
    }

    fn index(self) -> usize {
        match self {
            Self::FullFsync => 0,
            Self::BarrierFsync => 1,
            Self::Fsync => 2,
            Self::Fdatasync => 3,
        }
    }
}

impl SyncMode {
    pub const ALL: [Self; 3] = [Self::Full, Self::Barrier, Self::Fsync];

    /// The name accepted by [`parse`](Self::parse).
    pub fn name(self) -> &'static str {
        match self {
            Self::Full => "full",
            Self::Barrier => "barrier",
            Self::Fsync => "fsync",
        }
    }

    /// Parses a mode name, ignoring case and surrounding whitespace.
    pub fn parse(raw: &str) -> Result<Self, String> {
        let wanted = raw.trim();
        Self::ALL
            .into_iter()
            .find(|mode| mode.name().eq_ignore_ascii_case(wanted))
            .ok_or_else(|| {
                format!("unknown fsync mode {raw:?}; expected one of full, barrier, fsync")
            })
    }

    /// Resolves an optional configured value: `None` is the default, a value
    /// that does not parse is an error, never the default. A durability
    /// setting that was asked for and silently not applied is the failure
    /// this type exists to prevent.
    pub fn resolve(raw: Option<&str>) -> Result<Self, String> {
        raw.map_or(Ok(Self::default()), Self::parse)
    }

    /// The system call this mode issues for `scope` on this platform.
    pub fn call(self, scope: Scope) -> Call {
        if cfg!(target_vendor = "apple") {
            match self {
                Self::Full => Call::FullFsync,
                Self::Barrier => Call::BarrierFsync,
                Self::Fsync => Call::Fsync,
            }
        } else {
            match scope {
                Scope::All => Call::Fsync,
                Scope::Data => Call::Fdatasync,
            }
        }
    }

    /// Whether an acknowledged, synced write survives sudden power loss.
    pub fn survives_power_loss(self) -> bool {
        !cfg!(target_vendor = "apple") || matches!(self, Self::Full)
    }

    /// One line, for the startup log, saying what this mode guarantees on
    /// this platform. It never says "panic": the install smoke fails any
    /// startup log that does.
    #[cfg(target_vendor = "apple")]
    pub fn durability(self) -> &'static str {
        match self {
            Self::Full => {
                "F_FULLFSYNC: synced writes survive process crash, OS crash and power loss"
            }
            Self::Barrier => {
                "F_BARRIERFSYNC: synced writes survive process crash and OS crash; on power loss \
                 the tail since the drive last flushed its cache can be lost, in order"
            }
            Self::Fsync => {
                "fsync(2): synced writes survive process crash and OS crash; on power loss \
                 recent writes can be lost or reordered"
            }
        }
    }

    /// One line, for the startup log, saying what this mode guarantees on
    /// this platform. It never says "panic": the install smoke fails any
    /// startup log that does.
    #[cfg(not(target_vendor = "apple"))]
    pub fn durability(self) -> &'static str {
        "fsync(2)/fdatasync(2): synced writes survive process crash, OS crash and power loss \
         (the mode chooses the sync call only on Apple targets)"
    }
}

static PROCESS_MODE: OnceLock<SyncMode> = OnceLock::new();

/// Sets the process-wide mode. Called once, at startup, before any storage
/// opens. Setting it again to the same mode is a no-op; to a different mode
/// it is an error, because syncs already issued used the first one.
pub fn install_process_mode(mode: SyncMode) -> Result<(), String> {
    let installed = *PROCESS_MODE.get_or_init(|| mode);
    if installed == mode {
        Ok(())
    } else {
        Err(format!(
            "fsync mode already installed as {:?}; refusing to switch to {:?}",
            installed.name(),
            mode.name()
        ))
    }
}

/// The process-wide mode: the installed one, else the default.
pub fn process_mode() -> SyncMode {
    PROCESS_MODE.get().copied().unwrap_or_default()
}

static SYNCS_BY_CALL: [AtomicU64; 4] = [
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
];

/// Syncs issued with `call` since the process started.
pub fn syncs_issued(call: Call) -> u64 {
    SYNCS_BY_CALL[call.index()].load(Ordering::Relaxed)
}

/// Makes `file`'s data and metadata durable in the process-wide mode.
pub fn sync_all(file: &File) -> std::io::Result<()> {
    sync_with(file, process_mode(), Scope::All)
}

/// Makes `file`'s data durable in the process-wide mode.
pub fn sync_data(file: &File) -> std::io::Result<()> {
    sync_with(file, process_mode(), Scope::Data)
}

/// Makes the directory entries under `dir` durable in the process-wide mode.
pub fn sync_dir(dir: &Path) -> std::io::Result<()> {
    sync_all(&File::open(dir)?)
}

/// Makes `file` durable with the call `mode` selects for `scope`, and counts
/// the call. The commit log passes its own mode; everything else goes through
/// [`sync_all`] / [`sync_data`].
pub fn sync_with(file: &File, mode: SyncMode, scope: Scope) -> std::io::Result<()> {
    let call = mode.call(scope);
    issue(file, call)?;
    SYNCS_BY_CALL[call.index()].fetch_add(1, Ordering::Relaxed);
    Ok(())
}

#[cfg(not(target_vendor = "apple"))]
#[allow(clippy::disallowed_methods)] // the one sanctioned caller
fn issue(file: &File, call: Call) -> std::io::Result<()> {
    match call {
        Call::Fsync => file.sync_all(),
        Call::Fdatasync => file.sync_data(),
        Call::FullFsync | Call::BarrierFsync => Err(std::io::Error::other(format!(
            "{} is an Apple-only call",
            call.name()
        ))),
    }
}

/// An interrupted sync is reissued, as `std` does for `sync_all`. The bound
/// only stops a signal storm from spinning forever; it then fails loudly.
#[cfg(target_vendor = "apple")]
const MAX_EINTR_RETRIES: usize = 64;

#[cfg(target_vendor = "apple")]
fn issue(file: &File, call: Call) -> std::io::Result<()> {
    use std::os::fd::AsRawFd;
    if call == Call::Fdatasync {
        return Err(std::io::Error::other(
            "fdatasync is never selected on Apple targets",
        ));
    }
    let fd = file.as_raw_fd();
    for _ in 0..MAX_EINTR_RETRIES {
        // SAFETY: `fd` is owned by `file`, which outlives the call; none of
        // these calls reads or writes memory through a pointer.
        let rc = unsafe {
            match call {
                Call::FullFsync => libc::fcntl(fd, libc::F_FULLFSYNC),
                Call::BarrierFsync => libc::fcntl(fd, libc::F_BARRIERFSYNC),
                // `File::sync_all`/`sync_data` would be F_FULLFSYNC here.
                Call::Fsync | Call::Fdatasync => libc::fsync(fd),
            }
        };
        if rc != -1 {
            return Ok(());
        }
        let err = std::io::Error::last_os_error();
        if err.kind() != std::io::ErrorKind::Interrupted {
            return Err(err);
        }
    }
    Err(std::io::Error::other(format!(
        "{} interrupted {MAX_EINTR_RETRIES} times in a row",
        call.name()
    )))
}

/// Renders the per-call counters in Prometheus exposition format.
pub fn render_prometheus() -> String {
    let mut out = String::from(
        "# HELP ferrosa_durable_syncs_total File and directory syncs by the system call issued \
         (f_fullfsync survives power loss on macOS; fsync and f_barrierfsync do not).\n\
         # TYPE ferrosa_durable_syncs_total counter\n",
    );
    for call in Call::ALL {
        out.push_str(&format!(
            "ferrosa_durable_syncs_total{{call=\"{}\"}} {}\n",
            call.name(),
            syncs_issued(call)
        ));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn the_default_mode_is_plain_fsync() {
        assert_eq!(SyncMode::default(), SyncMode::Fsync);
        assert_eq!(SyncMode::resolve(None), Ok(SyncMode::Fsync));
    }

    #[test]
    fn every_mode_name_parses_and_round_trips() {
        for mode in SyncMode::ALL {
            assert_eq!(SyncMode::parse(mode.name()), Ok(mode));
            assert_eq!(SyncMode::resolve(Some(mode.name())), Ok(mode));
        }
        assert_eq!(SyncMode::parse(" Barrier "), Ok(SyncMode::Barrier));
    }

    #[test]
    fn an_unknown_mode_is_refused_naming_the_accepted_values() {
        let err = SyncMode::resolve(Some("fullfsync")).unwrap_err();
        assert!(err.contains("fullfsync"), "{err}");
        for accepted in ["full", "barrier", "fsync"] {
            assert!(err.contains(accepted), "{err}");
        }
    }

    #[test]
    fn no_durability_line_mentions_a_panic() {
        for mode in SyncMode::ALL {
            let line = mode.durability().to_ascii_lowercase();
            assert!(!line.contains("panic"), "{mode:?}: {line}");
        }
    }

    #[cfg(target_vendor = "apple")]
    #[test]
    fn on_apple_the_mode_picks_the_call_whatever_the_scope() {
        for scope in [Scope::All, Scope::Data] {
            assert_eq!(SyncMode::Full.call(scope), Call::FullFsync);
            assert_eq!(SyncMode::Barrier.call(scope), Call::BarrierFsync);
            assert_eq!(SyncMode::Fsync.call(scope), Call::Fsync);
        }
        assert!(SyncMode::Full.survives_power_loss());
        assert!(!SyncMode::Barrier.survives_power_loss());
        assert!(!SyncMode::Fsync.survives_power_loss());
    }

    /// Linux behaviour is unchanged: `sync_all` stays fsync(2) and
    /// `sync_data` stays fdatasync(2) in every mode.
    #[cfg(not(target_vendor = "apple"))]
    #[test]
    fn off_apple_the_scope_picks_the_call_whatever_the_mode() {
        for mode in SyncMode::ALL {
            assert_eq!(mode.call(Scope::All), Call::Fsync);
            assert_eq!(mode.call(Scope::Data), Call::Fdatasync);
            assert!(mode.survives_power_loss(), "{mode:?}");
        }
    }

    /// The helper issues, and counts, the call the configured mode selects.
    #[test]
    fn sync_with_issues_and_counts_the_selected_call() {
        let dir = tempfile::tempdir().unwrap();
        let mut file = File::create(dir.path().join("f")).unwrap();
        file.write_all(b"durable").unwrap();
        for mode in SyncMode::ALL {
            for scope in [Scope::All, Scope::Data] {
                let call = mode.call(scope);
                let before = syncs_issued(call);
                sync_with(&file, mode, scope).unwrap();
                assert!(
                    syncs_issued(call) > before,
                    "{mode:?}/{scope:?} not counted"
                );
            }
        }
        sync_dir(dir.path()).unwrap();
        assert!(render_prometheus().contains("ferrosa_durable_syncs_total{call=\"fsync\"}"));
    }

    #[test]
    fn installing_a_different_mode_twice_is_refused() {
        // The process mode is a OnceLock shared by every test in this binary,
        // so install the mode the rest of the suite already sees.
        let mode = process_mode();
        install_process_mode(mode).unwrap();
        assert_eq!(process_mode(), mode);
        let other = SyncMode::ALL.into_iter().find(|m| *m != mode).unwrap();
        let err = install_process_mode(other).unwrap_err();
        assert!(err.contains("refusing"), "{err}");
    }
}
