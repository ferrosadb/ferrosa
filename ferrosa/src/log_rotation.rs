//! Module: Size-based log rotation owned by the process, not the init system.
//! Correctness: Correct when a log file is rolled once it reaches the configured
//!   size, exactly `max_files` rolled generations are retained, and the file
//!   currently being written is never a pruning candidate.
//! Last revised: 2026-09-18
//! Last changed: New module.
//!
//! # Why the binary owns this
//!
//! `node1.out.log` reached 1.6 GB, and `~/.ferrosa/logs` 2.9 GB across three
//! nodes, because the process logged to stdout and something outside it —
//! launchd here, systemd elsewhere — redirected that to a file nobody rolled.
//! Those writes saturated the disk the storage engine needs, froze the CQL
//! request runtime for up to 86 seconds, and took consensus down with it
//! (t_e94d7d38). An unbounded log is not untidiness; it is a slow leak that
//! ends in a full disk, and a 1.5 GB file is unreadable at exactly the moment
//! it is needed.
//!
//! Rotation in the init system would have to be configured once per platform
//! and is absent by default on both. A database that cannot keep its own logs
//! bounded is not self-maintaining, so this lives in the process.

use serde::{Deserialize, Serialize};

/// Default size at which the live log rolls.
pub const DEFAULT_MAX_SIZE_MB: u64 = 64;
/// Default number of rolled generations kept beside the live file.
pub const DEFAULT_MAX_FILES: u32 = 5;
/// Floor for the size cap. Below this, rolling costs more than it saves and a
/// single burst can rotate away the context around it.
pub const MIN_MAX_SIZE_MB: u64 = 1;

/// How the process maintains its own log files.
///
/// Absent configuration means rotation ON at the defaults: the failure this
/// exists to prevent is an operator who never knew the setting existed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogRotationConfig {
    /// Roll the live file once it reaches this many megabytes.
    pub max_size_mb: u64,
    /// How many rolled generations to keep. The live file is extra.
    pub max_files: u32,
    /// When false, the process writes to stdout and rolls nothing — for a
    /// foreground run, or where something else genuinely owns rotation.
    pub enabled: bool,
}

impl Default for LogRotationConfig {
    fn default() -> Self {
        Self {
            max_size_mb: DEFAULT_MAX_SIZE_MB,
            max_files: DEFAULT_MAX_FILES,
            enabled: true,
        }
    }
}

/// Why a configuration was refused.
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum ConfigError {
    /// A size cap small enough that rotation would churn.
    SizeTooSmall { given: u64, minimum: u64 },
    /// Retention of zero: rolling and then immediately deleting keeps nothing,
    /// which is indistinguishable from having no log at all.
    NoGenerationsKept,
}

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::SizeTooSmall { given, minimum } => write!(
                f,
                "logging.max_size_mb is {given}, below the {minimum} MB floor -- rotation that \
                 frequent discards the context around a failure"
            ),
            Self::NoGenerationsKept => write!(
                f,
                "logging.max_files is 0, which rolls the log and deletes it immediately -- set \
                 logging.enabled = false if no log is wanted"
            ),
        }
    }
}

impl LogRotationConfig {
    /// Read `[logging]` from the config file, letting the environment override.
    ///
    /// `env` is injected so this stays a pure function of its inputs and the
    /// precedence is testable without touching process environment.
    ///
    /// A value that will not parse falls back to the default rather than
    /// disabling rotation: a typo in a size must not be the reason a disk
    /// fills. Turning rotation off has to be said explicitly.
    pub fn from_config(config: &toml::Value, env: impl Fn(&str) -> Option<String>) -> Self {
        let section = config.get("logging");
        let read_u64 = |key: &str, env_key: &str, default: u64| -> u64 {
            env(env_key)
                .and_then(|raw| raw.parse::<u64>().ok())
                .or_else(|| {
                    section
                        .and_then(|s| s.get(key))
                        .and_then(|v| v.as_integer())
                        .and_then(|i| u64::try_from(i).ok())
                })
                .unwrap_or(default)
        };
        let max_size_mb = read_u64(
            "max_size_mb",
            "FERROSA_LOG_MAX_SIZE_MB",
            DEFAULT_MAX_SIZE_MB,
        );
        let max_files = u32::try_from(read_u64(
            "max_files",
            "FERROSA_LOG_MAX_FILES",
            u64::from(DEFAULT_MAX_FILES),
        ))
        .unwrap_or(DEFAULT_MAX_FILES);
        let enabled = env("FERROSA_LOG_ROTATION_ENABLED")
            .map(|raw| raw != "false" && raw != "0")
            .or_else(|| {
                section
                    .and_then(|s| s.get("enabled"))
                    .and_then(|v| v.as_bool())
            })
            .unwrap_or(true);
        Self {
            max_size_mb,
            max_files,
            enabled,
        }
    }

    /// Refuse a configuration that cannot do its job, rather than silently
    /// substituting a default the operator did not choose.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if !self.enabled {
            return Ok(());
        }
        if self.max_size_mb < MIN_MAX_SIZE_MB {
            return Err(ConfigError::SizeTooSmall {
                given: self.max_size_mb,
                minimum: MIN_MAX_SIZE_MB,
            });
        }
        if self.max_files == 0 {
            return Err(ConfigError::NoGenerationsKept);
        }
        Ok(())
    }

    /// The size cap in bytes.
    pub fn max_size_bytes(&self) -> u64 {
        self.max_size_mb.saturating_mul(1024 * 1024)
    }

    /// Whether a live file of `current_bytes` has earned a roll.
    pub fn should_roll(&self, current_bytes: u64) -> bool {
        self.enabled && current_bytes >= self.max_size_bytes()
    }
}

/// Which rolled generations to delete, oldest first.
///
/// `rolled` is the rolled files already present, each with the instant it was
/// rolled. The live file is not a member and can never be selected: a log that
/// prunes the file it is writing loses the events that explain the present.
pub fn generations_to_prune<T: Clone>(
    rolled: &[(T, std::time::SystemTime)],
    max_files: u32,
) -> Vec<T> {
    let keep = max_files as usize;
    if rolled.len() <= keep {
        return Vec::new();
    }
    let mut by_age: Vec<&(T, std::time::SystemTime)> = rolled.iter().collect();
    by_age.sort_by_key(|(_, when)| *when);
    by_age
        .into_iter()
        .take(rolled.len() - keep)
        .map(|(name, _)| name.clone())
        .collect()
}

/// The rolled generations currently beside `base` in `dir`, with their mtimes.
///
/// A generation is `<base>.<unix-nanos>`. The live file is excluded by name, so
/// it can never be handed to the pruner.
pub fn rolled_generations(
    dir: &std::path::Path,
    base: &str,
) -> std::io::Result<Vec<(std::path::PathBuf, std::time::SystemTime)>> {
    let prefix = format!("{base}.");
    let mut found = Vec::new();
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        if name == base || !name.starts_with(&prefix) {
            continue;
        }
        // Only our own suffix shape, so an unrelated file is never deleted.
        if !name[prefix.len()..].chars().all(|c| c.is_ascii_digit()) {
            continue;
        }
        let when = entry.metadata()?.modified()?;
        found.push((entry.path(), when));
    }
    Ok(found)
}

/// A log file that rolls itself at a size cap and prunes its own history.
///
/// Deliberately not `tracing_appender::rolling`: that rotates on a time
/// boundary, and the failure here was size — 1.6 GB inside a single day. A
/// quiet week should not roll, and a burst should.
pub struct RotatingWriter {
    dir: std::path::PathBuf,
    base: String,
    config: LogRotationConfig,
    file: std::fs::File,
    written: u64,
}

impl RotatingWriter {
    /// Open (or create) the live file, appending to whatever is already there.
    pub fn open(
        dir: &std::path::Path,
        base: &str,
        config: LogRotationConfig,
    ) -> std::io::Result<Self> {
        std::fs::create_dir_all(dir)?;
        let path = dir.join(base);
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)?;
        let written = file.metadata()?.len();
        Ok(Self {
            dir: dir.to_path_buf(),
            base: base.to_string(),
            config,
            file,
            written,
        })
    }

    /// Move the live file aside, open a fresh one, and prune the excess.
    ///
    /// A failure to prune is reported but does not fail the write: losing a log
    /// line is worse than keeping one generation too many, and the next roll
    /// tries again.
    fn roll(&mut self) -> std::io::Result<()> {
        let stamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let live = self.dir.join(&self.base);
        let rolled = self.dir.join(format!("{}.{stamp}", self.base));
        std::fs::rename(&live, &rolled)?;
        self.file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&live)?;
        self.written = 0;

        match rolled_generations(&self.dir, &self.base) {
            Ok(generations) => {
                for path in generations_to_prune(&generations, self.config.max_files) {
                    if let Err(error) = std::fs::remove_file(&path) {
                        eprintln!(
                            "ferrosa: could not prune rolled log {}: {error}",
                            path.display()
                        );
                    }
                }
            }
            Err(error) => {
                eprintln!("ferrosa: could not scan rolled logs to prune them: {error}");
            }
        }
        Ok(())
    }
}

impl std::io::Write for RotatingWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        if self.config.should_roll(self.written) {
            self.roll()?;
        }
        let n = std::io::Write::write(&mut self.file, buf)?;
        self.written = self.written.saturating_add(n as u64);
        Ok(n)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        std::io::Write::flush(&mut self.file)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, SystemTime};

    fn at(secs: u64) -> SystemTime {
        SystemTime::UNIX_EPOCH + Duration::from_secs(secs)
    }

    #[test]
    fn an_unconfigured_process_still_rotates() {
        // The 1.6 GB file existed because nobody set anything. Defaults must
        // bound the log on their own.
        let config = LogRotationConfig::default();
        assert!(config.enabled);
        assert!(config.validate().is_ok());
        assert!(config.max_size_mb > 0);
        assert!(config.max_files > 0);
    }

    #[test]
    fn a_file_under_the_cap_does_not_roll() {
        let config = LogRotationConfig {
            max_size_mb: 64,
            ..Default::default()
        };
        assert!(!config.should_roll(63 * 1024 * 1024));
    }

    #[test]
    fn a_file_at_the_cap_rolls() {
        let config = LogRotationConfig {
            max_size_mb: 64,
            ..Default::default()
        };
        assert!(config.should_roll(64 * 1024 * 1024));
        assert!(config.should_roll(64 * 1024 * 1024 + 1));
    }

    #[test]
    fn the_one_point_six_gigabyte_file_would_have_rolled() {
        // The file that caused t_e94d7d38, against the shipped defaults.
        assert!(LogRotationConfig::default().should_roll(1_568_340_326));
    }

    #[test]
    fn rotation_turned_off_never_rolls() {
        let config = LogRotationConfig {
            enabled: false,
            ..Default::default()
        };
        assert!(!config.should_roll(u64::MAX));
        assert!(
            config.validate().is_ok(),
            "disabling is a choice, not an error"
        );
    }

    #[test]
    fn a_size_cap_below_the_floor_is_refused_with_the_number_given() {
        let config = LogRotationConfig {
            max_size_mb: 0,
            ..Default::default()
        };
        assert_eq!(
            config.validate(),
            Err(ConfigError::SizeTooSmall {
                given: 0,
                minimum: MIN_MAX_SIZE_MB
            })
        );
        assert!(config
            .validate()
            .unwrap_err()
            .to_string()
            .contains("max_size_mb"));
    }

    #[test]
    fn keeping_zero_generations_is_refused_and_says_what_to_do_instead() {
        let config = LogRotationConfig {
            max_files: 0,
            ..Default::default()
        };
        assert_eq!(config.validate(), Err(ConfigError::NoGenerationsKept));
        assert!(config
            .validate()
            .unwrap_err()
            .to_string()
            .contains("enabled = false"));
    }

    #[test]
    fn nothing_is_pruned_while_under_the_retention_limit() {
        let rolled = [("a", at(1)), ("b", at(2))];
        assert!(generations_to_prune(&rolled, 5).is_empty());
    }

    #[test]
    fn exactly_at_the_limit_prunes_nothing() {
        let rolled = [("a", at(1)), ("b", at(2)), ("c", at(3))];
        assert!(generations_to_prune(&rolled, 3).is_empty());
    }

    #[test]
    fn the_oldest_generations_beyond_the_limit_are_pruned() {
        let rolled = [
            ("newest", at(50)),
            ("oldest", at(10)),
            ("middle", at(30)),
            ("older", at(20)),
            ("newer", at(40)),
        ];
        // Keep 3 of 5: the two oldest go, oldest first.
        assert_eq!(generations_to_prune(&rolled, 3), vec!["oldest", "older"]);
    }

    #[test]
    fn age_decides_pruning_not_the_order_the_directory_listed_them() {
        // A directory read gives no ordering guarantee, and a lexicographic
        // accident would delete the wrong generation.
        let rolled = [("log.9", at(10)), ("log.10", at(20))];
        assert_eq!(generations_to_prune(&rolled, 1), vec!["log.9"]);
    }

    #[test]
    fn a_process_that_has_rolled_nothing_prunes_nothing() {
        let rolled: [(&str, SystemTime); 0] = [];
        assert!(generations_to_prune(&rolled, 3).is_empty());
    }

    // ---- the writer: rolling and pruning against a real directory ----

    #[test]
    fn writes_land_in_the_live_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut writer =
            RotatingWriter::open(dir.path(), "ferrosa.log", LogRotationConfig::default())
                .expect("open");
        use std::io::Write;
        writer.write_all(b"first line\n").expect("write");
        writer.flush().expect("flush");
        let live = std::fs::read_to_string(dir.path().join("ferrosa.log")).expect("read live");
        assert_eq!(live, "first line\n");
    }

    #[test]
    fn passing_the_cap_rolls_the_live_file_and_keeps_writing() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config = LogRotationConfig {
            max_size_mb: 1,
            max_files: 5,
            enabled: true,
        };
        let mut writer = RotatingWriter::open(dir.path(), "ferrosa.log", config).expect("open");
        use std::io::Write;
        let chunk = vec![b'x'; 256 * 1024];
        for _ in 0..5 {
            writer.write_all(&chunk).expect("write");
        }
        writer.write_all(b"after the roll\n").expect("write");
        writer.flush().expect("flush");

        let rolled = rolled_generations(dir.path(), "ferrosa.log").expect("scan");
        assert!(
            !rolled.is_empty(),
            "a roll should have produced a generation"
        );
        let live = std::fs::read_to_string(dir.path().join("ferrosa.log")).expect("read live");
        assert!(
            live.contains("after the roll"),
            "writing must continue into the new live file"
        );
    }

    #[test]
    fn generations_beyond_the_limit_are_deleted_so_the_directory_stays_bounded() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config = LogRotationConfig {
            max_size_mb: 1,
            max_files: 2,
            enabled: true,
        };
        let mut writer = RotatingWriter::open(dir.path(), "ferrosa.log", config).expect("open");
        use std::io::Write;
        let chunk = vec![b'y'; 256 * 1024];
        // Enough to roll several times over.
        for _ in 0..30 {
            writer.write_all(&chunk).expect("write");
        }
        writer.flush().expect("flush");

        let rolled = rolled_generations(dir.path(), "ferrosa.log").expect("scan");
        assert!(
            rolled.len() <= 2,
            "retention must bound the directory, found {} generations",
            rolled.len()
        );
        assert!(
            dir.path().join("ferrosa.log").exists(),
            "the live file must survive pruning"
        );
    }

    // ---- reading the operator's setting ----

    fn toml_of(text: &str) -> toml::Value {
        toml::from_str(text).expect("test toml")
    }

    #[test]
    fn an_empty_config_file_yields_the_rotating_defaults() {
        let parsed = LogRotationConfig::from_config(&toml_of(""), |_| None);
        assert_eq!(parsed, LogRotationConfig::default());
    }

    #[test]
    fn the_logging_section_is_read() {
        let parsed = LogRotationConfig::from_config(
            &toml_of("[logging]\nmax_size_mb = 128\nmax_files = 10\nenabled = true\n"),
            |_| None,
        );
        assert_eq!(parsed.max_size_mb, 128);
        assert_eq!(parsed.max_files, 10);
        assert!(parsed.enabled);
    }

    #[test]
    fn rotation_can_be_turned_off_in_the_file() {
        let parsed =
            LogRotationConfig::from_config(&toml_of("[logging]\nenabled = false\n"), |_| None);
        assert!(!parsed.enabled);
    }

    #[test]
    fn the_environment_overrides_the_file() {
        let parsed =
            LogRotationConfig::from_config(&toml_of("[logging]\nmax_size_mb = 128\n"), |key| {
                match key {
                    "FERROSA_LOG_MAX_SIZE_MB" => Some("32".to_string()),
                    _ => None,
                }
            });
        assert_eq!(parsed.max_size_mb, 32);
    }

    #[test]
    fn a_value_that_is_not_a_number_falls_back_to_the_default_rather_than_disabling_rotation() {
        let parsed = LogRotationConfig::from_config(
            &toml_of("[logging]\nmax_size_mb = \"enormous\"\n"),
            |_| None,
        );
        assert_eq!(parsed.max_size_mb, DEFAULT_MAX_SIZE_MB);
        assert!(parsed.enabled, "a typo must not silently stop rotation");
    }
}
