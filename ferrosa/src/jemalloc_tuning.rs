//! Runtime control of jemalloc's page-decay options.
//!
//! The binary ships a compile-time `malloc_conf` (`dirty_decay_ms:0,
//! muzzy_decay_ms:0`; see `main.rs`) that releases freed pages straight back to
//! the OS. That is the right default for a memory-capped node — but it is an
//! *assumption* about ferrosa's allocation pattern ("most freed pages are not
//! reused in-arena anyway; the throughput trade-off is small"), and an operator
//! on different hardware, or a load test hunting CPU, may want to change it
//! without a rebuild.
//!
//! `malloc_conf` is consumed by jemalloc **before `main` runs**, so it cannot be
//! overridden from a TOML or from ferrosa's own env plumbing. These knobs are
//! instead applied through jemalloc's runtime control API (`mallctl`) as the
//! first thing `main` does, before the tokio runtimes are built and before any
//! arena is meaningfully exercised.
//!
//! Knobs (all optional; unset **or empty** leaves jemalloc's current value —
//! matching how ferrosa treats an empty env var everywhere else):
//!
//! | env var | mallctl name | type |
//! |---|---|---|
//! | `FERROSA_JEMALLOC_DIRTY_DECAY_MS` | `arenas.dirty_decay_ms` | `ssize_t` |
//! | `FERROSA_JEMALLOC_MUZZY_DECAY_MS` | `arenas.muzzy_decay_ms` | `ssize_t` |
//!
//! A negative decay means "never purge" (the throughput-favouring setting);
//! `0` purges immediately (the shipped default).
//!
//! Three things learned the hard way, all encoded below:
//!
//! - The writable decay names live under **`arenas.`** (they set every arena at
//!   once). The root **`opt.*`** names carry only the *startup* values and are
//!   **read-only** — a write returns `EPERM` and silently changes nothing.
//! - Applying a knob is only proven by **reading it back**. A write that returns
//!   success but does not stick is worse than a loud failure, so every knob is
//!   re-read and the effective value is logged; a mismatch is reported as a
//!   failure to apply.
//! - Background threads are deliberately NOT exposed: `background_thread`
//!   requires jemalloc built with `background_threads_runtime_support`, which the
//!   `tikv-jemalloc-sys` build here does not enable — the mallctl node does not
//!   exist and every write returns `ENOENT`. A knob that can never be honoured is
//!   not shipped.
//!
//! Failures are logged to stderr and are **not** fatal: a tuning knob must never
//! stop a node from starting.

use std::ffi::{c_char, c_int, c_void, CString};

/// Parse a decay value in milliseconds. `None` for unset, empty, whitespace-only,
/// or unparseable input — in every one of those cases the caller leaves jemalloc
/// untouched rather than guessing. Negative values are meaningful (`-1` = never
/// purge), so they are accepted.
fn parse_decay(raw: Option<&str>) -> Option<i64> {
    let raw = raw?;
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }
    trimmed.parse::<i64>().ok()
}

/// `mallctl(name, NULL, NULL, &value, len)` — write one `ssize_t` option.
///
/// # Safety
///
/// `value` must point to a value of the type and size the named option expects,
/// as documented in jemalloc's `mallctl(3)`. `oldp`/`oldlenp` are null because we
/// only ever write.
unsafe fn mallctl_write_ssize(name: &str, value: &mut i64) -> std::io::Result<()> {
    let cname = CString::new(name).expect("mallctl option name has no interior NUL");
    let rc: c_int = unsafe {
        tikv_jemalloc_sys::mallctl(
            cname.as_ptr() as *const c_char,
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            value as *mut i64 as *mut c_void,
            std::mem::size_of::<i64>(),
        )
    };
    if rc == 0 {
        Ok(())
    } else {
        Err(std::io::Error::from_raw_os_error(rc))
    }
}

/// `mallctl(name, &value, &len, NULL, 0)` — read an `ssize_t` option.
fn mallctl_read_ssize(name: &str) -> std::io::Result<i64> {
    let cname = CString::new(name).expect("mallctl option name has no interior NUL");
    let mut value: i64 = 0;
    let mut len = std::mem::size_of::<i64>();
    let rc: c_int = unsafe {
        tikv_jemalloc_sys::mallctl(
            cname.as_ptr() as *const c_char,
            &mut value as *mut i64 as *mut c_void,
            &mut len,
            std::ptr::null_mut(),
            0,
        )
    };
    if rc == 0 {
        Ok(value)
    } else {
        Err(std::io::Error::from_raw_os_error(rc))
    }
}

/// Apply one decay knob and prove it took: write, then read back.
fn set_decay(name: &str, want: i64) {
    if let Err(e) = unsafe { mallctl_write_ssize(name, &mut want.clone()) } {
        eprintln!("jemalloc: could not apply {name}={want}: {e}; leaving it unchanged");
        return;
    }
    match mallctl_read_ssize(name) {
        Ok(got) if got == want => eprintln!("jemalloc: applied {name}={want}"),
        Ok(got) => eprintln!(
            "jemalloc: WARNING wrote {name}={want} but it reads back {got}; \
             the change did not take effect"
        ),
        Err(e) => {
            eprintln!("jemalloc: wrote {name}={want} but could not read it back to verify: {e}")
        }
    }
}

/// Apply the `FERROSA_JEMALLOC_*` decay knobs. Call once, first thing in `main`;
/// a no-op when every knob is unset.
pub fn apply_from_env() {
    let dirty = std::env::var("FERROSA_JEMALLOC_DIRTY_DECAY_MS").ok();
    if let Some(ms) = parse_decay(dirty.as_deref()) {
        set_decay("arenas.dirty_decay_ms", ms);
    }

    let muzzy = std::env::var("FERROSA_JEMALLOC_MUZZY_DECAY_MS").ok();
    if let Some(ms) = parse_decay(muzzy.as_deref()) {
        set_decay("arenas.muzzy_decay_ms", ms);
    }
}

#[cfg(test)]
mod tests {
    use super::parse_decay;

    #[test]
    fn decay_unset_or_empty_is_left_alone() {
        assert_eq!(parse_decay(None), None);
        assert_eq!(parse_decay(Some("")), None, "set-but-empty means unset");
        assert_eq!(parse_decay(Some("   ")), None);
        assert_eq!(
            parse_decay(Some("nonsense")),
            None,
            "unparseable is not a guess"
        );
    }

    #[test]
    fn decay_accepts_zero_and_negative() {
        assert_eq!(parse_decay(Some("0")), Some(0), "0 = purge immediately");
        assert_eq!(parse_decay(Some("-1")), Some(-1), "-1 = never purge");
        assert_eq!(parse_decay(Some(" 1000 ")), Some(1000));
    }

    #[test]
    fn decay_rejects_non_integers() {
        assert_eq!(parse_decay(Some("1.5")), None);
        assert_eq!(parse_decay(Some("10ms")), None);
        assert_eq!(parse_decay(Some("1_000")), None);
    }
}
