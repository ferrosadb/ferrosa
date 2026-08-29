//! Module: Where this node's errors go besides the log.
//!
//! Correctness: Correct when an absent DSN starts nothing, when what is sent
//! names the build it came from, and when a warning does not arrive as an
//! error.
//!
//! Last revised: 2026-08-28
//! Last changed: New.
//!
//! # Why the DSN is not in this file
//!
//! ferrosa is a PUBLIC repository. A Sentry DSN is not a secret — it is a
//! write-only ingest key meant to be embedded in a client — but one committed
//! to a public repo can be used by anyone to write into the project, and
//! rotating it then needs a release. The installer supplies it as
//! `FERROSA_SENTRY_DSN`.
//!
//! # Errors only
//!
//! Warnings become breadcrumbs, not events: they are context for the error
//! that follows, and as events they would bury it. Everything below WARN stays
//! in the log — a storage engine at info level would exhaust the quota in
//! minutes and hide every error underneath the noise.

/// The commit this binary was built from, or `unknown`. Set by build.rs.
pub const BUILD_SHA: &str = env!("FERROSA_BUILD_SHA");

/// The environment variable the installer sets.
const DSN_VAR: &str = "FERROSA_SENTRY_DSN";

/// How this build identifies itself: `ferrosa@version+sha`.
///
/// An unknown commit degrades to the version rather than shipping the word
/// "unknown" into a release name, which would group every unstamped build of
/// every version together.
pub fn release_id(version: &str, build_sha: &str) -> String {
    if build_sha.is_empty() || build_sha == "unknown" {
        format!("ferrosa@{version}")
    } else {
        format!("ferrosa@{version}+{build_sha}")
    }
}

/// Whether a configured value can be used as a DSN.
///
/// An unset variable reaches a process from a shell script as an empty string,
/// and starting the SDK with one reports a configuration error on every launch
/// about a side channel nobody asked for. Plain `http` is refused: it would put
/// diagnostics on the wire in the clear.
pub fn usable_dsn(value: Option<&str>) -> Option<&str> {
    value
        .map(str::trim)
        .filter(|dsn| !dsn.is_empty() && dsn.starts_with("https://"))
}

/// Start Sentry if the installer configured it.
///
/// The guard must live as long as the process: dropping it stops sending,
/// which quietly loses the errors that happen during shutdown — exactly the
/// ones worth having from a database.
#[must_use = "dropping this stops error reporting"]
pub fn start() -> Option<sentry::ClientInitGuard> {
    let configured = std::env::var(DSN_VAR).ok();
    let dsn = usable_dsn(configured.as_deref())?;
    let release = release_id(env!("CARGO_PKG_VERSION"), BUILD_SHA);
    // Field by field: ClientOptions is #[non_exhaustive], so a struct
    // expression does not compile from outside the SDK.
    let mut options = sentry::ClientOptions::default();
    options.release = Some(release.clone().into());
    // Never. A database node's environment names the machine and the operator.
    options.send_default_pii = false;
    let guard = sentry::init((dsn, options));
    tracing::info!(%release, "errors report to Sentry");
    Some(guard)
}

/// The layer that turns `tracing::error!` into a Sentry event.
pub fn layer<S>() -> sentry_tracing::SentryLayer<S>
where
    S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
{
    sentry_tracing::layer().event_filter(|meta| match *meta.level() {
        tracing::Level::ERROR => sentry_tracing::EventFilter::Event,
        tracing::Level::WARN => sentry_tracing::EventFilter::Breadcrumb,
        _ => sentry_tracing::EventFilter::Ignore,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_release_id_names_the_version_and_the_commit() {
        assert_eq!(
            release_id("0.17.0", "a1b2c3d4e5f6"),
            "ferrosa@0.17.0+a1b2c3d4e5f6"
        );
    }

    #[test]
    fn an_unknown_commit_falls_back_to_the_version() {
        assert_eq!(release_id("0.17.0", "unknown"), "ferrosa@0.17.0");
        assert_eq!(release_id("0.17.0", ""), "ferrosa@0.17.0");
    }

    /// A build nobody else can reproduce should say so.
    #[test]
    fn a_dirty_build_is_marked() {
        assert!(release_id("0.17.0", "abc123-dirty").ends_with("+abc123-dirty"));
    }

    /// The shape an unset variable takes in a shell script.
    #[test]
    fn an_empty_dsn_is_absent_not_broken() {
        assert!(usable_dsn(None).is_none());
        assert!(usable_dsn(Some("")).is_none());
        assert!(usable_dsn(Some("   ")).is_none());
    }

    /// Plain http would put diagnostics on the wire in the clear.
    #[test]
    fn a_non_https_dsn_is_refused() {
        assert!(usable_dsn(Some("http://k@example.com/1")).is_none());
        assert!(usable_dsn(Some("not-a-dsn")).is_none());
    }

    #[test]
    fn a_usable_dsn_is_accepted_and_trimmed() {
        assert_eq!(
            usable_dsn(Some("  https://k@o1.ingest.us.sentry.io/2 ")),
            Some("https://k@o1.ingest.us.sentry.io/2")
        );
    }

    /// A build.rs that silently stopped running fails here, not in a report
    /// months from now.
    #[test]
    fn this_build_carries_a_stamp() {
        assert!(
            !BUILD_SHA.is_empty(),
            "build.rs did not set FERROSA_BUILD_SHA"
        );
    }
}
