//! Module: Where this node's errors go besides the log.
//!
//! Correctness: Correct when an absent DSN starts nothing, when what is sent
//! names the build it came from, and when a warning does not arrive as an
//! error.
//!
//! Last revised: 2026-09-03
//! Last changed: Report whether reporting is on, once a subscriber exists.
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

/// Why this process is not reporting errors.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Inactive {
    /// `FERROSA_SENTRY_DSN` was unset, or set to an empty value.
    NotConfigured,
    /// A value was supplied but [`usable_dsn`] refused it.
    Rejected,
}

/// What [`start`] decided, kept so the outcome can be logged once a subscriber
/// exists to render it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Activation {
    /// Errors report to Sentry under this release identifier.
    Active {
        /// How this build identifies itself: `ferrosa@version+sha`.
        release: String,
    },
    /// Errors reach the log and nowhere else.
    Off(Inactive),
}

impl Activation {
    /// Say whether error reporting is on, and when it is not, why.
    ///
    /// Call this *after* the tracing subscriber is installed. [`start`] runs
    /// before it deliberately — the errors most worth having from a database
    /// are the ones raised while it is still starting — so a line logged from
    /// inside `start` has no subscriber to render it and is discarded. A
    /// control cluster ran for five days on 2026-09-03 with no DSN set and
    /// nothing in its logs said so, which is the failure this reports.
    ///
    /// Off is logged at WARN, not INFO: a database whose errors go nowhere but
    /// the local log is a degraded deployment, not a routine one.
    pub fn report(&self) {
        match self {
            Activation::Active { release } => {
                tracing::info!(%release, "errors report to Sentry");
            }
            Activation::Off(Inactive::NotConfigured) => {
                tracing::warn!(
                    dsn_var = DSN_VAR,
                    "errors report to the log only: no DSN is configured"
                );
            }
            Activation::Off(Inactive::Rejected) => {
                tracing::warn!(
                    dsn_var = DSN_VAR,
                    "errors report to the log only: the configured DSN is unusable (an https:// DSN is required)"
                );
            }
        }
    }
}

/// Decide whether a configured value activates error reporting.
///
/// Split from [`start`] so the decision is testable without the SDK or the
/// environment. An absent value and a malformed one are different outcomes
/// because they need different fixes: one deployment forgot, the other got it
/// wrong.
pub fn activation(configured: Option<&str>, version: &str, build_sha: &str) -> Activation {
    match configured.map(str::trim).filter(|v| !v.is_empty()) {
        None => Activation::Off(Inactive::NotConfigured),
        Some(value) => match usable_dsn(Some(value)) {
            Some(_) => Activation::Active {
                release: release_id(version, build_sha),
            },
            None => Activation::Off(Inactive::Rejected),
        },
    }
}

/// Start Sentry if the installer configured it.
///
/// The guard must live as long as the process: dropping it stops sending,
/// which quietly loses the errors that happen during shutdown — exactly the
/// ones worth having from a database.
///
/// Returns the decision alongside the guard so the caller can
/// [`Activation::report`] it once a subscriber exists.
#[must_use = "dropping the guard stops error reporting, and the activation must be reported"]
pub fn start() -> (Option<sentry::ClientInitGuard>, Activation) {
    let configured = std::env::var(DSN_VAR).ok();
    let decided = activation(configured.as_deref(), env!("CARGO_PKG_VERSION"), BUILD_SHA);
    let release = match &decided {
        Activation::Active { release } => release.clone(),
        Activation::Off(_) => return (None, decided),
    };
    // `Active` is only produced for a value `usable_dsn` accepted, so this
    // cannot be None. Matched rather than unwrapped: a panic here would take
    // down the node over its own diagnostics.
    let Some(dsn) = usable_dsn(configured.as_deref()) else {
        return (None, Activation::Off(Inactive::Rejected));
    };
    // Field by field: ClientOptions is #[non_exhaustive], so a struct
    // expression does not compile from outside the SDK.
    let mut options = sentry::ClientOptions::default();
    options.release = Some(release.into());
    // Never. A database node's environment names the machine and the operator.
    options.send_default_pii = false;
    let guard = sentry::init((dsn, options));
    (Some(guard), decided)
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

    /// An operator must be able to read that reporting is OFF, rather than
    /// infer it from the absence of a line. On 2026-09-03 a control cluster
    /// ran for five days with no DSN configured and nothing ever said so.
    #[test]
    fn an_unconfigured_dsn_is_off_and_says_why() {
        assert_eq!(
            activation(None, "0.22.0", "abc123"),
            Activation::Off(Inactive::NotConfigured)
        );
        // The shape an unset variable takes when a shell script exports it.
        assert_eq!(
            activation(Some("   "), "0.22.0", "abc123"),
            Activation::Off(Inactive::NotConfigured)
        );
    }

    /// A deployment that forgot the DSN and one that got it wrong need
    /// different answers, because they need different fixes.
    #[test]
    fn a_rejected_dsn_is_distinguishable_from_an_absent_one() {
        assert_eq!(
            activation(Some("http://k@example.com/1"), "0.22.0", "abc123"),
            Activation::Off(Inactive::Rejected)
        );
        assert_eq!(
            activation(Some("not-a-dsn"), "0.22.0", "abc123"),
            Activation::Off(Inactive::Rejected)
        );
    }

    #[test]
    fn an_active_activation_names_the_release() {
        assert_eq!(
            activation(
                Some("https://k@o1.ingest.us.sentry.io/2"),
                "0.22.0",
                "abc123"
            ),
            Activation::Active {
                release: "ferrosa@0.22.0+abc123".to_string()
            }
        );
    }

    /// The DSN embeds a write key. It must never reach a log line, and the
    /// value that gets reported is the one most likely to be logged.
    #[test]
    fn an_activation_never_carries_the_dsn() {
        let dsn = "https://deadbeefdeadbeef@o1.ingest.us.sentry.io/2";
        let rendered = format!("{:?}", activation(Some(dsn), "0.22.0", "abc123"));
        assert!(
            !rendered.contains("deadbeefdeadbeef"),
            "activation leaked the DSN key: {rendered}"
        );
    }
}
