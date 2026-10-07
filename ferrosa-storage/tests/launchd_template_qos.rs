//! The shipped macOS LaunchAgent must not run ferrosa as a Background job.
//!
//! launchd's `ProcessType = Background` puts every thread at darwinbg QoS:
//! scheduling priority 4, efficiency cores only, ~100 ms timer leeway, and
//! tier-3 throttled I/O. Measured on the local cluster (2026-10-07), a
//! background process doing the commit log's 440-byte append + F_FULLFSYNC
//! took up to 1.9 s (one `write()` alone 1.5 s) while a normal process on the
//! same disk at the same moment never exceeded 89 ms; the three nodes' slow
//! syncs and runtime stalls start within the same half second. A database
//! whose write acknowledgement waits on its own sync thread cannot run there.

const TEMPLATE: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../launchd/com.ferrosadb.ferrosa.plist"
));

/// The `<string>` value that follows `<key>name</key>`, if any.
fn plist_string_after_key<'a>(plist: &'a str, name: &str) -> Option<&'a str> {
    let key = format!("<key>{name}</key>");
    let rest = &plist[plist.find(&key)? + key.len()..];
    let start = rest.find("<string>")? + "<string>".len();
    let end = rest[start..].find("</string>")?;
    // The value must belong to this key, not to a later one.
    if rest[..start].contains("<key>") {
        return None;
    }
    Some(&rest[start..start + end])
}

#[test]
fn the_launch_agent_template_runs_ferrosa_interactive_not_background() {
    assert_eq!(
        plist_string_after_key(TEMPLATE, "ProcessType"),
        Some("Interactive"),
        "ferrosa must not be throttled by launchd; see this file's module doc"
    );
}

#[test]
fn the_key_lookup_does_not_borrow_a_later_keys_value() {
    let plist = "<key>ProcessType</key><true/><key>Label</key><string>x</string>";
    assert_eq!(plist_string_after_key(plist, "ProcessType"), None);
}
