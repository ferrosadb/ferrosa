#!/bin/sh
# Register a macOS LaunchAgent so the job ends up LOADED, or fail loudly.
#
# Sourced by the installers. Kept in `launchd/` beside the plist template it
# serves, and shipped in the release tarball, because the installers work out of
# the extracted tarball rather than a source clone.
#
# Two things this has to survive, both observed on this machine on 2026-10-14:
#
#  1. `launchctl bootout` returns BEFORE launchd has finished tearing the job
#     down. A `bootstrap` issued straight after therefore races that teardown and
#     fails with:
#
#         Bootstrap failed: 5: Input/output error
#
#     It is not a plist fault and not a permissions problem: `plutil -lint`
#     passes on the same file, and re-running the identical command a few seconds
#     later returns 0. So a bare bootout-then-bootstrap is a coin flip.
#
#  2. A `bootstrap` that fails leaves the job GONE. Announcing success over it
#     leaves a machine with no service while the installer says it is loaded --
#     which is how a three-node cluster was once left completely down by a rollout
#     that printed "restarted" for every node.
#
# Hence: wait for the job to actually be gone, retry with backoff, and PROVE the
# job is registered before returning success.
#
# Usage: register_launchd_agent <label> <plist-path>
register_launchd_agent() {
    _rla_label=$1
    _rla_plist=$2
    _rla_domain="gui/$(id -u)"

    if [ -z "$_rla_label" ] || [ -z "$_rla_plist" ]; then
        echo "launchd: register_launchd_agent needs a label and a plist path" >&2
        return 1
    fi
    if [ ! -f "$_rla_plist" ]; then
        echo "launchd: no plist at $_rla_plist" >&2
        return 1
    fi

    launchctl bootout "$_rla_domain/$_rla_label" 2>/dev/null || true

    # Let launchd finish the teardown. `print` succeeding is the only honest
    # signal that the job is still registered; the bootout return value is not.
    _rla_waited=0
    while launchctl print "$_rla_domain/$_rla_label" >/dev/null 2>&1; do
        if [ "$_rla_waited" -ge 15 ]; then
            echo "launchd: $_rla_label is still registered after ${_rla_waited}s; bootstrapping anyway" >&2
            break
        fi
        sleep 1
        _rla_waited=$((_rla_waited + 1))
    done

    # Backoff, because the teardown can outlive the wait above.
    _rla_attempt=1
    while ! launchctl bootstrap "$_rla_domain" "$_rla_plist" 2>/dev/null; do
        if [ "$_rla_attempt" -ge 6 ]; then
            echo "launchd: FAILED to load $_rla_label after $_rla_attempt attempts" >&2
            echo "launchd: the service is NOT running. By hand:" >&2
            echo "  launchctl bootstrap $_rla_domain $_rla_plist" >&2
            return 1
        fi
        sleep $((_rla_attempt * 2))
        _rla_attempt=$((_rla_attempt + 1))
    done

    # Never claim a success we have not verified: bootstrap can report OK for a
    # job that is not registered.
    if ! launchctl print "$_rla_domain/$_rla_label" >/dev/null 2>&1; then
        echo "launchd: $_rla_label reported a successful bootstrap but is NOT registered" >&2
        return 1
    fi

    return 0
}
