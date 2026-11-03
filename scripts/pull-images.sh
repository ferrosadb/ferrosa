#!/usr/bin/env bash
# Pull one or more container images with a bounded retry.
#
#   scripts/pull-images.sh IMAGE [IMAGE...]
#
# Why this exists: a bare `docker pull` in a workflow makes a CI job depend on
# a registry we do not control, and one transient failure fails the job. On
# 2026-09-29 a connection reset while pulling a manifest failed "Example CQL
# Scripts" outright, and the same class has already cost several runs on
# Docker Hub base-image pulls (see forge t_75ec87fd).
#
# This is a backstop, not a substitute for hosting the image ourselves: an
# image served from downloads.ferrosa.ai removes the upstream dependency, and
# the retry only covers the case where our own origin hiccups.
#
# Retries are bounded and the failure is reported as a pull failure naming the
# image, so a broken run never looks like a storage or cluster fault.
#
# A rate limit is not a blip: downloads.ferrosa.ai sits behind Cloudflare and
# answers a burst of concurrent CI pulls with a "Too Many Requests" page, which
# docker surfaces as `toomanyrequests`. That is a window to wait out, so those
# failures back off much further than a connection reset does. Both schedules
# are bounded (about 4 minutes worst case) and the last attempt is reported.
set -euo pipefail

[ $# -ge 1 ] || { echo "usage: $0 IMAGE [IMAGE...]" >&2; exit 2; }

MAX_ATTEMPTS=5

# Seconds to wait after a failed attempt. Rate limits get a window-sized wait;
# everything else gets the short linear schedule it always had.
backoff_seconds() {
    local attempt="$1" output_file="$2"
    if grep -qiE 'toomanyrequests|too many requests|rate limit|429' "$output_file"; then
        echo $((attempt * 20))
    else
        echo $((attempt * 5))
    fi
}

failed=0
for image in "$@"; do
    attempt=1
    while :; do
        if docker pull "$image" 2>&1 | tee /tmp/pull-images.$$.log; then
            break
        fi
        if [ "$attempt" -ge "$MAX_ATTEMPTS" ]; then
            echo "::error::could not pull $image after $MAX_ATTEMPTS attempts" >&2
            failed=1
            break
        fi
        wait_for=$(backoff_seconds "$attempt" /tmp/pull-images.$$.log)
        echo "pull failed for $image (attempt $attempt/$MAX_ATTEMPTS); retrying in ${wait_for}s" >&2
        sleep "$wait_for"
        attempt=$((attempt + 1))
    done
done
rm -f /tmp/pull-images.$$.log

exit "$failed"
