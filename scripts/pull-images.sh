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
# image, so a broken run never looks like a storage or cluster fault. Four
# attempts with linear backoff (5s, 10s, 15s).
set -euo pipefail

[ $# -ge 1 ] || { echo "usage: $0 IMAGE [IMAGE...]" >&2; exit 2; }

failed=0
for image in "$@"; do
    for attempt in 1 2 3 4; do
        if docker pull "$image"; then
            break
        fi
        if [ "$attempt" -eq 4 ]; then
            echo "::error::could not pull $image after 4 attempts" >&2
            failed=1
            break
        fi
        echo "pull failed for $image (attempt $attempt/4); retrying in $((attempt * 5))s" >&2
        sleep $((attempt * 5))
    done
done

exit "$failed"
