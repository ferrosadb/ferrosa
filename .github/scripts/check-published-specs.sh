#!/usr/bin/env bash
#
# Refuse to add a new file under specs/ in a repository that is published.
#
# Why this exists. This repository is public, and its whole specs/ tree is
# served with it. A threat model, an unbuilt permission design, or an
# information-flow analysis written here is a published one — and publishing
# cannot be walked back, because a closed PR still leaves refs/pull/<n>/head.
# The private specs tree lives in the coordination repo.
#
# Why it looks backwards. This repository already has a large, well-organised
# specs/ tree, so writing a new spec beside the existing ones is the obvious
# move. That is exactly the mistake, and it is why a habit is not enough.
#
# Scope. Only NEWLY ADDED files. Existing specs predate this rule and are
# grandfathered; editing one is not what this guard is for.
#
# Escape hatch. A spec that is deliberately public carries the marker line
# below anywhere in its text. The marker is read from the STAGED content, not
# the working tree, so what is committed is what was checked.

set -euo pipefail

MARKER='public-spec: intentional'

# Compare against HEAD, or against the empty tree on a repo with no commits.
if git rev-parse --verify --quiet HEAD >/dev/null; then
    base=HEAD
else
    base=$(git hash-object -t tree /dev/null)
fi

if ! added=$(git diff --cached --name-only --diff-filter=A "$base"); then
    echo "check-published-specs: cannot read the staged file list" >&2
    echo "check-published-specs: refusing to pass a check that did not run" >&2
    exit 1
fi

offenders=""
while IFS= read -r f; do
    if [ -z "$f" ]; then
        continue
    fi
    case "$f" in
        specs/*) ;;
        *) continue ;;
    esac
    if git show ":$f" 2>/dev/null | grep -qF "$MARKER"; then
        continue
    fi
    offenders="${offenders}    ${f}"$'\n'
done <<< "$added"

if [ -z "$offenders" ]; then
    exit 0
fi

{
    echo
    echo "Refusing to add a new spec to a published directory."
    echo
    printf '%s' "$offenders"
    echo
    echo "This repository is published. Everything under specs/ here becomes"
    echo "world-readable on push, and that cannot be undone."
    echo
    echo "New specs belong in the coordination repo's private specs/ tree."
    echo
    echo "If this spec is meant to be public, add this line to it:"
    echo
    echo "    $MARKER"
    echo
    echo "Existing specs are unaffected; this only looks at newly added files."
    echo
} >&2

exit 1
