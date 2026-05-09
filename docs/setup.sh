#!/usr/bin/env bash
set -euo pipefail

# Ferrosa Database local bootstrap.
# Safe by default: clone/update only; no volume or data deletion.

FERROSA_SUITE_DIR="${FERROSA_SUITE_DIR:-$HOME/src/ferrosa-suite}"
FERROSA_REPO="${FERROSA_REPO:-https://github.com/bkearns/ferrosa.git}"

printf '\nFerrosa Database bootstrap\n'
printf '==========================\n\n'
printf 'Install directory: %s\n' "$FERROSA_SUITE_DIR"
printf 'Ferrosa repo:      %s\n\n' "$FERROSA_REPO"

mkdir -p "$FERROSA_SUITE_DIR"

if [[ -d "$FERROSA_SUITE_DIR/ferrosa/.git" ]]; then
  git -C "$FERROSA_SUITE_DIR/ferrosa" fetch --all --prune
else
  git clone "$FERROSA_REPO" "$FERROSA_SUITE_DIR/ferrosa"
fi

cat <<EOF

Ferrosa source is ready at:
  $FERROSA_SUITE_DIR/ferrosa

Common next steps:
  cd $FERROSA_SUITE_DIR/ferrosa
  cargo build --release

For Ferrosa Memory setup, run:
  curl -fsSL https://ferrosadb.com/setup-memory.sh | bash

EOF
