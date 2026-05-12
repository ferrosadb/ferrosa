#!/usr/bin/env bash
set -euo pipefail

# Ferrosa Memory local onboarding bootstrap.
# This script is safe by default: it creates directories, downloads public files,
# and optionally clones/updates repos. It never deletes data or volumes.

FERROSA_SUITE_DIR="${FERROSA_SUITE_DIR:-$HOME/src/ferrosa-suite}"
FERROSA_MEMORY_REPO="${FERROSA_MEMORY_REPO:-https://github.com/ferrosadb/ferrosa-memory.git}"
FERROSA_REPO="${FERROSA_REPO:-https://github.com/ferrosadb/ferrosa.git}"
ONBOARDING_URL="${ONBOARDING_URL:-https://raw.githubusercontent.com/ferrosadb/ferrosa-memory/main/ONBOARDING.md}"
ONBOARDING_PATH="${ONBOARDING_PATH:-$FERROSA_SUITE_DIR/ferrosa-memory/ONBOARDING.md}"
NOMIC_MODEL="${NOMIC_MODEL:-nomic-embed-text-v2-moe}"

printf '\nFerrosa Memory onboarding bootstrap\n'
printf '=================================\n\n'
printf 'Install directory: %s\n' "$FERROSA_SUITE_DIR"
printf 'Ferrosa repo:      %s\n' "$FERROSA_REPO"
printf 'Memory repo:       %s\n' "$FERROSA_MEMORY_REPO"
printf 'Embedding model:   %s (optional)\n\n' "$NOMIC_MODEL"

mkdir -p "$FERROSA_SUITE_DIR"

ask_yes_no() {
  local prompt="$1" default="${2:-y}" answer
  if [[ ! -t 0 ]]; then
    [[ "$default" == "y" ]]
    return
  fi
  read -r -p "$prompt [$default] " answer || answer="$default"
  answer="${answer:-$default}"
  [[ "$answer" =~ ^[Yy]$|^[Yy][Ee][Ss]$ ]]
}

clone_or_update() {
  local url="$1" dir="$2"
  if [[ -d "$dir/.git" ]]; then
    printf '\nUpdating %s\n' "$dir"
    git -C "$dir" fetch --all --prune
  else
    printf '\nCloning %s -> %s\n' "$url" "$dir"
    git clone "$url" "$dir"
  fi
}

if ask_yes_no "Clone or update ferrosa and ferrosa-memory now?" "y"; then
  clone_or_update "$FERROSA_REPO" "$FERROSA_SUITE_DIR/ferrosa"
  clone_or_update "$FERROSA_MEMORY_REPO" "$FERROSA_SUITE_DIR/ferrosa-memory"
fi

if [[ ! -f "$ONBOARDING_PATH" ]]; then
  mkdir -p "$(dirname "$ONBOARDING_PATH")"
  printf '\nDownloading ONBOARDING.md from %s\n' "$ONBOARDING_URL"
  curl -fsSL "$ONBOARDING_URL" -o "$ONBOARDING_PATH"
fi

if command -v ollama >/dev/null 2>&1; then
  if ask_yes_no "Pull optional Nomic embedding model for semantic search?" "y"; then
    ollama pull "$NOMIC_MODEL"
  else
    printf '\nSkipping embeddings. Semantic/vector search will be degraded until %s is available.\n' "$NOMIC_MODEL"
  fi
else
  printf '\nOllama not found. Skipping embeddings. Semantic/vector search will be degraded until %s is available.\n' "$NOMIC_MODEL"
fi

printf '\nONBOARDING.md is ready at:\n  %s\n\n' "$ONBOARDING_PATH"

if command -v hermes >/dev/null 2>&1 && ask_yes_no "Launch Hermes with the onboard-me prompt now?" "y"; then
  exec hermes "onboard me using $ONBOARDING_PATH"
fi

printf 'Next: run your preferred LLM harness with the onboard-me prompt.\n\n'
printf 'Hermes:\n  hermes "onboard me using %s"\n\n' "$ONBOARDING_PATH"
printf 'Claude/Codex or another harness:\n  onboard me using %s\n\n' "$ONBOARDING_PATH"
printf 'The onboarding prompt will ask about native vs Compose runtime, skills, hooks, prompts, credentials, and ports.\n'
