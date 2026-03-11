#!/usr/bin/env bash
set -euo pipefail

# setup-precommit.sh — Install pre-commit and configure hooks for this project
#
# Copied to projects by /op-init. Run once after cloning:
#   ./setup-precommit.sh
#
# What it does:
#   1. Installs pre-commit if not present (pipx > pip > brew)
#   2. Installs git hooks (pre-commit, commit-msg, pre-push)
#   3. Runs validation on all files

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

info()  { echo -e "${BLUE}[info]${NC}  $*"; }
ok()    { echo -e "${GREEN}[ok]${NC}    $*"; }
warn()  { echo -e "${YELLOW}[warn]${NC}  $*"; }
err()   { echo -e "${RED}[error]${NC} $*"; }

# --- Preflight ---
if [[ ! -f ".pre-commit-config.yaml" ]]; then
  err "No .pre-commit-config.yaml found in current directory."
  err "Run this script from the project root."
  exit 1
fi

if ! git rev-parse --is-inside-work-tree &>/dev/null; then
  err "Not inside a git repository."
  exit 1
fi

# --- Install pre-commit ---
install_precommit() {
  info "pre-commit not found. Installing..."

  if command -v pipx &>/dev/null; then
    info "Installing via pipx..."
    pipx install pre-commit
  elif command -v pip3 &>/dev/null; then
    info "Installing via pip3..."
    pip3 install --user pre-commit
  elif command -v pip &>/dev/null; then
    info "Installing via pip..."
    pip install --user pre-commit
  elif command -v brew &>/dev/null; then
    info "Installing via Homebrew..."
    brew install pre-commit
  else
    err "Cannot find pipx, pip, or brew to install pre-commit."
    err "Install manually: https://pre-commit.com/#install"
    exit 1
  fi

  # Verify installation
  if ! command -v pre-commit &>/dev/null; then
    # pip --user installs to ~/.local/bin which may not be in PATH
    if [[ -f "$HOME/.local/bin/pre-commit" ]]; then
      export PATH="$HOME/.local/bin:$PATH"
      warn "Added ~/.local/bin to PATH. Add this to your shell profile:"
      warn "  export PATH=\"\$HOME/.local/bin:\$PATH\""
    else
      err "pre-commit installed but not found in PATH."
      exit 1
    fi
  fi

  ok "pre-commit $(pre-commit --version) installed"
}

if command -v pre-commit &>/dev/null; then
  ok "pre-commit $(pre-commit --version) already installed"
else
  install_precommit
fi

# --- Install hooks ---
info "Installing git hooks..."
pre-commit install --install-hooks
pre-commit install --hook-type commit-msg
pre-commit install --hook-type pre-push
ok "Git hooks installed (pre-commit, commit-msg, pre-push)"

# --- Make Claude hooks executable ---
if [[ -d ".claude/hooks" ]]; then
  chmod +x .claude/hooks/*.sh 2>/dev/null || true
  ok "Claude Code hooks made executable"
fi

# --- Validate ---
info "Running pre-commit on all files (first run may be slow)..."
echo ""
if pre-commit run --all-files; then
  ok "All checks passed!"
else
  warn "Some checks failed. This is normal for a new project."
  warn "Fix issues and re-run: pre-commit run --all-files"
fi

# --- Summary ---
echo ""
echo "========================================="
ok "Pre-commit setup complete!"
echo ""
info "Hooks installed:"
echo "  pre-commit  — Runs on every commit (format, lint, secrets)"
echo "  commit-msg  — Enforces conventional commit messages"
echo "  pre-push    — Runs tests before push"
echo ""
info "Claude Code hooks:"
if [[ -f ".claude/hooks/require-branch.sh" ]]; then
  echo "  require-branch.sh — Blocks edits on main/master"
fi
echo ""
info "Useful commands:"
echo "  pre-commit run --all-files    # Run all checks"
echo "  pre-commit autoupdate         # Update hook versions"
echo "  git commit -m 'feat: ...'     # Conventional commit format"
echo "========================================="
