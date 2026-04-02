#!/bin/bash
# Remove Docker Desktop remnants that require sudo.
# Run: sudo bash scripts/uninstall-docker.sh

set -e

echo "Removing Docker CLI symlinks from /usr/local/bin..."
rm -f /usr/local/bin/docker
rm -f /usr/local/bin/docker-compose
rm -f /usr/local/bin/docker-credential-desktop
rm -f /usr/local/bin/docker-credential-ecr-login
rm -f /usr/local/bin/docker-credential-osxkeychain
rm -f /usr/local/bin/com.docker.cli
rm -f /usr/local/bin/kubectl.docker

echo "Removing protected Docker container directory..."
rm -rf "$HOME/Library/Containers/com.docker.docker"

echo "Docker fully removed."
