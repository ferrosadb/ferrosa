#!/usr/bin/env bash
set -euo pipefail

if [[ "$#" -ne 3 ]]; then
  echo "usage: $0 <run-id> <owner/repository> <destination>" >&2
  exit 64
fi

run_id="$1"
repository="$2"
destination="$3"

if [[ ! "$run_id" =~ ^[0-9]+$ ]]; then
  echo "invalid GitHub Actions run ID: $run_id" >&2
  exit 64
fi
if [[ "$repository" != */* || "$repository" == *[[:space:]]* ]]; then
  echo "invalid GitHub repository: $repository" >&2
  exit 64
fi

mkdir -p "$destination"
manifest="$destination/.artifact-manifest.tsv"

gh api --paginate \
  "repos/${repository}/actions/runs/${run_id}/artifacts?per_page=100" \
  --jq '.artifacts[] | [.name, (.id | tostring), .digest] | @tsv' \
  > "$manifest"

artifact_count=0
while IFS=$'\t' read -r artifact_name artifact_id expected_digest; do
  if [[ -z "$artifact_name" || "$artifact_name" == "." || "$artifact_name" == ".." || "$artifact_name" == */* ]]; then
    echo "invalid artifact name returned by GitHub: $artifact_name" >&2
    exit 65
  fi
  if [[ ! "$artifact_id" =~ ^[0-9]+$ ]]; then
    echo "invalid artifact ID returned by GitHub for $artifact_name: $artifact_id" >&2
    exit 65
  fi
  if [[ ! "$expected_digest" =~ ^sha256:[0-9a-f]{64}$ ]]; then
    echo "missing or malformed SHA-256 digest for artifact $artifact_name" >&2
    exit 65
  fi

  archive="$destination/.${artifact_name}.zip"
  gh api \
    -H "Accept: application/vnd.github+json" \
    "repos/${repository}/actions/artifacts/${artifact_id}/zip" \
    > "$archive"

  actual_digest="sha256:$(sha256sum "$archive" | awk '{print $1}')"
  if [[ "$actual_digest" != "$expected_digest" ]]; then
    echo "artifact digest mismatch for $artifact_name: expected $expected_digest, got $actual_digest" >&2
    exit 65
  fi

  artifact_directory="$destination/$artifact_name"
  mkdir -p "$artifact_directory"
  unzip -q "$archive" -d "$artifact_directory"
  rm -f "$archive"
  artifact_count=$((artifact_count + 1))
done < "$manifest"

if [[ "$artifact_count" -eq 0 ]]; then
  echo "GitHub Actions run $run_id has no downloadable artifacts" >&2
  exit 66
fi

rm -f "$manifest"
echo "Downloaded and verified $artifact_count artifact(s)."
