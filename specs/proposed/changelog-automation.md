# Proposed: Automated CHANGELOG and docs/LATEST Updates on Release

## Problem

`CHANGELOG.md` and `docs/LATEST` drift from shipped reality because they are updated
by hand. The nightly release workflow (`nightly-release.yml`) and the promote workflow
(`promote-release.yml`) do not touch either file.

## Why a direct workflow patch is risky

The `main` branch ruleset requires pull requests — direct pushes (even from
`github-actions[bot]`) are rejected with `GH013`. The nightly release workflow
deliberately avoids this by tagging only (the release commit is not pushed to `main`).
Wiring file-update steps into either workflow would require one of:

1. Opening a PR from the bot (a second `gh pr create` call), which adds merge
   ordering complexity and can create a queue of pending changelog PRs.
2. Using a bypass token with admin push permissions, which is a security expansion.
3. Appending to the files in the release commit and pushing to a separate ref.

Option 1 is the most maintainable. The proposed steps below implement it.

## Proposed addition to `promote-release.yml`

Add these steps **after** the existing `Promote tag to stable` step.
All steps use plain `run:` with no new third-party actions.

```yaml
      - uses: actions/checkout@de0fac2e4500dabe0009e67214ff5f5447ce83dd # v6.0.2
        with:
          fetch-depth: 0
          # Use a PAT or the default GITHUB_TOKEN; note GITHUB_TOKEN cannot
          # push to a branch that is then used to open a PR against main if
          # the branch protection requires signed commits. Use a bot PAT.
          token: ${{ secrets.GITHUB_TOKEN }}

      - name: Update docs/LATEST
        env:
          TAG: ${{ github.event.inputs.tag }}
        run: |
          echo "${TAG}" > docs/LATEST

      - name: Prepend CHANGELOG entry for stable release
        env:
          TAG: ${{ github.event.inputs.tag }}
          REPO: ${{ github.repository }}
        run: |
          version="${TAG#v}"
          date="$(date -u +%F)"
          # Fetch the release notes body (strip the boilerplate "What's Included" block).
          notes="$(gh release view "${TAG}" --repo "${REPO}" --json body -q .body \
            | sed '/^## What'\''s Included/,$d' \
            | sed '/^[[:space:]]*$/d')"
          # Build the entry — if notes are empty (pure boilerplate release),
          # write a minimal placeholder so the entry is at least present.
          if [ -z "$notes" ]; then
            notes="See [release page](https://github.com/${REPO}/releases/tag/${TAG}) for assets and checksums."
          fi
          entry="## [${version}] - ${date} <!-- STABLE -->

${notes}

[${version}]: https://github.com/${REPO}/compare/${TAG}...${TAG}
"
          # Prepend after the header block (after the second blank line).
          python3 - <<'PYEOF'
          import os, re, sys
          tag = os.environ['TAG'].lstrip('v')
          entry = os.environ.get('ENTRY', '')
          with open('CHANGELOG.md', 'r') as f:
              content = f.read()
          # Insert after the second occurrence of a blank line following the header.
          marker = '<!-- NIGHTLY releases'
          idx = content.find(marker)
          if idx == -1:
              # Fallback: insert after first H2
              idx = content.find('\n## ')
          insert_at = idx if idx != -1 else len(content)
          content = content[:insert_at] + entry + '\n' + content[insert_at:]
          with open('CHANGELOG.md', 'w') as f:
              f.write(content)
          PYEOF

      - name: Open PR for changelog + LATEST update
        env:
          GH_TOKEN: ${{ secrets.GITHUB_TOKEN }}
          TAG: ${{ github.event.inputs.tag }}
        run: |
          branch="docs/stable-${TAG}"
          git config user.name "github-actions[bot]"
          git config user.email "41898282+github-actions[bot]@users.noreply.github.com"
          git checkout -b "${branch}"
          git add docs/LATEST CHANGELOG.md
          git commit -m "docs: update LATEST and CHANGELOG for stable ${TAG}"
          git push origin "${branch}"
          gh pr create \
            --title "docs: update LATEST and CHANGELOG for stable ${TAG}" \
            --body "Automated follow-up from the Promote Release workflow. Updates \`docs/LATEST\` to \`${TAG}\` and prepends a CHANGELOG entry for the newly promoted stable release." \
            --base main \
            --head "${branch}"
```

## Implementation notes

- Pin `actions/checkout` by SHA (already pinned above to the same SHA used elsewhere
  in the repo). No new actions are introduced beyond what already exists.
- The `sed` pipeline strips the boilerplate "What's Included" section that the release
  build workflow appends automatically. If the release body is pure boilerplate, a
  minimal placeholder is written so the CHANGELOG entry is not empty.
- The PR is opened against `main` and goes through normal CI + review. It will not
  block the promotion itself (the `Promote tag to stable` step runs first).
- The CHANGELOG prepend uses `python3` (available on all GitHub-hosted runners) to
  avoid fragile `sed` multi-line insert patterns across platforms.
- If `GITHUB_TOKEN` cannot push to branches that target protected `main`, replace it
  with a repository secret holding a bot PAT with `contents: write` on the repo.

## Alternative: nightly-release.yml (not recommended)

Adding this to the nightly workflow would create a PR for every prerelease cut,
generating noise. The promote workflow is the right trigger because it is the moment
a release becomes user-facing stable.
