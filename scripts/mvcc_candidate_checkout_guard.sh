#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:?repository root is required}"
CANDIDATE_SHA="${2:?candidate SHA is required}"

CHECKED_OUT_SHA=$(git -C "$ROOT" rev-parse "HEAD^{commit}")
if [[ "$CHECKED_OUT_SHA" != "$CANDIDATE_SHA" ]]; then
  echo "candidate mismatch: checkout=$CHECKED_OUT_SHA requested=$CANDIDATE_SHA" >&2
  exit 2
fi

# The candidate binaries are compiled from the live checkout. Fail closed when
# tracked or ordinary untracked files could make those binaries differ from the
# recorded SHA. Git status excludes ignored benchmark artifacts by default;
# --untracked-files=normal still includes ordinary untracked source files.
DIRTY_STATUS=$(git -C "$ROOT" status --porcelain --untracked-files=normal)
if [[ -n "$DIRTY_STATUS" ]]; then
  echo "candidate worktree is dirty; refusing to attribute live-checkout binaries to $CANDIDATE_SHA" >&2
  echo "$DIRTY_STATUS" >&2
  exit 2
fi
