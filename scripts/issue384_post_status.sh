#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$ROOT"

ISSUE_NUMBER="${ISSUE_NUMBER:-384}"
REPO="${REPO:-snissn/gomap}"
MILESTONE="${MILESTONE:-unspecified}"
SUMMARY_PATH="${SUMMARY_PATH:-}"
ARTIFACT_DIR="${ARTIFACT_DIR:-}"
PR_NUMBER="${PR_NUMBER:-}"

if [[ -z "$SUMMARY_PATH" ]]; then
  echo "SUMMARY_PATH is required" >&2
  exit 2
fi
if [[ ! -f "$SUMMARY_PATH" ]]; then
  echo "summary not found: $SUMMARY_PATH" >&2
  exit 2
fi

CAND_HASH=$(git rev-parse HEAD)
BRANCH=$(git rev-parse --abbrev-ref HEAD)
TS=$(date -u +"%Y-%m-%dT%H:%M:%SZ")

BODY_FILE=$(mktemp)
{
  echo "Issue #$ISSUE_NUMBER status update ($MILESTONE)"
  echo
  echo "- timestamp (UTC): \\`$TS\\`"
  echo "- branch: \\`$BRANCH\\`"
  echo "- candidate commit: \\`$CAND_HASH\\`"
  if [[ -n "$ARTIFACT_DIR" ]]; then
    echo "- artifact dir: \\`$ARTIFACT_DIR\\`"
  fi
  echo
  echo "### Gate summary"
  echo
  cat "$SUMMARY_PATH"
} > "$BODY_FILE"

gh issue comment "$ISSUE_NUMBER" --repo "$REPO" --body-file "$BODY_FILE"

if [[ -n "$PR_NUMBER" ]]; then
  gh pr comment "$PR_NUMBER" --repo "$REPO" --body-file "$BODY_FILE"
fi

rm -f "$BODY_FILE"
