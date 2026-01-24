# Prompt: Execute `wal_deprecation_merge_blockers_runbook.md`

You are a Codex-style coding agent working in:
- `/Users/michaelseiler/dev/snissn/gomap`

Your task:
- Execute the merge-blocker runbook:
  - `slab-optimization/wal_deprecation_merge_blockers_runbook.md`

Hard requirements:
- Follow the runbook’s scope strictly: only #177/#178/#179.
- Treat #179 as merge-blocking; do not declare success without it.
- Use stacked PRs as described (one blocker per PR).
- Use GitHub CLI (`gh`) for PR creation and to watch CI:
  - `gh pr create ... --body-file ...`
  - `gh pr checks ... --watch`
- Do not paste PR bodies with escaped newlines; always use a body file.
- Run `GOWORK=off go test ./TreeDB/... -count=1` for each PR (or justify any narrower scope).

When done:
- Post a final comment on PR #176 summarizing what shipped and linking the blocker PRs.

