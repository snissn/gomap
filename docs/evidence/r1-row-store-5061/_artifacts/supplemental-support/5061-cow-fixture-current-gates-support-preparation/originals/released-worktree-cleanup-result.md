# Released graph5056 worktree cleanup

Removed all11 exact released worktrees with `git worktree remove` without force. Kept0 allowlisted worktrees. No branch deletion, clean, prune, Go, source or GitHub operations.

Observed available space: 4139140 → 6618488 KiB; observed increase 2479348 KiB (2.364 GiB). Released worktree size estimate: 2465656 KiB (2.351 GiB). These are separate measures; concurrent filesystem activity can affect the observed delta.

All11 immediate inode/device/type, full sorted entry-metadata SHA, clean/untracked/ignored, registration/lock/nested/mount, commit/ref and live lsof cwd/open-use checks matched. All removed paths are absent and unregistered; their branch refs and commits remain exact in primary common Git. Existing protected paths remain present, explicit protected filesystem identities match, and unrelated registration path set is unchanged.

An unrelated Quicksilver worktree `/private/tmp/gomap-qs-5016` advanced HEAD `130979993704af3215767ca496da78e6d9a155e0` to `6c3859e02d0e71c54ca9ef7258884024b10fb2a7` on its existing branch during the batch. This triggered the initial strict final equality assertion; reconciliation records it as observed, rather than claiming all global refs were unchanged. The cleaner performed no operation on that worktree.

Removed:

- `/private/tmp/gomap-r1-5057`
- `/private/tmp/gomap-r1-5058`
- `/private/tmp/gomap-r1-5059`
- `/private/tmp/gomap-r1-5060-profile`
- `/private/tmp/gomap-r1-5061`
- `/private/tmp/gomap-r1-5065-ci-fix`
- `/private/tmp/gomap-r1-5065-range-fix`
- `/private/tmp/gomap-r1-5065-review-tooling-repair`
- `/private/tmp/gomap-r1-5065-scalar-stats`
- `/private/tmp/gomap-r1-5066-pack-projection`
- `/private/tmp/gomap-r1-native-appender-metadata`

Detailed original release snapshots, immediate checks, lsof evidence, native removal receipts and protected verification are in `released-worktree-cleanup-result.json`.
