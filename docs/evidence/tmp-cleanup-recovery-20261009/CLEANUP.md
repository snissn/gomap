# Gomap temporary-file cleanup

Verified: 2026-10-09T01:39:26.072857+00:00

Deleted all 640 initially present authorized temporary targets from the explicit allowlist. Unregistered 18 temporary worktrees, including one already absent test checkout. The scope covered `/private/tmp`, the macOS per-user temporary directory, gomap's `tmp` directory, and gomap scratch directories on FlashDrive. The inventory represented approximately 180.72 GiB of logical data; shared filesystem blocks mean this is not a reclaimed-space measurement.

All deletion targets and their worktree registrations are absent. All 2331 protected primary files, primary HEAD, and source status match their original snapshots. Non-temporary development worktrees remain present. No gomap benchmark/build was started and no Linux evidence was modified. The active physics owner's shared coordination file was preserved.

Additional pre-existing source drafts and two historical CI repair commits were preserved on GitHub before deleting their local temporary copies: https://github.com/snissn/gomap/blob/ce89de9f6d1fd16b14f36f6a9f4bc7277c5a13c3/docs/evidence/tmp-cleanup-recovery-20261009/README.md. Existing graph handoff packets and source branches remain the recovery authority; preservation does not validate or select these drafts.

Observed free-space changes during cleanup:
- /System/Volumes/Data: 12.327 to 54.108 GiB free (increase 41.780 GiB).
- /Volumes/FlashDrive: 25.912 to 41.223 GiB free (increase 15.311 GiB).
