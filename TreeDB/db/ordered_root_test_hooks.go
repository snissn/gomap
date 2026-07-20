package db

// OrderedRootStaleBuildHooksForTest exposes the two deterministic publication
// rendezvous points needed by package-external crash certification. These
// hooks are inert unless explicitly installed by a test.
type OrderedRootStaleBuildHooksForTest struct {
	AfterFinalizeRootSerializationRelease func()
	BeforeFinalizeCommit                  func()
}

// SetOrderedRootStaleBuildHooksForTest replaces the stale-build rendezvous
// hooks and returns a restore function. Callers must install or replace hooks
// only while their test controls the publication rendezvous.
func (db *DB) SetOrderedRootStaleBuildHooksForTest(hooks OrderedRootStaleBuildHooksForTest) func() {
	if db == nil {
		return func() {}
	}
	previous := OrderedRootStaleBuildHooksForTest{
		AfterFinalizeRootSerializationRelease: db.testAfterFinalizeRootSerializationReleaseHook,
		BeforeFinalizeCommit:                  db.testBeforeFinalizeCommitHook,
	}
	db.testAfterFinalizeRootSerializationReleaseHook = hooks.AfterFinalizeRootSerializationRelease
	db.testBeforeFinalizeCommitHook = hooks.BeforeFinalizeCommit
	return func() {
		db.testAfterFinalizeRootSerializationReleaseHook = previous.AfterFinalizeRootSerializationRelease
		db.testBeforeFinalizeCommitHook = previous.BeforeFinalizeCommit
	}
}
