package db

import "testing"

// OrderedRootStaleBuildHooksForTest exposes the two deterministic publication
// rendezvous points needed by package-external crash certification. Because
// this file is compiled only by `go test`, no hook installation API is present
// in production binaries.
type OrderedRootStaleBuildHooksForTest struct {
	AfterFinalizeRootSerializationRelease func()
	BeforeFinalizeCommit                  func()
}

// SetOrderedRootStaleBuildHooksForTest replaces the stale-build rendezvous
// hooks and returns a restore function. The certification test serializes hook
// replacement with the rendezvous it controls.
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

// CreateSavedPrimaryFormatV5ForTest binds the retained META witness to the
// complete production V5 initializer; it is absent from production builds.
func CreateSavedPrimaryFormatV5ForTest(t *testing.T, dir string) {
	createSavedPrimaryFormatV5(t, dir, false)
}
