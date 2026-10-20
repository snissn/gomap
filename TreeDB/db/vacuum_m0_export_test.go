package db

// SetVacuumAfterBaseSnapshotHookForTesting exposes the existing recorder/base
// snapshot witness to the external M0 fixture only in test builds. Callers must
// install before starting vacuum, restore after joining it, and never run this
// package-global witness in parallel with another hook-using fixture.
func SetVacuumAfterBaseSnapshotHookForTesting(hook func()) func() {
	previous := testHookVacuumAfterBaseSnapshot
	testHookVacuumAfterBaseSnapshot = hook
	return func() { testHookVacuumAfterBaseSnapshot = previous }
}
