package valuelog

// PauseDirectDeletionForTest exposes the existing admission barrier only to
// external-package integration tests. Install before starting deletion and join
// the deletion before disposing the manager.
func PauseDirectDeletionForTest(manager *Manager, admitted chan<- struct{}, resume <-chan struct{}) {
	manager.directDeleteAdmissionHook = func() {
		close(admitted)
		<-resume
	}
}
