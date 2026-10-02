// Package commandwalbarrier contains the handoff between collection drains and
// the backend pre-append boundary. It is internal to TreeDB.
package commandwalbarrier

// PendingDrain asks the owning pre-append boundary to release raw publication,
// prepared admission, and teardown before draining higher-level pending work.
// After Drain succeeds the owner must acquire its leases again and rerun every
// barrier. This is never authority to retry an append or root publication.
type PendingDrain struct {
	Drain func() error
}

func (*PendingDrain) Error() string {
	return "treedb: pending collection commands require a drain outside raw publication"
}
