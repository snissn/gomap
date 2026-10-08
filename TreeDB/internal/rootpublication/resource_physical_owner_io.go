package rootpublication

import (
	"errors"
	"io"
	"os"
	"unsafe"
)

// ErrStableSegmentIO is deliberately scalar. A platform PathError can alias
// the pinned file's account-owned name; selected operational methods do not
// export that backing beyond the checked producer operation.
var ErrStableSegmentIO = errors.New("rootpublication: exact segment operation failed")

// prepareStableSegmentIO admits every attempted primitive's Go heap backing
// before the primitive runs. These are cumulative operation births, not a
// resident loan certificate and not a claim about process-global runtime pools.
func prepareStableSegmentIO(account StableMetadataAccount, stats, errors uint64) error {
	if account == nil {
		return nil
	}
	if err := finiteStablePlatform(); err != nil {
		return err
	}
	var total uint64
	for _, item := range [...]struct{ count, bytes uint64 }{
		{stats, uint64(unsafe.Sizeof(finiteLinuxStat{}))},
		{errors, uint64(unsafe.Sizeof(os.PathError{}))},
	} {
		class, err := StableBackingClassBytes(item.bytes, true)
		if err != nil || item.count != 0 && class > ^uint64(0)/item.count {
			return ErrStableMetadataShapeUnsupported
		}
		total, err = finiteStableAdd(total, class*item.count)
		if err != nil {
			return err
		}
	}
	return account.ReserveStableMetadata(total)
}

// Size reads the exact installed handle. The caller holds the existing segment
// serializer through projection and installation; this method neither creates
// a handle nor exports one. Its numeric result carries no publication authority.
func (o *StableSegmentOwner) Size(account StableMetadataAccount) (uint64, error) {
	if o == nil {
		return 0, ErrResourceOwnership
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.released.Load() || o.token == nil || o.refs.Load() <= 0 || o.token.pinned == nil {
		return 0, ErrResourceOwnership
	}
	if err := prepareStableSegmentIO(account, 1, 1); err != nil {
		return 0, err
	}
	info, err := o.token.pinned.Stat()
	if err != nil {
		return 0, ErrStableSegmentIO
	}
	if info.Size() < 0 {
		return 0, ErrResourceConflict
	}
	return uint64(info.Size()), nil
}

// WriteAt appends to the checked exact frontier without sharing a file cursor.
// Every writer of this installed segment must hold the same existing segment
// stripe. The actual written count survives partial I/O; callers retain the
// real owner/credit and refuse success rather than retrying consumed COW or
// inventing another address. No truncate/rollback or durability is implied.
func (o *StableSegmentOwner) WriteAt(expectedLength uint64, payload []byte, account StableMetadataAccount) (int, error) {
	if o == nil {
		return 0, ErrResourceOwnership
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.released.Load() || o.token == nil || o.refs.Load() <= 0 || o.token.pinned == nil {
		return 0, ErrResourceOwnership
	}
	const maxInt64 = uint64(1<<63 - 1)
	if len(payload) == 0 || expectedLength > maxInt64 || uint64(len(payload)) > maxInt64-expectedLength {
		return 0, ErrStableMetadataShapeUnsupported
	}
	if err := prepareStableSegmentIO(account, 1, 2); err != nil {
		return 0, err
	}
	info, err := o.token.pinned.Stat()
	if err != nil {
		return 0, ErrStableSegmentIO
	}
	if info.Size() < 0 || uint64(info.Size()) != expectedLength {
		return 0, ErrResourceConflict
	}
	n, err := o.token.pinned.WriteAt(payload, int64(expectedLength))
	if err != nil {
		return n, ErrStableSegmentIO
	}
	if n != len(payload) {
		return n, io.ErrShortWrite
	}
	return n, nil
}

// Sync performs only the exact physical file barrier. The caller records its
// actually synchronized frontier and captures the ordinary owner afterwards;
// successful Sync does not itself construct a frontier token or namespace proof.
func (o *StableSegmentOwner) Sync(account StableMetadataAccount) error {
	if o == nil {
		return ErrResourceOwnership
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.released.Load() || o.token == nil || o.refs.Load() <= 0 || o.token.pinned == nil {
		return ErrResourceOwnership
	}
	if err := prepareStableSegmentIO(account, 0, 1); err != nil {
		return err
	}
	if err := SyncStableFile(o.token.pinned); err != nil {
		return ErrStableSegmentIO
	}
	return nil
}

// Identity returns a value only; neither the pinned handle nor namespace/control
// escapes. The owner remains retained by its installed producer or output token.
func (o *StableSegmentOwner) Identity() (StableIdentity, error) {
	if o == nil {
		return StableIdentity{}, ErrResourceOwnership
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.token == nil || o.refs.Load() == 0 || o.released.Load() {
		return StableIdentity{}, ErrResourceOwnership
	}
	return o.token.identity, nil
}

// RollbackAppend restores only the exact just-appended frontier under the same
// producer stripe. Older captured frontiers are unchanged; no path is reopened.
func (o *StableSegmentOwner) RollbackAppend(expected, previous uint64, account StableMetadataAccount) error {
	if o == nil || previous > expected || expected > uint64(^uint64(0)>>1) {
		return ErrResourceConflict
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.token == nil || o.token.pinned == nil || o.refs.Load() == 0 || o.released.Load() {
		return ErrResourceOwnership
	}
	if err := prepareStableSegmentIO(account, 1, 3); err != nil {
		return err
	}
	info, err := o.token.pinned.Stat()
	if err != nil {
		return ErrStableSegmentIO
	}
	if info.Size() < 0 {
		return ErrResourceConflict
	}
	actual := uint64(info.Size())
	if actual != expected && actual != previous {
		return ErrResourceConflict
	}
	// A previous attempt may have truncated successfully and failed its sync.
	// The SAME stripe and retained debt authorize only this exact idempotent pair.
	if actual == expected && actual != previous {
		if err := o.token.pinned.Truncate(int64(previous)); err != nil {
			return ErrStableSegmentIO
		}
	}
	if err := SyncStableFile(o.token.pinned); err != nil {
		return ErrStableSegmentIO
	}
	return nil
}

// RemoveForDelete uses the same existing registry reservation and exact retained
// parent/child closure. The caller holds the existing segment stripe; the lease
// denies new captures. Missing entries are retryable only under this exact gate.
func (o *StableSegmentOwner) RemoveForDelete(lease *IdentityDeleteLease, account StableMetadataAccount) error {
	if o == nil || lease == nil {
		return ErrResourceOwnership
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.released.Load() || o.token == nil || o.token.pinned == nil || o.token.namespace == nil {
		return ErrResourceOwnership
	}
	token := o.token
	if lease.identity != physicalStableIdentity(token.identity) {
		return ErrResourceConflict
	}
	if err := lease.CheckDrained(); err != nil {
		return err
	}
	ns := token.namespace
	ns.mu.Lock()
	defer ns.mu.Unlock()
	if ns.parent == nil || !ns.hasLinkedResource || !sameStableObject(ns.linkedResourceIdentity, token.identity) {
		return ErrResourceOwnership
	}
	if err := prepareStableSegmentIO(account, 0, 2); err != nil {
		return err
	}
	if err := ValidateStableChildLink(ns.parent, token.pinned, ns.newName); err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return ErrStableSegmentIO
		}
	} else {
		if err := RemoveStableChildFile(ns.parent, ns.newName); err != nil && !errors.Is(err, os.ErrNotExist) {
			return ErrStableSegmentIO
		}
	}
	return nil
}
func (o *StableSegmentOwner) SyncDeletedNamespace(account StableMetadataAccount) error {
	if o == nil {
		return ErrResourceOwnership
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.released.Load() || o.token == nil || o.token.namespace == nil {
		return ErrResourceOwnership
	}
	ns := o.token.namespace
	ns.mu.Lock()
	defer ns.mu.Unlock()
	if ns.persistence == nil {
		return ErrResourceOwnership
	}
	if err := prepareStableSegmentIO(account, 0, 1); err != nil {
		return err
	}
	if err := ns.adapter.Sync(ns.persistence); err != nil {
		return ErrStableSegmentIO
	}
	return nil
}
