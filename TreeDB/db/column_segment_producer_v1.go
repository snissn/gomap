package db

import (
	"strings"
	"sync"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

const columnSegmentProducerCapacityV1 = 32

type columnSegmentProducerStateV1 uint8

const (
	columnSegmentConstructingV1 columnSegmentProducerStateV1 = iota + 1
	columnSegmentWritableV1
	columnSegmentRetiringV1
	columnSegmentRollbackV1
)

type columnSegmentProducerSlotV1 struct {
	namespace        string
	fileID           uint32
	incarnation      uint64
	state            columnSegmentProducerStateV1
	identity         rootpublication.StableIdentity
	owner            *rootpublication.StableSegmentOwner
	deletion         *rootpublication.IdentityDeleteLease
	unlinked         bool
	stripe           uint8
	operations       uint32
	finishing        bool
	rollbackExpected uint64
	rollbackPrevious uint64
}
type columnSegmentProducerTableV1 struct {
	mu            sync.Mutex
	cleanup       sync.Mutex // synchronous Close/retry invocation join; no callback or debt queue
	operations    sync.WaitGroup
	closing       bool
	next          uint64
	slots         [columnSegmentProducerCapacityV1]columnSegmentProducerSlotV1
	classBytes    uint64
	keyClassBytes uint64
}

// BeginColumnSegmentProducerV1 reserves one actual bounded DB slot before any
// file/owner birth. Its incarnation is a caller-owned scalar, not a handle.
// Ordinary construction stamps actual backing. Finite table creation remains
// unsupported until the explicit resident constructor is bound to this route.
func (db *DB) BeginColumnSegmentProducerV1(namespace string, fileID uint32, stripe uint8, account rootpublication.StableMetadataAccount) (incarnation uint64, ready bool, err error) {
	if db == nil || namespace == "" || fileID == 0 || int(stripe) >= rootpublication.SegmentWriteStripeCount {
		return 0, false, rootpublication.ErrResourceOwnership
	}
	if account != nil {
		return 0, false, rootpublication.ErrStableMetadataShapeUnsupported
	}
	db.columnSegmentProducersMu.Lock()
	if db.closing.Load() {
		db.columnSegmentProducersMu.Unlock()
		return 0, false, ErrClosed
	}
	table := db.columnSegmentProducers
	if table == nil {
		class, e := rootpublication.StableBackingClassBytes(uint64(unsafe.Sizeof(columnSegmentProducerTableV1{})), true)
		if e != nil {
			db.columnSegmentProducersMu.Unlock()
			return 0, false, e
		}
		table = &columnSegmentProducerTableV1{classBytes: class}
		db.columnSegmentProducers = table
	}
	db.columnSegmentProducersMu.Unlock()
	table.mu.Lock()
	defer table.mu.Unlock()
	if table.closing || db.closing.Load() {
		return 0, false, ErrClosed
	}
	empty := -1
	for i := range table.slots {
		slot := &table.slots[i]
		if slot.state == 0 {
			if empty < 0 {
				empty = i
			}
			continue
		}
		if slot.namespace == namespace && slot.fileID == fileID {
			if slot.state != columnSegmentWritableV1 || slot.stripe != stripe {
				return 0, false, rootpublication.ErrResourceOwnership
			}
			return slot.incarnation, true, nil
		}
	}
	if empty < 0 || table.next == ^uint64(0) {
		return 0, false, rootpublication.ErrStableMetadataShapeUnsupported
	}
	keyClass, e := rootpublication.StableBackingClassBytes(uint64(len(namespace)), false)
	if e != nil {
		return 0, false, e
	}
	// This is one real key allocation; the table has no mutable map/growth family.
	ownedNamespace := strings.Clone(namespace)
	table.next++
	table.keyClassBytes += keyClass
	// The real constructor owns one execution edge from this reservation until
	// exact Install or Cancel. Close joins this attempt outside all engine gates;
	// a placeholder alone cannot certify that its file/namespace work ended.
	table.operations.Add(1)
	table.slots[empty] = columnSegmentProducerSlotV1{namespace: ownedNamespace, fileID: fileID, incarnation: table.next, state: columnSegmentConstructingV1, stripe: stripe, operations: 1}
	return table.next, false, nil
}

func (db *DB) columnSegmentProducerTableV1() *columnSegmentProducerTableV1 {
	if db == nil {
		return nil
	}
	db.columnSegmentProducersMu.Lock()
	table := db.columnSegmentProducers
	db.columnSegmentProducersMu.Unlock()
	return table
}
func findColumnSegmentProducerV1(table *columnSegmentProducerTableV1, namespace string, fileID uint32, incarnation uint64) *columnSegmentProducerSlotV1 {
	for i := range table.slots {
		s := &table.slots[i]
		if s.state != 0 && s.namespace == namespace && s.fileID == fileID && s.incarnation == incarnation {
			return s
		}
	}
	return nil
}

// InstallColumnSegmentProducerV1 transfers the ordinary producer edge only
// when installed is true. Observe occurs before attachment, under the same
// table -> registry order used by capture. The constructor keeps its own edge.
func (db *DB) InstallColumnSegmentProducerV1(namespace string, fileID uint32, incarnation uint64, owner *rootpublication.StableSegmentOwner) (installed bool, err error) {
	identity, err := owner.Identity()
	if err != nil {
		return false, err
	}
	table := db.columnSegmentProducerTableV1()
	if table == nil {
		return false, rootpublication.ErrResourceOwnership
	}
	table.mu.Lock()
	defer table.mu.Unlock()
	slot := findColumnSegmentProducerV1(table, namespace, fileID, incarnation)
	if table.closing || db.closing.Load() || slot == nil || slot.state != columnSegmentConstructingV1 || slot.owner != nil || slot.operations != 1 {
		return false, rootpublication.ErrResourceOwnership
	}
	registry := db.StableResourceIdentityPinRegistry()
	if registry == nil {
		return false, rootpublication.ErrResourceOwnership
	}
	if err := registry.Observe(identity); err != nil {
		return false, err
	}
	slot.owner, slot.identity, slot.state = owner, identity, columnSegmentWritableV1
	slot.operations = 0
	table.operations.Done()
	return true, nil
}
func (db *DB) CancelColumnSegmentProducerV1(namespace string, fileID uint32, incarnation uint64) error {
	table := db.columnSegmentProducerTableV1()
	if table == nil {
		return rootpublication.ErrResourceOwnership
	}
	table.mu.Lock()
	slot := findColumnSegmentProducerV1(table, namespace, fileID, incarnation)
	if slot == nil || slot.state != columnSegmentConstructingV1 || slot.operations != 1 {
		table.mu.Unlock()
		return rootpublication.ErrResourceOwnership
	}
	keyClass, _ := rootpublication.StableBackingClassBytes(uint64(len(slot.namespace)), false)
	table.keyClassBytes -= keyClass
	*slot = columnSegmentProducerSlotV1{}
	table.mu.Unlock()
	table.operations.Done()
	return nil
}
func (db *DB) beginColumnSegmentOperationV1(namespace string, fileID uint32, incarnation uint64) (*columnSegmentProducerTableV1, *rootpublication.StableSegmentOwner, error) {
	table := db.columnSegmentProducerTableV1()
	if table == nil {
		return nil, nil, rootpublication.ErrResourceOwnership
	}
	table.mu.Lock()
	slot := findColumnSegmentProducerV1(table, namespace, fileID, incarnation)
	if table.closing || db.closing.Load() || slot == nil || slot.state != columnSegmentWritableV1 || slot.owner == nil {
		table.mu.Unlock()
		return nil, nil, rootpublication.ErrResourceOwnership
	}
	if slot.operations == ^uint32(0) {
		table.mu.Unlock()
		return nil, nil, rootpublication.ErrResourceOwnership
	}
	slot.operations++
	table.operations.Add(1)
	owner := slot.owner
	table.mu.Unlock()
	return table, owner, nil
}
func (db *DB) endColumnSegmentOperationV1(table *columnSegmentProducerTableV1, namespace string, fileID uint32, incarnation uint64) {
	table.mu.Lock()
	slot := findColumnSegmentProducerV1(table, namespace, fileID, incarnation)
	if slot == nil || slot.operations == 0 {
		table.mu.Unlock()
		panic("db: column producer operation lost its exact owner")
	}
	slot.operations--
	table.mu.Unlock()
	table.operations.Done()
}
func (db *DB) ColumnSegmentProducerSizeV1(namespace string, fileID uint32, incarnation uint64, account rootpublication.StableMetadataAccount) (uint64, error) {
	table, owner, err := db.beginColumnSegmentOperationV1(namespace, fileID, incarnation)
	if err != nil {
		return 0, err
	}
	defer db.endColumnSegmentOperationV1(table, namespace, fileID, incarnation)
	return owner.Size(account)
}
func (db *DB) WriteColumnSegmentProducerV1(namespace string, fileID uint32, incarnation uint64, expected uint64, payload []byte, account rootpublication.StableMetadataAccount) (int, error) {
	table, owner, err := db.beginColumnSegmentOperationV1(namespace, fileID, incarnation)
	if err != nil {
		return 0, err
	}
	defer db.endColumnSegmentOperationV1(table, namespace, fileID, incarnation)
	return owner.WriteAt(expected, payload, account)
}

// RollbackColumnSegmentProducerV1 is called under the stamped segment stripe.
// Admission becomes nonwritable BEFORE rollback IO. A failed truncate/sync
// retains the exact old/new lengths and owner, never an optimistic frontier.
func (db *DB) RollbackColumnSegmentProducerV1(namespace string, fileID uint32, incarnation uint64, expected, previous uint64, account rootpublication.StableMetadataAccount) error {
	table := db.columnSegmentProducerTableV1()
	if table == nil || previous > expected {
		return rootpublication.ErrResourceOwnership
	}
	table.mu.Lock()
	slot := findColumnSegmentProducerV1(table, namespace, fileID, incarnation)
	if slot == nil || slot.state != columnSegmentWritableV1 || slot.owner == nil || slot.operations != 0 {
		table.mu.Unlock()
		return rootpublication.ErrResourceOwnership
	}
	slot.state, slot.rollbackExpected, slot.rollbackPrevious = columnSegmentRollbackV1, expected, previous
	slot.operations, slot.finishing = 1, true
	table.operations.Add(1)
	owner := slot.owner
	table.mu.Unlock()
	err := owner.RollbackAppend(expected, previous, account)
	table.mu.Lock()
	slot = findColumnSegmentProducerV1(table, namespace, fileID, incarnation)
	if slot == nil || slot.state != columnSegmentRollbackV1 || !slot.finishing {
		table.mu.Unlock()
		panic("db: column rollback lost its owner")
	}
	slot.finishing = false
	if err == nil {
		slot.state = columnSegmentWritableV1
		slot.rollbackExpected, slot.rollbackPrevious = 0, 0
	}
	table.mu.Unlock()
	db.endColumnSegmentOperationV1(table, namespace, fileID, incarnation)
	return err
}
func (db *DB) SyncColumnSegmentProducerV1(namespace string, fileID uint32, incarnation uint64, account rootpublication.StableMetadataAccount) error {
	table, owner, err := db.beginColumnSegmentOperationV1(namespace, fileID, incarnation)
	if err != nil {
		return err
	}
	defer db.endColumnSegmentOperationV1(table, namespace, fileID, incarnation)
	return owner.Sync(account)
}
func (db *DB) CaptureColumnSegmentProducerV1(namespace string, fileID uint32, incarnation uint64, lane, id, path string, frontier rootpublication.DurableFrontier, reach rootpublication.ReachabilityField, obligations []rootpublication.StableLogicalObligation, account rootpublication.StableMetadataAccount) (*rootpublication.StableResourceToken, error) {
	table, owner, err := db.beginColumnSegmentOperationV1(namespace, fileID, incarnation)
	if err != nil {
		return nil, err
	}
	defer db.endColumnSegmentOperationV1(table, namespace, fileID, incarnation)
	return owner.CaptureLogicalObligations(lane, id, path, frontier, reach, obligations, true, account)
}

// closeColumnSegmentProducersV1 executes after writer/terminal drain and after
// ALL inherited engine gates have been discharged. Captured tokens independently
// retain each owner and its real pinned handles beyond this DB edge's retirement.
func (db *DB) closeColumnSegmentProducersV1() error {
	table := db.columnSegmentProducerTableV1()
	if table == nil {
		return nil
	}
	table.cleanup.Lock()
	defer table.cleanup.Unlock()
	table.mu.Lock()
	table.closing = true
	table.mu.Unlock()
	table.operations.Wait()
	// Closing joins this DB's admitted operations. Foreign ordinary writers
	// still use the SAME shared stripe: shutdown never assumes they disappeared.
	// Every wait occurs after DB/table/registry gates have been discharged.
	for i := range table.slots {
		table.mu.Lock()
		slot := table.slots[i]
		table.mu.Unlock()
		if slot.state == columnSegmentRetiringV1 || slot.state == columnSegmentRollbackV1 {
			if err := db.retryColumnSegmentProducerRecoveryJoinedV1(table, slot.namespace, slot.fileID, slot.incarnation); err != nil {
				return err
			}
		}
	}
	// Refuse the complete remaining group before dropping any ordinary owner.
	table.mu.Lock()
	for i := range table.slots {
		slot := &table.slots[i]
		if slot.state == columnSegmentConstructingV1 || slot.state == columnSegmentRetiringV1 || slot.state == columnSegmentRollbackV1 || slot.operations != 0 || slot.finishing {
			table.mu.Unlock()
			return rootpublication.ErrResourceOwnership
		}
	}
	table.mu.Unlock()
	registry := db.StableResourceIdentityPinRegistry()
	for i := range table.slots {
		table.mu.Lock()
		saved := table.slots[i]
		table.mu.Unlock()
		if saved.state == 0 {
			continue
		}
		lock := rootpublication.SegmentWriteStripe(saved.stripe)
		lock.Lock()
		table.mu.Lock()
		slot := &table.slots[i]
		if slot.incarnation != saved.incarnation || slot.state != saved.state || slot.operations != 0 || slot.finishing {
			table.mu.Unlock()
			lock.Unlock()
			return rootpublication.ErrResourceOwnership
		}
		owner, identity := slot.owner, slot.identity
		keyClass, _ := rootpublication.StableBackingClassBytes(uint64(len(slot.namespace)), false)
		table.keyClassBytes -= keyClass
		*slot = columnSegmentProducerSlotV1{}
		table.mu.Unlock()
		lock.Unlock()
		// The exact operational edge is detached and no new DB operation can enter.
		// Final handle/ref retirement occurs outside ALL serializer/table gates.
		if owner != nil {
			owner.Release()
			if registry != nil {
				_ = registry.Unobserve(identity)
			}
		}
	}
	db.columnSegmentProducersMu.Lock()
	if db.columnSegmentProducers == table {
		db.columnSegmentProducers = nil
	}
	db.columnSegmentProducersMu.Unlock()
	return nil
}

// RetireColumnSegmentProducerV1 detaches writable admission before unlink.
// The actual owner and SAME registry lease remain on this bounded DB slot until
// exact unlink + parent sync succeeds. A failure neither drops the slot nor
// releases resident/account ownership.
func (db *DB) RetireColumnSegmentProducerV1(namespace string, fileID uint32, identity rootpublication.StableIdentity, lease *rootpublication.IdentityDeleteLease) (bool, error) {
	if lease == nil {
		return false, rootpublication.ErrResourceOwnership
	}
	table := db.columnSegmentProducerTableV1()
	if table == nil {
		return false, nil
	}
	if err := lease.CheckDrained(); err != nil {
		return false, err
	}
	table.mu.Lock()
	defer table.mu.Unlock()
	if table.closing || db.closing.Load() {
		return false, ErrClosed
	}
	for i := range table.slots {
		slot := &table.slots[i]
		if slot.state == 0 || slot.namespace != namespace || slot.fileID != fileID {
			continue
		}
		if slot.state != columnSegmentWritableV1 || slot.owner == nil || slot.operations != 0 || !rootpublication.SamePhysicalIdentity(slot.identity, identity) {
			return false, rootpublication.ErrResourceOwnership
		}
		// Same segment stripe proves no append/capture operation for this slot is
		// in flight. Unrelated slots may operate; do not wait while holding gates.
		slot.state, slot.deletion = columnSegmentRetiringV1, lease
		return true, nil
	}
	return false, nil
}
func (db *DB) MarkColumnSegmentProducerUnlinkedV1(namespace string, fileID uint32) error {
	table := db.columnSegmentProducerTableV1()
	if table == nil {
		return rootpublication.ErrResourceOwnership
	}
	table.mu.Lock()
	defer table.mu.Unlock()
	for i := range table.slots {
		slot := &table.slots[i]
		if slot.state == columnSegmentRetiringV1 && slot.namespace == namespace && slot.fileID == fileID && slot.deletion != nil {
			slot.unlinked = true
			return nil
		}
	}
	return rootpublication.ErrResourceOwnership
}
func (db *DB) completeColumnSegmentProducerRetirementLockedV1(namespace string, fileID uint32) (*rootpublication.StableSegmentOwner, rootpublication.StableIdentity, error) {
	table := db.columnSegmentProducerTableV1()
	if table == nil {
		return nil, rootpublication.StableIdentity{}, rootpublication.ErrResourceOwnership
	}
	var owner *rootpublication.StableSegmentOwner
	var identity rootpublication.StableIdentity
	var lease *rootpublication.IdentityDeleteLease
	table.mu.Lock()
	for i := range table.slots {
		slot := &table.slots[i]
		if slot.state == columnSegmentRetiringV1 && slot.namespace == namespace && slot.fileID == fileID && slot.unlinked && slot.deletion != nil && slot.operations == 0 {
			if err := slot.deletion.CheckDrained(); err != nil {
				table.mu.Unlock()
				return nil, rootpublication.StableIdentity{}, err
			}
			owner, identity, lease = slot.owner, slot.identity, slot.deletion
			keyClass, _ := rootpublication.StableBackingClassBytes(uint64(len(slot.namespace)), false)
			table.keyClassBytes -= keyClass
			*slot = columnSegmentProducerSlotV1{}
			break
		}
	}
	table.mu.Unlock()
	if owner == nil || lease == nil {
		return nil, rootpublication.StableIdentity{}, rootpublication.ErrResourceOwnership
	}
	// SAME stripe still excludes append/new installation until this exact gate is
	// committed. Final handle/ref retirement is returned to the unlocked caller.
	lease.CommitDeleted()
	return owner, identity, nil
}
func (db *DB) releaseColumnSegmentProducerV1(owner *rootpublication.StableSegmentOwner, identity rootpublication.StableIdentity) error {
	if owner == nil {
		return nil
	}
	owner.Release()
	if registry := db.StableResourceIdentityPinRegistry(); registry != nil {
		return registry.Unobserve(identity)
	}
	return nil
}

// ColumnSegmentProducerRetiringFileV1 returns only scalar identifiers. It does
// not export a namespace/key/control alias or confer deletion authority.
func (db *DB) ColumnSegmentProducerRetiringFileV1(namespace string, index int) (fileID uint32, incarnation uint64) {
	if index < 0 || index >= columnSegmentProducerCapacityV1 {
		return 0, 0
	}
	table := db.columnSegmentProducerTableV1()
	if table == nil {
		return 0, 0
	}
	table.mu.Lock()
	defer table.mu.Unlock()
	slot := &table.slots[index]
	if slot.state != columnSegmentRetiringV1 && slot.state != columnSegmentRollbackV1 || slot.namespace != namespace {
		return 0, 0
	}
	return slot.fileID, slot.incarnation
}

// CompleteColumnSegmentProducerRetirementV1 joins the SAME stripe after
// dropping every table/DB gate. Completion never exposes a writable slot before
// the existing registry lease commits the physical retirement.
func (db *DB) CompleteColumnSegmentProducerRetirementV1(namespace string, fileID uint32) error {
	table := db.columnSegmentProducerTableV1()
	if table == nil {
		return rootpublication.ErrResourceOwnership
	}
	table.cleanup.Lock()
	defer table.cleanup.Unlock()
	table.mu.Lock()
	var stripe uint8
	var incarnation uint64
	for i := range table.slots {
		slot := &table.slots[i]
		if slot.state == columnSegmentRetiringV1 && slot.namespace == namespace && slot.fileID == fileID {
			stripe, incarnation = slot.stripe, slot.incarnation
			break
		}
	}
	table.mu.Unlock()
	if incarnation == 0 {
		return rootpublication.ErrResourceOwnership
	}
	lock := rootpublication.SegmentWriteStripe(stripe)
	lock.Lock()
	table.mu.Lock()
	slot := findColumnSegmentProducerV1(table, namespace, fileID, incarnation)
	valid := slot != nil && slot.state == columnSegmentRetiringV1 && slot.operations == 0 && !slot.finishing
	table.mu.Unlock()
	if !valid {
		lock.Unlock()
		return rootpublication.ErrResourceOwnership
	}
	owner, identity, err := db.completeColumnSegmentProducerRetirementLockedV1(namespace, fileID)
	lock.Unlock()
	if err != nil {
		return err
	}
	return db.releaseColumnSegmentProducerV1(owner, identity)
}

// RetryColumnSegmentProducerRecoveryV1 consumes exact bounded rollback/delete
// debt through the SAME segment stripe. No callback, queue or owner map exists.
// It is also usable after DB Close; the actual table remains until debt drains.
func (db *DB) RetryColumnSegmentProducerRecoveryV1(namespace string, fileID uint32, incarnation uint64) error {
	table := db.columnSegmentProducerTableV1()
	if table == nil {
		return rootpublication.ErrResourceOwnership
	}
	table.cleanup.Lock()
	defer table.cleanup.Unlock()
	return db.retryColumnSegmentProducerRecoveryJoinedV1(table, namespace, fileID, incarnation)
}
func (db *DB) retryColumnSegmentProducerRecoveryJoinedV1(table *columnSegmentProducerTableV1, namespace string, fileID uint32, incarnation uint64) error {
	table.mu.Lock()
	slot := findColumnSegmentProducerV1(table, namespace, fileID, incarnation)
	if slot == nil || slot.owner == nil || slot.state != columnSegmentRetiringV1 && slot.state != columnSegmentRollbackV1 {
		table.mu.Unlock()
		return rootpublication.ErrResourceOwnership
	}
	stripe := slot.stripe
	table.mu.Unlock()
	lock := rootpublication.SegmentWriteStripe(stripe)
	lock.Lock()
	table.mu.Lock()
	slot = findColumnSegmentProducerV1(table, namespace, fileID, incarnation)
	if slot == nil || slot.owner == nil || slot.operations != 0 || slot.finishing || slot.state != columnSegmentRetiringV1 && slot.state != columnSegmentRollbackV1 {
		table.mu.Unlock()
		lock.Unlock()
		return rootpublication.ErrResourceOwnership
	}
	owner, lease, unlinked, state := slot.owner, slot.deletion, slot.unlinked, slot.state
	expected, previous := slot.rollbackExpected, slot.rollbackPrevious
	slot.operations, slot.finishing = 1, true
	table.operations.Add(1)
	table.mu.Unlock()
	var err error
	if state == columnSegmentRollbackV1 {
		err = owner.RollbackAppend(expected, previous, nil)
	} else if lease == nil {
		err = rootpublication.ErrResourceOwnership
	} else if err = lease.CheckDrained(); err == nil {
		if !unlinked {
			err = owner.RemoveForDelete(lease, nil)
			if err == nil {
				err = db.MarkColumnSegmentProducerUnlinkedV1(namespace, fileID)
			}
		}
		if err == nil {
			err = owner.SyncDeletedNamespace(nil)
		}
	}
	table.mu.Lock()
	slot = findColumnSegmentProducerV1(table, namespace, fileID, incarnation)
	if slot == nil || !slot.finishing {
		table.mu.Unlock()
		panic("db: column recovery lost its exact owner")
	}
	slot.finishing = false
	if err == nil && state == columnSegmentRollbackV1 {
		slot.state = columnSegmentWritableV1
		slot.rollbackExpected, slot.rollbackPrevious = 0, 0
	}
	table.mu.Unlock()
	db.endColumnSegmentOperationV1(table, namespace, fileID, incarnation)
	var retired *rootpublication.StableSegmentOwner
	var identity rootpublication.StableIdentity
	if err == nil && state == columnSegmentRetiringV1 {
		retired, identity, err = db.completeColumnSegmentProducerRetirementLockedV1(namespace, fileID)
	}
	lock.Unlock()
	if err == nil && retired != nil {
		err = db.releaseColumnSegmentProducerV1(retired, identity)
	}
	return err
}

func (db *DB) RetryColumnSegmentProducerRetirementV1(namespace string, fileID uint32, incarnation uint64) error {
	return db.RetryColumnSegmentProducerRecoveryV1(namespace, fileID, incarnation)
}

// RetryColumnSegmentProducerCleanupV1 is an explicit checked foreground drain
// after a failed Close. Repeated generic Close retains its existing saved-error
// semantics; this method never reconsumes publication or repeats close hooks.
func (db *DB) RetryColumnSegmentProducerCleanupV1() error {
	if db == nil || !db.closing.Load() {
		return rootpublication.ErrResourceOwnership
	}
	return db.closeColumnSegmentProducersV1()
}

// ColumnSegmentProducerIdentityV1 exports only exact scalar installation facts.
// It creates no table/slot/FD/observation and reserves no durable identity.
func (db *DB) ColumnSegmentProducerIdentityV1(namespace string, fileID uint32, stripe uint8) (uint64, error) {
	if db == nil || db.closing.Load() {
		return 0, ErrClosed
	}
	table := db.columnSegmentProducerTableV1()
	if table == nil {
		return 0, rootpublication.ErrStableMetadataShapeUnsupported
	}
	table.mu.Lock()
	defer table.mu.Unlock()
	if table.closing {
		return 0, ErrClosed
	}
	for i := range table.slots {
		slot := &table.slots[i]
		if slot.state != 0 && slot.namespace == namespace && slot.fileID == fileID {
			if slot.state != columnSegmentWritableV1 || slot.owner == nil || slot.stripe != stripe {
				return 0, rootpublication.ErrResourceOwnership
			}
			return slot.incarnation, nil
		}
	}
	return 0, rootpublication.ErrStableMetadataShapeUnsupported
}
