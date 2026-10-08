package db

import (
	"github.com/snissn/gomap/TreeDB/freelist"
	"github.com/snissn/gomap/TreeDB/internal/residentcredit"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

// NativePublicationResidentScopeV1 is one intrinsic destination from the
// installed DB owner. Metadata and allocator backing share its actual ledger.
// Interface satisfaction is not an installation or admission certificate.
type NativePublicationResidentScopeV1 interface {
	rootpublication.StableMetadataAccount
	freelist.AllocationResidentCreditV1
}

// NativePublicationResidentOwnerV1 is the scalar resident authority attached to
// this actual DB. Its methods only lock its independent credit mutex. It must
// not retain a DB, producer, callback, request input or publication plan.
// Installation alone grants no finite publication or metadata-export authority.
type NativePublicationResidentOwnerV1 interface {
	StableMetadataResidentLimitV1() uint64
	NewStableMetadataResidentScopeV1() (rootpublication.StableMetadataAccount, error)
	NewNativePublicationResidentRolesV1(rootpublication.StableMetadataAccount) (NativePublicationResidentScopeV1, NativePublicationResidentScopeV1, error)
	CloseStableMetadataResidentOwnerV1()
}

func (db *DB) HasNativePublicationResidentOwnerV1() bool {
	if db == nil {
		return false
	}
	db.mu.RLock()
	installed := db.nativePublicationResident != nil && !db.closing.Load()
	db.mu.RUnlock()
	return installed
}

// InstallNativePublicationResidentOwnerV1 preserves the legacy rejection and
// duplicate-install API. Actual owner custody is constructor-only: an arbitrary
// structural owner cannot become installation proof or retrofit an old Pager.
func (db *DB) InstallNativePublicationResidentOwnerV1(owner NativePublicationResidentOwnerV1, limit uint64) (installed bool, err error) {
	if db == nil || owner == nil || limit != nativePublicationResidentLimitV1 || owner.StableMetadataResidentLimitV1() != limit {
		return false, rootpublication.ErrStableMetadataShapeUnsupported
	}
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closing.Load() {
		return false, ErrClosed
	}
	if db.nativePublicationResident == nil {
		return false, rootpublication.ErrStableMetadataShapeUnsupported
	}
	return false, nil
}

// This defined type is a concrete, allocation-free DB adapter over the SAME
// actual scalar owner, not another wrapper/ledger. No request/DB edge is stored.
type nativePublicationResidentOwner residentcredit.Owner

const nativePublicationResidentLimitV1 = uint64(128 << 20)

func newNativePublicationResidentOwnerV1() (*nativePublicationResidentOwner, error) {
	owner, err := residentcredit.NewOrdinary(nativePublicationResidentLimitV1)
	return (*nativePublicationResidentOwner)(owner), err
}
func (c *nativePublicationResidentOwner) StableMetadataResidentLimitV1() uint64 {
	return (*residentcredit.Owner)(c).Limit()
}
func (c *nativePublicationResidentOwner) NewStableMetadataResidentScopeV1() (rootpublication.StableMetadataAccount, error) {
	return (*residentcredit.Owner)(c).NewScope()
}
func (c *nativePublicationResidentOwner) NewNativePublicationResidentRolesV1(request rootpublication.StableMetadataAccount) (NativePublicationResidentScopeV1, NativePublicationResidentScopeV1, error) {
	retained, scratch, err := (*residentcredit.Owner)(c).NewRoles(request)
	if err != nil {
		return nil, nil, err
	}
	return retained, scratch, nil
}
func (c *nativePublicationResidentOwner) CloseStableMetadataResidentOwnerV1() {
	(*residentcredit.Owner)(c).Close()
}

// NewNativePublicationResidentScopeV1 creates one real destination scope. The
// independent owner mutex serializes its creation against DB-edge retirement;
// a scope successfully created before retirement survives the DB's Close.
func (db *DB) NewNativePublicationResidentScopeV1() (rootpublication.StableMetadataAccount, error) {
	if db == nil {
		return nil, ErrClosed
	}
	db.mu.RLock()
	owner := db.nativePublicationResident
	db.mu.RUnlock()
	if owner == nil {
		return nil, rootpublication.ErrStableMetadataShapeUnsupported
	}
	return owner.NewStableMetadataResidentScopeV1()
}

// NewNativePublicationResidentRolesV1 obtains distinct retained and scratch
// scopes from the SAME actual installed owner. The current request pays both
// scope controls before their construction and is never stored in either
// scope. Closing races are resolved by the independent resident mutex; both
// scopes are born or neither is born. Successful scopes survive DB retirement.
// The caller must drop scratch after all scratch aliases have been scrubbed.
func (db *DB) NewNativePublicationResidentRolesV1(request rootpublication.StableMetadataAccount) (NativePublicationResidentScopeV1, NativePublicationResidentScopeV1, error) {
	if db == nil || request == nil {
		return nil, nil, rootpublication.ErrStableMetadataShapeUnsupported
	}
	db.mu.RLock()
	owner := db.nativePublicationResident
	db.mu.RUnlock()
	if owner == nil {
		return nil, nil, rootpublication.ErrStableMetadataShapeUnsupported
	}
	return owner.NewNativePublicationResidentRolesV1(request)
}

func (db *DB) closeNativePublicationResidentOwnerV1() {
	db.mu.Lock()
	owner := db.nativePublicationResident
	db.nativePublicationResident = nil
	db.mu.Unlock()
	if owner != nil {
		owner.CloseStableMetadataResidentOwnerV1()
	}
}
