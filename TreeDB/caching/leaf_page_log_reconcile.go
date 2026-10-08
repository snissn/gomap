package caching

import (
	"fmt"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
)

type currentWritableValueLogBindingValidator interface {
	ValidateCurrentWritableValueLogBinding(string, uint32, rootpublication.StableIdentity, *rootpublication.IdentityPinRegistry) error
}

// Generic reconciliation only moves the future reservation floor. Installed
// healthy leaf writers need not have the latest shared sequence: both Managers
// support multiple current files in the reserved leaf lane. Explicit native or
// CompactStorage retirement continues to use the forced advance method.
func (db *DB) reconcileLeafLogAppendWriterAfterBackendMaintenance(l *lane, observedMaxSeq int) error {
	if db == nil || l == nil {
		return nil
	}
	backend, supported := db.backend.(currentWritableValueLogBindingValidator)
	if !supported || !rootpublication.StableRelativeNamespaceSupported() {
		// Caller-owned backends retain their existing generation policy. They do
		// not gain preservation authority from a successful directory refresh.
		return db.advanceLeafLogAppendWriterPastObservedSeq(l, observedMaxSeq)
	}
	l.vlogMu.Lock()
	defer l.vlogMu.Unlock()
	if db.closing.Load() {
		return errWALClosed
	}
	if l.vlog == nil {
		if l.vlogPath != "" || l.vlogModeWriter != nil {
			return fmt.Errorf("%w: leaf writer is absent with retained installed state", rootpublication.ErrResourceConflict)
		}
		if observedMaxSeq > l.vlogSeq {
			l.vlogSeq = observedMaxSeq
		}
		return nil
	}
	w, supported := l.vlog.(*valuelog.Writer)
	if !supported {
		return fmt.Errorf("%w: installed leaf writer has no exact binding inspector", rootpublication.ErrResourceConflict)
	}
	if l.id != leafLogLaneID || l.vlogSeq <= 0 || db.leafLogAppendSeq.Load() < uint32(l.vlogSeq) {
		return fmt.Errorf("%w: leaf writer differs from shared reservation authority", rootpublication.ErrResourceConflict)
	}
	fileID, err := valuelog.EncodeFileID(uint32(l.id), uint32(l.vlogSeq))
	if err != nil {
		return err
	}
	identity, err := w.ValidateCurrentBinding(l.vlogPath, fileID, db.valueLogIdentityPins)
	if err != nil {
		return err
	}
	if err := db.valueLogReader.ValidateCurrentWritableBinding(l.vlogPath, fileID, identity, db.valueLogIdentityPins); err != nil {
		return err
	}
	return backend.ValidateCurrentWritableValueLogBinding(l.vlogPath, fileID, identity, db.valueLogIdentityPins)
}
