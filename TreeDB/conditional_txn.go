package treedb

import (
	"errors"

	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

// Snapshot returns the transaction's pinned opening snapshot when available.
// NewConditionalTxnWithSnapshot and InitConditionalTxnWithSnapshot always
// return a transaction with a snapshot. The transaction owns the snapshot and
// closes it on Commit, CommitSync, or Close.
//
// Point and versioned point reads are supported. Range iteration on this
// transaction-owned snapshot fails closed because conditional range guards are
// not part of the public transaction contract yet.
func (tx *ConditionalTxn) Snapshot() Snapshot {
	if tx == nil || !tx.snapshotExposed {
		return nil
	}
	if tx.cachedActive {
		snap := tx.cached.Snapshot()
		if snap == nil {
			return nil
		}
		return conditionalTxnSnapshot{Snapshot: snap, tx: tx}
	}
	if tx.backend != nil {
		snap := tx.backend.Snapshot()
		if snap == nil {
			return nil
		}
		return conditionalTxnSnapshot{Snapshot: snap, tx: tx}
	}
	return nil
}

type conditionalTxnSnapshot struct {
	Snapshot
	tx *ConditionalTxn
}

// Close is a no-op. The transaction owns the underlying snapshot and closes it
// on Commit, CommitSync, or Close; callers can safely defer this method without
// releasing the transaction-owned snapshot prematurely.
func (s conditionalTxnSnapshot) Close() error {
	return nil
}

func (s conditionalTxnSnapshot) Get(key []byte) ([]byte, error) {
	value, _, err := s.GetVersioned(key)
	return value, err
}

func (s conditionalTxnSnapshot) GetAppend(key, dst []byte) ([]byte, error) {
	out, _, err := s.GetVersionedAppend(key, dst)
	return out, err
}

func (s conditionalTxnSnapshot) GetVersioned(key []byte) ([]byte, EntryRevision, error) {
	out, revision, err := s.GetVersionedAppend(key, nil)
	if err != nil {
		return nil, revision, err
	}
	if len(out) == 0 {
		return []byte{}, revision, nil
	}
	if cap(out) == len(out) {
		return out, revision, nil
	}
	owned := make([]byte, len(out))
	copy(owned, out)
	return owned, revision, nil
}

func (s conditionalTxnSnapshot) GetVersionedAppend(key, dst []byte) ([]byte, EntryRevision, error) {
	if s.tx == nil || s.Snapshot == nil {
		return dst, LegacyEntryRevision, ErrConditionalTxnClosed
	}
	key = normalizeRawKVPointKey(key)
	out, revision, err := s.Snapshot.GetVersionedAppend(key, dst)
	if errors.Is(err, ErrKeyNotFound) {
		if recErr := s.tx.recordSnapshotReadVersion(key, revision, false); recErr != nil {
			return dst, revision, recErr
		}
		return dst, revision, err
	}
	if err != nil {
		return dst, revision, err
	}
	if recErr := s.tx.recordSnapshotReadVersion(key, revision, true); recErr != nil {
		return dst, revision, recErr
	}
	return out, revision, nil
}

func (s conditionalTxnSnapshot) GetManyView(keys [][]byte, fn GetManyViewFunc) error {
	if fn == nil {
		return errors.New("treedb: GetManyView nil callback")
	}
	for i, key := range keys {
		key = normalizeRawKVPointKey(key)
		value, _, err := s.GetVersionedAppend(key, nil)
		if errors.Is(err, ErrKeyNotFound) {
			if err := fn(i, key, nil, false); err != nil {
				return err
			}
			continue
		}
		if err != nil {
			return err
		}
		if len(value) == 0 {
			value = []byte{}
		}
		if err := fn(i, key, value, true); err != nil {
			return err
		}
	}
	return nil
}

func (s conditionalTxnSnapshot) GetUnsafe(key []byte) ([]byte, error) {
	return s.Get(key)
}

func (s conditionalTxnSnapshot) Has(key []byte) (bool, error) {
	_, _, err := s.GetVersionedAppend(key, nil)
	if errors.Is(err, ErrKeyNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func (s conditionalTxnSnapshot) HasMany(keys [][]byte) ([]bool, error) {
	out := make([]bool, len(keys))
	for i, key := range keys {
		found, err := s.Has(key)
		if err != nil {
			return nil, err
		}
		out[i] = found
	}
	return out, nil
}

func (s conditionalTxnSnapshot) HasPrefixes(prefixes [][]byte) ([]bool, error) {
	return nil, ErrConditionalTxnUnsupported
}

func (s conditionalTxnSnapshot) GetEntry(key []byte) (node.LeafEntry, error) {
	return node.LeafEntry{}, ErrConditionalTxnUnsupported
}

func (s conditionalTxnSnapshot) GetEntryExact(key []byte) (node.LeafEntry, error) {
	return node.LeafEntry{}, ErrConditionalTxnUnsupported
}

func (s conditionalTxnSnapshot) Iterate(start, end []byte, fn func(key, value []byte) error) error {
	return ErrConditionalTxnUnsupported
}

func (s conditionalTxnSnapshot) ReverseIterate(start, end []byte, fn func(key, value []byte) error) error {
	return ErrConditionalTxnUnsupported
}

func (s conditionalTxnSnapshot) Iterator(start, end []byte) (Iterator, error) {
	return nil, ErrConditionalTxnUnsupported
}

func (s conditionalTxnSnapshot) ReverseIterator(start, end []byte) (Iterator, error) {
	return nil, ErrConditionalTxnUnsupported
}

func (tx *ConditionalTxn) recordSnapshotReadVersion(key []byte, revision EntryRevision, found bool) error {
	if tx == nil {
		return ErrConditionalTxnClosed
	}
	if tx.cachedActive {
		return tx.cached.RecordReadVersion(key, page.EntryRevision(revision), found)
	}
	if tx.backend != nil {
		return tx.backend.RecordReadVersion(key, page.EntryRevision(revision), found)
	}
	return ErrConditionalTxnClosed
}

// ReserveReadSet reserves read-precondition capacity for high-fanout
// transactions. It is optional and does not change transaction semantics.
func (tx *ConditionalTxn) ReserveReadSet(n int) {
	if tx == nil {
		return
	}
	if tx.cachedActive {
		tx.cached.ReserveReadSet(n)
		return
	}
	if tx.backend != nil {
		tx.backend.ReserveReadSet(n)
	}
}

// ReserveWrites reserves native batch capacity for staged writes.
func (tx *ConditionalTxn) ReserveWrites(n int) {
	if tx == nil {
		return
	}
	if tx.cachedActive {
		tx.cached.ReserveWrites(n)
		return
	}
	if tx.backend != nil {
		tx.backend.ReserveWrites(n)
	}
}

// Get returns the transaction-visible value for key and records that key as a
// commit precondition. Missing keys return nil, nil.
func (tx *ConditionalTxn) Get(key []byte) ([]byte, error) {
	if tx == nil {
		return nil, ErrConditionalTxnClosed
	}
	if tx.cachedActive {
		return tx.cached.Get(key)
	}
	if tx.backend != nil {
		return tx.backend.Get(key)
	}
	return nil, ErrConditionalTxnClosed
}

// GetVersioned returns the transaction-visible value and native entry revision
// for key and records that key as a commit precondition.
func (tx *ConditionalTxn) GetVersioned(key []byte) ([]byte, EntryRevision, error) {
	if tx == nil {
		return nil, LegacyEntryRevision, ErrConditionalTxnClosed
	}
	if tx.cachedActive {
		return tx.cached.GetVersioned(key)
	}
	if tx.backend != nil {
		return tx.backend.GetVersioned(key)
	}
	return nil, LegacyEntryRevision, ErrConditionalTxnClosed
}

// GetVersionedAppend appends the transaction-visible value for key to dst,
// returns the native entry revision, and records key as a commit precondition.
func (tx *ConditionalTxn) GetVersionedAppend(key, dst []byte) ([]byte, EntryRevision, error) {
	if tx == nil {
		return dst, LegacyEntryRevision, ErrConditionalTxnClosed
	}
	if tx.cachedActive {
		return tx.cached.GetVersionedAppend(key, dst)
	}
	if tx.backend != nil {
		return tx.backend.GetVersionedAppend(key, dst)
	}
	return dst, LegacyEntryRevision, ErrConditionalTxnClosed
}

// RequireReadVersion records a caller-observed key revision as a commit
// precondition. It is intended for values read through an external pinned
// snapshot owned by the caller.
func (tx *ConditionalTxn) RequireReadVersion(key []byte, revision EntryRevision, found bool) error {
	if tx == nil {
		return ErrConditionalTxnClosed
	}
	if tx.cachedActive {
		return tx.cached.RequireReadVersion(key, page.EntryRevision(revision), found)
	}
	if tx.backend != nil {
		return tx.backend.RequireReadVersion(key, page.EntryRevision(revision), found)
	}
	return ErrConditionalTxnClosed
}

// Has reports whether key exists in the transaction-visible state and records
// the key as a commit precondition.
func (tx *ConditionalTxn) Has(key []byte) (bool, error) {
	if tx == nil {
		return false, ErrConditionalTxnClosed
	}
	if tx.cachedActive {
		return tx.cached.Has(key)
	}
	if tx.backend != nil {
		return tx.backend.Has(key)
	}
	return false, ErrConditionalTxnClosed
}

// Set stages a conditional put in the native TreeDB batch.
func (tx *ConditionalTxn) Set(key, value []byte) error {
	if tx == nil {
		return ErrConditionalTxnClosed
	}
	if tx.cachedActive {
		return tx.cached.Set(key, value)
	}
	if tx.backend != nil {
		return tx.backend.Set(key, value)
	}
	return ErrConditionalTxnClosed
}

// SetWithRevision stages a put with an explicit native entry revision.
func (tx *ConditionalTxn) SetWithRevision(key, value []byte, revision EntryRevision) error {
	if tx == nil {
		return ErrConditionalTxnClosed
	}
	if tx.cachedActive {
		return tx.cached.SetWithRevision(key, value, page.EntryRevision(revision))
	}
	if tx.backend != nil {
		return tx.backend.SetWithRevision(key, value, page.EntryRevision(revision))
	}
	return ErrConditionalTxnClosed
}

// SetView stages a put without copying key/value bytes. The caller must keep
// both slices immutable until Commit, CommitSync, or Close returns.
func (tx *ConditionalTxn) SetView(key, value []byte) error {
	if tx == nil {
		return ErrConditionalTxnClosed
	}
	if tx.cachedActive {
		return tx.cached.SetView(key, value)
	}
	if tx.backend != nil {
		return tx.backend.SetView(key, value)
	}
	return ErrConditionalTxnClosed
}

// SetViewWithRevision is SetView with an explicit native entry revision.
func (tx *ConditionalTxn) SetViewWithRevision(key, value []byte, revision EntryRevision) error {
	if tx == nil {
		return ErrConditionalTxnClosed
	}
	if tx.cachedActive {
		return tx.cached.SetViewWithRevision(key, value, page.EntryRevision(revision))
	}
	if tx.backend != nil {
		return tx.backend.SetViewWithRevision(key, value, page.EntryRevision(revision))
	}
	return ErrConditionalTxnClosed
}

// Delete stages a conditional tombstone in the native TreeDB batch.
func (tx *ConditionalTxn) Delete(key []byte) error {
	if tx == nil {
		return ErrConditionalTxnClosed
	}
	if tx.cachedActive {
		return tx.cached.Delete(key)
	}
	if tx.backend != nil {
		return tx.backend.Delete(key)
	}
	return ErrConditionalTxnClosed
}

// DeleteWithRevision stages a tombstone with an explicit native entry revision.
func (tx *ConditionalTxn) DeleteWithRevision(key []byte, revision EntryRevision) error {
	if tx == nil {
		return ErrConditionalTxnClosed
	}
	if tx.cachedActive {
		return tx.cached.DeleteWithRevision(key, page.EntryRevision(revision))
	}
	if tx.backend != nil {
		return tx.backend.DeleteWithRevision(key, page.EntryRevision(revision))
	}
	return ErrConditionalTxnClosed
}

// DeleteView stages a tombstone without copying key bytes. The caller must keep
// key immutable until Commit, CommitSync, or Close returns.
func (tx *ConditionalTxn) DeleteView(key []byte) error {
	if tx == nil {
		return ErrConditionalTxnClosed
	}
	if tx.cachedActive {
		return tx.cached.DeleteView(key)
	}
	if tx.backend != nil {
		return tx.backend.DeleteView(key)
	}
	return ErrConditionalTxnClosed
}

// DeleteViewWithRevision is DeleteView with an explicit native entry revision.
func (tx *ConditionalTxn) DeleteViewWithRevision(key []byte, revision EntryRevision) error {
	if tx == nil {
		return ErrConditionalTxnClosed
	}
	if tx.cachedActive {
		return tx.cached.DeleteViewWithRevision(key, page.EntryRevision(revision))
	}
	if tx.backend != nil {
		return tx.backend.DeleteViewWithRevision(key, page.EntryRevision(revision))
	}
	return ErrConditionalTxnClosed
}

// Commit validates recorded reads and publishes staged writes without forcing a
// sync durability boundary.
func (tx *ConditionalTxn) Commit() error {
	if tx == nil {
		return ErrConditionalTxnClosed
	}
	if tx.cachedActive {
		err := tx.cached.Commit()
		if err != nil {
			_ = tx.cached.Close()
		}
		tx.cachedActive = false
		tx.snapshotExposed = false
		return err
	}
	if tx.backend != nil {
		err := tx.backend.Commit()
		if err != nil {
			_ = tx.backend.Close()
		}
		tx.backend = nil
		tx.snapshotExposed = false
		return err
	}
	return ErrConditionalTxnClosed
}

// CommitSync validates recorded reads and publishes staged writes with a sync
// durability boundary.
func (tx *ConditionalTxn) CommitSync() error {
	if tx == nil {
		return ErrConditionalTxnClosed
	}
	if tx.cachedActive {
		err := tx.cached.CommitSync()
		if err != nil {
			_ = tx.cached.Close()
		}
		tx.cachedActive = false
		tx.snapshotExposed = false
		return err
	}
	if tx.backend != nil {
		err := tx.backend.CommitSync()
		if err != nil {
			_ = tx.backend.Close()
		}
		tx.backend = nil
		tx.snapshotExposed = false
		return err
	}
	return ErrConditionalTxnClosed
}

// Close releases the transaction without publishing staged writes.
func (tx *ConditionalTxn) Close() error {
	if tx == nil {
		return nil
	}
	if tx.cachedActive {
		err := tx.cached.Close()
		tx.cachedActive = false
		tx.snapshotExposed = false
		return err
	}
	if tx.backend != nil {
		err := tx.backend.Close()
		tx.backend = nil
		tx.snapshotExposed = false
		return err
	}
	return nil
}
