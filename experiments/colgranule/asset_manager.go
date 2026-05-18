package colgranule

import (
	"fmt"
	"sync"
)

type ColumnAssetPin struct {
	Ref    ColumnAssetRef `json:"ref"`
	Reason string         `json:"reason,omitempty"`
}

type ColumnAssetManager struct {
	mu            sync.Mutex
	store         ColumnAssetStore
	pins          map[ColumnAssetRef]int
	zombies       map[ColumnAssetRef]string
	quarantine    map[ColumnAssetRef]string
	publishFailed map[ColumnAssetRef]struct{}
	rewriteDebt   map[ColumnAssetRef]string
	published     map[ColumnAssetRef]string
}

type ColumnAssetManagerReclamationPlan struct {
	Entries            []ColumnAssetManagerReclamationEntry `json:"entries"`
	CandidateBytes     int                                  `json:"candidate_bytes"`
	ReadyToDeleteBytes int                                  `json:"ready_to_delete_bytes"`
	PinnedBytes        int                                  `json:"pinned_bytes"`
	ZombieBytes        int                                  `json:"zombie_bytes"`
	RewriteDebtBytes   int                                  `json:"rewrite_debt_bytes"`
}

type ColumnAssetPublishClosure struct {
	PreparedAssets []ColumnPreparedAsset `json:"prepared_assets,omitempty"`
	RequiredAssets int                   `json:"required_assets"`
	RequiredBytes  int                   `json:"required_bytes"`
	FlushRequired  bool                  `json:"flush_required,omitempty"`
	SyncRequired   bool                  `json:"sync_required,omitempty"`

	preparedIdentity []ColumnPreparedAsset
}

type ColumnAssetSyncedPublishClosure struct {
	closure ColumnAssetPublishClosure
	sealed  bool
}

type ColumnAssetManagerReclamationEntry struct {
	Ref              ColumnAssetRef            `json:"ref"`
	State            ColumnAssetLifecycleState `json:"state"`
	Bytes            int                       `json:"bytes"`
	Candidate        bool                      `json:"candidate,omitempty"`
	Pinned           bool                      `json:"pinned,omitempty"`
	Zombie           bool                      `json:"zombie,omitempty"`
	Quarantined      bool                      `json:"quarantined,omitempty"`
	RewriteRequired  bool                      `json:"rewrite_required,omitempty"`
	ReadyToDelete    bool                      `json:"ready_to_delete,omitempty"`
	ManagerReason    string                    `json:"manager_reason,omitempty"`
	ReachableReasons []string                  `json:"reachable_reasons,omitempty"`
}

func NewColumnAssetManager(store ColumnAssetStore) (*ColumnAssetManager, error) {
	if store == nil {
		return nil, fmt.Errorf("colgranule: nil column asset manager store")
	}
	return &ColumnAssetManager{
		store:         store,
		pins:          make(map[ColumnAssetRef]int),
		zombies:       make(map[ColumnAssetRef]string),
		quarantine:    make(map[ColumnAssetRef]string),
		publishFailed: make(map[ColumnAssetRef]struct{}),
		rewriteDebt:   make(map[ColumnAssetRef]string),
		published:     make(map[ColumnAssetRef]string),
	}, nil
}

func (m *ColumnAssetManager) Put(kind ColumnAssetKind, payload []byte) (ColumnAssetRef, error) {
	if m == nil || m.store == nil {
		return ColumnAssetRef{}, fmt.Errorf("colgranule: closed column asset manager")
	}
	return m.store.Put(kind, payload)
}

func (m *ColumnAssetManager) PutOwned(kind ColumnAssetKind, payload []byte) (ColumnAssetRef, error) {
	if m == nil || m.store == nil {
		return ColumnAssetRef{}, fmt.Errorf("colgranule: closed column asset manager")
	}
	if owned, ok := m.store.(columnAssetOwnedStore); ok {
		return owned.PutOwned(kind, payload)
	}
	return m.store.Put(kind, payload)
}

func (m *ColumnAssetManager) Read(ref ColumnAssetRef) ([]byte, error) {
	if m == nil || m.store == nil {
		return nil, fmt.Errorf("colgranule: closed column asset manager")
	}
	return m.store.Read(ref)
}

func (m *ColumnAssetManager) ReadTo(ref ColumnAssetRef, dst []byte) ([]byte, error) {
	if m == nil || m.store == nil {
		return nil, fmt.Errorf("colgranule: closed column asset manager")
	}
	return m.store.ReadTo(ref, dst)
}

func (m *ColumnAssetManager) ReadRange(ref ColumnAssetRef, offset int64, length int) ([]byte, error) {
	if m == nil || m.store == nil {
		return nil, fmt.Errorf("colgranule: closed column asset manager")
	}
	if ranged, ok := m.store.(ColumnAssetRangeReader); ok {
		return ranged.ReadRange(ref, offset, length)
	}
	payload, err := m.store.Read(ref)
	if err != nil {
		return nil, err
	}
	if offset < 0 {
		return nil, fmt.Errorf("colgranule: negative asset range offset %d", offset)
	}
	if length < 0 {
		return nil, fmt.Errorf("colgranule: negative asset range length %d", length)
	}
	if offset > int64(len(payload)) || int64(length) > int64(len(payload))-offset {
		return nil, fmt.Errorf("colgranule: asset range offset=%d length=%d outside payload bytes=%d", offset, length, len(payload))
	}
	start := int(offset)
	out := make([]byte, length)
	copy(out, payload[start:start+length])
	return out, nil
}

func (m *ColumnAssetManager) Close() error {
	if m == nil {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.store == nil {
		return nil
	}
	store := m.store
	m.store = nil
	if closer, ok := store.(interface{ Close() error }); ok {
		return closer.Close()
	}
	return nil
}

func (m *ColumnAssetManager) Pin(ref ColumnAssetRef, reason string) (ColumnAssetPin, error) {
	if err := validateColumnAssetRef(ref); err != nil {
		return ColumnAssetPin{}, err
	}
	if m == nil {
		return ColumnAssetPin{}, fmt.Errorf("colgranule: nil column asset manager")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.pins[ref]++
	return ColumnAssetPin{Ref: ref, Reason: reason}, nil
}

func (m *ColumnAssetManager) Release(pin ColumnAssetPin) error {
	if err := validateColumnAssetRef(pin.Ref); err != nil {
		return err
	}
	if m == nil {
		return fmt.Errorf("colgranule: nil column asset manager")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	count := m.pins[pin.Ref]
	if count <= 0 {
		return fmt.Errorf("colgranule: release unpinned column asset %+v", pin.Ref)
	}
	if count == 1 {
		delete(m.pins, pin.Ref)
	} else {
		m.pins[pin.Ref] = count - 1
	}
	return nil
}

func (m *ColumnAssetManager) MarkZombie(ref ColumnAssetRef, reason string) error {
	if err := validateColumnAssetRef(ref); err != nil {
		return err
	}
	if m == nil {
		return fmt.Errorf("colgranule: nil column asset manager")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.zombies[ref] = reason
	return nil
}

func (m *ColumnAssetManager) MarkRewriteDebt(ref ColumnAssetRef, reason string) error {
	if err := validateColumnAssetRef(ref); err != nil {
		return err
	}
	if m == nil {
		return fmt.Errorf("colgranule: nil column asset manager")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.rewriteDebt[ref] = reason
	return nil
}

func (m *ColumnAssetManager) Quarantine(ref ColumnAssetRef, reason string) error {
	if err := validateColumnAssetRef(ref); err != nil {
		return err
	}
	if m == nil {
		return fmt.Errorf("colgranule: nil column asset manager")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.quarantine[ref] = reason
	return nil
}

func (m *ColumnAssetManager) PreparePublishClosure(prepared []ColumnPreparedAsset) (ColumnAssetPublishClosure, error) {
	if m == nil {
		return ColumnAssetPublishClosure{}, fmt.Errorf("colgranule: nil column asset manager")
	}
	m.mu.Lock()
	store := m.store
	m.mu.Unlock()
	if store == nil {
		return ColumnAssetPublishClosure{}, fmt.Errorf("colgranule: closed column asset manager")
	}
	return prepareColumnAssetPublishClosure(store, prepared)
}

func prepareColumnAssetPublishClosure(store ColumnAssetStore, prepared []ColumnPreparedAsset) (ColumnAssetPublishClosure, error) {
	if store == nil {
		return ColumnAssetPublishClosure{}, fmt.Errorf("colgranule: closed column asset manager")
	}
	closure := ColumnAssetPublishClosure{
		PreparedAssets:   cloneColumnPreparedAssets(prepared),
		FlushRequired:    len(prepared) > 0,
		preparedIdentity: cloneColumnPreparedAssets(prepared),
	}
	// Sync is tied to the explicit publish closure: unreferenced buffered
	// assets are not made durable until a root-visible prepared ref names them.
	if _, ok := store.(columnAssetSyncer); ok && len(prepared) > 0 {
		closure.SyncRequired = true
	}
	seen := make(map[ColumnAssetRef]struct{}, len(prepared))
	for _, asset := range prepared {
		if err := validateColumnPreparedAsset(asset); err != nil {
			return ColumnAssetPublishClosure{}, err
		}
		if _, ok := seen[asset.Ref]; ok {
			return ColumnAssetPublishClosure{}, fmt.Errorf("colgranule: duplicate prepared asset ref %+v", asset.Ref)
		}
		seen[asset.Ref] = struct{}{}
		if err := verifyColumnAssetStoreRef(store, asset.Ref); err != nil {
			return ColumnAssetPublishClosure{}, fmt.Errorf("colgranule: missing required prepared asset %+v: %w", asset.Ref, err)
		}
		closure.RequiredAssets++
		bytes := asset.Bytes
		if bytes == 0 {
			if asset.Ref.Length > int64(^uint(0)>>1) {
				return ColumnAssetPublishClosure{}, fmt.Errorf("colgranule: prepared asset length=%d exceeds host int", asset.Ref.Length)
			}
			bytes = int(asset.Ref.Length)
		}
		closure.RequiredBytes += bytes
	}
	return closure, nil
}

func (m *ColumnAssetManager) SyncPublishClosure(closure ColumnAssetPublishClosure) (ColumnAssetSyncedPublishClosure, error) {
	if m == nil {
		return ColumnAssetSyncedPublishClosure{}, fmt.Errorf("colgranule: nil column asset manager")
	}
	m.mu.Lock()
	store := m.store
	m.mu.Unlock()
	if store == nil {
		return ColumnAssetSyncedPublishClosure{}, fmt.Errorf("colgranule: closed column asset manager")
	}
	verified, err := prepareColumnAssetPublishClosure(store, closure.PreparedAssets)
	if err != nil {
		return ColumnAssetSyncedPublishClosure{}, err
	}
	// Re-derive the closure from prepared refs so sync requirements cannot be
	// cleared by caller mutation, but require the caller-visible accounting to
	// still match the prepared asset set that was originally reviewed.
	if err := validateColumnAssetPublishClosureMatches(closure, verified); err != nil {
		return ColumnAssetSyncedPublishClosure{}, err
	}
	if verified.SyncRequired {
		if syncer, ok := store.(columnAssetSyncer); ok {
			if err := syncer.Sync(); err != nil {
				return ColumnAssetSyncedPublishClosure{}, err
			}
		}
	}
	return ColumnAssetSyncedPublishClosure{
		closure: verified,
		sealed:  true,
	}, nil
}

func (m *ColumnAssetManager) MarkPublishSucceeded(synced ColumnAssetSyncedPublishClosure, reason string) error {
	if m == nil {
		return fmt.Errorf("colgranule: nil column asset manager")
	}
	if !synced.sealed {
		return fmt.Errorf("colgranule: publish succeeded requires synced publish closure")
	}
	if reason == "" {
		reason = "publish succeeded"
	}
	for _, asset := range synced.closure.PreparedAssets {
		if err := validateColumnPreparedAsset(asset); err != nil {
			return err
		}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, asset := range synced.closure.PreparedAssets {
		if _, ok := m.publishFailed[asset.Ref]; ok {
			delete(m.quarantine, asset.Ref)
			delete(m.publishFailed, asset.Ref)
		}
		m.published[asset.Ref] = reason
	}
	return nil
}

func (m *ColumnAssetManager) MarkPublishFailed(prepared []ColumnPreparedAsset, reason string) error {
	if m == nil {
		return fmt.Errorf("colgranule: nil column asset manager")
	}
	if reason == "" {
		reason = "publish failed"
	}
	for _, asset := range prepared {
		if err := validateColumnPreparedAsset(asset); err != nil {
			return err
		}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, asset := range prepared {
		m.quarantine[asset.Ref] = reason
		m.publishFailed[asset.Ref] = struct{}{}
	}
	return nil
}

func (m *ColumnAssetManager) PlanReclamation(reachability ColumnAssetReachabilityPlan) ColumnAssetManagerReclamationPlan {
	var plan ColumnAssetManagerReclamationPlan
	if m == nil {
		return plan
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	plan.Entries = make([]ColumnAssetManagerReclamationEntry, 0, len(reachability.Entries))
	for _, reachable := range reachability.Entries {
		pinCount := m.pins[reachable.Ref]
		zombieReason, zombie := m.zombies[reachable.Ref]
		quarantineReason, quarantined := m.quarantine[reachable.Ref]
		rewriteReason, rewrite := m.rewriteDebt[reachable.Ref]
		publishedReason, published := m.published[reachable.Ref]
		entry := ColumnAssetManagerReclamationEntry{
			Ref:              reachable.Ref,
			State:            reachable.State,
			Bytes:            reachable.Bytes,
			Candidate:        reachable.DeleteEligible,
			Pinned:           pinCount > 0,
			Zombie:           zombie,
			Quarantined:      quarantined,
			RewriteRequired:  rewrite || (!reachable.DeleteEligible && reachable.State == ColumnAssetStateCleanupSafe),
			ReachableReasons: reachable.Reasons,
		}
		switch {
		case quarantined:
			entry.ManagerReason = quarantineReason
		case rewrite:
			entry.ManagerReason = rewriteReason
		case zombie:
			entry.ManagerReason = zombieReason
		case published:
			entry.ManagerReason = publishedReason
		}
		if entry.Candidate {
			plan.CandidateBytes += entry.Bytes
		}
		if entry.Pinned {
			plan.PinnedBytes += entry.Bytes
		}
		if entry.Zombie {
			plan.ZombieBytes += entry.Bytes
		}
		if entry.RewriteRequired {
			plan.RewriteDebtBytes += entry.Bytes
		}
		entry.ReadyToDelete = entry.Candidate && entry.Zombie && !entry.Pinned && !entry.Quarantined
		if entry.ReadyToDelete {
			plan.ReadyToDeleteBytes += entry.Bytes
		}
		plan.Entries = append(plan.Entries, entry)
	}
	return plan
}

func validateColumnPreparedAsset(asset ColumnPreparedAsset) error {
	if err := validateColumnAssetRef(asset.Ref); err != nil {
		return err
	}
	if asset.Bytes < 0 {
		return fmt.Errorf("colgranule: negative prepared asset bytes %d", asset.Bytes)
	}
	if asset.Bytes == 0 && asset.Ref.Length > int64(^uint(0)>>1) {
		return fmt.Errorf("colgranule: prepared asset length=%d exceeds host int", asset.Ref.Length)
	}
	return nil
}

func validateColumnAssetPublishClosureMatches(caller ColumnAssetPublishClosure, verified ColumnAssetPublishClosure) error {
	if !columnPreparedAssetsEqual(caller.PreparedAssets, caller.preparedIdentity) {
		return fmt.Errorf("colgranule: publish closure prepared assets changed after prepare")
	}
	if !columnPreparedAssetsEqual(verified.PreparedAssets, caller.preparedIdentity) {
		return fmt.Errorf("colgranule: publish closure verified prepared assets changed after prepare")
	}
	if caller.RequiredAssets != verified.RequiredAssets {
		return fmt.Errorf("colgranule: publish closure required assets=%d want %d", caller.RequiredAssets, verified.RequiredAssets)
	}
	if caller.RequiredBytes != verified.RequiredBytes {
		return fmt.Errorf("colgranule: publish closure required bytes=%d want %d", caller.RequiredBytes, verified.RequiredBytes)
	}
	if caller.FlushRequired != verified.FlushRequired {
		return fmt.Errorf("colgranule: publish closure flush required=%t want %t", caller.FlushRequired, verified.FlushRequired)
	}
	return nil
}

func columnPreparedAssetsEqual(left, right []ColumnPreparedAsset) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

func verifyColumnAssetStoreRef(store ColumnAssetStore, ref ColumnAssetRef) error {
	if verifier, ok := store.(columnAssetVerifier); ok {
		return verifier.Verify(ref)
	}
	_, err := store.ReadTo(ref, nil)
	return err
}
