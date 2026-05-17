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
	mu          sync.Mutex
	store       ColumnAssetStore
	pins        map[ColumnAssetRef]int
	zombies     map[ColumnAssetRef]string
	quarantine  map[ColumnAssetRef]string
	rewriteDebt map[ColumnAssetRef]string
}

type ColumnAssetManagerReclamationPlan struct {
	Entries            []ColumnAssetManagerReclamationEntry `json:"entries"`
	CandidateBytes     int                                  `json:"candidate_bytes"`
	ReadyToDeleteBytes int                                  `json:"ready_to_delete_bytes"`
	PinnedBytes        int                                  `json:"pinned_bytes"`
	ZombieBytes        int                                  `json:"zombie_bytes"`
	RewriteDebtBytes   int                                  `json:"rewrite_debt_bytes"`
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
		store:       store,
		pins:        make(map[ColumnAssetRef]int),
		zombies:     make(map[ColumnAssetRef]string),
		quarantine:  make(map[ColumnAssetRef]string),
		rewriteDebt: make(map[ColumnAssetRef]string),
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
		entry := ColumnAssetManagerReclamationEntry{
			Ref:              reachable.Ref,
			State:            reachable.State,
			Bytes:            reachable.Bytes,
			Candidate:        reachable.DeleteEligible,
			Pinned:           pinCount > 0,
			Zombie:           zombie,
			Quarantined:      quarantined,
			RewriteRequired:  rewrite || (!reachable.DeleteEligible && reachable.State == ColumnAssetStateSuperseded),
			ReachableReasons: reachable.Reasons,
		}
		switch {
		case quarantined:
			entry.ManagerReason = quarantineReason
		case rewrite:
			entry.ManagerReason = rewriteReason
		case zombie:
			entry.ManagerReason = zombieReason
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
