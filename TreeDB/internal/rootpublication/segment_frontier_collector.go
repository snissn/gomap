package rootpublication

import (
	"math"
	"sync"
	"unsafe"
)

// StableSegmentRetention is the actual registrar File lifetime edge supplied by
// the concrete producer. It excludes eviction, rather than merely unlinking.
// Its census must cover the full retained closure; unknown registrar backing
// remains unsupported. This capability never escapes through generic getters.
type StableSegmentRetention interface {
	RetainedBackingCensus() (BackingCensus, error)
	Release() error
}

// StableSegmentRegistrar is supplied only on the concrete producer call stack.
// It is never retained. Acquisition validates exact installed identity and
// prepays its complete scalar hold/cell/incarnation closure before construction.
type StableSegmentRegistrar interface {
	AcquireStableSegmentRetentionForIdentity(uint32, StableIdentity, uint64, StableMetadataAccount) (StableSegmentRetention, BackingCensus, error)
}

// StableSegmentFrontierCollector is scoped to the existing serial workspace.
// Record runs under that workspace's actual append serializer. Numeric frontier
// advancement allocates nothing; one final token is born per exact owner.
// No global scheduler, registry, pool or quota is introduced.
type StableSegmentFrontierCollector struct {
	mu          sync.Mutex
	account     StableMetadataAccount
	entries     []stableSegmentFrontier
	max         uint64
	closed      bool
	terminating bool
	failedSet   *StableResourceSet
	backing     uint64
}
type stableSegmentFrontier struct {
	owner     *StableSegmentOwner
	borrower  *stableRegistryBorrower
	retention StableSegmentRetention
	frontier  DurableFrontier
	synced    bool
}

func NewStableSegmentFrontierCollector(maxDistinctSegments uint64, account StableMetadataAccount) (*StableSegmentFrontierCollector, error) {
	if account == nil || maxDistinctSegments == 0 || maxDistinctSegments > uint64(math.MaxInt)/uint64(unsafe.Sizeof(stableSegmentFrontier{})) {
		return nil, ErrStableMetadataShapeUnsupported
	}
	n, err := StableBackingClassBytes(uint64(unsafe.Sizeof(StableSegmentFrontierCollector{})), true)
	if err != nil {
		return nil, err
	}
	n, err = finiteStableClassAdd(n, maxDistinctSegments*uint64(unsafe.Sizeof(stableSegmentFrontier{})), true)
	if err != nil {
		return nil, err
	}
	if err = finiteStableBegin(account, n); err != nil {
		return nil, err
	}
	return &StableSegmentFrontierCollector{account: account, entries: make([]stableSegmentFrontier, 0, int(maxDistinctSegments)), max: maxDistinctSegments, backing: n}, nil
}

// Record consumes retention only on success. Repeated records must supply nil:
// the previously retained exact owner and registrar edge remain authoritative.
// HasRetainedSegmentOwner checks the collector's actual held edge, rather than
// a raw address or a historical census. A concrete writer loan may use this
// owner as its key only while this collector retains the registrar/source refs.
func (c *StableSegmentFrontierCollector) HasRetainedSegmentOwner(owner *StableSegmentOwner) bool {
	if c == nil || owner == nil {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || c.terminating {
		return false
	}
	for i := range c.entries {
		if c.entries[i].owner == owner && c.entries[i].retention != nil {
			return true
		}
	}
	return false
}

func (c *StableSegmentFrontierCollector) Record(owner *StableSegmentOwner, frontier DurableFrontier, synced bool, retention StableSegmentRetention) error {
	return c.record(owner, frontier, synced, retention, nil, 0, 0)
}

// RecordFromRegistrar acquires the real registration edge only after all
// collector/source/registry admission has succeeded. Once acquisition returns,
// installing the held frontier is nonallocating and cannot fail. This avoids an
// unowned failed hold when MarkZombie races an account refusal.
func (c *StableSegmentFrontierCollector) RecordFromRegistrar(owner *StableSegmentOwner, frontier DurableFrontier, synced bool, registrar StableSegmentRegistrar, fileID uint32, maximum uint64) error {
	if registrar == nil || maximum == 0 {
		return ErrStableMetadataShapeUnsupported
	}
	return c.record(owner, frontier, synced, nil, registrar, fileID, maximum)
}

func (c *StableSegmentFrontierCollector) record(owner *StableSegmentOwner, frontier DurableFrontier, synced bool, retention StableSegmentRetention, registrar StableSegmentRegistrar, fileID uint32, maximum uint64) error {
	if c == nil || owner == nil || frontier.exactRIDs != nil {
		return ErrStableMetadataShapeUnsupported
	}
	if err := validateDurableFrontier(frontier); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || c.terminating {
		return ErrResourceOwnership
	}
	for i := range c.entries {
		e := &c.entries[i]
		if e.owner == owner {
			if retention != nil || frontier.Bytes < e.frontier.Bytes || frontier.MaxRID < e.frontier.MaxRID || frontier.MaxLSN < e.frontier.MaxLSN {
				return ErrResourceConflict
			}
			e.frontier = frontier
			e.synced = synced
			return nil
		}
	}
	if uint64(len(c.entries)) >= c.max || retention == nil && registrar == nil {
		return ErrStableMetadataShapeUnsupported
	}
	var resident BackingCensus
	var err error
	if retention != nil {
		resident, err = retention.RetainedBackingCensus()
		if err != nil {
			return err
		}
	}
	owner.mu.Lock()
	defer owner.mu.Unlock()
	source := owner.token
	if source == nil || owner.refs.Load() == 0 || source.reachability != ReachabilityOuterLeafRawPointer || source.syncedFrontier.exactRIDs != nil || !source.backingCertified || source.metadataAccount != nil && !stableMetadataAccountsEqual(source.metadataAccount, owner.resident) {
		return ErrStableMetadataShapeUnsupported
	}
	// Full source backing is charged once, while the actual owner is held.
	n, err := finiteStableAdd(owner.census.LiveClassBytes, resident.LiveClassBytes)
	if err != nil {
		return err
	}
	if err = c.account.ReserveStableMetadata(n); err != nil {
		return err
	}
	var borrower *stableRegistryBorrower
	if source.pinRegistry != nil {
		borrower, err = source.pinRegistry.acquireBorrower(c.account)
		if err != nil {
			return err
		}
	}
	if registrar != nil {
		retention, resident, err = registrar.AcquireStableSegmentRetentionForIdentity(fileID, source.identity, maximum, c.account)
		if err != nil {
			borrower.release()
			return err
		}
		// The concrete registrar already debited the complete hold loan before its
		// construction; no fallible account call follows successful acquisition.
		n += resident.LiveClassBytes
	}
	owner.refs.Add(1)
	c.entries = append(c.entries, stableSegmentFrontier{owner: owner, borrower: borrower, retention: retention, frontier: frontier, synced: synced})
	c.backing += n
	return nil
}

// HasOwner returns independent membership only; no backing/control is exported.
func (c *StableSegmentFrontierCollector) HasOwner(owner *StableSegmentOwner) bool {
	if c == nil {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || c.terminating {
		return false
	}
	for i := range c.entries {
		if c.entries[i].owner == owner {
			return true
		}
	}
	return false
}

// Freeze uses the same owned set entries and token ownership states. It prepares
// ALL set/entry/field backing before the first token clone/claim and transfers
// registrar holds to final tokens. There is no generic accounted builder API.
func (c *StableSegmentFrontierCollector) Freeze() (*StableResourceSet, error) {
	if c == nil {
		return nil, ErrResourceOwnership
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || c.terminating || len(c.entries) == 0 {
		return nil, ErrResourceOwnership
	}
	n, err := StableBackingClassBytes(uint64(unsafe.Sizeof(StableResourceSet{})), true)
	if err != nil {
		return nil, err
	}
	n, err = finiteStableClassAdd(n, uint64(len(c.entries))*uint64(unsafe.Sizeof(stableResourceEntry{})), true)
	if err != nil {
		return nil, err
	}
	// The existing deterministic small field table has exact capacity one.
	for range c.entries {
		n, err = finiteStableClassAdd(n, uint64(unsafe.Sizeof(stableSmallTable[ReachabilityField, struct{}]{})), true)
		if err != nil {
			return nil, err
		}
		n, err = finiteStableClassAdd(n, uint64(unsafe.Sizeof(stableSmallBinding[ReachabilityField, struct{}]{})), true)
		if err != nil {
			return nil, err
		}
		n, err = finiteStableClassAdd(n, uint64(len(ReachabilityOuterLeafRawPointer)), false)
		if err != nil {
			return nil, err
		}
	}
	for i := range c.entries {
		owner := c.entries[i].owner
		owner.mu.Lock()
		source := owner.token
		if source != nil && source.namespace != nil && source.namespace.state.Load() != namespaceStable {
			owner.mu.Unlock()
			return nil, ErrNamespaceUnstable
		}
		if source == nil || source.metadataAccount != nil && !stableMetadataAccountsEqual(source.metadataAccount, owner.resident) || !source.backingCertified {
			owner.mu.Unlock()
			return nil, ErrStableMetadataShapeUnsupported
		}
		var p stableBackingSizePlan
		p.add(uint64(unsafe.Sizeof(StableResourceToken{})), true)
		p.add(uint64(unsafe.Sizeof(IdentityPin{})), true)
		p.string(source.logicalLane)
		p.string(source.resourceID)
		p.string(source.diagnosticPath)
		p.string(string(source.reachability))
		owner.mu.Unlock()
		if p.err != nil {
			return nil, p.err
		}
		n, err = finiteStableAdd(n, p.bytes)
		if err != nil {
			return nil, err
		}
	}
	if err = c.account.ReserveStableMetadata(n); err != nil {
		return nil, err
	}
	// Namespace validation uses only retained concrete native authority and runs
	// after the complete predebit, before any claim or output backing is born.
	for i := range c.entries {
		owner := c.entries[i].owner
		owner.mu.Lock()
		err := owner.token.namespace.validateStable()
		owner.mu.Unlock()
		if err != nil {
			return nil, err
		}
	}
	retained := 0
	for retained < len(c.entries)+1 {
		if err = c.account.RetainStableMetadata(); err != nil {
			for retained > 0 {
				c.account.ReleaseStableMetadata()
				retained--
			}
			return nil, err
		}
		retained++
	}
	set := &StableResourceSet{metadataAccount: c.account, finiteMetadata: true, metadataBacking: n, entries: make([]stableResourceEntry, 0, len(c.entries))}
	set.owner.Store(uint32(ResourceOwnerBuilder))
	for i := range c.entries {
		e := &c.entries[i]
		token, err := e.owner.captureCollectedFrontier(e.frontier, e.synced, c.account, e.borrower)
		if err != nil {
			// A failed prepaid clone consumes its unit only after its private validation.
			// Source owner/shape was certified above under immutable owner provenance.
			for j := i + 1; j < len(c.entries); j++ {
				c.account.ReleaseStableMetadata()
			}
			// Retain the actual partially constructed set and registrar edges on
			// cleanup refusal. Its account is not released before checked terminal.
			c.failedSet = set
			c.terminating = true
			return nil, err
		}
		if err = token.claim(ResourceOwnerBuilder); err != nil {
			panic("fresh collector token ownership changed")
		}
		token.segmentRetention = e.retention
		e.retention = nil
		set.entries = append(set.entries, stableResourceEntry{token: token, logicalLane: token.logicalLane, resourceID: token.resourceID, diagnosticPath: token.diagnosticPath, frontier: e.frontier, reachability: newStableReachabilitySet(1, ReachabilityOuterLeafRawPointer)})
	}
	_ = c.closeLocked(nil)
	return set, nil
}
func (c *StableSegmentFrontierCollector) prepareTerminalLocked(consumer StableSegmentTerminalConsumer) error {
	count := 0
	for i := range c.entries {
		if c.entries[i].retention != nil {
			if err := validateStableSegmentTerminalRetention(c.entries[i].retention, consumer); err != nil {
				return err
			}
			count++
		}
	}
	failed := c.failedSet
	if failed != nil {
		for i := range failed.entries {
			token := failed.entries[i].token
			if err := validateStableSegmentTerminalRetention(token.segmentRetention, consumer); err != nil {
				return err
			}
			if token.segmentRetention != nil {
				count++
			}
		}
	}
	if count == 0 || consumer == nil {
		return nil
	}
	var plan stableBackingSizePlan
	plan.add(uint64(count)*uint64(unsafe.Sizeof(StableSegmentTerminalGroup{})), true)
	plan.add(uint64(count)*2*uint64(unsafe.Sizeof((*IdentityPin)(nil))), true)
	if plan.err != nil {
		return plan.err
	}
	if err := c.account.ReserveStableMetadata(plan.bytes); err != nil {
		return err
	}
	groups := make([]StableSegmentTerminalGroup, 0, count)
	pins := make([]*IdentityPin, 0, count*2)
	add := func(retention StableSegmentRetention, token *StableResourceToken, owner *StableSegmentOwner) {
		if retention == nil {
			return
		}
		start := len(pins)
		if token != nil && !token.released.Load() && token.identityPin != nil {
			pins = append(pins, token.identityPin)
		}
		if owner != nil {
			owner.mu.Lock()
			refs := int64(0)
			for i := range c.entries {
				if c.entries[i].owner == owner {
					refs++
				}
			}
			if failed != nil {
				for i := range failed.entries {
					if failed.entries[i].token.segmentOwner == owner {
						refs++
					}
				}
			}
			if owner.token != nil && owner.refs.Load() == refs && owner.token.identityPin != nil {
				first := true
				for _, group := range groups {
					for _, pin := range group.OwnedPins {
						if pin == owner.token.identityPin {
							first = false
						}
					}
				}
				if first {
					pins = append(pins, owner.token.identityPin)
				}
			}
			owner.mu.Unlock()
		}
		groups = append(groups, StableSegmentTerminalGroup{Retention: retention, OwnedPins: pins[start:len(pins)]})
	}
	for i := range c.entries {
		add(c.entries[i].retention, nil, c.entries[i].owner)
	}
	if failed != nil {
		for i := range failed.entries {
			token := failed.entries[i].token
			add(token.segmentRetention, token, token.segmentOwner)
		}
	}
	return consumer.PrepareTerminalRelease(groups)
}

func (c *StableSegmentFrontierCollector) closeLocked(consumer StableSegmentTerminalConsumer) error {
	if c.closed {
		return nil
	}
	if err := c.prepareTerminalLocked(consumer); err != nil {
		return err
	}
	c.terminating = true
	// Release all exact duplicate handles/pins before the registrar's final
	// physical unlink. Transient plans already excluded foreign pins.
	for i := range c.entries {
		e := &c.entries[i]
		if e.owner != nil {
			e.owner.release()
			e.owner = nil
		}
	}
	if c.failedSet != nil {
		if err := c.failedSet.releaseFromWithTerminal(ResourceOwnerBuilder, consumer); err != nil {
			return err
		}
		c.failedSet = nil
	}
	for i := range c.entries {
		e := &c.entries[i]
		if e.retention != nil {
			if err := releaseStableSegmentTerminalRetention(e.retention, consumer); err != nil {
				return err
			}
			e.retention = nil
		}
		e.borrower.release()
		e.borrower = nil
	}
	clear(c.entries[:cap(c.entries)])
	c.entries = nil
	c.closed = true
	account := c.account
	c.account = nil
	account.ReleaseStableMetadata()
	return nil
}
func (c *StableSegmentFrontierCollector) Close() error { return c.CloseWithTerminal(nil) }
func (c *StableSegmentFrontierCollector) CloseWithTerminal(consumer StableSegmentTerminalConsumer) error {
	if c == nil {
		return nil
	}
	joined := false
	if consumer != nil {
		var err error
		joined, err = consumer.BeginTerminalRelease()
		if err != nil {
			return err
		}
		defer consumer.EndTerminalRelease(joined)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closeLocked(consumer)
}

// This concrete collector loan, held under owner.mu, prepays shared source
// backing once. Generic Capture continues to acquire its own complete loan.
func (o *StableSegmentOwner) captureCollectedFrontier(frontier DurableFrontier, synced bool, account StableMetadataAccount, borrower *stableRegistryBorrower) (*StableResourceToken, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	source := o.token
	if source == nil || o.refs.Load() == 0 || source.metadataAccount != nil && !stableMetadataAccountsEqual(source.metadataAccount, o.resident) {
		account.ReleaseStableMetadata()
		return nil, ErrStableMetadataShapeUnsupported
	}
	token, err := source.cloneSharedPinnedAccountWithCredit(source.logicalLane, source.resourceID, source.diagnosticPath, frontier, source.reachability, nil, nil, nil, account, 0, true, o)
	if err != nil {
		return nil, err
	}
	if borrower != nil {
		borrower.retain()
	}
	token.registryBorrower = borrower
	o.refs.Add(1)
	token.segmentOwner = o
	if synced {
		token.syncedFrontier = frontier
		token.hasSyncedFrontier = true
		token.metrics.physicalFileSyncs.Store(1)
	}
	return token, nil
}
