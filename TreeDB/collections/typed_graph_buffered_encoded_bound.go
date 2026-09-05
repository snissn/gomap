package collections

import (
	"errors"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

func (c *Collection) typedGraphEncodedAdmissionEnabled() bool {
	state := c.typedGraphPublicationSnapshot()
	return state != nil && state.limits.EncodedOutputBytes > 0
}

func typedGraphRootEncodedEntry(total *int64, key, value int) error {
	for _, term := range [...]int64{int64(key), int64(value), typedGraphRootEntryFrameBytes} {
		if err := addTypedGraphEncodedBytes(total, term); err != nil {
			return err
		}
	}
	return nil
}

// This is a fresh borrowed iterator over an owning prepared table, not the
// one-shot publication iterator and not a value-pointer dereference.
func typedGraphTableEncodedBound(table memtable.Table) (total int64, err error) {
	if table == nil {
		return 0, nil
	}
	it := table.NewIterator(nil, nil)
	defer func() { err = errors.Join(err, it.Close()) }()
	for ; it.Valid(); it.Next() {
		value, _, flags := it.UnsafeEntry()
		n := len(value)
		if flags&node.FlagPointer != 0 {
			n = page.ValuePtrSize
		}
		if err := typedGraphRootEncodedEntry(&total, len(it.UnsafeKey()), n); err != nil {
			return 0, err
		}
	}
	return total, it.Error()
}

// Caller holds the existing validated staging/domain ownership. Tail reads
// happen before taking the short debt lock; documents and analyzed text stay
// owned by the existing plan/receipts, with no secondary payload store.
func (c *Collection) bufferedTypedGraphEncodedBound(snap *backenddb.Snapshot, catalog *collectionCatalog, domain *collectionWriteDomain, plan *insertBatchPlan, docs []columnWriteDocument, prepared []preparedTextIndexInsert) (typedGraphEncodedCost, error) {
	var cost typedGraphEncodedCost
	if snap == nil || catalog == nil || normalizedDocumentFormat(catalog.meta.Options.DocumentFormat) != DocumentFormatJSON || columnStoreRetainedPayloadUsesTemplateV1(catalog.meta.Options.ColumnStore) || columnStoreRetainedPayloadUsesSemanticStreamV1(catalog.meta.Options.ColumnStore) {
		return cost, ErrHybridSearchUnsupported
	}
	typed, err := typedGraphTypedAssetsEncodedBound(columnWritePublishInput{meta: catalog.meta, documents: docs, operation: ColumnPublishOperationInsert})
	if err != nil {
		return cost, err
	}
	cost.flush = typed
	if direct := plan.directBufferedInsert; direct != nil {
		if len(direct.templateEntries) != 0 || len(direct.uniqueValueRootPlans) != 0 {
			return cost, ErrHybridSearchUnsupported
		}
		for _, group := range []struct {
			entries []directBufferedRootEntry
			target  *int64
		}{{direct.indexStateEntries, &cost.flush}} {
			for _, entry := range group.entries {
				if entry.flags&node.FlagPointer != 0 {
					return cost, ErrHybridSearchUnsupported // raw primary must precede pointerization
				}
				if err := typedGraphRootEncodedEntry(group.target, len(entry.key), len(entry.value)); err != nil {
					return cost, err
				}
			}
		}
		for _, entry := range direct.primaryEntries {
			if entry.flags&node.FlagPointer != 0 {
				return cost, ErrHybridSearchUnsupported
			}
			valueBytes := len(entry.value)
			if direct.pointerizePrimary && valueBytes > 0 && entry.flags&node.FlagTombstone == 0 {
				if err := typedGraphRootEncodedEntry(&cost.primary, 0, valueBytes); err != nil {
					return cost, err
				}
				valueBytes = page.ValuePtrSize
			}
			if err := typedGraphRootEncodedEntry(&cost.flush, len(entry.key), valueBytes); err != nil {
				return cost, err
			}
		}
		for _, secondary := range direct.secondaryRootPlans {
			for _, entry := range secondary.entries {
				if err := typedGraphRootEncodedEntry(&cost.flush, len(entry.key), 0); err != nil {
					return cost, err
				}
			}
		}
	} else {
		for _, run := range plan.runs {
			n, err := typedGraphTableEncodedBound(run.table)
			if err != nil {
				return cost, err
			}
			if err := addTypedGraphEncodedBytes(&cost.flush, n); err != nil {
				return cost, err
			}
		}
	}
	var pendingIDs int64
	chargeReceipts := func(receipts []*typedGraphPublicationReceipt) error {
		for _, receipt := range receipts {
			for _, doc := range receipt.documents {
				if err := addTypedGraphEncodedBytes(&pendingIDs, int64(len(doc.ID))); err != nil {
					return err
				}
			}
		}
		return nil
	}
	if err := chargeReceipts(domain.typedReceipts); err != nil {
		return cost, err
	}
	// Detached predecessors can still allocate V2 ordinals before this receipt.
	// Existing domain ownership retains both queued and publishing units. A pin
	// that already sees a publishing unit merely causes conservative double charge.
	for _, units := range [][]indexedFlushUnit{domain.indexedFlushUnits, domain.indexedPublishingUnits} {
		for _, unit := range units {
			if err := chargeReceipts(unit.typedReceipts); err != nil {
				return cost, err
			}
		}
	}
	for _, def := range catalog.meta.TextIndexes {
		var states []textDocumentStateValue
		for _, insert := range prepared {
			if insert.indexName == def.Name {
				states = insert.states
				break
			}
		}
		status, found, err := readTextV2StatusAtRoot(snap, catalog, collectionTextV2GenerationsRootName(catalog.meta.Name, def.Name))
		if err != nil || !found || status.NextOrdinal == 0 {
			return cost, errors.Join(err, ErrVectorIndexSnapshotMismatch)
		}
		start := textV2OrdinalBlockStart(status.NextOrdinal, textV2DefaultDocMapBlockSize)
		tail, _, err := collectionGetAppendAtCatalogRoot(snap, catalog, collectionTextV2DocMapRootName(catalog.meta.Name, def.Name), encodeTextV2BlockKey(start), nil)
		if err != nil {
			return cost, err
		}
		n, err := typedGraphTextInsertEncodedBound(def, docs, states, int64(len(tail)), pendingIDs)
		if err != nil {
			return cost, err
		}
		if err := addTypedGraphEncodedBytes(&cost.flush, n); err != nil {
			return cost, err
		}
	}
	return cost, nil
}

func (c *Collection) beginBufferedTypedGraphEncodedFlush(input columnWritePublishInput, runs map[string][]memtable.Table) error {
	if !c.typedGraphEncodedAdmissionEnabled() {
		return nil
	}
	coord := c.collectionSchemaCoordinator()
	state := coord.typedPublication.Load()
	if state.invalid && (c.typedGraphReconcile == nil || state.reconciling != c.typedGraphReconcile) {
		return ErrVectorIndexSnapshotMismatch
	}
	logical, err := typedGraphPublicationInputCost(input, state.limits)
	if err != nil {
		return err
	}
	if err := validateTypedGraphReceiptInput(coord, input, logical); err != nil {
		return err
	}
	actual, err := typedGraphTypedAssetsEncodedBound(input)
	if err != nil {
		return err
	}
	for _, tables := range runs {
		for _, table := range tables {
			n, err := typedGraphTableEncodedBound(table)
			if err != nil {
				return err
			}
			if err := addTypedGraphEncodedBytes(&actual, n); err != nil {
				return err
			}
		}
	}
	var reserved int64
	for _, receipt := range input.typedReceipts {
		if err := addTypedGraphEncodedBytes(&reserved, receipt.encoded.flush); err != nil {
			return err
		}
	}
	if reserved < actual {
		return ErrHybridSearchUnsupported // invariant failure, never a post-append quota rejection
	}
	return beginTypedGraphEncodedAttempt(input.typedReceipts, false)
}
