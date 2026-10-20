package collections

import (
	"bytes"
	"errors"
	"unsafe"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/txnhistory"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/tree"
)

// This resolver has no cache or independent lifetime. Each read is against its
// caller's exact captured templates domain; parsing and copies spend that same
// operation's admission. The maintained preparation resolver overlays new IDs.
type nativeTemplateResolver struct {
	read  func([]byte) ([]byte, error)
	admit func(uint64) error
}

func nativeTemplateParseCharge(source int) (uint64, error) {
	// Each field consumes at least a length byte; include its owned string header,
	// bytes, parsed record and decoder's object/map entry backing. Two original
	// resolver maps may grow while their old buckets remain live. Record count is
	// bounded by the input envelope's 32-byte hash + length + TD1T header.
	if source < 0 {
		return 0, memtable.ErrCOWCapacity
	}
	n := uint64(source)
	field := uint64(unsafe.Sizeof(string(""))) + 8
	record := 3*uint64(unsafe.Sizeof(templateV1Record{})) + uint64(unsafe.Sizeof(templateV1Template{})) + 4*768
	if n > ^uint64(0)/(field+8) {
		return 0, memtable.ErrCOWCapacity
	}
	amount := memtable.COWAllocationCharge(n * (field + 8))
	count := n/38 + 1
	if count > ^uint64(0)/record {
		return 0, memtable.ErrCOWCapacity
	}
	extra := memtable.COWAllocationCharge(count * record)
	if extra > ^uint64(0)-amount {
		return 0, memtable.ErrCOWCapacity
	}
	return amount + extra, nil
}
func (r *nativeTemplateResolver) lookupTemplateV1(id uint64) (*templateV1Template, error) {
	if err := r.admit(memtable.COWAllocationCharge(12)); err != nil {
		return nil, err
	}
	raw, err := r.read(templateV1RecordKey(id))
	if errors.Is(err, tree.ErrKeyNotFound) {
		return nil, errTemplateV1TemplateNotFound
	}
	if err != nil {
		return nil, err
	}
	amount, err := nativeTemplateParseCharge(len(raw))
	if err != nil {
		return nil, err
	}
	if err = r.admit(amount); err != nil {
		return nil, err
	}
	record, err := parseTemplateV1RecordWithID(id, raw)
	return record.tpl, err
}
func (r *nativeTemplateResolver) lookupTemplateV1ByHash(hash [32]byte) (*templateV1Template, error) {
	if err := r.admit(memtable.COWAllocationCharge(33)); err != nil {
		return nil, err
	}
	raw, err := r.read(templateV1HashKey(hash))
	if errors.Is(err, tree.ErrKeyNotFound) {
		return nil, errTemplateV1TemplateNotFound
	}
	if err != nil {
		return nil, err
	}
	id, err := decodeTemplateV1ID(raw)
	if err != nil {
		return nil, err
	}
	tpl, err := r.lookupTemplateV1(id)
	if err != nil {
		return nil, err
	}
	if tpl.hash != hash {
		return nil, errors.New("collections: template-v1 template hash mismatch")
	}
	return tpl, nil
}
func (r *nativeTemplateResolver) nextTemplateV1ID() (uint64, error) {
	if err := r.admit(memtable.COWAllocationCharge(2)); err != nil {
		return 0, err
	}
	raw, err := r.read(templateV1NextIDKey())
	if errors.Is(err, tree.ErrKeyNotFound) {
		return 1, nil
	}
	if err != nil {
		return 0, err
	}
	return decodeTemplateV1ID(raw)
}
func (r *NativeJSONRead) templateValue(key []byte, admit func(uint64) error) ([]byte, error) {
	if r.collection == nil {
		return nil, errTemplateV1MissingTemplateRoot
	}
	domain, ok := r.collection.domains.Get("templates")
	if !ok {
		return nil, tree.ErrKeyNotFound
	}
	saved := r.domain
	var err error
	r.domain, err = newNativeDomainRead(domain, r.capture.Snapshot())
	if err != nil {
		return nil, err
	}
	defer func() { r.domain = saved }()
	entry, _, err := r.lookupEntry(key, false)
	if err != nil {
		return nil, err
	}
	if entry.Flags&node.FlagTombstone != 0 {
		return nil, tree.ErrKeyNotFound
	}
	if entry.Flags&node.FlagPointer != 0 {
		return r.readValue(entry.ValuePtr, admit)
	}
	if err = admit(memtable.COWAllocationCharge(uint64(len(entry.Value)))); err != nil {
		return nil, err
	}
	return bytes.Clone(entry.Value), nil
}
func (r *NativeJSONRead) templateResolver(admit func(uint64) error) templateV1Resolver {
	return &nativeTemplateResolver{read: func(key []byte) ([]byte, error) { return r.templateValue(key, admit) }, admit: admit}
}
func (t *NativeJSONTxn) templateOptions(g *nativeJSONTxnBinding, opts collectionOptions) (collectionOptions, error) {
	if normalizedDocumentFormat(opts.documentFormat) != DocumentFormatTemplateV1 {
		return opts, nil
	}
	if g.templates == nil {
		// The resolver and its read/admit closures (including the synchronous
		// nested read admission closure) belong to this same stage. Each closure
		// carries only its entry address and the original transaction/binding.
		environment := uint64(unsafe.Sizeof(struct {
			entry   uintptr
			txn     *NativeJSONTxn
			binding *nativeJSONTxnBinding
		}{}))
		charge := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(nativeTemplateResolver{}))) + 3*memtable.COWAllocationCharge(environment)
		if err := t.admitStage(g, charge); err != nil {
			return opts, err
		}
		g.templates = &nativeTemplateResolver{read: func(key []byte) ([]byte, error) {
			namespace := g.namespace()
			namespace.Domain = 2
			if err := t.trackDependency(namespace, txnhistory.PointEffect, key, nil); err != nil {
				return nil, err
			}
			return g.probe.templateValue(key, func(n uint64) error { return t.admitStage(g, n) })
		}, admit: func(n uint64) error { return t.admitStage(g, n) }}
	}
	opts.templateResolver = g.templates
	opts.allowTemplateV1Stored = g.allowStored
	opts.learnTemplateIDs = g.learnTemplates
	return opts, nil
}
func (t *NativeJSONTxn) prepareTemplateDocuments(g *nativeJSONTxnBinding, documents [][]byte, opts collectionOptions) ([][]byte, collectionOptions, error) {
	var err error
	opts, err = t.templateOptions(g, opts)
	if err != nil {
		return nil, opts, err
	}
	if normalizedDocumentFormat(opts.documentFormat) != DocumentFormatTemplateV1 {
		prepared, _, _, _, err := prepareInsertDocuments(documents, opts)
		return prepared, opts, err
	}
	for _, doc := range documents {
		amount, err := nativeTemplateParseCharge(len(doc))
		if err != nil {
			return nil, opts, err
		}
		if err = t.admitStage(g, amount); err != nil {
			return nil, opts, err
		}
	}
	prepared, records, learned, resolver, err := prepareInsertDocuments(documents, opts)
	if err != nil {
		return nil, opts, err
	}
	if err = admitNativeInsertArray(len(g.templateRecords)+len(records), uint64(unsafe.Sizeof(templateV1Record{})), func(n uint64) error { return t.admitStage(g, n) }); err != nil {
		return nil, opts, err
	}
	if err = admitNativeInsertArray(len(g.templateLearned)+len(learned), uint64(unsafe.Sizeof(templateV1LearnedTemplate{})), func(n uint64) error { return t.admitStage(g, n) }); err != nil {
		return nil, opts, err
	}
	all := make([]templateV1Record, len(g.templateRecords)+len(records))
	copy(all, g.templateRecords)
	copy(all[len(g.templateRecords):], records)
	g.templateRecords = all
	ids := make([]templateV1LearnedTemplate, len(g.templateLearned)+len(learned))
	copy(ids, g.templateLearned)
	copy(ids[len(g.templateLearned):], learned)
	g.templateLearned = ids
	g.templates = resolver
	opts.templateResolver = resolver
	return prepared, opts, nil
}

// Reuse the maintained entry encoder, without creating a legacy run table.
func nativeTemplatePlanEntries(plan *directBufferedInsertPlan, name string, records []templateV1Record, opts collectionOptions, admit func(uint64) error) error {
	if len(records) == 0 {
		return nil
	}
	count := len(records)*2 + 1
	if count <= 0 || count < len(records) {
		return memtable.ErrCOWCapacity
	}
	if err := admitNativeInsertArray(count, uint64(unsafe.Sizeof(directBufferedRootEntry{}))+48, admit); err != nil {
		return err
	}
	if len(name) > int(^uint(0)>>1)-len("/templates") {
		return memtable.ErrCOWCapacity
	}
	if err := admit(memtable.COWAllocationCharge(uint64(len(name) + len("/templates")))); err != nil {
		return err
	}
	if err := admitNativeInsertArray(len(plan.rootNames)+1, uint64(unsafe.Sizeof(string("")))+uint64(unsafe.Sizeof(backenddb.OrderedRootStoragePolicy(0))), admit); err != nil {
		return err
	}
	planner := insertBatchPlanner{options: opts}
	entries, err := planner.directBufferedTemplateRootEntries(records)
	if err != nil {
		return err
	}
	plan.templateRootName = collectionTemplateRootName(name)
	plan.templateEntries = entries
	plan.addRoot(plan.templateRootName, opts.dataStoragePolicy)
	return nil
}

// The materializer retains the original exact read capture, never a template
// cache. All parser/object/output working storage is operation-owned. Returned
// JSON is an independent caller-owned byte slice before this receipt refunds.
func (r *NativeJSONRead) templateJSON(document []byte) (out []byte, resultErr error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.capture == nil || r.closing || r.failure != nil {
		return nil, backenddb.ErrClosed
	}
	lease, err := r.capture.NativeAllocator().AdmitNativeBytes(0)
	if err != nil {
		return nil, err
	}
	var charge uint64
	admit := func(n uint64) error {
		if n > ^uint64(0)-charge {
			return memtable.ErrCOWCapacity
		}
		if err := lease.Resize(charge + n); err != nil {
			return err
		}
		charge += n
		return nil
	}
	// Every encoded value consumes a source byte. The original object decoder's
	// map/key/value cells plus JSON's simultaneous growing buffers are bounded by
	// one maintained map reserve per source byte; field names are covered below.
	n := uint64(len(document))
	if n > ^uint64(0)/768 {
		lease.Close()
		return nil, memtable.ErrCOWCapacity
	}
	if err = admit(memtable.COWAllocationCharge(n * 768)); err != nil {
		lease.Close()
		return nil, err
	}
	// Resolve names for every occurrence (including repeated nested templates),
	// admitting decoded map cells and escaped JSON field names before parsing.
	wrapped := &nativeTemplateResolver{read: func(key []byte) ([]byte, error) {
		raw, err := r.templateValue(key, admit)
		if err != nil {
			return nil, err
		}
		n := uint64(len(raw))
		if n > ^uint64(0)/768 {
			return nil, memtable.ErrCOWCapacity
		}
		if err = admit(memtable.COWAllocationCharge(n * 768)); err != nil {
			return nil, err
		}
		return raw, nil
	}, admit: admit}
	defer func() { wrapped = nil; lease.Close() }()
	return templateV1StoredDocumentJSON(document, wrapped)
}
