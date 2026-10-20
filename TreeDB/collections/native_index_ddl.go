package collections

import (
	"bytes"
	"errors"
	"reflect"
	"sort"
	"strings"
	"unsafe"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/cowbtree"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/txnhistory"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

// nativeIndexMutation is synchronous logical work belonging to the opening
// capture. It retains no mutable collection facade or physical publication role.
type nativeIndexMutation struct {
	name                        string
	incarnation, interpretation uint64
	bootstrap                   *nativeEngineVersion
	meta                        CollectionMeta
	domains                     []nativeJSONDomainChange
	catalog                     []memtable.COWMutation
	custody                     *nativeMutationCustody
}

func (p *nativeIndexMutation) PrepareNativePublication(latest backenddb.NativeSemanticState, exact *backenddb.Snapshot) (backenddb.NativeFinalizedPublication, error) {
	base, ok := latest.(*nativeEngineVersion)
	if latest == nil {
		base = p.bootstrap
		ok = base != nil
	}
	if !ok || exact == nil {
		return backenddb.NativeFinalizedPublication{}, backenddb.ErrNativeSemanticUnsupported
	}
	old, found := base.names.Get(p.name)
	if !found || old.incarnation != p.incarnation {
		return backenddb.NativeFinalizedPublication{}, backenddb.ErrConcurrentModification
	}
	counter, err := nativeCatalogIdentityRecord(base.nextIncarnation, true)
	if err != nil {
		return backenddb.NativeFinalizedPublication{}, err
	}
	for i := range p.catalog {
		if string(p.catalog[i].Key) == nativeCatalogNextIncarnationKey {
			copy(p.catalog[i].Value, counter[:])
		}
	}
	next, err := prepareNativeIndexMutation(base, old, p.meta, p.domains, p.catalog, p.interpretation)
	if err == nil {
		c, _ := next.collections.Get(p.incarnation)
		(nativeCollectionLifetime{}).Retain(c)
		p.custody.adoptCollection(c)
	}
	return backenddb.NativeFinalizedPublication{State: next}, err
}

func nativeIndexDomain(name string) bool {
	return name == "index-state" || strings.HasPrefix(name, "index/")
}

// A pure drop is an ordered subset with exactly the same index definitions.
// Index-state values encode names, so neither surviving roots nor their pending
// sources need to be rewritten when a declaration is removed.
func nativeIndexSubsetRemoval(before, after []IndexDefinition) bool {
	if len(after) >= len(before) {
		return false
	}
	next := 0
	for _, prior := range before {
		if next < len(after) && prior.Name == after[next].Name {
			if !indexDefinitionValuesEqual(prior, after[next]) {
				return false
			}
			next++
		}
	}
	return next == len(after)
}

func nativeIndexRemovedDomain(name string, after []IndexDefinition) bool {
	if name == "index-state" {
		return len(after) == 0
	}
	indexName, ok := strings.CutPrefix(name, "index/")
	if !ok {
		return false
	}
	for _, index := range after {
		if index.Name == indexName {
			return false
		}
	}
	return true
}

// Preserve the latest primary/templates and their original physical resources.
// Creation replaces schema-derived domains from the complete opening cut. Pure
// removal retains unchanged roots, source boundaries and interpretations.
func prepareNativeIndexMutation(base *nativeEngineVersion, old *nativeCollectionState, meta CollectionMeta, changes []nativeJSONDomainChange, effects []memtable.COWMutation, interpretation uint64) (next *nativeEngineVersion, resultErr error) {
	scratch, err := newNativeEnginePreparation(base, 1, len(changes), base.allocator)
	if err != nil {
		return nil, err
	}
	p := scratch.preparation
	defer func() {
		p.sealPrivateBuilders(scratch)
		p = nil
		work := scratch.Release()
		scratch = nil
		resultErr = errors.Join(resultErr, finishPrivateNative(work))
	}()
	admit := func(n uint64) (cowbtree.OwnedMapLease, error) { return base.allocator.AdmitNativeBytes(n) }
	p.schema, err = newNativeSchemaState(&meta, admit)
	if err != nil {
		return nil, err
	}
	pureDrop := nativeIndexSubsetRemoval(old.schema.meta.Indexes, meta.Indexes)
	mutations := old.domains.Len() + len(changes)
	p.domainBuilder, err = cowbtree.NewOwnedMapBuilder(old.domains, mutations, nativeDomainLifetime{}, admit)
	if err != nil {
		return nil, err
	}
	p.pendingDomainBuilder, err = cowbtree.NewOwnedMapBuilder(old.pending, mutations, nativeDomainLifetime{}, admit)
	if err != nil {
		return nil, err
	}
	old.domains.Scan(func(name string, _ *nativeDomainState) bool {
		if nativeIndexDomain(name) && (!pureDrop || nativeIndexRemovedDomain(name, meta.Indexes)) {
			err = p.domainBuilder.Delete(name)
			if err == nil {
				err = p.pendingDomainBuilder.Delete(name)
			}
		}
		return err == nil
	})
	if err != nil {
		return nil, err
	}
	for _, change := range changes {
		if !nativeIndexDomain(change.name) {
			return nil, backenddb.ErrNativeSemanticUnsupported
		}
		domain, e := prepareNativeDomainUpdate(nil, change.name, 0, interpretation, change.policy, change.entries, base.allocator)
		if domain != nil {
			p.ownedDomains = append(p.ownedDomains, domain)
		}
		if e != nil {
			return nil, e
		}
		if err = p.domainBuilder.Set(change.name, domain); err != nil {
			return nil, err
		}
		if err = p.pendingDomainBuilder.Set(change.name, domain); err != nil {
			return nil, err
		}
	}
	p.domains, p.pendingDomains = p.domainBuilder.Seal(), p.pendingDomainBuilder.Seal()
	p.domainBuilder, p.pendingDomainBuilder = nil, nil
	collection, err := newNativeCollectionState(old.incarnation, p.schema, p.domains, p.pendingDomains, admit)
	if err != nil {
		return nil, err
	}
	p.schema, p.domains, p.pendingDomains = nil, nil, nil
	p.ownedCollections = append(p.ownedCollections, collection)
	next, err = prepareNativeCatalogReplacement(base, old, collection, effects)
	return next, err
}

// Every DDL operation conflicts on the same catalog name already read by ALL
// native transaction bindings, including bindings used only to derive a write.
func (m *NativeJSONManager) mutateIndexes(name string, create *IndexDefinition, remove map[string]struct{}, all bool) (out *CollectionMeta, resultErr error) {
	if m == nil || m.db == nil {
		return nil, backenddb.ErrClosed
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	read, err := m.Capture(name)
	if err != nil {
		return nil, err
	}
	defer func() { resultErr = errors.Join(resultErr, read.Close()) }()
	if read.meta.Options.ColumnStore != nil {
		return nil, backenddb.ErrNativeSemanticUnsupported
	}
	base, _ := read.capture.State().(*nativeEngineVersion)
	if base == nil {
		base = read.privateEngine
	}
	// All temporary arrays, decoder objects and encoded bytes remain within this
	// operation receipt until private COW sources own their copies and Write returns.
	lease, err := base.allocator.AdmitNativeBytes(0)
	if err != nil {
		return nil, err
	}
	var charge uint64
	admit := func(n uint64) error {
		n = memtable.COWAllocationCharge(n)
		if n > ^uint64(0)-charge {
			return memtable.ErrCOWCapacity
		}
		if e := lease.Resize(charge + n); e != nil {
			return e
		}
		charge += n
		return nil
	}
	defer lease.Close()
	return func() (*CollectionMeta, error) {
		if err = nativeIndexSchemaScratch(read.meta, create, admit); err != nil {
			return nil, err
		}
		meta := read.meta
		if create != nil {
			meta, _, err = addIndexToCollectionMeta(meta, *create)
		} else {
			meta = *meta.copy()
			kept := meta.Indexes[:0]
			dropped := 0
			for _, index := range meta.Indexes {
				_, selected := remove[index.Name]
				if all || selected {
					dropped++
				} else {
					kept = append(kept, index)
				}
			}
			if !all && dropped != len(remove) {
				return nil, ErrIndexNotFound
			}
			if dropped == 0 {
				return meta.copy(), nil
			}
			meta.Indexes = kept
		}
		if err != nil {
			return nil, err
		}
		meta, err = nativeJSONMeta(meta)
		if err != nil {
			return nil, err
		}
		var rows nativeJSONRows
		var domains []nativeJSONDomainChange
		var catalog []memtable.COWMutation
		var plan *directBufferedInsertPlan
		var logical *nativeIndexMutation
		if err = admit(uint64(unsafe.Sizeof(nativeIndexMutation{}))); err != nil {
			return nil, err
		}
		pureDrop := nativeIndexSubsetRemoval(read.meta.Indexes, meta.Indexes)
		if !pureDrop {
			if err = admit(uint64(page.PageSize)); err != nil {
				return nil, err
			}
			cursor := make([]byte, page.PageSize)
			if read.domain.domain != nil {
				var start []byte
				for {
					entry, found, e := nativeJSONSeekLive(&read.domain, read, start, nil, cursor)
					if e != nil {
						return nil, e
					}
					if !found {
						break
					}
					if len(entry.Key) >= len(cursor) {
						return nil, memtable.ErrCOWCapacity
					}
					if err = admit(uint64(len(entry.Key))); err != nil {
						return nil, err
					}
					id := bytes.Clone(entry.Key)
					var value []byte
					if entry.Flags&node.FlagPointer != 0 {
						value, e = read.readValue(entry.ValuePtr, admit)
					} else {
						if e = admit(uint64(len(entry.Value))); e == nil {
							value = bytes.Clone(entry.Value)
						}
					}
					if e != nil {
						return nil, e
					}
					row, e := rows.touch(id, func([]byte) ([]byte, error) { return nil, nil }, admit)
					if e != nil {
						return nil, e
					}
					row.current = value
					n := copy(cursor, id)
					cursor[n] = 0
					start = cursor[:n+1]
				}
			}
			var scratchPeak uint64
			scratch := func(doc []byte) error {
				n, e := nativeJSONIndexedPlanCharge(meta, [][]byte{cursor}, [][]byte{doc})
				if e != nil {
					return e
				}
				if n <= scratchPeak {
					return nil
				}
				if e = admit(n - scratchPeak); e != nil {
					return e
				}
				scratchPeak = n
				return nil
			}
			plan, _, _, err = nativeJSONFinalRows(name, meta, &rows, admit, scratch)
			if err != nil {
				return nil, err
			}
			if err = admitNativeInsertArray(len(plan.primaryEntries), uint64(unsafe.Sizeof(memtable.COWMutation{})), admit); err != nil {
				return nil, err
			}
			primary := make([]memtable.COWMutation, len(plan.primaryEntries))
			domains, _, err = extractNativeInsertEffects(name, read.collection.incarnation, plan, primary, admit)
			if err != nil {
				return nil, err
			}
			if err = admitNativeInsertArray(len(meta.Indexes)+1, uint64(unsafe.Sizeof(nativeJSONDomainChange{})), admit); err != nil {
				return nil, err
			}
			complete := make([]nativeJSONDomainChange, 0, len(meta.Indexes)+1)
			for _, domain := range domains {
				if nativeIndexDomain(domain.name) {
					complete = append(complete, domain)
				}
			}
			domains = complete
			// Missing/empty index roots must also replace their old physical domain.
			for _, index := range meta.Indexes {
				if err = admit(uint64(len("index/") + len(index.Name))); err != nil {
					return nil, err
				}
				relative := "index/" + index.Name
				found := false
				for _, d := range domains {
					found = found || d.name == relative
				}
				if !found {
					policy, _ := backendRootStoragePolicy(index.StoragePolicy)
					if err = admit(uint64(unsafe.Sizeof(nativeJSONDomainChange{})) + uint64(len(relative))); err != nil {
						return nil, err
					}
					domains = append(domains, nativeJSONDomainChange{name: relative, policy: policy})
				}
			}
		}
		before, err := encodeNormalizedCollectionMeta(read.meta)
		if err != nil {
			return nil, err
		}
		after, err := encodeNormalizedCollectionMeta(meta)
		if err != nil {
			return nil, err
		}
		catalog, err = nativeIndexCatalogEffects(read.meta, meta, after, read.collection.incarnation, base.nextIncarnation, admit)
		if err != nil {
			return nil, err
		}
		if err = admit(uint64(22 + len(name) + len(before) + len(after))); err != nil {
			return nil, err
		}
		payload, err := commitlog.EncodeCatalogIndexMutationPayload(commitlog.CatalogIndexMutationPayload{Collection: name, Incarnation: read.collection.incarnation, BeforeMetadata: before, Metadata: after})
		if err != nil {
			return nil, err
		}
		if read.capture.LogicalSequence() == ^uint64(0) {
			return nil, memtable.ErrCOWCapacity
		}
		if err = admit(uint64(unsafe.Sizeof(backenddb.NativePreparedPublication{})) + uint64(unsafe.Sizeof(backenddb.CommandWALIntent{})) + uint64(unsafe.Sizeof(txnhistory.Effect{})) + 2*uint64(unsafe.Sizeof(txnhistory.Dependency{})) + uint64(len(name))); err != nil {
			return nil, err
		}
		custody, err := newNativeMutationCustody(0, 1, base.allocator)
		if err != nil {
			return nil, err
		}
		logical = &nativeIndexMutation{name: name, incarnation: read.collection.incarnation, interpretation: read.capture.LogicalSequence() + 1, bootstrap: base, meta: meta, domains: domains, catalog: catalog, custody: custody}
		changed := []txnhistory.Effect{{Namespace: txnhistory.Namespace{Kind: txnhistory.CatalogNamespace}, Kind: txnhistory.PointEffect, Key: []byte(name)}}
		dependencies := []txnhistory.Dependency{changed[0], {Namespace: txnhistory.Namespace{Kind: txnhistory.CollectionNamespace, Collection: read.collection.incarnation, Domain: 1, Interpretation: 1}, Kind: txnhistory.RangeEffect}}
		publication := &backenddb.NativePreparedPublication{Basis: read.capture.State(), StartSequence: read.capture.LogicalSequence(), Reprepare: logical, Custody: custody, Effects: changed, Dependencies: dependencies}
		intent, err := m.db.NewTrustedCommandWALIntent(commitlog.CommandKindCatalogIndexMutation, commitlog.CommandScopeCatalog, commitlog.PayloadFormatCatalogIndexMutationV1, payload)
		if err != nil {
			return nil, errors.Join(err, cancelPrivateNativePublication(publication))
		}
		result, err := m.db.WriteNativePublication(publication, intent, false)
		if !result.Consumed {
			err = errors.Join(err, cancelPrivateNativePublication(publication))
		}
		if err != nil {
			return nil, err
		}
		return meta.copy(), nil
	}()
}

func nativeIndexCatalogEffects(before, after CollectionMeta, encoded []byte, incarnation, next uint64, admit func(uint64) error) ([]memtable.COWMutation, error) {
	identity, err := nativeCatalogIdentityRecord(incarnation, false)
	if err != nil {
		return nil, err
	}
	counter, err := nativeCatalogIdentityRecord(next, true)
	if err != nil {
		return nil, err
	}
	if err = admit(uint64(len(after.Name)+len(nativeCatalogIncarnationPrefix)+len(nativeCatalogNextIncarnationKey)) + 32); err != nil {
		return nil, err
	}
	count := 4 + len(before.Indexes)
	if err = admitNativeInsertArray(count, uint64(unsafe.Sizeof(memtable.COWMutation{})), admit); err != nil {
		return nil, err
	}
	effects := make([]memtable.COWMutation, 0, count)
	add := func(key string, value []byte, flags byte) error {
		if e := admit(uint64(len(key)) + uint64(len(value))); e != nil {
			return e
		}
		effects = append(effects, memtable.COWMutation{Key: []byte(key), Value: value, Flags: flags})
		return nil
	}
	if err = add(systemCollectionMetaKey(after.Name), encoded, node.FlagInline); err != nil {
		return nil, err
	}
	if err = add(nativeCatalogIncarnationPrefix+after.Name, identity[:], node.FlagInline); err != nil {
		return nil, err
	}
	if err = add(nativeCatalogNextIncarnationKey, counter[:], node.FlagInline); err != nil {
		return nil, err
	}
	pureDrop := nativeIndexSubsetRemoval(before.Indexes, after.Indexes)
	for _, index := range before.Indexes {
		if pureDrop && !nativeIndexRemovedDomain("index/"+index.Name, after.Indexes) {
			continue
		}
		if err = add(systemCollectionRootKey(collectionSecondaryRootName(before.Name, index.Name)), nil, node.FlagTombstone); err != nil {
			return nil, err
		}
	}
	if !pureDrop || len(after.Indexes) == 0 {
		if err = add(systemCollectionRootKey(collectionIndexStateRootName(before.Name)), nil, node.FlagTombstone); err != nil {
			return nil, err
		}
	}
	sort.Slice(effects, func(i, j int) bool { return bytes.Compare(effects[i].Key, effects[j].Key) < 0 })
	return effects, nil
}

// Admission precedes the maintained normalization and encoding helpers. Four
// schema backings cover candidate slice growth, both normalization passes and
// the returned API copy. Normalization's name/component maps use the same
// conservative small-group/growth allowance as nativeJSONFinalPlanCharge.
// Marshal may retain its growable buffer alongside the returned encoded copy;
// both before/after images are reserved using a checked JSON byte upper bound.
func nativeIndexSchemaScratch(meta CollectionMeta, create *IndexDefinition, admit func(uint64) error) error {
	backing, err := nativeSchemaBackingCharge(reflect.ValueOf(&meta), 0)
	if err != nil {
		return err
	}
	encoded, err := nativeIndexJSONBound(reflect.ValueOf(collectionMetaDisk{Version: collectionMetaVersion, Name: meta.Name, Options: meta.Options, Indexes: meta.Indexes, VectorIndexes: meta.VectorIndexes, TextIndexes: meta.TextIndexes}), 0)
	if err != nil {
		return err
	}
	count := uint64(len(meta.Indexes))
	if create != nil {
		extra, e := nativeSchemaBackingCharge(reflect.ValueOf(create), 0)
		if e != nil {
			return e
		}
		if extra > ^uint64(0)-backing {
			return memtable.ErrCOWCapacity
		}
		backing += extra
		extra, e = nativeIndexJSONBound(reflect.ValueOf(*create), 0)
		if e != nil {
			return e
		}
		if extra >= ^uint64(0)-encoded {
			return memtable.ErrCOWCapacity
		}
		encoded += extra + 1
		count++
	}
	// Appending an index may replace a full slice with a doubled backing.
	if count > ^uint64(0)/(2*uint64(unsafe.Sizeof(IndexDefinition{}))) {
		return memtable.ErrCOWCapacity
	}
	if err = admit(count * 2 * uint64(unsafe.Sizeof(IndexDefinition{}))); err != nil {
		return err
	}
	for copy := 0; copy < 4; copy++ {
		if err = admit(backing); err != nil {
			return err
		}
	}
	for normalization := 0; normalization < 2; normalization++ {
		if err = admit(256); err != nil {
			return err
		}
		for i := uint64(0); i < count; i++ {
			if err = admit(768 + 256 + 768 + uint64(unsafe.Sizeof(IndexComponent{}))); err != nil {
				return err
			}
		}
	}
	// bytes.Buffer growth is bounded by twice its final size; json.Marshal
	// copies that buffer once. Reserve each actual allocation independently.
	for image := 0; image < 2; image++ {
		if encoded > ^uint64(0)/2 {
			return memtable.ErrCOWCapacity
		}
		if err = admit(2 * encoded); err != nil {
			return err
		}
		if err = admit(encoded); err != nil {
			return err
		}
	}
	return nil
}

// Metadata's finite scalar/struct/slice shape permits a no-output size census.
// Omitempty fields are included conservatively; strings may escape every byte
// as six JSON bytes. This is construction scratch, never a catalog authority.
func nativeIndexJSONBound(v reflect.Value, depth int) (uint64, error) {
	if depth > 32 {
		return 0, backenddb.ErrNativeSemanticUnsupported
	}
	var total uint64
	add := func(n uint64) error {
		if n > ^uint64(0)-total {
			return memtable.ErrCOWCapacity
		}
		total += n
		return nil
	}
	child := func(value reflect.Value) error {
		n, err := nativeIndexJSONBound(value, depth+1)
		if err != nil {
			return err
		}
		return add(n)
	}
	switch v.Kind() {
	case reflect.Pointer:
		if v.IsNil() {
			return 4, nil
		}
		return nativeIndexJSONBound(v.Elem(), depth+1)
	case reflect.String:
		if uint64(v.Len()) > (^uint64(0)-2)/6 {
			return 0, memtable.ErrCOWCapacity
		}
		return 2 + 6*uint64(v.Len()), nil
	case reflect.Bool:
		return 5, nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Float32, reflect.Float64:
		return 24, nil
	case reflect.Slice, reflect.Array:
		if v.Kind() == reflect.Slice && v.IsNil() {
			return 4, nil
		}
		total = 2
		for i := 0; i < v.Len(); i++ {
			if err := add(1); err != nil {
				return 0, err
			}
			if err := child(v.Index(i)); err != nil {
				return 0, err
			}
		}
	case reflect.Struct:
		total = 2
		for i := 0; i < v.NumField(); i++ {
			field := v.Type().Field(i)
			if field.PkgPath != "" {
				continue
			}
			name, _, _ := strings.Cut(field.Tag.Get("json"), ",")
			if name == "-" {
				continue
			}
			if name == "" {
				name = field.Name
			}
			if err := add(4 + 6*uint64(len(name))); err != nil {
				return 0, err
			}
			if err := child(v.Field(i)); err != nil {
				return 0, err
			}
		}
	case reflect.Map:
		if v.Len() != 0 {
			return 0, backenddb.ErrNativeSemanticUnsupported
		}
		return 4, nil
	default:
		return 0, backenddb.ErrNativeSemanticUnsupported
	}
	return total, nil
}
