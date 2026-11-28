package db

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"slices"

	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/tree"
)

const (
	defaultKeepRecentPublishedDicts     = 2
	defaultKeepRecentPublishedTemplates = 16
	// MaxKeepRecentPublishedSystemMetaIDs is the maximum session-recent grace
	// window supported by SystemMetadataAuditOptions.
	MaxKeepRecentPublishedSystemMetaIDs = 64
	maxSystemMetaSegIDCacheEntries      = 4096
)

var (
	systemDictPrefix         = []byte("dict/")
	systemDictPrefixEnd      = []byte("dict0") // '/' + 1 == '0'
	systemDictBytesPrefix    = []byte("dict/bytes/")
	systemDictChunkPrefix    = []byte("dict/chunk/")
	systemDictHashPrefix     = []byte("dict/hash/")
	systemDictKPrefix        = []byte("dict/k/")
	systemDictLeafModePrefix = []byte("dict/leafmode/")
	systemDictCurrentKey     = []byte("dict/current")
	systemDictCurrentPrefix  = []byte("dict/current/")

	systemTemplatePrefix      = []byte("template/")
	systemTemplatePrefixEnd   = []byte("template0") // '/' + 1 == '0'
	systemTemplateChunkPrefix = []byte("template/chunk/")
	systemTemplateFPPrefix    = []byte("template/fp/")
)

type systemMetaSegIDCacheKey struct {
	fileID          uint32
	size            int64
	modTimeUnixNano int64
	scanTemplates   bool
}

type systemMetaSegIDCacheVal struct {
	dictIDs     []uint64
	templateIDs []uint64
}

// SystemMetadataAuditOptions configures read-only reachability auditing for
// dictionaries (dict/*) and templates (template/*) stored in SystemRootPageID.
type SystemMetadataAuditOptions struct {
	// KeepRecentPublishedDicts retains the newest N dictionary IDs published
	// during the current process session as an online-writer grace window.
	// 0 uses the default (2); negative disables session-recent grace.
	// Values above MaxKeepRecentPublishedSystemMetaIDs (64) are rejected.
	KeepRecentPublishedDicts int
	// KeepRecentPublishedTemplates retains the newest N template IDs published
	// during the current process session as an online-writer grace window.
	// 0 uses the default (16); negative disables session-recent grace.
	// Values above MaxKeepRecentPublishedSystemMetaIDs (64) are rejected.
	KeepRecentPublishedTemplates int
	// ProtectedDictIDs are additional caller-pinned dictionary IDs (for example
	// active writer generations held in memory by caching.DB).
	ProtectedDictIDs []uint64
	// ProtectedTemplateIDs are additional caller-pinned template IDs (for
	// example recent templates held in memory by template.Engine).
	ProtectedTemplateIDs []uint64
}

// SystemMetadataAuditReport summarizes live versus reclaimable dictionary and
// template metadata inside SystemRootPageID.
//
// Classification buckets are mutually exclusive:
//   - DictsTotal == DictsLive + DictsActiveCurrent + DictsGraceProtected + DictsReclaimable
//   - TemplatesTotal == TemplatesLive + TemplatesGraceProtected + TemplatesReclaimable
type SystemMetadataAuditReport struct {
	DictsTotal                     int
	DictsLive                      int
	DictsActiveCurrent             int
	DictsGraceProtected            int
	DictsReclaimable               int
	DictKeysTotal                  int
	DictKeysReclaimable            int
	DictInlineBytesReclaimable     int64
	TemplatesTotal                 int
	TemplatesLive                  int
	TemplatesGraceProtected        int
	TemplatesReclaimable           int
	TemplateKeysTotal              int
	TemplateKeysReclaimable        int
	TemplateFPKeysTotal            int
	TemplateFPKeysPrunable         int
	TemplateFPKeysDeletable        int
	TemplateInlineBytesReclaimable int64

	// PointerSegmentsReferenced counts distinct value-log segments referenced by
	// dict/bytes/* or template/def/* ValuePtr entries (typically lane 254).
	PointerSegmentsReferenced int
	// PointerSegmentsOnlyReclaimable counts distinct pointer segments referenced
	// exclusively by reclaimable dicts/templates (eligible for ValueLogGC once
	// those system keys are deleted).
	PointerSegmentsOnlyReclaimable int

	SegmentsScannedValueLog int
	SegmentsScannedLeafLog  int
	SegmentsCacheHits       int

	ReclaimableDictIDs     []uint64
	ReclaimableTemplateIDs []uint64
}

type systemDictInventoryEntry struct {
	id           uint64
	keyCount     int
	inlineBytes  int64
	pointerSegID uint32
	chunkCount   uint32
	isChunked    bool
	hasBytesKey  bool
	chunkKeys    [][]byte
	hashKeys     [][]byte
	hasK         bool
	hasLeafMode  bool
}

type systemTemplateInventoryEntry struct {
	id           uint64
	keyCount     int
	inlineBytes  int64
	pointerSegID uint32
	chunkCount   uint32
	isChunked    bool
	hasDefKey    bool
	chunkKeys    [][]byte
}

type systemTemplateFPEntry struct {
	key        []byte
	candidates []systemTemplateFPCandidate
}

type systemTemplateFPCandidate struct {
	id   uint64
	size uint64
}

type systemMetadataInventory struct {
	dicts         map[uint64]*systemDictInventoryEntry
	templates     map[uint64]*systemTemplateInventoryEntry
	activeDictIDs map[uint64]struct{}
	templateFPs   []systemTemplateFPEntry
	dictKeysTotal int
	tplKeysTotal  int
}

func (db *DB) recordRecentSystemMetadataIDs(ops []SystemKVOp) {
	if db == nil || len(ops) == 0 {
		return
	}
	var dictIDs []uint64
	var tplIDs []uint64
	for _, op := range ops {
		if op.Delete {
			continue
		}
		if id, ok := parseSystemDictBytesKey(op.Key); ok {
			dictIDs = append(dictIDs, id)
		} else if id, ok := parseSystemTemplateDefKey(op.Key); ok {
			tplIDs = append(tplIDs, id)
		}
	}
	if len(dictIDs) == 0 && len(tplIDs) == 0 {
		return
	}
	db.recentSystemMetaMu.Lock()
	for _, id := range dictIDs {
		db.recentPublishedDictIDs = appendRecentUniqueID(db.recentPublishedDictIDs, id, MaxKeepRecentPublishedSystemMetaIDs)
	}
	for _, id := range tplIDs {
		db.recentPublishedTemplateIDs = appendRecentUniqueID(db.recentPublishedTemplateIDs, id, MaxKeepRecentPublishedSystemMetaIDs)
	}
	db.recentSystemMetaMu.Unlock()
}

func appendRecentUniqueID(dst []uint64, id uint64, limit int) []uint64 {
	if id == 0 {
		return dst
	}
	for i, existing := range dst {
		if existing == id {
			copy(dst[i:], dst[i+1:])
			dst[len(dst)-1] = id
			return dst
		}
	}
	dst = append(dst, id)
	if limit > 0 && len(dst) > limit {
		copy(dst, dst[len(dst)-limit:])
		dst = dst[:limit]
	}
	return dst
}

func (db *DB) forgetRecentSystemMetadataIDs(dictIDs, tplIDs []uint64) {
	if db == nil || (len(dictIDs) == 0 && len(tplIDs) == 0) {
		return
	}
	db.recentSystemMetaMu.Lock()
	defer db.recentSystemMetaMu.Unlock()
	if len(dictIDs) > 0 && len(db.recentPublishedDictIDs) > 0 {
		drop := make(map[uint64]struct{}, len(dictIDs))
		for _, id := range dictIDs {
			drop[id] = struct{}{}
		}
		kept := db.recentPublishedDictIDs[:0]
		for _, id := range db.recentPublishedDictIDs {
			if _, ok := drop[id]; !ok {
				kept = append(kept, id)
			}
		}
		db.recentPublishedDictIDs = kept
	}
	if len(tplIDs) > 0 && len(db.recentPublishedTemplateIDs) > 0 {
		drop := make(map[uint64]struct{}, len(tplIDs))
		for _, id := range tplIDs {
			drop[id] = struct{}{}
		}
		kept := db.recentPublishedTemplateIDs[:0]
		for _, id := range db.recentPublishedTemplateIDs {
			if _, ok := drop[id]; !ok {
				kept = append(kept, id)
			}
		}
		db.recentPublishedTemplateIDs = kept
	}
}

func (db *DB) snapshotRecentSystemMetadataIDs(keepDicts, keepTemplates int) (dictIDs []uint64, tplIDs []uint64) {
	if db == nil || (keepDicts <= 0 && keepTemplates <= 0) {
		return nil, nil
	}
	db.recentSystemMetaMu.Lock()
	defer db.recentSystemMetaMu.Unlock()
	if keepDicts > 0 && len(db.recentPublishedDictIDs) > 0 {
		n := keepDicts
		if n > len(db.recentPublishedDictIDs) {
			n = len(db.recentPublishedDictIDs)
		}
		dictIDs = append([]uint64(nil), db.recentPublishedDictIDs[len(db.recentPublishedDictIDs)-n:]...)
	}
	if keepTemplates > 0 && len(db.recentPublishedTemplateIDs) > 0 {
		n := keepTemplates
		if n > len(db.recentPublishedTemplateIDs) {
			n = len(db.recentPublishedTemplateIDs)
		}
		tplIDs = append([]uint64(nil), db.recentPublishedTemplateIDs[len(db.recentPublishedTemplateIDs)-n:]...)
	}
	return dictIDs, tplIDs
}

func (db *DB) snapshotStagedSystemMetadataIDs() (dictIDs map[uint64]struct{}, tplIDs map[uint64]struct{}) {
	if db == nil {
		return nil, nil
	}
	db.stagedSystemOpsMu.Lock()
	defer db.stagedSystemOpsMu.Unlock()
	for _, batch := range db.stagedSystemOps {
		for _, op := range batch.ops {
			if op.Delete {
				continue
			}
			if id, ok := parseSystemDictBytesKey(op.Key); ok {
				if dictIDs == nil {
					dictIDs = make(map[uint64]struct{}, 4)
				}
				dictIDs[id] = struct{}{}
			} else if bytes.Equal(op.Key, systemDictCurrentKey) || bytes.HasPrefix(op.Key, systemDictCurrentPrefix) {
				if len(op.Value) == 8 {
					if id := binary.BigEndian.Uint64(op.Value); id != 0 {
						if dictIDs == nil {
							dictIDs = make(map[uint64]struct{}, 4)
						}
						dictIDs[id] = struct{}{}
					}
				}
			} else if id, ok := parseSystemTemplateDefKey(op.Key); ok {
				if tplIDs == nil {
					tplIDs = make(map[uint64]struct{}, 4)
				}
				tplIDs[id] = struct{}{}
			}
		}
	}
	return dictIDs, tplIDs
}

func parseSystemDictBytesKey(key []byte) (uint64, bool) {
	if len(key) != len(systemDictBytesPrefix)+8 || !bytes.HasPrefix(key, systemDictBytesPrefix) {
		return 0, false
	}
	id := binary.BigEndian.Uint64(key[len(systemDictBytesPrefix):])
	return id, id != 0
}

func parseSystemDictChunkKey(key []byte) (uint64, uint32, bool) {
	if len(key) != len(systemDictChunkPrefix)+8+1+4 || !bytes.HasPrefix(key, systemDictChunkPrefix) {
		return 0, 0, false
	}
	if key[len(systemDictChunkPrefix)+8] != '/' {
		return 0, 0, false
	}
	id := binary.BigEndian.Uint64(key[len(systemDictChunkPrefix) : len(systemDictChunkPrefix)+8])
	idx := binary.BigEndian.Uint32(key[len(systemDictChunkPrefix)+9:])
	return id, idx, id != 0
}

func parseSystemDictSuffixID(key, prefix []byte) (uint64, bool) {
	if len(key) != len(prefix)+8 || !bytes.HasPrefix(key, prefix) {
		return 0, false
	}
	id := binary.BigEndian.Uint64(key[len(prefix):])
	return id, id != 0
}

func parseSystemTemplateDefKey(key []byte) (uint64, bool) {
	if len(key) != len(systemTemplateDefPrefix)+8 || !bytes.HasPrefix(key, systemTemplateDefPrefix) {
		return 0, false
	}
	id := binary.BigEndian.Uint64(key[len(systemTemplateDefPrefix):])
	return id, id != 0
}

func parseSystemTemplateChunkKey(key []byte) (uint64, uint32, bool) {
	if len(key) != len(systemTemplateChunkPrefix)+8+1+4 || !bytes.HasPrefix(key, systemTemplateChunkPrefix) {
		return 0, 0, false
	}
	if key[len(systemTemplateChunkPrefix)+8] != '/' {
		return 0, 0, false
	}
	id := binary.BigEndian.Uint64(key[len(systemTemplateChunkPrefix) : len(systemTemplateChunkPrefix)+8])
	idx := binary.BigEndian.Uint32(key[len(systemTemplateChunkPrefix)+9:])
	return id, idx, id != 0
}

func decodeSystemTemplateFPCandidates(buf []byte) ([]systemTemplateFPCandidate, error) {
	if len(buf) == 0 {
		return nil, nil
	}
	out := make([]systemTemplateFPCandidate, 0, 8)
	off := 0
	for off < len(buf) {
		id, n := binary.Uvarint(buf[off:])
		if n <= 0 {
			return nil, fmt.Errorf("treedb: corrupt template/fp candidate list")
		}
		off += n
		size, n := binary.Uvarint(buf[off:])
		if n <= 0 {
			return nil, fmt.Errorf("treedb: corrupt template/fp candidate list")
		}
		off += n
		out = append(out, systemTemplateFPCandidate{id: id, size: size})
	}
	return out, nil
}

func inspectSystemMetadataInventory(snap *Snapshot) (*systemMetadataInventory, error) {
	inv := &systemMetadataInventory{
		dicts:         make(map[uint64]*systemDictInventoryEntry),
		templates:     make(map[uint64]*systemTemplateInventoryEntry),
		activeDictIDs: make(map[uint64]struct{}),
	}
	if snap == nil || snap.state == nil || snap.state.SystemRootPageID == 0 || snap.idx == nil || snap.idx.pager == nil {
		return inv, nil
	}
	tr := tree.New(snap.idx.pager, &snap.reader, snap.state.SystemRootPageID)

	ensureDict := func(id uint64) *systemDictInventoryEntry {
		e := inv.dicts[id]
		if e == nil {
			e = &systemDictInventoryEntry{id: id}
			inv.dicts[id] = e
		}
		return e
	}
	ensureTemplate := func(id uint64) *systemTemplateInventoryEntry {
		e := inv.templates[id]
		if e == nil {
			e = &systemTemplateInventoryEntry{id: id}
			inv.templates[id] = e
		}
		return e
	}

	// Scan dict/* keys.
	dictHashTargets := make(map[uint64][][]byte)
	if it := tr.Iterator(systemDictPrefix, systemDictPrefixEnd); it != nil {
		defer func() { _ = it.Close() }()
		for ; it.Valid(); it.Next() {
			key := it.UnsafeKey()
			val, ptr, flags := it.UnsafeEntry()
			if flags&node.FlagTombstone != 0 {
				continue
			}
			inv.dictKeysTotal++
			switch {
			case bytes.Equal(key, systemDictCurrentKey) || bytes.HasPrefix(key, systemDictCurrentPrefix):
				if flags&node.FlagPointer == 0 && len(val) == 8 {
					if id := binary.BigEndian.Uint64(val); id != 0 {
						inv.activeDictIDs[id] = struct{}{}
					}
				}
			case bytes.HasPrefix(key, systemDictBytesPrefix):
				id, ok := parseSystemDictBytesKey(key)
				if !ok {
					continue
				}
				e := ensureDict(id)
				e.hasBytesKey = true
				e.keyCount++
				if flags&node.FlagPointer != 0 {
					e.pointerSegID = ptr.FileID
				} else {
					e.inlineBytes += int64(len(key) + len(val))
					if len(val) == SystemDictChunkHeaderSize && val[0] == SystemDictEncodingChunked {
						if cc, _, _, err := DecodeSystemDictChunkHeader(val); err == nil {
							e.isChunked = true
							e.chunkCount = cc
						}
					}
				}
			case bytes.HasPrefix(key, systemDictChunkPrefix):
				id, _, ok := parseSystemDictChunkKey(key)
				if !ok {
					continue
				}
				e := ensureDict(id)
				e.keyCount++
				e.chunkKeys = append(e.chunkKeys, append([]byte(nil), key...))
				e.inlineBytes += int64(len(key) + len(val))
			case bytes.HasPrefix(key, systemDictKPrefix):
				id, ok := parseSystemDictSuffixID(key, systemDictKPrefix)
				if !ok {
					continue
				}
				e := ensureDict(id)
				e.keyCount++
				e.hasK = true
				e.inlineBytes += int64(len(key) + len(val))
			case bytes.HasPrefix(key, systemDictLeafModePrefix):
				id, ok := parseSystemDictSuffixID(key, systemDictLeafModePrefix)
				if !ok {
					continue
				}
				e := ensureDict(id)
				e.keyCount++
				e.hasLeafMode = true
				e.inlineBytes += int64(len(key) + len(val))
			case bytes.HasPrefix(key, systemDictHashPrefix):
				if flags&node.FlagPointer == 0 && len(val) == 8 {
					if id := binary.BigEndian.Uint64(val); id != 0 {
						keyCopy := append([]byte(nil), key...)
						dictHashTargets[id] = append(dictHashTargets[id], keyCopy)
					}
				}
			}
		}
		if err := it.Error(); err != nil {
			return nil, err
		}
	}
	for id, hKeys := range dictHashTargets {
		if e, ok := inv.dicts[id]; ok {
			e.hashKeys = hKeys
			e.keyCount += len(hKeys)
			for _, hk := range hKeys {
				e.inlineBytes += int64(len(hk) + 8)
			}
		}
	}

	// Scan template/* keys.
	if it := tr.Iterator(systemTemplatePrefix, systemTemplatePrefixEnd); it != nil {
		defer func() { _ = it.Close() }()
		for ; it.Valid(); it.Next() {
			key := it.UnsafeKey()
			val, ptr, flags := it.UnsafeEntry()
			if flags&node.FlagTombstone != 0 {
				continue
			}
			inv.tplKeysTotal++
			switch {
			case bytes.HasPrefix(key, systemTemplateDefPrefix):
				id, ok := parseSystemTemplateDefKey(key)
				if !ok {
					continue
				}
				e := ensureTemplate(id)
				e.hasDefKey = true
				e.keyCount++
				if flags&node.FlagPointer != 0 {
					e.pointerSegID = ptr.FileID
				} else {
					e.inlineBytes += int64(len(key) + len(val))
					if len(val) == SystemDictChunkHeaderSize && val[0] == SystemDictEncodingChunked {
						if cc, _, _, err := DecodeSystemDictChunkHeader(val); err == nil {
							e.isChunked = true
							e.chunkCount = cc
						}
					}
				}
			case bytes.HasPrefix(key, systemTemplateChunkPrefix):
				id, _, ok := parseSystemTemplateChunkKey(key)
				if !ok {
					continue
				}
				e := ensureTemplate(id)
				e.keyCount++
				e.chunkKeys = append(e.chunkKeys, append([]byte(nil), key...))
				e.inlineBytes += int64(len(key) + len(val))
			case bytes.HasPrefix(key, systemTemplateFPPrefix):
				if flags&node.FlagPointer != 0 {
					continue
				}
				cands, err := decodeSystemTemplateFPCandidates(val)
				if err != nil {
					return nil, err
				}
				inv.templateFPs = append(inv.templateFPs, systemTemplateFPEntry{
					key:        append([]byte(nil), key...),
					candidates: cands,
				})
			}
		}
		if err := it.Error(); err != nil {
			return nil, err
		}
	}

	return inv, nil
}

// SystemMetadataAudit computes a read-only reachability report for all
// dictionary (dict/*) and template (template/*) definitions stored in
// SystemRootPageID. It works on both read-write and read-only DB handles.
func (db *DB) SystemMetadataAudit(ctx context.Context, opts SystemMetadataAuditOptions) (SystemMetadataAuditReport, error) {
	var report SystemMetadataAuditReport
	if db == nil {
		return report, ErrClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return report, err
	}
	db.maintenanceMu.Lock()
	defer db.maintenanceMu.Unlock()

	plan, err := db.buildSystemMetadataAuditPlanWithMaintenanceLockHeld(ctx, opts, false)
	if err != nil {
		return report, err
	}
	return plan.report, nil
}

type systemMetadataAuditPlan struct {
	report     SystemMetadataAuditReport
	inv        *systemMetadataInventory
	scannedSeq uint64
	deadDicts  map[uint64]struct{}
	deadTpls   map[uint64]struct{}
}

func (db *DB) buildSystemMetadataAuditPlanWithMaintenanceLockHeld(ctx context.Context, opts SystemMetadataAuditOptions, requireMaintenanceReady bool) (*systemMetadataAuditPlan, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if opts.KeepRecentPublishedDicts > MaxKeepRecentPublishedSystemMetaIDs {
		return nil, fmt.Errorf("treedb: KeepRecentPublishedDicts=%d exceeds max supported history %d", opts.KeepRecentPublishedDicts, MaxKeepRecentPublishedSystemMetaIDs)
	}
	if opts.KeepRecentPublishedTemplates > MaxKeepRecentPublishedSystemMetaIDs {
		return nil, fmt.Errorf("treedb: KeepRecentPublishedTemplates=%d exceeds max supported history %d", opts.KeepRecentPublishedTemplates, MaxKeepRecentPublishedSystemMetaIDs)
	}
	var (
		recoverableRoots *RecoverableRootSet
		err              error
	)
	if requireMaintenanceReady {
		recoverableRoots, err = db.captureRecoverableRootSetWithMaintenanceLockHeld(ctx)
	} else {
		recoverableRoots, err = db.captureRecoverableRootSetForInspectionWithMaintenanceLockHeld(ctx)
	}
	if err != nil {
		return nil, err
	}
	defer recoverableRoots.Release()

	scannedSeq := db.currentCommitSeq()
	inv, err := inspectSystemMetadataInventory(recoverableRoots.stableSnapshot)
	if err != nil {
		return nil, err
	}

	plan := &systemMetadataAuditPlan{
		inv:        inv,
		scannedSeq: scannedSeq,
		deadDicts:  make(map[uint64]struct{}),
		deadTpls:   make(map[uint64]struct{}),
	}
	report := &plan.report
	report.DictsTotal = len(inv.dicts)
	report.DictKeysTotal = inv.dictKeysTotal
	report.TemplatesTotal = len(inv.templates)
	report.TemplateKeysTotal = inv.tplKeysTotal
	report.TemplateFPKeysTotal = len(inv.templateFPs)

	// Fast exit: no dictionaries and no templates in SystemRootPageID.
	if len(inv.dicts) == 0 && len(inv.templates) == 0 {
		return plan, nil
	}

	liveDictIDs := make(map[uint64]struct{}, len(inv.dicts))
	liveTemplateIDs := make(map[uint64]struct{}, len(inv.templates))

	// Scan all live/active value_vlog and leaf_vlog segments.
	scanTemplates := len(inv.templates) > 0
	if vm := db.valueLogManager; vm != nil {
		if err := vm.Refresh(); err != nil {
			return nil, err
		}
		set := vm.CurrentSetNoRefresh()
		if set != nil {
			defer func() { _ = vm.Release(set) }()
			activeIDs := currentValueLogIDs(set)
			pendingIDs := db.pendingValueLogAppendFileIDs()
			dictLookup := db.valueLogDictLookup
			if dictLookup == nil {
				dictLookup = db.LookupSystemDictBytes
			}
			for id, f := range set.Files {
				if f == nil || f.Path == "" {
					continue
				}
				if err := ctx.Err(); err != nil {
					return nil, err
				}
				isLeaf := db.isLeafLogValueFile(id, f)
				if !isLeaf {
					lane, _ := valuelog.DecodeFileID(id)
					if lane == SystemValueLogLaneID {
						// System value-log lane stores raw dictID=0, templateID=0 records only.
						continue
					}
				}
				segScanTemplates := scanTemplates && !isLeaf
				_, isActive := activeIDs[id]
				_, isPending := pendingIDs[id]
				canCache := !isActive && !isPending

				info, statErr := os.Stat(f.Path)
				if statErr != nil {
					return nil, statErr
				}
				size := info.Size()
				if size == 0 {
					continue
				}
				cacheKey := systemMetaSegIDCacheKey{
					fileID:          id,
					size:            size,
					modTimeUnixNano: info.ModTime().UnixNano(),
					scanTemplates:   segScanTemplates,
				}
				if canCache {
					if cached, ok := db.systemMetaSegIDCache.Load(cacheKey); ok {
						val := cached.(systemMetaSegIDCacheVal)
						for _, did := range val.dictIDs {
							liveDictIDs[did] = struct{}{}
						}
						for _, tid := range val.templateIDs {
							liveTemplateIDs[tid] = struct{}{}
						}
						report.SegmentsCacheHits++
						continue
					}
				}
				scanned, scanErr := valuelog.ScanSegmentCompressionIDs(f.Path, id, segScanTemplates, dictLookup)
				if scanErr != nil {
					return nil, fmt.Errorf("treedb: scan segment %d (%s) compression ids: %w", id, f.Path, scanErr)
				}
				if isLeaf {
					report.SegmentsScannedLeafLog++
				} else {
					report.SegmentsScannedValueLog++
				}
				var cachedVal systemMetaSegIDCacheVal
				for did := range scanned.DictIDs {
					liveDictIDs[did] = struct{}{}
					if canCache {
						cachedVal.dictIDs = append(cachedVal.dictIDs, did)
					}
				}
				for tid := range scanned.TemplateIDs {
					liveTemplateIDs[tid] = struct{}{}
					if canCache {
						cachedVal.templateIDs = append(cachedVal.templateIDs, tid)
					}
				}
				if canCache && db.systemMetaSegIDCacheCount.Load() < maxSystemMetaSegIDCacheEntries {
					if _, loaded := db.systemMetaSegIDCache.LoadOrStore(cacheKey, cachedVal); !loaded {
						db.systemMetaSegIDCacheCount.Add(1)
					}
				}
			}
		}
	}

	// Build protected sets: active current pointers, staged ops, caller pins,
	// and session-recent grace windows.
	keepRecentDicts := opts.KeepRecentPublishedDicts
	if keepRecentDicts == 0 {
		keepRecentDicts = defaultKeepRecentPublishedDicts
	} else if keepRecentDicts < 0 {
		keepRecentDicts = 0
	}
	keepRecentTemplates := opts.KeepRecentPublishedTemplates
	if keepRecentTemplates == 0 {
		keepRecentTemplates = defaultKeepRecentPublishedTemplates
	} else if keepRecentTemplates < 0 {
		keepRecentTemplates = 0
	}
	recentDictList, recentTplList := db.snapshotRecentSystemMetadataIDs(keepRecentDicts, keepRecentTemplates)
	stagedDicts, stagedTpls := db.snapshotStagedSystemMetadataIDs()

	graceDictIDs := make(map[uint64]struct{}, len(recentDictList)+len(stagedDicts)+len(opts.ProtectedDictIDs))
	for _, id := range recentDictList {
		graceDictIDs[id] = struct{}{}
	}
	for id := range stagedDicts {
		graceDictIDs[id] = struct{}{}
	}
	for _, id := range opts.ProtectedDictIDs {
		if id != 0 {
			graceDictIDs[id] = struct{}{}
		}
	}

	graceTplIDs := make(map[uint64]struct{}, len(recentTplList)+len(stagedTpls)+len(opts.ProtectedTemplateIDs))
	for _, id := range recentTplList {
		graceTplIDs[id] = struct{}{}
	}
	for id := range stagedTpls {
		graceTplIDs[id] = struct{}{}
	}
	for _, id := range opts.ProtectedTemplateIDs {
		if id != 0 {
			graceTplIDs[id] = struct{}{}
		}
	}

	// Track lane-254 (or any pointer) segment usage by live vs reclaimable entries.
	ptrSegLive := make(map[uint32]bool)
	ptrSegAny := make(map[uint32]bool)

	for id, entry := range inv.dicts {
		_, inSegment := liveDictIDs[id]
		_, isCurrent := inv.activeDictIDs[id]
		_, inGrace := graceDictIDs[id]
		if entry.pointerSegID != 0 {
			ptrSegAny[entry.pointerSegID] = true
		}
		switch {
		case inSegment:
			report.DictsLive++
			if entry.pointerSegID != 0 {
				ptrSegLive[entry.pointerSegID] = true
			}
		case isCurrent:
			report.DictsActiveCurrent++
			if entry.pointerSegID != 0 {
				ptrSegLive[entry.pointerSegID] = true
			}
		case inGrace:
			report.DictsGraceProtected++
			if entry.pointerSegID != 0 {
				ptrSegLive[entry.pointerSegID] = true
			}
		default:
			report.DictsReclaimable++
			report.DictKeysReclaimable += entry.keyCount
			report.DictInlineBytesReclaimable += entry.inlineBytes
			report.ReclaimableDictIDs = append(report.ReclaimableDictIDs, id)
			plan.deadDicts[id] = struct{}{}
		}
	}
	slices.Sort(report.ReclaimableDictIDs)

	for id, entry := range inv.templates {
		_, inSegment := liveTemplateIDs[id]
		_, inGrace := graceTplIDs[id]
		if entry.pointerSegID != 0 {
			ptrSegAny[entry.pointerSegID] = true
		}
		switch {
		case inSegment:
			report.TemplatesLive++
			if entry.pointerSegID != 0 {
				ptrSegLive[entry.pointerSegID] = true
			}
		case inGrace:
			report.TemplatesGraceProtected++
			if entry.pointerSegID != 0 {
				ptrSegLive[entry.pointerSegID] = true
			}
		default:
			report.TemplatesReclaimable++
			report.TemplateKeysReclaimable += entry.keyCount
			report.TemplateInlineBytesReclaimable += entry.inlineBytes
			report.ReclaimableTemplateIDs = append(report.ReclaimableTemplateIDs, id)
			plan.deadTpls[id] = struct{}{}
		}
	}
	slices.Sort(report.ReclaimableTemplateIDs)

	// Classify template/fp/* candidate lists.
	for _, fp := range inv.templateFPs {
		if len(fp.candidates) == 0 {
			continue
		}
		deadCount := 0
		for _, c := range fp.candidates {
			if _, dead := plan.deadTpls[c.id]; dead {
				deadCount++
			}
		}
		if deadCount == 0 {
			continue
		}
		if deadCount == len(fp.candidates) {
			report.TemplateFPKeysDeletable++
			report.TemplateKeysReclaimable++
		} else {
			report.TemplateFPKeysPrunable++
		}
	}

	report.PointerSegmentsReferenced = len(ptrSegAny)
	for segID := range ptrSegAny {
		if !ptrSegLive[segID] {
			report.PointerSegmentsOnlyReclaimable++
		}
	}

	return plan, nil
}
