package valuelog

import (
	"encoding/binary"
	"os"
	"runtime"
	"strconv"
	"strings"

	"github.com/snissn/gomap/TreeDB/internal/crc"
	"github.com/snissn/gomap/TreeDB/internal/limits"
	"github.com/snissn/gomap/TreeDB/page"
	templ "github.com/snissn/gomap/TreeDB/template"
)

func appendDecodedTemplatePayload(dst, payload []byte, lookup TemplateLookup, cache *templateDefCache, opts templ.DecodeOptions) ([]byte, error) {
	if lookup == nil || !templ.IsEncodedPayload(payload) {
		noteGrowReadAppendDecodedPayload(dst, len(payload))
		oldLen := len(dst)
		dst = grow(dst, len(payload))
		copy(dst[oldLen:], payload)
		return dst, nil
	}
	noteGrowReadAppendTemplateEncodedPayload(len(payload))
	return templ.DecodePayloadAppend(dst, payload, func(id uint64) (templ.TemplateDef, error) {
		return resolveTemplateDef(id, lookup, cache)
	}, opts)
}

// readViaMmapViewPrefixCacheEnabled controls whether unsafe mmap-view reads
// consult/publish the shared per-file grouped prefix cache.
//
// Random parallel key-lookups in fence mode tend to have low temporal locality
// and high fan-in to the same file cache mutex; disabling this avoids
// lock-contention amplification on the unsafe view path.
const readViaMmapViewPrefixCacheEnabled = false

func (f *File) ensureMmapRecordInitialRange(data []byte, ptr page.ValuePtr, start int64) ([]byte, bool, bool) {
	if hint := page.ValuePtrRecordLength(ptr); hint > 0 {
		end := int64(ptr.Offset) + int64(hint)
		if headerEnd := start + HeaderSize; end < headerEnd {
			end = headerEnd
		}
		if refreshed, ok := f.ensureMmapRangeReadable(data, start, end); ok {
			return refreshed, true, true
		}
		return nil, false, false
	}
	if refreshed, ok := f.ensureMmapRangeReadable(data, start, start+HeaderSize); ok {
		return refreshed, true, false
	}
	return nil, false, false
}

// MaxDeadMappings is the base cap for old mmaps retained to avoid exhausting
// vm.max_map_count. Unless explicitly configured, the effective cap can grow
// with mapped size up to maxAdaptiveDeadMappings. Set <= 0 to disable the cap.
//
// Each remap retains the previous mapping until the file is closed to avoid
// use-after-unmap with concurrent readers.
var MaxDeadMappings = defaultMaxDeadMappings

const (
	defaultMaxDeadMappings           = 64
	maxAdaptiveDeadMappings          = 4096
	deadMappingBytesPerStep          = 256 << 10 // increase cap by 1 per 256KiB mapped
	maxDeadMappingsEnvKey            = "TREEDB_VLOG_MAX_DEAD_MAPPINGS"
	enableAdaptiveCapEnvKey          = "TREEDB_VLOG_ADAPTIVE_DEAD_MAPPINGS"
	enableCurrentWritableEnvKey      = "TREEDB_VLOG_ENABLE_CURRENT_WRITABLE_MMAP"
	enableCurrentLeafWritableEnvKey  = "TREEDB_VLOG_ENABLE_CURRENT_LEAF_WRITABLE_MMAP"
	currentWritableMapTargetEnvKey   = "TREEDB_VLOG_CURRENT_WRITABLE_MMAP_TARGET_BYTES"
	maxMappedSealedEnvKey            = "TREEDB_VLOG_MAX_MAPPED_SEALED_SEGMENTS"
	maxMappedSealedBytesEnvKey       = "TREEDB_VLOG_MAX_MAPPED_SEALED_BYTES"
	maxMappedLeafSealedEnvKey        = "TREEDB_VLOG_MAX_MAPPED_LEAF_SEALED_SEGMENTS"
	maxMappedLeafBytesEnvKey         = "TREEDB_VLOG_MAX_MAPPED_LEAF_SEALED_BYTES"
	defaultAdaptiveCapEnabled        = true
	defaultMaxMappedSealed           = 8
	defaultMaxMappedSealedBytes      = 64 << 20
	defaultMaxMappedLeafSealed       = defaultMaxMappedSealed * 64
	defaultLeafSealedBytesMultiplier = 24
	// Leaf-log reads are on the BTree traversal hot path, but long sync/restore
	// workloads can keep multiple GiB of sealed leaf generations mapped into the
	// process RSS. Keep a bounded 1.5GiB sealed-leaf window by default and let
	// older generations fall back to ReadAt; operators can raise the env cap for
	// throughput-biased experiments.
	defaultMaxMappedLeafSealedBytes = defaultMaxMappedSealedBytes * defaultLeafSealedBytesMultiplier
	defaultCurrentWritableMapTarget = 32 << 20
)

var (
	maxDeadMappingsExplicit        bool
	adaptiveDeadMappings                 = defaultAdaptiveCapEnabled
	enableCurrentWritableMmap            = false
	enableCurrentLeafWritableMmap        = false
	CurrentWritableMmapTargetBytes       = int64(defaultCurrentWritableMapTarget)
	MaxMappedSealedSegments              = defaultMaxMappedSealed
	MaxMappedSealedBytes           int64 = defaultMaxMappedSealedBytes
	MaxMappedLeafSealedSegments          = defaultMaxMappedLeafSealed
	MaxMappedLeafSealedBytes       int64 = defaultMaxMappedLeafSealedBytes
)

func init() {
	if raw := strings.TrimSpace(os.Getenv(maxDeadMappingsEnvKey)); raw != "" {
		if v, err := strconv.Atoi(raw); err == nil {
			MaxDeadMappings = v
			maxDeadMappingsExplicit = true
		}
	}
	if raw := strings.TrimSpace(os.Getenv(enableAdaptiveCapEnvKey)); raw != "" {
		switch strings.ToLower(raw) {
		case "0", "false", "off", "no":
			adaptiveDeadMappings = false
		case "1", "true", "on", "yes":
			adaptiveDeadMappings = true
		}
	}
	if raw := strings.TrimSpace(os.Getenv(enableCurrentWritableEnvKey)); raw != "" {
		switch strings.ToLower(raw) {
		case "0", "false", "off", "no":
			enableCurrentWritableMmap = false
		case "1", "true", "on", "yes":
			enableCurrentWritableMmap = true
		}
	}
	if raw := strings.TrimSpace(os.Getenv(enableCurrentLeafWritableEnvKey)); raw != "" {
		switch strings.ToLower(raw) {
		case "0", "false", "off", "no":
			enableCurrentLeafWritableMmap = false
		case "1", "true", "on", "yes":
			enableCurrentLeafWritableMmap = true
		}
	}
	if raw := strings.TrimSpace(os.Getenv(currentWritableMapTargetEnvKey)); raw != "" {
		if v, err := strconv.ParseInt(raw, 10, 64); err == nil && v >= 0 {
			CurrentWritableMmapTargetBytes = v
		}
	}
	if raw := strings.TrimSpace(os.Getenv(maxMappedSealedEnvKey)); raw != "" {
		if v, err := strconv.Atoi(raw); err == nil && v >= 0 {
			MaxMappedSealedSegments = v
		}
	}
	if raw := strings.TrimSpace(os.Getenv(maxMappedSealedBytesEnvKey)); raw != "" {
		if v, err := strconv.ParseInt(raw, 10, 64); err == nil && v >= 0 {
			MaxMappedSealedBytes = v
		}
	}
	if raw := strings.TrimSpace(os.Getenv(maxMappedLeafSealedEnvKey)); raw != "" {
		if v, err := strconv.Atoi(raw); err == nil && v >= 0 {
			MaxMappedLeafSealedSegments = v
		}
	}
	if raw := strings.TrimSpace(os.Getenv(maxMappedLeafBytesEnvKey)); raw != "" {
		if v, err := strconv.ParseInt(raw, 10, 64); err == nil && v >= 0 {
			MaxMappedLeafSealedBytes = v
		}
	}
}

func isLeafLogFileID(fileID uint32) bool {
	lane, _ := DecodeFileID(fileID)
	return lane == ReservedLeafLogLaneID
}

func (f *File) currentWritablePersistentMmapEnabled() bool {
	if f == nil || !f.currentWritable.Load() {
		return false
	}
	if f.manager != nil && f.manager.currentWritableMmap.Load() {
		return true
	}
	if enableCurrentWritableMmap {
		return true
	}
	return enableCurrentLeafWritableMmap && isLeafLogFileID(f.ID)
}

func effectiveMaxDeadMappings(mappedLen int) int {
	maxMappings := MaxDeadMappings
	if maxMappings <= 0 {
		return maxMappings
	}
	if maxDeadMappingsExplicit || !adaptiveDeadMappings {
		return maxMappings
	}
	if mappedLen <= 0 {
		return maxMappings
	}
	boost := mappedLen / deadMappingBytesPerStep
	if boost <= 0 {
		return maxMappings
	}

	base := uint64(maxMappings)
	maxCap := uint64(maxAdaptiveDeadMappings)
	// Never let adaptive clamping reduce the configured base cap.
	if base >= maxCap {
		return maxMappings
	}
	boost64 := uint64(boost)
	maxBoost := maxCap - base
	if boost64 > maxBoost {
		boost64 = maxBoost
	}
	return int(base + boost64)
}

func deadMappingsCapExhausted(deadMappingsCount uint64, mappedLen int) bool {
	maxMappings := effectiveMaxDeadMappings(mappedLen)
	return maxMappings > 0 && deadMappingsCount >= uint64(maxMappings)
}

func (f *File) usesPersistentMmap() bool {
	if f == nil {
		return false
	}
	if f.manager == nil {
		return true
	}
	return f.currentWritablePersistentMmapEnabled()
}

func (f *File) tryEnableSealedLazyMmap() bool {
	if f == nil || f.closed.Load() || f.File == nil {
		return false
	}
	if f.currentWritable.Load() && !f.currentWritablePersistentMmapEnabled() {
		return false
	}
	if f.usesPersistentMmap() {
		f.remapToFileSize()
		data, _ := f.mmapData.Load().([]byte)
		return len(data) > 0
	}
	m := f.manager
	if m == nil {
		return false
	}
	if f.sealedLazyMmapDenied.Load() {
		deniedCountCapWant, deniedBytesCapWant := f.sealedLazyMmapBudgetSnapshot()
		deniedCountCap := int(f.sealedLazyMmapDeniedCountCap.Load())
		deniedBytesCap := f.sealedLazyMmapDeniedBytesCap.Load()
		// Preserve the cheap deny fast-path while budgets stay unchanged.
		if int64(deniedCountCap) == deniedCountCapWant && deniedBytesCap == deniedBytesCapWant {
			return false
		}
		// Budgets changed; re-check once to allow in-process recovery.
		targetSize := f.sealedLazyMmapTargetSize()
		m.mu.Lock()
		allow, _ := m.allowSealedLazyMmapLocked(f, targetSize)
		m.mu.Unlock()
		if !allow {
			f.storeSealedLazyMmapBudgetSnapshot()
			return false
		}
		f.sealedLazyMmapDenied.Store(false)
		f.sealedLazyMmapDeniedCountCap.Store(0)
		f.sealedLazyMmapDeniedBytesCap.Store(0)
	}
	// Already mapped sealed segments may still be stale if they were current
	// writable before sealing. Re-check budget gates before growth remap so
	// disabled/capped sealed mappings are still respected.
	if data, _ := f.mmapData.Load().([]byte); len(data) > 0 {
		targetSize := f.sealedLazyMmapTargetSize()
		m.mu.Lock()
		allow, denyReason := m.allowSealedLazyMmapLocked(f, targetSize)
		m.mu.Unlock()
		if !allow {
			f.sealedLazyMmapDenied.Store(true)
			f.storeSealedLazyMmapBudgetSnapshot()
			switch denyReason {
			case sealedLazyMmapDenyCountCap:
				f.sealedMapDeniedByCount.Add(1)
			case sealedLazyMmapDenyBytesCap:
				f.sealedMapDeniedByBytes.Add(1)
			default:
				f.sealedMapDeniedByCount.Add(1)
			}
			return false
		}
		f.remapToFileSize()
		data, _ = f.mmapData.Load().([]byte)
		return len(data) > 0
	}
	targetSize := f.sealedLazyMmapTargetSize()
	m.mu.Lock()
	allow, denyReason := m.allowSealedLazyMmapLocked(f, targetSize)
	m.mu.Unlock()
	if !allow {
		f.sealedLazyMmapDenied.Store(true)
		f.storeSealedLazyMmapBudgetSnapshot()
		switch denyReason {
		case sealedLazyMmapDenyCountCap:
			f.sealedMapDeniedByCount.Add(1)
		case sealedLazyMmapDenyBytesCap:
			f.sealedMapDeniedByBytes.Add(1)
		default:
			f.sealedMapDeniedByCount.Add(1)
		}
		return false
	}
	f.sealedLazyMmapDenied.Store(false)
	f.sealedLazyMmapDeniedCountCap.Store(0)
	f.sealedLazyMmapDeniedBytesCap.Store(0)
	f.remapToFileSize()
	data, _ := f.mmapData.Load().([]byte)
	return len(data) > 0
}

func (f *File) sealedLazyMmapBudgetSnapshot() (countCap int64, bytesCap int64) {
	if f != nil && isLeafLogFileID(f.ID) {
		return int64(MaxMappedLeafSealedSegments), MaxMappedLeafSealedBytes
	}
	return int64(MaxMappedSealedSegments), MaxMappedSealedBytes
}

func (f *File) storeSealedLazyMmapBudgetSnapshot() {
	countCap, bytesCap := f.sealedLazyMmapBudgetSnapshot()
	f.sealedLazyMmapDeniedCountCap.Store(countCap)
	f.sealedLazyMmapDeniedBytesCap.Store(bytesCap)
}

func (f *File) sealedLazyMmapTargetSize() int64 {
	if f == nil || f.File == nil {
		return 0
	}
	targetSize := f.fileSize.Load()
	if targetSize <= 0 {
		if info, err := f.File.Stat(); err == nil {
			if sz := info.Size(); sz > 0 {
				targetSize = sz
				f.noteVerifiedFileSize(sz)
			}
		}
	}
	return targetSize
}

func currentWritableMmapTargetSize(currentSize int64) int64 {
	target := CurrentWritableMmapTargetBytes
	if currentSize <= 0 || target <= 0 {
		return currentSize
	}
	const maxInt64 = int64(^uint64(0) >> 1)
	if currentSize >= maxInt64-target {
		return currentSize
	}
	rem := currentSize % target
	add := target - rem
	if rem == 0 {
		add = target
	}
	if add <= 0 || currentSize > maxInt64-add {
		return currentSize
	}
	return currentSize + add
}

func (f *File) retirePersistentMmapToDead() {
	if f == nil {
		return
	}
	f.remapMu.Lock()
	defer f.remapMu.Unlock()
	f.retirePersistentMmapToDeadLocked()
}

func (f *File) retirePersistentMmapToDeadLocked() bool {
	if f == nil {
		return false
	}
	data, _ := f.mmapData.Load().([]byte)
	if len(data) == 0 {
		return false
	}
	if deadMappingsCapExhausted(f.deadMappingsCount.Load(), len(data)) {
		// Keep the existing mapping live as a sealed mapping once the dead-mapping
		// budget is exhausted. That preserves reader safety without allowing the
		// dead-mapping list to grow without bound.
		return false
	}
	f.deadMappings = append(f.deadMappings, data)
	f.deadMappingsCount.Add(1)
	f.deadMappedBytes.Add(uint64(len(data)))
	f.mmapData.Store([]byte(nil))
	return true
}

func (f *File) maybeScheduleRemap() {
	if f == nil || f.closed.Load() || !f.usesPersistentMmap() {
		return
	}
	data, _ := f.mmapData.Load().([]byte)
	if deadMappingsCapExhausted(f.deadMappingsCount.Load(), len(data)) {
		// If we have already exhausted our dead-mapping budget, we will not be
		// able to remap again without risking use-after-unmap for callers holding
		// unsafe mmap views. Avoid spawning remap goroutines that will just bail.
		if data != nil {
			return
		}
	}
	if !f.remapRequested.CompareAndSwap(false, true) {
		return
	}
	go func() {
		defer f.remapRequested.Store(false)
		f.remapToFileSizePersistentOnly()
	}()
}

func (f *File) tryRefreshMmapRange(start, end int64) ([]byte, bool) {
	if f == nil || start < 0 || end < start {
		return nil, false
	}
	// Perform a synchronous refresh on out-of-range misses so stale mappings
	// do not repeatedly fall back to ReadAt. This is especially important for
	// safe read paths, which do not re-enter tryEnableSealedLazyMmap.
	f.remapToFileSize()
	data, _ := f.mmapData.Load().([]byte)
	if data == nil || end > int64(len(data)) {
		return nil, false
	}
	return data, true
}

func (f *File) ensureMmapRangeReadable(data []byte, start, end int64) ([]byte, bool) {
	if f == nil || start < 0 || end < start {
		return nil, false
	}
	if data == nil || end > int64(len(data)) {
		return f.tryRefreshMmapRange(start, end)
	}
	if known := f.fileSize.Load(); known >= end {
		if known <= int64(len(data)) || f.verifiedFileSize.Load() >= end {
			return data, true
		}
	}
	info, err := f.File.Stat()
	if err != nil {
		return nil, false
	}
	currentSize := info.Size()
	if currentSize > 0 {
		f.noteVerifiedFileSize(currentSize)
	}
	if currentSize < end {
		return nil, false
	}
	return data, true
}

func (f *File) remapToFileSizePersistentOnly() {
	f.remapToFileSizeWithPolicy(true)
}

func (f *File) remapToFileSize() {
	f.remapToFileSizeWithPolicy(false)
}

func (f *File) remapToFileSizeWithPolicy(requirePersistent bool) {
	if f == nil || f.closed.Load() || f.File == nil {
		return
	}

	f.remapMu.Lock()
	defer f.remapMu.Unlock()
	if f.closed.Load() || f.File == nil {
		return
	}
	if requirePersistent && !f.usesPersistentMmap() {
		return
	}
	// If we have already hit the dead-mapping cap, we cannot remap again without
	// risking use-after-unmap for callers holding unsafe mmap views. Avoid the
	// per-call Stat allocation when reads keep missing the current mapping.
	data, _ := f.mmapData.Load().([]byte)
	if data != nil && deadMappingsCapExhausted(f.deadMappingsCount.Load(), len(data)) {
		return
	}

	info, err := f.File.Stat()
	if err != nil {
		return
	}
	currentSize := info.Size()
	if currentSize > 0 {
		f.noteVerifiedFileSize(currentSize)
	}
	if currentSize <= 0 || currentSize > int64(int(currentSize)) {
		return
	}
	if runtime.GOARCH == "386" && currentSize > int64(^uint32(0)) {
		return
	}

	mapSize := currentSize
	if f.currentWritablePersistentMmapEnabled() {
		mapSize = currentWritableMmapTargetSize(currentSize)
	}
	if mapSize <= 0 || mapSize > int64(int(mapSize)) {
		return
	}
	if runtime.GOARCH == "386" && mapSize > int64(^uint32(0)) {
		return
	}
	if data != nil && int64(len(data)) >= mapSize {
		return
	}
	if data != nil {
		f.deadMappings = append(f.deadMappings, data)
		f.deadMappingsCount.Add(1)
		f.deadMappedBytes.Add(uint64(len(data)))
	}

	b, err := mmapReadOnly(f.File, int(mapSize))
	if err != nil {
		return
	}
	f.mmapData.Store(b)
	f.remapCount.Add(1)
}

func (f *File) readViaMmap(ptr page.ValuePtr, verifyCRC bool) ([]byte, error, bool) {
	val, err, ok := f.readViaMmapAppend(ptr, verifyCRC, nil)
	if !ok || err != nil {
		return nil, err, ok
	}
	// readViaMmapAppend already returns a copy (it appends into dst).
	return val, nil, true
}

// readViaMmapView returns a best-effort zero-copy view into the current mmap.
//
// It mirrors readViaMmapAppend's decoding logic but avoids copying for
// uncompressed records. The returned slice is only valid as long as the mapping
// remains alive; callers must treat it as ephemeral/unsafe.
func (f *File) readViaMmapView(ptr page.ValuePtr, verifyCRC bool) ([]byte, error, bool) {
	data, _ := f.mmapData.Load().([]byte)
	if data == nil {
		f.mmapReadMissNoMapping.Add(1)
		f.maybeScheduleRemap()
		return nil, nil, false
	}
	if ptr.Offset < 4 {
		return nil, ErrCorrupt, true
	}
	start := int64(ptr.Offset - 4)
	fullRangeReadable := false
	if refreshed, ok, full := f.ensureMmapRecordInitialRange(data, ptr, start); ok {
		data = refreshed
		fullRangeReadable = full
	} else {
		f.mmapReadMissOutOfRange.Add(1)
		return nil, nil, false
	}

	header := data[start : start+HeaderSize]
	crcVal := binary.LittleEndian.Uint32(header[0:4])
	version := header[4]
	if version != Version {
		return nil, ErrCorrupt, true
	}
	flags := header[5]
	valueLen := binary.LittleEndian.Uint32(header[16:20])
	if recordSizeExceedsMax(valueLen) {
		return nil, ErrRecordTooLarge, true
	}
	expectedLen := uint32(headerWithoutCRC) + valueLen
	if !page.ValuePtrRecordLengthHintMatches(ptr, expectedLen) {
		return nil, ErrCorrupt, true
	}

	end := start + HeaderSize + int64(valueLen)
	if !fullRangeReadable {
		if refreshed, ok := f.ensureMmapRangeReadable(data, start, end); ok {
			data = refreshed
			header = data[start : start+HeaderSize]
		} else {
			f.mmapReadMissOutOfRange.Add(1)
			return nil, nil, false
		}
	}

	payload := data[start+HeaderSize : end]
	if verifyCRC {
		f.noteRecordCRCCheck()
		sum := crc.ChecksumParts(header[4:], payload)
		if sum != crcVal {
			return nil, ErrCorrupt, true
		}
	}
	// Non-grouped record: the payload is the value.
	if flags&recordFlagGrouped == 0 {
		if f.templateLookup != nil && templ.IsEncodedPayload(payload) {
			decoded, err := templ.DecodePayloadAppend(nil, payload, func(id uint64) (templ.TemplateDef, error) {
				return resolveTemplateDef(id, f.templateLookup, f.templateDefCache)
			}, f.templateDecodeOpts)
			if err != nil {
				return nil, err, true
			}
			return decoded, nil, true
		}
		return payload, nil, true
	}

	if !page.ValuePtrIsGrouped(ptr) {
		return nil, ErrCorrupt, true
	}
	if int64(FrameHeaderSize) > int64(len(payload)) {
		return nil, ErrCorrupt, true
	}
	frameHeader := payload[:FrameHeaderSize]
	if frameHeader[0] != FrameVersion {
		return nil, ErrCorrupt, true
	}
	k := int(frameHeader[2])
	if k <= 0 || k > MaxFrameK {
		return nil, ErrCorrupt, true
	}
	fFlags := frameHeader[1]

	decodeTemplatePayload := func(val []byte) ([]byte, error) {
		if f.templateLookup == nil || !templ.IsEncodedPayload(val) {
			return val, nil
		}
		return templ.DecodePayloadAppend(nil, val, func(id uint64) (templ.TemplateDef, error) {
			return resolveTemplateDef(id, f.templateLookup, f.templateDefCache)
		}, f.templateDecodeOpts)
	}

	subIndex := int(page.ValuePtrSubIndex(ptr))
	if subIndex < 0 || subIndex >= k {
		return nil, ErrCorrupt, true
	}
	ridBytes := k * 8
	offsetBytes := (k + 1) * 4
	prefixLen := FrameHeaderSize + ridBytes + offsetBytes
	if int(valueLen) < prefixLen {
		return nil, ErrCorrupt, true
	}

	if fFlags&FrameFlagCompressed == 0 && readViaMmapViewPrefixCacheEnabled && f.cacheStart.Load() == start {
		f.cacheMu.Lock()
		hit := f.cacheStart.Load() == start &&
			f.cacheK > 0 &&
			f.cacheLen > 0 &&
			subIndex < f.cacheK &&
			(f.cacheFlags&FrameFlagCompressed) == 0
		if hit {
			cPrefixLen := f.cacheLen
			valStart := f.cacheOffs[subIndex]
			valEnd := f.cacheOffs[subIndex+1]
			rawLen := f.cacheOffs[f.cacheK]
			f.cacheMu.Unlock()

			if valEnd < valStart || valEnd > rawLen || cPrefixLen+int(rawLen) != int(valueLen) {
				return nil, ErrCorrupt, true
			}

			srcStart := cPrefixLen + int(valStart)
			srcEnd := cPrefixLen + int(valEnd)
			if srcStart < 0 || srcEnd < srcStart || srcEnd > len(payload) {
				return nil, ErrCorrupt, true
			}
			val, err := decodeTemplatePayload(payload[srcStart:srcEnd])
			if err != nil {
				return nil, err, true
			}
			return val, nil, true
		}
		f.cacheMu.Unlock()
	}

	off := FrameHeaderSize + ridBytes
	var offsets [MaxFrameK + 1]uint32
	prev := uint32(0)
	for i := 0; i < k+1; i++ {
		cur := binary.LittleEndian.Uint32(payload[off : off+4])
		if cur < prev {
			return nil, ErrCorrupt, true
		}
		offsets[i] = cur
		prev = cur
		off += 4
	}

	rawLen := offsets[k]
	if limits.MaxRecordSize > 0 && int64(rawLen) > limits.MaxRecordSize {
		return nil, ErrRecordTooLarge, true
	}
	if fFlags&FrameFlagCompressed == 0 && prefixLen+int(rawLen) != int(valueLen) {
		return nil, ErrCorrupt, true
	}
	valStart := offsets[subIndex]
	valEnd := offsets[subIndex+1]
	if valEnd < valStart || valEnd > rawLen {
		return nil, ErrCorrupt, true
	}

	if fFlags&FrameFlagCompressed != 0 {
		if val, _, err, hit := f.groupedFrameCacheReadTo(start, verifyCRC, k, offsets, rawLen, subIndex, nil); hit {
			if err != nil {
				return nil, err, true
			}
			decoded, err := decodeTemplatePayload(val)
			if err != nil {
				return nil, err, true
			}
			return decoded, nil, true
		}
		if len(payload) < prefixLen {
			return nil, ErrCorrupt, true
		}
		frame := FrameHeader{
			Version:  frameHeader[0],
			Flags:    fFlags,
			K:        uint8(k),
			Reserved: frameHeader[3],
			DictID:   binary.LittleEndian.Uint64(frameHeader[4:12]),
		}

		// Special-case: a single-entry grouped frame where the selected value
		// spans the full decoded payload. Decode directly into the output slice
		// to avoid an intermediate decode scratch + copy.
		if f.templateLookup == nil && k == 1 && valStart == 0 && valEnd == rawLen {
			out := make([]byte, 0, int(rawLen))
			out, err := decodeFramePayloadTo(frame, payload[prefixLen:], f.dictLookup, rawLen, out)
			if err != nil {
				return nil, err, true
			}
			if uint32(len(out)) != rawLen {
				return nil, ErrCorrupt, true
			}
			return out, nil, true
		}

		cacheableRaw := f.groupedFrameCacheAllowsRaw(int(rawLen))

		retainRaw := cacheableRaw || readViaMmapViewPrefixCacheEnabled
		pooledRaw := !retainRaw
		var raw []byte
		if retainRaw {
			// Cached grouped payload bytes must not be backed by pooled decode
			// scratch, since readViaMmapView may hand out views into cached raw.
			raw = make([]byte, 0, int(rawLen))
		} else {
			raw = f.takeDecodeScratch(int(rawLen))
		}

		raw, err := decodeFramePayloadTo(frame, payload[prefixLen:], f.dictLookup, rawLen, raw)
		if err != nil {
			if pooledRaw {
				f.releaseDecodeScratch(raw)
			}
			return nil, err, true
		}
		if uint32(len(raw)) != rawLen {
			if pooledRaw {
				f.releaseDecodeScratch(raw)
			}
			return nil, ErrCorrupt, true
		}

		groupedStored := false
		if cacheableRaw {
			groupedStored = f.groupedFrameCacheStore(start, verifyCRC, k, offsets, raw, false)
		}

		prefixStored := false
		if readViaMmapViewPrefixCacheEnabled {
			// Publish cache for subsequent reads from this grouped record.
			f.cacheMu.Lock()
			f.cacheK = k
			f.cacheFlags = fFlags
			f.cacheLen = prefixLen
			f.cacheOffs = offsets
			if groupedStored || !pooledRaw {
				f.setCacheRawLocked(raw, false)
			} else {
				owned := make([]byte, len(raw))
				copy(owned, raw)
				f.setCacheRawLocked(owned, false)
			}
			f.cacheStart.Store(start)
			f.cacheMu.Unlock()
			prefixStored = true
		}

		val := raw[valStart:valEnd]
		if f.templateLookup != nil && templ.IsEncodedPayload(val) {
			decoded, err := templ.DecodePayloadAppend(nil, val, func(id uint64) (templ.TemplateDef, error) {
				return resolveTemplateDef(id, f.templateLookup, f.templateDefCache)
			}, f.templateDecodeOpts)
			if err != nil {
				if pooledRaw && !groupedStored && !prefixStored {
					f.releaseDecodeScratch(raw)
				}
				return nil, err, true
			}
			if pooledRaw && !groupedStored && !prefixStored {
				f.releaseDecodeScratch(raw)
			}
			return decoded, nil, true
		}
		out := make([]byte, int(valEnd-valStart))
		copy(out, val)
		if pooledRaw && !groupedStored && !prefixStored {
			f.releaseDecodeScratch(raw)
		}
		return out, nil, true
	}

	if readViaMmapViewPrefixCacheEnabled {
		// Publish prefix cache for subsequent uncompressed grouped reads.
		f.cacheMu.Lock()
		f.cacheK = k
		f.cacheFlags = fFlags
		f.cacheLen = prefixLen
		f.cacheOffs = offsets
		f.setCacheRawLocked(nil, false)
		f.cacheStart.Store(start)
		f.cacheMu.Unlock()
	}

	srcStart := prefixLen + int(valStart)
	srcEnd := prefixLen + int(valEnd)
	if srcStart < 0 || srcEnd < srcStart || srcEnd > len(payload) {
		return nil, ErrCorrupt, true
	}
	val, err := decodeTemplatePayload(payload[srcStart:srcEnd])
	if err != nil {
		return nil, err, true
	}
	return val, nil, true
}

// readViaMmapViewTo is like readViaMmapView, but decodes compressed grouped
// frames into dst (when provided) and returns a view into that buffer.
//
// This avoids allocating a fresh decode buffer on the unsafe view path while
// also avoiding the extra copy that readViaMmapAppend performs.
func (f *File) readViaMmapViewTo(ptr page.ValuePtr, verifyCRC bool, dst []byte) ([]byte, bool, error, bool) {
	data, _ := f.mmapData.Load().([]byte)
	if data == nil {
		f.mmapReadMissNoMapping.Add(1)
		f.maybeScheduleRemap()
		return nil, false, nil, false
	}
	if ptr.Offset < 4 {
		return nil, false, ErrCorrupt, true
	}
	start := int64(ptr.Offset - 4)
	fullRangeReadable := false
	if refreshed, ok, full := f.ensureMmapRecordInitialRange(data, ptr, start); ok {
		data = refreshed
		fullRangeReadable = full
	} else {
		f.mmapReadMissOutOfRange.Add(1)
		return nil, false, nil, false
	}

	header := data[start : start+HeaderSize]
	crcVal := binary.LittleEndian.Uint32(header[0:4])
	version := header[4]
	if version != Version {
		return nil, false, ErrCorrupt, true
	}
	flags := header[5]
	valueLen := binary.LittleEndian.Uint32(header[16:20])
	if recordSizeExceedsMax(valueLen) {
		return nil, false, ErrRecordTooLarge, true
	}
	expectedLen := uint32(headerWithoutCRC) + valueLen
	if !page.ValuePtrRecordLengthHintMatches(ptr, expectedLen) {
		return nil, false, ErrCorrupt, true
	}

	end := start + HeaderSize + int64(valueLen)
	if !fullRangeReadable {
		if refreshed, ok := f.ensureMmapRangeReadable(data, start, end); ok {
			data = refreshed
			header = data[start : start+HeaderSize]
		} else {
			f.mmapReadMissOutOfRange.Add(1)
			return nil, false, nil, false
		}
	}

	payload := data[start+HeaderSize : end]
	if verifyCRC {
		f.noteRecordCRCCheck()
		sum := crc.ChecksumParts(header[4:], payload)
		if sum != crcVal {
			return nil, false, ErrCorrupt, true
		}
	}

	// Non-grouped record: the payload is the value.
	if flags&recordFlagGrouped == 0 {
		if f.templateLookup != nil && templ.IsEncodedPayload(payload) {
			decoded, err := templ.DecodePayloadAppend(nil, payload, func(id uint64) (templ.TemplateDef, error) {
				return resolveTemplateDef(id, f.templateLookup, f.templateDefCache)
			}, f.templateDecodeOpts)
			if err != nil {
				return nil, false, err, true
			}
			return decoded, false, nil, true
		}
		return payload, false, nil, true
	}

	if !page.ValuePtrIsGrouped(ptr) {
		return nil, false, ErrCorrupt, true
	}
	if int64(FrameHeaderSize) > int64(len(payload)) {
		return nil, false, ErrCorrupt, true
	}
	frameHeader := payload[:FrameHeaderSize]
	if frameHeader[0] != FrameVersion {
		return nil, false, ErrCorrupt, true
	}
	k := int(frameHeader[2])
	if k <= 0 || k > MaxFrameK {
		return nil, false, ErrCorrupt, true
	}
	fFlags := frameHeader[1]

	decodeTemplatePayload := func(val []byte) ([]byte, bool, error) {
		if f.templateLookup == nil || !templ.IsEncodedPayload(val) {
			return val, false, nil
		}
		decoded, err := templ.DecodePayloadAppend(nil, val, func(id uint64) (templ.TemplateDef, error) {
			return resolveTemplateDef(id, f.templateLookup, f.templateDefCache)
		}, f.templateDecodeOpts)
		if err != nil {
			return nil, false, err
		}
		return decoded, true, nil
	}

	subIndex := int(page.ValuePtrSubIndex(ptr))
	if subIndex < 0 || subIndex >= k {
		return nil, false, ErrCorrupt, true
	}
	ridBytes := k * 8
	offsetBytes := (k + 1) * 4
	prefixLen := FrameHeaderSize + ridBytes + offsetBytes
	if int(valueLen) < prefixLen {
		return nil, false, ErrCorrupt, true
	}

	off := FrameHeaderSize + ridBytes
	var offsets [MaxFrameK + 1]uint32
	prev := uint32(0)
	for i := 0; i < k+1; i++ {
		cur := binary.LittleEndian.Uint32(payload[off : off+4])
		if cur < prev {
			return nil, false, ErrCorrupt, true
		}
		offsets[i] = cur
		prev = cur
		off += 4
	}

	rawLen := offsets[k]
	if limits.MaxRecordSize > 0 && int64(rawLen) > limits.MaxRecordSize {
		return nil, false, ErrRecordTooLarge, true
	}
	if fFlags&FrameFlagCompressed == 0 && prefixLen+int(rawLen) != int(valueLen) {
		return nil, false, ErrCorrupt, true
	}
	valStart := offsets[subIndex]
	valEnd := offsets[subIndex+1]
	if valEnd < valStart || valEnd > rawLen {
		return nil, false, ErrCorrupt, true
	}

	if fFlags&FrameFlagCompressed != 0 {
		if val, usedDst, err, hit := f.groupedFrameCacheReadTo(start, verifyCRC, k, offsets, rawLen, subIndex, dst); hit {
			if err != nil {
				return nil, false, err, true
			}
			val, decoded, err := decodeTemplatePayload(val)
			if err != nil {
				return nil, false, err, true
			}
			if decoded {
				return val, false, nil, true
			}
			return val, usedDst, nil, true
		}
	}

	if fFlags&FrameFlagCompressed == 0 {
		srcStart := prefixLen + int(valStart)
		srcEnd := prefixLen + int(valEnd)
		if srcStart < 0 || srcEnd < srcStart || srcEnd > len(payload) {
			return nil, false, ErrCorrupt, true
		}
		val, _, err := decodeTemplatePayload(payload[srcStart:srcEnd])
		if err != nil {
			return nil, false, err, true
		}
		return val, false, nil, true
	}

	if len(payload) < prefixLen {
		return nil, false, ErrCorrupt, true
	}
	frame := FrameHeader{
		Version:  frameHeader[0],
		Flags:    fFlags,
		K:        uint8(k),
		Reserved: frameHeader[3],
		DictID:   binary.LittleEndian.Uint64(frameHeader[4:12]),
	}
	valLen := int(valEnd - valStart)
	copyValueToDst := dst != nil && cap(dst) >= valLen && cap(dst) < int(rawLen)
	// One-off read paths (dst=nil) should avoid grouped-cache writeback; this
	// path is dominated by high-concurrency point reads where cache-store lock
	// traffic can outweigh reuse.
	cacheableRaw := copyValueToDst && f.groupedFrameCacheAllowsRaw(int(rawLen))

	// When raw payload may be cached, decode into pooled scratch so the grouped
	// cache can retain ownership and return buffers to the pool on eviction.
	decodeDst := dst
	rawPooled := false
	if copyValueToDst || cacheableRaw {
		decodeDst = f.takeDecodeScratch(int(rawLen))
		rawPooled = true
	}
	raw, err := decodeFramePayloadTo(frame, payload[prefixLen:], f.dictLookup, rawLen, decodeDst)
	if err != nil {
		if rawPooled {
			f.releaseDecodeScratch(decodeDst)
		}
		return nil, false, err, true
	}
	if uint32(len(raw)) != rawLen {
		if rawPooled {
			f.releaseDecodeScratch(raw)
		}
		return nil, false, ErrCorrupt, true
	}
	val, decoded, err := decodeTemplatePayload(raw[valStart:valEnd])
	if err != nil {
		if rawPooled {
			f.releaseDecodeScratch(raw)
		}
		return nil, false, err, true
	}
	publishRaw := func() {
		if cacheableRaw && f.groupedFrameCacheStore(start, verifyCRC, k, offsets, raw, rawPooled) && rawPooled {
			rawPooled = false
		}
	}
	// Template decoding allocates new bytes; the returned slice is no longer
	// backed by dst even if we decoded into it.
	if decoded {
		publishRaw()
		if rawPooled {
			f.releaseDecodeScratch(raw)
		}
		return val, false, nil, true
	}
	if copyValueToDst {
		out := dst[:valLen]
		copy(out, val)
		// Publish after copying the selected value. Once grouped cache accepts a
		// pooled raw buffer, another reader may evict and recycle it.
		publishRaw()
		if rawPooled {
			f.releaseDecodeScratch(raw)
		}
		return out, true, nil, true
	}
	// dst is used iff it was provided with enough capacity to hold rawLen.
	return val, dst != nil && cap(dst) >= int(rawLen), nil, true
}

func (f *File) readViaMmapAppend(ptr page.ValuePtr, verifyCRC bool, dst []byte) ([]byte, error, bool) {
	data, _ := f.mmapData.Load().([]byte)
	if data == nil {
		f.mmapReadMissNoMapping.Add(1)
		f.maybeScheduleRemap()
		return nil, nil, false
	}
	if ptr.Offset < 4 {
		return nil, ErrCorrupt, true
	}
	start := int64(ptr.Offset - 4)
	fullRangeReadable := false
	if refreshed, ok, full := f.ensureMmapRecordInitialRange(data, ptr, start); ok {
		data = refreshed
		fullRangeReadable = full
	} else {
		f.mmapReadMissOutOfRange.Add(1)
		return nil, nil, false
	}

	header := data[start : start+HeaderSize]
	crcVal := binary.LittleEndian.Uint32(header[0:4])
	version := header[4]
	if version != Version {
		return nil, ErrCorrupt, true
	}
	flags := header[5]
	valueLen := binary.LittleEndian.Uint32(header[16:20])
	if recordSizeExceedsMax(valueLen) {
		return nil, ErrRecordTooLarge, true
	}
	expectedLen := uint32(headerWithoutCRC) + valueLen
	if !page.ValuePtrRecordLengthHintMatches(ptr, expectedLen) {
		return nil, ErrCorrupt, true
	}

	end := start + HeaderSize + int64(valueLen)
	if !fullRangeReadable {
		if refreshed, ok := f.ensureMmapRangeReadable(data, start, end); ok {
			data = refreshed
			header = data[start : start+HeaderSize]
		} else {
			f.mmapReadMissOutOfRange.Add(1)
			return nil, nil, false
		}
	}

	payload := data[start+HeaderSize : end]
	if verifyCRC {
		f.noteRecordCRCCheck()
		sum := crc.ChecksumParts(header[4:], payload)
		if sum != crcVal {
			return nil, ErrCorrupt, true
		}
	}

	if flags&recordFlagGrouped == 0 {
		oldLen := len(dst)
		dst, err := appendDecodedTemplatePayload(dst, payload, f.templateLookup, f.templateDefCache, f.templateDecodeOpts)
		if err != nil {
			return nil, err, true
		}
		dst, err = f.appendMaybeDecodeLeafLogPayload(dst[:oldLen], dst[oldLen:])
		if err != nil {
			return nil, err, true
		}
		return dst, nil, true
	}
	if !page.ValuePtrIsGrouped(ptr) {
		return nil, ErrCorrupt, true
	}
	if int64(FrameHeaderSize) > int64(len(payload)) {
		return nil, ErrCorrupt, true
	}
	frameHeader := payload[:FrameHeaderSize]
	if frameHeader[0] != FrameVersion {
		return nil, ErrCorrupt, true
	}
	k := int(frameHeader[2])
	if k <= 0 || k > MaxFrameK {
		return nil, ErrCorrupt, true
	}
	fFlags := frameHeader[1]

	if fFlags&FrameFlagCompressed == 0 {
		subIndex := int(page.ValuePtrSubIndex(ptr))
		if subIndex < 0 || subIndex >= k {
			return nil, ErrCorrupt, true
		}
		ridBytes := k * 8
		offsetBytes := (k + 1) * 4
		prefixLen := FrameHeaderSize + ridBytes + offsetBytes
		if int(valueLen) < prefixLen {
			return nil, ErrCorrupt, true
		}

		if f.cacheStart.Load() == start {
			f.cacheMu.Lock()
			hit := f.cacheStart.Load() == start && f.cacheK > 0 && f.cacheLen > 0 && subIndex < f.cacheK && f.cacheFlags&FrameFlagCompressed == 0
			if hit {
				cPrefixLen := f.cacheLen
				valStart := f.cacheOffs[subIndex]
				valEnd := f.cacheOffs[subIndex+1]
				rawLen := f.cacheOffs[f.cacheK]
				f.cacheMu.Unlock()

				if valEnd < valStart || valEnd > rawLen {
					return nil, ErrCorrupt, true
				}
				if cPrefixLen+int(rawLen) != int(valueLen) {
					return nil, ErrCorrupt, true
				}
				var err error
				oldLen := len(dst)
				dst, err = appendDecodedTemplatePayload(dst, payload[cPrefixLen+int(valStart):cPrefixLen+int(valEnd)], f.templateLookup, f.templateDefCache, f.templateDecodeOpts)
				if err != nil {
					return nil, err, true
				}
				dst, err = f.appendMaybeDecodeLeafLogPayload(dst[:oldLen], dst[oldLen:])
				if err != nil {
					return nil, err, true
				}
				return dst, nil, true
			}
			f.cacheMu.Unlock()
		}

		off := FrameHeaderSize + ridBytes
		var offsets [MaxFrameK + 1]uint32
		prev := uint32(0)
		for i := 0; i < k+1; i++ {
			cur := binary.LittleEndian.Uint32(payload[off : off+4])
			if cur < prev {
				return nil, ErrCorrupt, true
			}
			offsets[i] = cur
			prev = cur
			off += 4
		}

		rawLen := offsets[k]
		if limits.MaxRecordSize > 0 && int64(rawLen) > limits.MaxRecordSize {
			return nil, ErrRecordTooLarge, true
		}
		if prefixLen+int(rawLen) != int(valueLen) {
			return nil, ErrCorrupt, true
		}
		valStart := offsets[subIndex]
		valEnd := offsets[subIndex+1]
		if valEnd < valStart || valEnd > rawLen {
			return nil, ErrCorrupt, true
		}

		f.cacheMu.Lock()
		f.cacheK = k
		f.cacheFlags = fFlags
		f.cacheLen = prefixLen
		f.cacheOffs = offsets
		f.setCacheRawLocked(nil, false)
		f.cacheStart.Store(start)
		f.cacheMu.Unlock()

		var err error
		oldLen := len(dst)
		dst, err = appendDecodedTemplatePayload(dst, payload[prefixLen+int(valStart):prefixLen+int(valEnd)], f.templateLookup, f.templateDefCache, f.templateDecodeOpts)
		if err != nil {
			return nil, err, true
		}
		dst, err = f.appendMaybeDecodeLeafLogPayload(dst[:oldLen], dst[oldLen:])
		if err != nil {
			return nil, err, true
		}
		return dst, nil, true
	}

	subIndex := int(page.ValuePtrSubIndex(ptr))
	if subIndex < 0 || subIndex >= k {
		return nil, ErrCorrupt, true
	}
	ridBytes := k * 8
	offsetBytes := (k + 1) * 4
	prefixLen := FrameHeaderSize + ridBytes + offsetBytes
	if int(valueLen) < prefixLen {
		return nil, ErrCorrupt, true
	}

	off := FrameHeaderSize + ridBytes
	var offsets [MaxFrameK + 1]uint32
	prev := uint32(0)
	for i := 0; i < k+1; i++ {
		cur := binary.LittleEndian.Uint32(payload[off : off+4])
		if cur < prev {
			return nil, ErrCorrupt, true
		}
		offsets[i] = cur
		prev = cur
		off += 4
	}
	rawLen := offsets[k]
	if limits.MaxRecordSize > 0 && int64(rawLen) > limits.MaxRecordSize {
		return nil, ErrRecordTooLarge, true
	}
	valStart := offsets[subIndex]
	valEnd := offsets[subIndex+1]
	if valEnd < valStart || valEnd > rawLen {
		return nil, ErrCorrupt, true
	}

	if cachedVal, usedDst, err, hit := f.groupedFrameCacheReadTo(start, verifyCRC, k, offsets, rawLen, subIndex, dst[len(dst):len(dst)]); hit {
		if err != nil {
			return nil, err, true
		}
		oldLen := len(dst)
		if usedDst {
			dst = dst[:oldLen+len(cachedVal)]
		} else {
			dst, err = appendDecodedTemplatePayload(dst, cachedVal, f.templateLookup, f.templateDefCache, f.templateDecodeOpts)
			if err != nil {
				return nil, err, true
			}
		}
		dst, err = f.appendMaybeDecodeLeafLogPayload(dst[:oldLen], dst[oldLen:])
		if err != nil {
			return nil, err, true
		}
		return dst, nil, true
	}

	frame := FrameHeader{
		Version:  frameHeader[0],
		Flags:    fFlags,
		K:        uint8(k),
		Reserved: frameHeader[3],
		DictID:   binary.LittleEndian.Uint64(frameHeader[4:12]),
	}
	// Hot random-read case: a single-entry grouped frame (k=1) where the
	// selected value spans the full decoded payload. Decode directly into dst
	// to avoid a decode-scratch allocation + extra copy.
	if f.templateLookup == nil && k == 1 && subIndex == 0 && valStart == 0 && valEnd == rawLen {
		oldLen := len(dst)
		noteGrowReadAppendCurrentMmapDirectDecode(int(rawLen))
		dst = grow(dst, int(rawLen))
		out, err := decodeFramePayloadTo(frame, payload[prefixLen:], f.dictLookup, rawLen, dst[oldLen:oldLen])
		if err != nil {
			return nil, err, true
		}
		if uint32(len(out)) != rawLen {
			return nil, ErrCorrupt, true
		}
		if len(out) > 0 {
			base := dst[oldLen : oldLen+len(out)]
			if &out[0] != &base[0] {
				copy(base, out)
			}
		}
		f.cacheMu.Lock()
		f.cacheK = k
		f.cacheFlags = fFlags
		f.cacheLen = prefixLen
		f.cacheOffs = offsets
		f.setCacheRawLocked(nil, false)
		f.cacheStart.Store(start)
		f.cacheMu.Unlock()
		dst, err = f.appendMaybeDecodeLeafLogPayload(dst[:oldLen], dst[oldLen:oldLen+int(rawLen)])
		if err != nil {
			return nil, err, true
		}
		return dst, nil, true
	}
	// One-shot reads (dst=nil) are typically point gets. Avoid caching decoded
	// grouped frames there to limit memory overhead in random-read-heavy paths.
	cacheableRaw := dst != nil && f.groupedFrameCacheAllowsRaw(int(rawLen))

	// Decode into pooled scratch even when we plan to cache decoded raw bytes.
	// When cached, ownership transfers to groupedFrameCacheStore(..., pooled=true)
	// and is returned to scratch pool on eviction; this avoids per-read heap
	// allocations in random-read parallel miss paths.
	raw := f.takeDecodeScratch(int(rawLen))
	pooledRaw := true
	raw, err := decodeFramePayloadTo(frame, payload[prefixLen:], f.dictLookup, rawLen, raw)
	if err != nil {
		if pooledRaw {
			f.releaseDecodeScratch(raw)
		}
		return nil, err, true
	}
	if uint32(len(raw)) != rawLen {
		if pooledRaw {
			f.releaseDecodeScratch(raw)
		}
		return nil, ErrCorrupt, true
	}
	oldLen := len(dst)

	f.cacheMu.Lock()
	f.cacheK = k
	f.cacheFlags = fFlags
	f.cacheLen = prefixLen
	f.cacheOffs = offsets
	f.setCacheRawLocked(nil, false)
	f.cacheStart.Store(start)
	f.cacheMu.Unlock()

	dst, err = appendDecodedTemplatePayload(dst, raw[valStart:valEnd], f.templateLookup, f.templateDefCache, f.templateDecodeOpts)
	if err != nil {
		if pooledRaw {
			f.releaseDecodeScratch(raw)
		}
		return nil, err, true
	}
	if len(dst) < oldLen {
		if pooledRaw {
			f.releaseDecodeScratch(raw)
		}
		return nil, ErrCorrupt, true
	}
	dst, err = f.appendMaybeDecodeLeafLogPayload(dst[:oldLen], dst[oldLen:])
	if err != nil {
		if pooledRaw {
			f.releaseDecodeScratch(raw)
		}
		return nil, err, true
	}
	if cacheableRaw {
		// Publish after copying the selected value. Once grouped cache accepts a
		// pooled raw buffer, another reader may evict and recycle it.
		if f.groupedFrameCacheStore(start, verifyCRC, k, offsets, raw, true) {
			pooledRaw = false
		}
	}
	if pooledRaw {
		f.releaseDecodeScratch(raw)
	}
	return dst, nil, true
}
