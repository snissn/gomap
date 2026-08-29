package mappedresource

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"sync/atomic"
)

// Source identifies how a handle's bytes are backed.
type Source string

const (
	SourceMapped          Source = "mmap"
	SourceHeapCopy        Source = "heap_copy"
	SourceDerivedMetadata Source = "derived_metadata"
)

// DenyReason and FallbackReason provide stable accounting labels.
type DenyReason string
type FallbackReason string
type ValidationMode string

// ErrMmapUnsupported identifies platforms without an mmap implementation. It
// deliberately does not cover ordinary mmap failures, which must fail closed.
var ErrMmapUnsupported = errors.New("mappedresource: mmap unsupported")

const (
	DenyInvalidKey      DenyReason = "invalid_key"
	DenyInvalidScope    DenyReason = "invalid_scope"
	DenyUnsupported     DenyReason = "unsupported"
	DenyOpenFailed      DenyReason = "open_failed"
	DenyOutOfBounds     DenyReason = "out_of_bounds"
	DenyReadFailed      DenyReason = "read_failed"
	DenyMmapFailed      DenyReason = "mmap_failed"
	DenyReleaseMismatch DenyReason = "release_mismatch"
)

const (
	FallbackMmapFailed FallbackReason = "mmap_failed"
	FallbackReadAt     FallbackReason = "readat"
)

const (
	ValidationVerify       ValidationMode = "verify"
	ValidationCachedVerify ValidationMode = "cached_verify"
	ValidationSkipChecksum ValidationMode = "skip_checksums"
)

// Stats is the common row+column resource accounting shape used by adapters and
// future column-part readers.
type Stats struct {
	ActiveHandles              int64
	ActiveMappedBytes          int64
	ActiveHeapCopyBytes        int64
	ActiveDerivedMetadataBytes int64

	TotalAcquires             uint64
	TotalReleases             uint64
	TotalMappedBytes          uint64
	TotalHeapCopyBytes        uint64
	TotalDerivedMetadataBytes uint64

	Hits          uint64
	Misses        uint64
	FallbackReads uint64
	Opens         uint64
	Closes        uint64
	Errors        uint64

	DirectViewSuccesses uint64
	DirectViewFailures  uint64

	DeniedByReason      map[DenyReason]uint64
	FallbacksByReason   map[FallbackReason]uint64
	ValidationModeReads map[ValidationMode]uint64
}

// Pin describes one active handle visible to maintenance planning. Root and
// Path carry storage identity for DB-local filtering; AcquireFileRange fills
// Path from the opened file path when callers leave ResourcePath empty.
type Pin struct {
	ID     uint64
	Key    Key
	Scope  Scope
	Source Source
	Bytes  int64
	Reason string
	Root   string
	Path   string
}

// Manager tracks resource handles and accounting. It intentionally does not
// choose a global cache policy; subsystems can wrap existing caches while using
// this manager for lifetime, pin, and stats visibility.
type Manager struct {
	mu     sync.Mutex
	nextID uint64
	stats  Stats
	active map[uint64]Pin
}

type globalPinKey struct {
	manager *Manager
	id      uint64
}

var globalResourceState = struct {
	mu     sync.Mutex
	stats  Stats
	active map[globalPinKey]Pin
}{active: make(map[globalPinKey]Pin)}

// NewManager constructs an empty manager.
func NewManager() *Manager {
	return &Manager{active: make(map[uint64]Pin)}
}

// AcquireOptions controls resource acquisition and accounting labels. ResourceRoot
// and ResourcePath identify the durable storage root/path for maintenance
// filtering when the handle protects file-backed resources.
type AcquireOptions struct {
	Reason         string
	ValidationMode ValidationMode
	PreferMapped   bool
	AllowHeapCopy  bool
	FallbackReason FallbackReason
	ResourceRoot   string
	ResourcePath   string
}

// Handle owns a live resource view. Bytes are valid until Release. Release is
// idempotent and updates accounting only once.
type Handle struct {
	mu         sync.Mutex
	mgr        *Manager
	id         uint64
	key        Key
	scope      Scope
	source     Source
	bytes      []byte
	release    func() error
	err        error
	done       bool
	doneAtomic atomic.Bool
}

// Key returns the resource key.
func (h *Handle) Key() Key {
	if h == nil {
		return Key{}
	}
	return h.key
}

// Scope returns the lifetime scope.
func (h *Handle) Scope() Scope {
	if h == nil {
		return Scope{}
	}
	return h.scope
}

// Source returns the backing source.
func (h *Handle) Source() Source {
	if h == nil {
		return ""
	}
	return h.source
}

// Bytes returns the live byte view. After Release it returns nil.
func (h *Handle) Bytes() []byte {
	if h == nil {
		return nil
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.done {
		return nil
	}
	return h.bytes
}

// Released reports whether the handle has been released.
func (h *Handle) Released() bool {
	if h == nil {
		return true
	}
	return h.doneAtomic.Load()
}

// Release releases the handle and removes its maintenance pin.
func (h *Handle) Release() error {
	if h == nil {
		return nil
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.done {
		return h.err
	}
	h.done = true
	h.doneAtomic.Store(true)
	mgr := h.mgr
	id := h.id
	source := h.source
	bytes := int64(len(h.bytes))
	release := h.release
	h.bytes = nil

	var err error
	if release != nil {
		err = release()
	}
	if mgr != nil {
		mgr.release(id, source, bytes, err)
	}
	h.err = err
	return err
}

// AcquireBytes registers caller-provided immutable bytes as a resource handle.
// The manager does not copy bytes; callers must ensure the slice remains stable
// for the handle lifetime.
func (m *Manager) AcquireBytes(key Key, scope Scope, source Source, data []byte, opts AcquireOptions) (*Handle, error) {
	if m == nil {
		return nil, errors.New("mappedresource: nil manager")
	}
	if err := scope.ValidateForKey(key); err != nil {
		m.recordDenied(classifyValidationDeny(err))
		return nil, err
	}
	if int64(len(data)) != key.Length {
		m.recordDenied(DenyOutOfBounds)
		return nil, fmt.Errorf("mappedresource: data bytes=%d do not match key length=%d", len(data), key.Length)
	}
	if source == "" {
		source = SourceHeapCopy
	}
	switch source {
	case SourceMapped, SourceHeapCopy, SourceDerivedMetadata:
	default:
		m.recordDenied(DenyUnsupported)
		return nil, fmt.Errorf("mappedresource: unsupported source %q", source)
	}
	return m.acquireRegistered(key, scope, source, data, nil, opts), nil
}

// AcquireFileRange opens path and returns either an mmap-backed or heap-copy
// handle for key.Offset:key.Offset+key.Length. Mmap is best-effort when
// PreferMapped is set; AllowHeapCopy controls fallback.
func (m *Manager) AcquireFileRange(key Key, scope Scope, path string, opts AcquireOptions) (*Handle, error) {
	if m == nil {
		return nil, errors.New("mappedresource: nil manager")
	}
	if err := scope.ValidateForKey(key); err != nil {
		m.recordDenied(classifyValidationDeny(err))
		return nil, err
	}
	if path == "" {
		m.recordDenied(DenyOpenFailed)
		return nil, errors.New("mappedresource: empty file path")
	}
	if key.Length > int64(int(^uint(0)>>1)) {
		m.recordDenied(DenyOutOfBounds)
		return nil, fmt.Errorf("mappedresource: length=%d exceeds host int", key.Length)
	}
	file, err := os.Open(path)
	if err != nil {
		m.recordDenied(DenyOpenFailed)
		m.recordError()
		return nil, err
	}
	m.recordOpen()
	if opts.PreferMapped {
		mapped, mapErr := mmapFile(file)
		if mapErr == nil {
			end := key.Offset + key.Length
			if key.Offset < 0 || end < key.Offset || end > int64(len(mapped)) {
				_ = munmapFile(mapped)
				_ = file.Close()
				m.recordClose()
				m.recordDenied(DenyOutOfBounds)
				return nil, fmt.Errorf("mappedresource: range offset=%d length=%d outside mapped bytes=%d", key.Offset, key.Length, len(mapped))
			}
			view := mapped[key.Offset:end]
			release := func() error {
				err := errors.Join(munmapFile(mapped), file.Close())
				m.recordClose()
				return err
			}
			opts.ResourcePath = mappedResourceOptionPath(opts.ResourcePath, path)
			return m.acquireRegistered(key, scope, SourceMapped, view, release, opts), nil
		}
		if !opts.AllowHeapCopy {
			_ = file.Close()
			m.recordClose()
			m.recordDenied(DenyMmapFailed)
			m.recordError()
			return nil, mapErr
		}
		m.recordFallback(FallbackMmapFailed)
	}
	if !opts.AllowHeapCopy && opts.PreferMapped {
		_ = file.Close()
		m.recordClose()
		m.recordDenied(DenyUnsupported)
		return nil, errors.New("mappedresource: heap-copy fallback disabled")
	}
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		m.recordClose()
		m.recordDenied(DenyReadFailed)
		m.recordError()
		return nil, err
	}
	end := key.Offset + key.Length
	if key.Offset < 0 || end < key.Offset || end > info.Size() {
		_ = file.Close()
		m.recordClose()
		m.recordDenied(DenyOutOfBounds)
		return nil, fmt.Errorf("mappedresource: range offset=%d length=%d outside file bytes=%d", key.Offset, key.Length, info.Size())
	}
	raw := make([]byte, int(key.Length))
	n, err := file.ReadAt(raw, key.Offset)
	closeErr := file.Close()
	m.recordClose()
	if err != nil && err != io.EOF {
		m.recordDenied(DenyReadFailed)
		m.recordError()
		return nil, err
	}
	if n != len(raw) {
		m.recordDenied(DenyOutOfBounds)
		m.recordError()
		return nil, io.ErrUnexpectedEOF
	}
	if closeErr != nil {
		m.recordError()
		return nil, closeErr
	}
	opts.ResourcePath = mappedResourceOptionPath(opts.ResourcePath, path)
	return m.acquireRegistered(key, scope, SourceHeapCopy, raw, nil, opts), nil
}

func mappedResourceOptionPath(explicit, fallback string) string {
	if explicit != "" {
		return explicit
	}
	return fallback
}

func (m *Manager) acquireRegistered(key Key, scope Scope, source Source, data []byte, release func() error, opts AcquireOptions) *Handle {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nextID++
	id := m.nextID
	bytes := int64(len(data))
	m.stats.ActiveHandles++
	m.stats.TotalAcquires++
	if opts.ValidationMode != "" {
		ensureValidationMap(&m.stats)[opts.ValidationMode]++
	}
	if opts.FallbackReason != "" {
		ensureFallbackMap(&m.stats)[opts.FallbackReason]++
		m.stats.FallbackReads++
	}
	switch source {
	case SourceMapped:
		m.stats.ActiveMappedBytes += bytes
		m.stats.TotalMappedBytes += uint64(bytes)
	case SourceHeapCopy:
		m.stats.ActiveHeapCopyBytes += bytes
		m.stats.TotalHeapCopyBytes += uint64(bytes)
	case SourceDerivedMetadata:
		m.stats.ActiveDerivedMetadataBytes += bytes
		m.stats.TotalDerivedMetadataBytes += uint64(bytes)
	}
	pin := Pin{ID: id, Key: key, Scope: scope, Source: source, Bytes: bytes, Reason: opts.Reason, Root: opts.ResourceRoot, Path: opts.ResourcePath}
	m.active[id] = pin
	globalRegisterPin(m, id, pin, opts.ValidationMode, opts.FallbackReason)
	return &Handle{mgr: m, id: id, key: key, scope: scope, source: source, bytes: data, release: release}
}

func (m *Manager) release(id uint64, source Source, bytes int64, releaseErr error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.active[id]
	if ok {
		delete(m.active, id)
	} else {
		ensureDeniedMap(&m.stats)[DenyReleaseMismatch]++
		globalRecordDenied(DenyReleaseMismatch)
	}
	m.stats.TotalReleases++
	if releaseErr != nil {
		m.stats.Errors++
	}
	if !ok {
		return
	}
	globalReleasePin(m, id, source, bytes, releaseErr)
	if m.stats.ActiveHandles > 0 {
		m.stats.ActiveHandles--
	}
	switch source {
	case SourceMapped:
		m.stats.ActiveMappedBytes -= bytes
	case SourceHeapCopy:
		m.stats.ActiveHeapCopyBytes -= bytes
	case SourceDerivedMetadata:
		m.stats.ActiveDerivedMetadataBytes -= bytes
	}
}

// Stats returns a stable snapshot.
func (m *Manager) Stats() Stats {
	if m == nil {
		return Stats{}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return cloneStats(m.stats)
}

// PinSummary returns active handles visible to maintenance.
func (m *Manager) PinSummary() []Pin {
	if m == nil {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]Pin, 0, len(m.active))
	for _, pin := range m.active {
		out = append(out, pin)
	}
	return out
}

// GlobalStats returns process-wide resource accounting across mappedresource
// managers. It is intended for maintenance diagnostics; subsystem-local Stats
// remains the precise per-manager view.
func GlobalStats() Stats {
	globalResourceState.mu.Lock()
	defer globalResourceState.mu.Unlock()
	return cloneStats(globalResourceState.stats)
}

// GlobalPinSummary returns all active process-local resource handles visible to
// destructive maintenance. Callers must still filter by resource class,
// namespace, and subsystem-specific identity before acting.
func GlobalPinSummary() []Pin {
	globalResourceState.mu.Lock()
	defer globalResourceState.mu.Unlock()
	out := make([]Pin, 0, len(globalResourceState.active))
	for _, pin := range globalResourceState.active {
		out = append(out, pin)
	}
	return out
}

// RecordHit records an adapter cache hit.
func (m *Manager) RecordHit() {
	if m == nil {
		return
	}
	m.mu.Lock()
	m.stats.Hits++
	m.mu.Unlock()
}

// RecordMiss records an adapter cache miss.
func (m *Manager) RecordMiss() {
	if m == nil {
		return
	}
	m.mu.Lock()
	m.stats.Misses++
	m.mu.Unlock()
}

func (m *Manager) recordDirectView(err error) {
	if m == nil {
		return
	}
	m.mu.Lock()
	if err != nil {
		m.stats.DirectViewFailures++
	} else {
		m.stats.DirectViewSuccesses++
	}
	m.mu.Unlock()
	globalRecordDirectView(err)
}

func (m *Manager) recordDenied(reason DenyReason) {
	m.mu.Lock()
	ensureDeniedMap(&m.stats)[reason]++
	m.mu.Unlock()
	globalRecordDenied(reason)
}

func (m *Manager) recordFallback(reason FallbackReason) {
	m.mu.Lock()
	ensureFallbackMap(&m.stats)[reason]++
	m.stats.FallbackReads++
	m.mu.Unlock()
	globalRecordFallback(reason)
}

func (m *Manager) recordOpen() {
	m.mu.Lock()
	m.stats.Opens++
	m.mu.Unlock()
	globalRecordOpen()
}

func (m *Manager) recordClose() {
	m.mu.Lock()
	m.stats.Closes++
	m.mu.Unlock()
	globalRecordClose()
}

func (m *Manager) recordError() {
	m.mu.Lock()
	m.stats.Errors++
	m.mu.Unlock()
	globalRecordError()
}

func globalRegisterPin(manager *Manager, id uint64, pin Pin, validation ValidationMode, fallback FallbackReason) {
	globalResourceState.mu.Lock()
	defer globalResourceState.mu.Unlock()
	bytes := pin.Bytes
	globalResourceState.stats.ActiveHandles++
	globalResourceState.stats.TotalAcquires++
	if validation != "" {
		ensureValidationMap(&globalResourceState.stats)[validation]++
	}
	if fallback != "" {
		ensureFallbackMap(&globalResourceState.stats)[fallback]++
		globalResourceState.stats.FallbackReads++
	}
	switch pin.Source {
	case SourceMapped:
		globalResourceState.stats.ActiveMappedBytes += bytes
		globalResourceState.stats.TotalMappedBytes += uint64(bytes)
	case SourceHeapCopy:
		globalResourceState.stats.ActiveHeapCopyBytes += bytes
		globalResourceState.stats.TotalHeapCopyBytes += uint64(bytes)
	case SourceDerivedMetadata:
		globalResourceState.stats.ActiveDerivedMetadataBytes += bytes
		globalResourceState.stats.TotalDerivedMetadataBytes += uint64(bytes)
	}
	globalResourceState.active[globalPinKey{manager: manager, id: id}] = pin
}

func globalReleasePin(manager *Manager, id uint64, source Source, bytes int64, releaseErr error) {
	globalResourceState.mu.Lock()
	defer globalResourceState.mu.Unlock()
	key := globalPinKey{manager: manager, id: id}
	if _, ok := globalResourceState.active[key]; ok {
		delete(globalResourceState.active, key)
	}
	globalResourceState.stats.TotalReleases++
	if releaseErr != nil {
		globalResourceState.stats.Errors++
	}
	if globalResourceState.stats.ActiveHandles > 0 {
		globalResourceState.stats.ActiveHandles--
	}
	switch source {
	case SourceMapped:
		globalResourceState.stats.ActiveMappedBytes -= bytes
	case SourceHeapCopy:
		globalResourceState.stats.ActiveHeapCopyBytes -= bytes
	case SourceDerivedMetadata:
		globalResourceState.stats.ActiveDerivedMetadataBytes -= bytes
	}
}

func globalRecordDirectView(err error) {
	globalResourceState.mu.Lock()
	if err != nil {
		globalResourceState.stats.DirectViewFailures++
	} else {
		globalResourceState.stats.DirectViewSuccesses++
	}
	globalResourceState.mu.Unlock()
}

func globalRecordDenied(reason DenyReason) {
	globalResourceState.mu.Lock()
	ensureDeniedMap(&globalResourceState.stats)[reason]++
	globalResourceState.mu.Unlock()
}

func globalRecordFallback(reason FallbackReason) {
	globalResourceState.mu.Lock()
	ensureFallbackMap(&globalResourceState.stats)[reason]++
	globalResourceState.stats.FallbackReads++
	globalResourceState.mu.Unlock()
}

func globalRecordOpen() {
	globalResourceState.mu.Lock()
	globalResourceState.stats.Opens++
	globalResourceState.mu.Unlock()
}

func globalRecordClose() {
	globalResourceState.mu.Lock()
	globalResourceState.stats.Closes++
	globalResourceState.mu.Unlock()
}

func globalRecordError() {
	globalResourceState.mu.Lock()
	globalResourceState.stats.Errors++
	globalResourceState.mu.Unlock()
}

func classifyValidationDeny(err error) DenyReason {
	if err == nil {
		return DenyUnsupported
	}
	if strings.Contains(err.Error(), "scope") {
		return DenyInvalidScope
	}
	return DenyInvalidKey
}

func ensureDeniedMap(stats *Stats) map[DenyReason]uint64 {
	if stats.DeniedByReason == nil {
		stats.DeniedByReason = make(map[DenyReason]uint64)
	}
	return stats.DeniedByReason
}

func ensureFallbackMap(stats *Stats) map[FallbackReason]uint64 {
	if stats.FallbacksByReason == nil {
		stats.FallbacksByReason = make(map[FallbackReason]uint64)
	}
	return stats.FallbacksByReason
}

func ensureValidationMap(stats *Stats) map[ValidationMode]uint64 {
	if stats.ValidationModeReads == nil {
		stats.ValidationModeReads = make(map[ValidationMode]uint64)
	}
	return stats.ValidationModeReads
}

func cloneStats(in Stats) Stats {
	out := in
	if in.DeniedByReason != nil {
		out.DeniedByReason = make(map[DenyReason]uint64, len(in.DeniedByReason))
		for k, v := range in.DeniedByReason {
			out.DeniedByReason[k] = v
		}
	}
	if in.FallbacksByReason != nil {
		out.FallbacksByReason = make(map[FallbackReason]uint64, len(in.FallbacksByReason))
		for k, v := range in.FallbacksByReason {
			out.FallbacksByReason[k] = v
		}
	}
	if in.ValidationModeReads != nil {
		out.ValidationModeReads = make(map[ValidationMode]uint64, len(in.ValidationModeReads))
		for k, v := range in.ValidationModeReads {
			out.ValidationModeReads[k] = v
		}
	}
	return out
}
