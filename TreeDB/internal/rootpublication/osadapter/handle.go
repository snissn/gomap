// Package osadapter binds stable-publication resource obligations to
// already-open operating-system handles.
//
// Paths are deliberately not accepted. Construction retains an independent
// descriptor for the same open-file description, so producer close or
// rotation cannot redirect a later flush or sync. The pin callbacks must also
// prevent the producer's deletion path from retiring the captured identity.
package osadapter

import (
	"errors"
	"fmt"
	"os"
	"sync"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

var (
	ErrUnsupportedPlatform = errors.New("stable OS handle adapter unsupported on this platform")
	ErrInvalidOpenHandle   = errors.New("invalid already-open stable handle")
	ErrMissingHook         = errors.New("stable OS handle adapter hook is required")
	ErrUnbalancedRelease   = errors.New("stable OS handle release without a matching pin")
	ErrUnpinnedOperation   = errors.New("stable OS handle operation requires an active pin")
	ErrFrontierNotFlushed  = errors.New("stable resource frontier has not been flushed")
	ErrHandleClosed        = errors.New("stable OS handle adapter is closed")
	ErrHandlePinned        = errors.New("stable OS handle adapter is pinned")
)

// ResourceHooks connect an open OS file to its producer and deletion owner.
// All hooks are required. FlushThrough must flush producer-owned buffering to
// the supplied, already-open file; it must not resolve or reopen a path. Pin
// and Release must protect and release the captured identity exactly once per
// successful call.
type ResourceHooks struct {
	FlushThrough func(file *os.File, frontier uint64) error
	Pin          func(identity rootpublication.StableIdentity) error
	Release      func(identity rootpublication.StableIdentity) error
}

// NamespaceHooks connect an open parent directory to its lifecycle owner.
// Both hooks are required and have the same exact pairing contract as the
// resource hooks.
type NamespaceHooks struct {
	Pin     func(identity rootpublication.StableIdentity) error
	Release func(identity rootpublication.StableIdentity) error
}

type openSnapshot struct {
	identity    rootpublication.StableIdentity
	length      uint64
	regularFile bool
	directory   bool
}

type syncOpenHandle func(*os.File) error

// ResourceHandle implements rootpublication.StableResourceHandle for an
// already-open regular file.
type ResourceHandle struct {
	mu             sync.RWMutex
	file           *os.File
	identity       rootpublication.StableIdentity
	hooks          ResourceHooks
	syncOpen       syncOpenHandle
	pins           uint64
	flushedThrough uint64
	closed         bool
}

// NewResourceHandle duplicates file's descriptor and captures identity from
// the duplicate. It never opens file.Name or any other diagnostic path. The
// duplicate closes automatically after the final Release. If the handle is
// not registered into a token, the caller must call Close.
func NewResourceHandle(file *os.File, hooks ResourceHooks) (*ResourceHandle, error) {
	return newResourceHandle(file, hooks, syncOpenResource)
}

// ResourceIdentity returns the platform identity of an already-open regular
// file without resolving its diagnostic name.
func ResourceIdentity(file *os.File) (rootpublication.StableIdentity, error) {
	snapshot, err := inspectOpenHandle(file)
	if err != nil {
		return rootpublication.StableIdentity{}, err
	}
	if !snapshot.regularFile {
		return rootpublication.StableIdentity{}, fmt.Errorf("%w: resource is not a regular file", ErrInvalidOpenHandle)
	}
	return snapshot.identity, nil
}

func newResourceHandle(file *os.File, hooks ResourceHooks, syncOpen syncOpenHandle) (*ResourceHandle, error) {
	if !stableOSHandlesSupported() {
		return nil, ErrUnsupportedPlatform
	}
	if file == nil {
		return nil, fmt.Errorf("%w: nil resource file", ErrInvalidOpenHandle)
	}
	if hooks.FlushThrough == nil || hooks.Pin == nil || hooks.Release == nil {
		return nil, ErrMissingHook
	}
	if syncOpen == nil {
		return nil, fmt.Errorf("%w: nil resource sync operation", ErrInvalidOpenHandle)
	}
	retained, err := duplicateOpenHandle(file)
	if err != nil {
		return nil, fmt.Errorf("%w: retain resource descriptor: %w", ErrInvalidOpenHandle, err)
	}
	keepRetained := false
	defer func() {
		if !keepRetained {
			_ = retained.Close()
		}
	}()
	snapshot, err := inspectOpenHandle(retained)
	if err != nil {
		return nil, fmt.Errorf("%w: inspect resource: %w", ErrInvalidOpenHandle, err)
	}
	if !snapshot.regularFile {
		return nil, fmt.Errorf("%w: resource is not a regular file", ErrInvalidOpenHandle)
	}
	keepRetained = true
	return &ResourceHandle{
		file: retained, identity: snapshot.identity, hooks: hooks, syncOpen: syncOpen,
	}, nil
}

// RegisterResourceToken is the leak-safe registration path. It closes the
// retained descriptor on every token-construction failure; successful token
// ownership closes it after the token's final Release.
func RegisterResourceToken(file *os.File, hooks ResourceHooks, spec rootpublication.StableResourceSpec) (*rootpublication.StableResourceToken, error) {
	if spec.Handle != nil {
		return nil, fmt.Errorf("%w: resource spec already has a handle", ErrInvalidOpenHandle)
	}
	handle, err := NewResourceHandle(file, hooks)
	if err != nil {
		return nil, err
	}
	spec.Handle = handle
	token, err := rootpublication.NewStableResourceToken(spec)
	if err != nil {
		return nil, errors.Join(err, handle.Close())
	}
	return token, nil
}

func (h *ResourceHandle) StableIdentity() (rootpublication.StableIdentity, error) {
	if h == nil {
		return rootpublication.StableIdentity{}, fmt.Errorf("%w: nil resource handle", ErrInvalidOpenHandle)
	}
	h.mu.RLock()
	defer h.mu.RUnlock()
	snapshot, err := h.validateOpenLocked()
	if err != nil {
		return rootpublication.StableIdentity{}, err
	}
	return snapshot.identity, nil
}

func (h *ResourceHandle) StableLength() (uint64, error) {
	if h == nil {
		return 0, fmt.Errorf("%w: nil resource handle", ErrInvalidOpenHandle)
	}
	h.mu.RLock()
	defer h.mu.RUnlock()
	snapshot, err := h.validateOpenLocked()
	if err != nil {
		return 0, err
	}
	return snapshot.length, nil
}

func (h *ResourceHandle) Pin() error {
	if h == nil {
		return fmt.Errorf("%w: nil resource handle", ErrInvalidOpenHandle)
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, err := h.validateOpenLocked(); err != nil {
		return err
	}
	if err := h.hooks.Pin(h.identity); err != nil {
		return err
	}
	if _, err := h.validateOpenLocked(); err != nil {
		return errors.Join(err, h.hooks.Release(h.identity))
	}
	h.pins++
	return nil
}

func (h *ResourceHandle) Release() error {
	if h == nil {
		return fmt.Errorf("%w: nil resource handle", ErrInvalidOpenHandle)
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.pins == 0 {
		return ErrUnbalancedRelease
	}
	// Consume the pairing before invoking user code. Even when Release reports
	// an error, retrying it could release the producer's pin twice.
	h.pins--
	releaseErr := h.hooks.Release(h.identity)
	if h.pins != 0 {
		return releaseErr
	}
	return errors.Join(releaseErr, h.closeLocked())
}

// Close releases an unregistered retained descriptor. It is idempotent, but
// fails closed while any token pin remains active.
func (h *ResourceHandle) Close() error {
	if h == nil {
		return nil
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.pins != 0 {
		return ErrHandlePinned
	}
	return h.closeLocked()
}

func (h *ResourceHandle) closeLocked() error {
	if h.closed {
		return nil
	}
	h.closed = true
	return h.file.Close()
}

func (h *ResourceHandle) FlushThrough(frontier uint64) error {
	if h == nil {
		return fmt.Errorf("%w: nil resource handle", ErrInvalidOpenHandle)
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, err := h.validateFrontierLocked(frontier); err != nil {
		return err
	}
	if err := h.hooks.FlushThrough(h.file, frontier); err != nil {
		return err
	}
	if _, err := h.validateFrontierLocked(frontier); err != nil {
		return err
	}
	if frontier > h.flushedThrough {
		h.flushedThrough = frontier
	}
	return nil
}

func (h *ResourceHandle) SyncThrough(frontier uint64) error {
	if h == nil {
		return fmt.Errorf("%w: nil resource handle", ErrInvalidOpenHandle)
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, err := h.validateFrontierLocked(frontier); err != nil {
		return err
	}
	if frontier > h.flushedThrough {
		return fmt.Errorf("%w: required=%d flushed=%d", ErrFrontierNotFlushed, frontier, h.flushedThrough)
	}
	if err := h.syncOpen(h.file); err != nil {
		return err
	}
	_, err := h.validateFrontierLocked(frontier)
	return err
}

func (h *ResourceHandle) validateOpenLocked() (openSnapshot, error) {
	if h.closed {
		return openSnapshot{}, ErrHandleClosed
	}
	snapshot, err := inspectOpenHandle(h.file)
	if err != nil {
		return openSnapshot{}, fmt.Errorf("%w: inspect captured resource: %w", ErrInvalidOpenHandle, err)
	}
	if !snapshot.regularFile {
		return openSnapshot{}, fmt.Errorf("%w: captured resource is not a regular file", ErrInvalidOpenHandle)
	}
	if snapshot.identity != h.identity {
		return openSnapshot{}, fmt.Errorf("%w: captured resource identity changed", rootpublication.ErrResourceConflict)
	}
	return snapshot, nil
}

func (h *ResourceHandle) validateFrontierLocked(frontier uint64) (openSnapshot, error) {
	if h.pins == 0 {
		return openSnapshot{}, ErrUnpinnedOperation
	}
	if frontier == 0 {
		return openSnapshot{}, fmt.Errorf("%w: zero resource frontier", rootpublication.ErrInvalidStableResource)
	}
	snapshot, err := h.validateOpenLocked()
	if err != nil {
		return openSnapshot{}, err
	}
	if frontier > snapshot.length {
		return openSnapshot{}, fmt.Errorf("%w: required=%d length=%d", rootpublication.ErrResourceFrontierBeyondLength, frontier, snapshot.length)
	}
	return snapshot, nil
}

// NamespaceHandle implements rootpublication.StableNamespaceHandle for an
// already-open parent directory.
type NamespaceHandle struct {
	mu         sync.RWMutex
	parent     *os.File
	identity   rootpublication.StableIdentity
	generation uint64
	hooks      NamespaceHooks
	syncOpen   syncOpenHandle
	pins       uint64
	closed     bool
}

// NewNamespaceHandle duplicates parent and records the producer's nonzero
// namespace generation. Validation is non-mutating; the constructor does not
// fsync the directory. The duplicate survives producer close or rotation.
func NewNamespaceHandle(parent *os.File, generation uint64, hooks NamespaceHooks) (*NamespaceHandle, error) {
	return newNamespaceHandle(parent, generation, hooks, syncOpenNamespace)
}

func newNamespaceHandle(parent *os.File, generation uint64, hooks NamespaceHooks, syncOpen syncOpenHandle) (*NamespaceHandle, error) {
	if !stableOSHandlesSupported() {
		return nil, ErrUnsupportedPlatform
	}
	if parent == nil || generation == 0 {
		return nil, fmt.Errorf("%w: nil parent or zero generation", ErrInvalidOpenHandle)
	}
	if hooks.Pin == nil || hooks.Release == nil {
		return nil, ErrMissingHook
	}
	if syncOpen == nil {
		return nil, fmt.Errorf("%w: nil namespace sync operation", ErrInvalidOpenHandle)
	}
	retained, err := duplicateOpenHandle(parent)
	if err != nil {
		return nil, fmt.Errorf("%w: retain parent namespace descriptor: %w", ErrInvalidOpenHandle, err)
	}
	keepRetained := false
	defer func() {
		if !keepRetained {
			_ = retained.Close()
		}
	}()
	snapshot, err := inspectOpenHandle(retained)
	if err != nil {
		return nil, fmt.Errorf("%w: inspect parent namespace: %w", ErrInvalidOpenHandle, err)
	}
	if !snapshot.directory {
		return nil, fmt.Errorf("%w: namespace parent is not a directory", ErrInvalidOpenHandle)
	}
	handle := &NamespaceHandle{
		parent: retained, identity: snapshot.identity, generation: generation,
		hooks: hooks, syncOpen: syncOpen,
	}
	if err := handle.ValidateNamespacePersistence(); err != nil {
		return nil, err
	}
	keepRetained = true
	return handle, nil
}

// RegisterNamespaceToken is the leak-safe namespace registration path.
func RegisterNamespaceToken(parent, child *os.File, generation uint64, hooks NamespaceHooks, spec rootpublication.StableNamespaceSpec) (*rootpublication.StableNamespaceToken, error) {
	if spec.Parent != nil || spec.Child != nil {
		return nil, fmt.Errorf("%w: namespace spec already has a retained handle", ErrInvalidOpenHandle)
	}
	childIdentity, err := ResourceIdentity(child)
	if err != nil {
		return nil, fmt.Errorf("%w: capture namespace child identity: %w", ErrInvalidOpenHandle, err)
	}
	handle, err := NewNamespaceHandle(parent, generation, hooks)
	if err != nil {
		return nil, err
	}
	spec.Parent = handle
	spec.Child = retainedIdentity{identity: childIdentity}
	token, err := rootpublication.NewStableNamespaceToken(spec)
	if err != nil {
		return nil, errors.Join(err, handle.Close())
	}
	return token, nil
}

type retainedIdentity struct {
	identity rootpublication.StableIdentity
}

func (i retainedIdentity) StableIdentity() (rootpublication.StableIdentity, error) {
	return i.identity, nil
}

func (h *NamespaceHandle) StableIdentity() (rootpublication.StableIdentity, error) {
	if h == nil {
		return rootpublication.StableIdentity{}, fmt.Errorf("%w: nil namespace handle", ErrInvalidOpenHandle)
	}
	h.mu.RLock()
	defer h.mu.RUnlock()
	if err := h.validateOpenLocked(); err != nil {
		return rootpublication.StableIdentity{}, err
	}
	return h.identity, nil
}

func (h *NamespaceHandle) StableGeneration() (uint64, error) {
	if h == nil {
		return 0, fmt.Errorf("%w: nil namespace handle", ErrInvalidOpenHandle)
	}
	h.mu.RLock()
	defer h.mu.RUnlock()
	if err := h.validateOpenLocked(); err != nil {
		return 0, err
	}
	return h.generation, nil
}

func (h *NamespaceHandle) ValidateNamespacePersistence() error {
	if h == nil {
		return fmt.Errorf("%w: nil namespace handle", ErrInvalidOpenHandle)
	}
	h.mu.RLock()
	defer h.mu.RUnlock()
	if err := h.validateOpenLocked(); err != nil {
		return err
	}
	return validateNamespacePersistence(h.parent)
}

func (h *NamespaceHandle) Pin() error {
	if h == nil {
		return fmt.Errorf("%w: nil namespace handle", ErrInvalidOpenHandle)
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if err := h.validateOpenLocked(); err != nil {
		return err
	}
	if err := validateNamespacePersistence(h.parent); err != nil {
		return err
	}
	if err := h.hooks.Pin(h.identity); err != nil {
		return err
	}
	if err := h.validateOpenLocked(); err != nil {
		return errors.Join(err, h.hooks.Release(h.identity))
	}
	h.pins++
	return nil
}

func (h *NamespaceHandle) Release() error {
	if h == nil {
		return fmt.Errorf("%w: nil namespace handle", ErrInvalidOpenHandle)
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.pins == 0 {
		return ErrUnbalancedRelease
	}
	h.pins--
	releaseErr := h.hooks.Release(h.identity)
	if h.pins != 0 {
		return releaseErr
	}
	return errors.Join(releaseErr, h.closeLocked())
}

// Close releases an unregistered retained directory descriptor.
func (h *NamespaceHandle) Close() error {
	if h == nil {
		return nil
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.pins != 0 {
		return ErrHandlePinned
	}
	return h.closeLocked()
}

func (h *NamespaceHandle) closeLocked() error {
	if h.closed {
		return nil
	}
	h.closed = true
	return h.parent.Close()
}

func (h *NamespaceHandle) SyncNamespace() error {
	if h == nil {
		return fmt.Errorf("%w: nil namespace handle", ErrInvalidOpenHandle)
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.pins == 0 {
		return ErrUnpinnedOperation
	}
	if err := h.validateOpenLocked(); err != nil {
		return err
	}
	if err := h.syncOpen(h.parent); err != nil {
		return err
	}
	return h.validateOpenLocked()
}

func (h *NamespaceHandle) validateOpenLocked() error {
	if h.closed {
		return ErrHandleClosed
	}
	snapshot, err := inspectOpenHandle(h.parent)
	if err != nil {
		return fmt.Errorf("%w: inspect captured namespace: %w", ErrInvalidOpenHandle, err)
	}
	if !snapshot.directory {
		return fmt.Errorf("%w: captured namespace is not a directory", ErrInvalidOpenHandle)
	}
	if snapshot.identity != h.identity {
		return fmt.Errorf("%w: captured namespace identity changed", rootpublication.ErrResourceConflict)
	}
	return nil
}

var (
	_ rootpublication.StableResourceHandle  = (*ResourceHandle)(nil)
	_ rootpublication.StableNamespaceHandle = (*NamespaceHandle)(nil)
)
