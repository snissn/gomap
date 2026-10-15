package rootpublication

import (
	"strings"
	"sync/atomic"
	"time"
)

// Generic sets retain immutable diagnostic evidence after their physical pins
// close. This backing contains no namespace, pin, file, or cleanup-owner alias.
// Selected tokens instead use their existing guarded, admitted descriptor edge.
type genericResourceDiagnostics struct {
	namespace *StableNamespaceDescriptor
	linked    StableIdentity
	hasLinked bool
	syncs     *genericNamespaceSyncMetrics
	registry  *IdentityPinRegistry
}
type genericNamespaceSyncMetrics struct{ count, nanos atomic.Uint64 }

func newGenericResourceDiagnostics(namespace *StableNamespaceToken, registry *IdentityPinRegistry) *genericResourceDiagnostics {
	if namespace == nil && registry == nil {
		return nil
	}
	if namespace == nil {
		return &genericResourceDiagnostics{registry: registry}
	}
	namespace.mu.Lock()
	defer namespace.mu.Unlock()
	// The descriptor and header have the same immutable lifetime. Registry-only
	// diagnostics above do not pay for the additional containing storage.
	owned := &struct {
		diagnostics genericResourceDiagnostics
		namespace   StableNamespaceDescriptor
	}{}
	d := &owned.diagnostics
	d.registry, d.namespace = registry, &owned.namespace
	*d.namespace = StableNamespaceDescriptor{ParentIdentity: namespace.parentIdentity, Operation: namespace.operation}
	d.linked, d.hasLinked = namespace.linkedResourceIdentity, namespace.hasLinkedResource
	// Keep all five operands, including empty ones: Join must copy even when
	// only one field is nonempty. The returned fields own no namespace storage.
	// A separately retained short field can keep this whole text backing alive.
	parts := [5]string{namespace.oldName, namespace.newName, namespace.diagnosticPath, namespace.parentIdentity.Platform, namespace.linkedResourceIdentity.Platform}
	text := strings.Join(parts[:], "")
	for i, part := range parts {
		parts[i] = ""
		if part != "" {
			parts[i], text = text[:len(part)], text[len(part):]
		}
	}
	d.namespace.OldName, d.namespace.NewName, d.namespace.DiagnosticPath = parts[0], parts[1], parts[2]
	d.namespace.ParentIdentity.Platform, d.linked.Platform = parts[3], parts[4]
	if namespace.diagnosticSyncs.Load() == nil {
		metrics := &genericNamespaceSyncMetrics{}
		count, duration := namespace.physicalSyncStats()
		metrics.count.Store(count)
		metrics.nanos.Store(uint64(duration))
		namespace.diagnosticSyncs.Store(metrics)
	}
	d.syncs = namespace.diagnosticSyncs.Load()
	return d
}

// Both runtime namespace sync sites call this under the original namespace mu.
// Construction-time sync counts are captured before generic publication.
func (namespace *StableNamespaceToken) recordDiagnosticSync(nanos uint64) {
	if metrics := namespace.diagnosticSyncs.Load(); metrics != nil {
		metrics.count.Add(1)
		metrics.nanos.Add(nanos)
	}
}
func (token *StableResourceToken) diagnosticNamespace() (StableNamespaceDescriptor, bool) {
	if token == nil {
		return StableNamespaceDescriptor{}, false
	}
	if token.metadata == nil {
		if token.diagnostics == nil || token.diagnostics.namespace == nil {
			return StableNamespaceDescriptor{}, false
		}
		return *token.diagnostics.namespace, true
	}
	if ns := token.namespace; ns != nil {
		return StableNamespaceDescriptor{ParentIdentity: ns.parentIdentity, Operation: ns.operation, OldName: ns.oldName, NewName: ns.newName, DiagnosticPath: ns.diagnosticPath}, true
	}
	return StableNamespaceDescriptor{}, false
}
func (token *StableResourceToken) hasDiagnosticNamespace() bool {
	if token == nil {
		return false
	}
	if token.metadata != nil {
		return token.namespace != nil
	}
	return token.diagnostics != nil && token.diagnostics.namespace != nil
}
func (token *StableResourceToken) diagnosticRegistry() *IdentityPinRegistry {
	if token == nil {
		return nil
	}
	if token.metadata == nil {
		if token.diagnostics != nil {
			return token.diagnostics.registry
		}
		return nil
	}
	if token.identityPin != nil {
		return token.identityPin.registry
	}
	return nil
}
func (token *StableResourceToken) diagnosticNamespaceSyncStats() (any, uint64, time.Duration) {
	if token.metadata == nil {
		if token.diagnostics == nil || token.diagnostics.syncs == nil {
			return nil, 0, 0
		}
		m := token.diagnostics.syncs
		return m, m.count.Load(), time.Duration(m.nanos.Load())
	}
	if token.namespace == nil {
		return nil, 0, 0
	}
	if metrics := token.namespace.diagnosticSyncs.Load(); metrics != nil {
		return metrics, metrics.count.Load(), time.Duration(metrics.nanos.Load())
	}
	count, duration := token.namespace.physicalSyncStats()
	return token.namespace, count, duration
}

// The selected caller must retain its token edge through this comparison.
// Public logical Union refuses selected inputs rather than exporting that edge.
func (namespace *StableNamespaceToken) compatibleGenericDiagnostics(generic *genericResourceDiagnostics) bool {
	if namespace == nil || generic == nil || generic.namespace == nil {
		return true
	}
	namespace.mu.Lock()
	defer namespace.mu.Unlock()
	if namespace.metadataScoped && namespace.released.Load() {
		return false
	}
	other := generic.namespace
	return namespace.parentIdentity == other.ParentIdentity && namespace.operation == other.Operation &&
		namespace.oldName == other.OldName && namespace.newName == other.NewName &&
		namespace.hasLinkedResource == generic.hasLinked &&
		(!namespace.hasLinkedResource || sameStableObject(namespace.linkedResourceIdentity, generic.linked))
}

func genericNamespaceDiagnosticsCompatible(left, right *genericResourceDiagnostics) bool {
	if left == nil || right == nil || left.namespace == nil || right.namespace == nil {
		return true
	}
	a, b := left.namespace, right.namespace
	return a.ParentIdentity == b.ParentIdentity && a.Operation == b.Operation && a.OldName == b.OldName && a.NewName == b.NewName &&
		left.hasLinked == right.hasLinked && (!left.hasLinked || sameStableObject(left.linked, right.linked))
}
