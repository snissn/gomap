from pathlib import Path
w=Path('/mnt/fast4tb/gomap-mvcc-4871-20261015/ordinary-repair/TreeDB/internal/rootpublication')
p=w/'resource_token.go';s=p.read_text();s=s.replace('type StableResourceToken struct {','type StableResourceToken struct {\n\t// Generic immutable diagnostics contain no physical cleanup aliases.\n\tdiagnostics *genericResourceDiagnostics')
s=s.replace('\tcloseOnError = false\n\tphysicalTransferred = true','\tif token.metadata == nil {\n\t\ttoken.diagnostics = newGenericResourceDiagnostics(token.namespace, spec.PinRegistry)\n\t}\n\tcloseOnError = false\n\tphysicalTransferred = true',1)
s=s.replace('\tcloned.owner.Store(uint32(ResourceOwnerToken))','\tcloned.diagnostics = token.diagnostics\n\tcloned.owner.Store(uint32(ResourceOwnerToken))',1)
s=s.replace('type StableNamespaceToken struct {','type StableNamespaceToken struct {\n\t// Allocated only for generic diagnostic consumers; holds counters, no owner.\n\tdiagnosticSyncs *genericNamespaceSyncMetrics')
s=s.replace('\ttoken.syncNanos.Add(uint64(time.Since(started)))\n\treturn err','\tduration := uint64(time.Since(started))\n\ttoken.syncNanos.Add(duration)\n\ttoken.recordDiagnosticSync(duration)\n\treturn err',1)
# Only namespace Stabilize remaining exact pair; resource Sync uses token.metrics.
s=s.replace('\ttoken.syncs.Add(1)\n\ttoken.syncNanos.Add(uint64(time.Since(started)))','\ttoken.syncs.Add(1)\n\tduration := uint64(time.Since(started))\n\ttoken.syncNanos.Add(duration)\n\ttoken.recordDiagnosticSync(duration)',1)
old='''\tif token.namespace == nil || other.namespace == nil {''';s=s.replace(old,'''\tif token.diagnostics != nil && other.diagnostics != nil {\n\t\treturn token.diagnostics.namespaceCompatible(other.diagnostics)\n\t}\n\tif token.namespace == nil || other.namespace == nil {''',1);p.write_text(s)
p=w/'resource_set.go';s=p.read_text()
# All nil namespace selections are diagnostic identity decisions; live mutations still use actual namespace.
import re
s=re.sub(r'([\w.]+)\.namespace == nil',r'!\1.hasDiagnosticNamespace()',s)
s=re.sub(r'([\w.]+)\.namespace != nil',r'\1.hasDiagnosticNamespace()',s)
s=s.replace('if namespace := entry.token.namespace; namespace != nil {','if namespace, ok := entry.token.diagnosticNamespace(); ok {')
s=s.replace('namespace.parentIdentity','namespace.ParentIdentity').replace('namespace.operation','namespace.Operation').replace('namespace.oldName','namespace.OldName').replace('namespace.newName','namespace.NewName').replace('namespace.diagnosticPath','namespace.DiagnosticPath')
s=s.replace('''if token == nil || token.identityPin == nil || token.identityPin.registry == nil {\n\t\t\t\tcontinue\n\t\t\t}\n\t\t\tregistries[token.identityPin.registry] = struct{}{}''','''if registry := token.diagnosticRegistry(); registry != nil {\n\t\t\t\tregistries[registry] = struct{}{}\n\t\t\t}''')
s=s.replace('seenNamespaces := make(map[*StableNamespaceToken]struct{})','seenNamespaces := make(map[any]struct{})')
s=s.replace('''if token.hasDiagnosticNamespace() {\n\t\t\tif _, seen := seenNamespaces[token.namespace]; !seen {\n\t\t\t\tseenNamespaces[token.namespace] = struct{}{}\n\t\t\t\tsyncs, duration := token.namespace.physicalSyncStats()''','''if namespace, syncs, duration := token.diagnosticNamespaceSyncStats(); namespace != nil {\n\t\t\tif _, seen := seenNamespaces[namespace]; !seen {\n\t\t\t\tseenNamespaces[namespace] = struct{}{}''')
s=s.replace('!prior.token.namespace.compatible(entry.token.namespace)','!prior.token.namespaceCompatible(entry.token)')
p.write_text(s)
