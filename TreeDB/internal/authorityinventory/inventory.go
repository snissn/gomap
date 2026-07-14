// Package authorityinventory is the typed source of truth for every field that
// can make a root publication depend on storage outside the page file.
package authorityinventory

//go:generate go run ../../cmd/authority_inventory

import (
	"bytes"
	"fmt"
	"sort"
	"strings"
)

type ActivationState string

const (
	ActivationActive           ActivationState = "active"
	ActivationAdjacent         ActivationState = "adjacent"
	ActivationQuarantined      ActivationState = "quarantined"
	ActivationNonAuthoritative ActivationState = "non_authoritative"
)

// Row assigns one persisted field or closed family to its publication authority.
// Field is a stable, source-shaped key used by the drift tests.
type Row struct {
	Field             string
	Scope             string
	ResourceClass     string
	Producer          string
	IdentitySource    string
	FrontierOrDigest  string
	NamespaceSite     string
	Registrar         string
	RecoveryValidator string
	DeletionOwner     string
	ActivationState   ActivationState
	AdjacentIssue     string
	ExclusionReason   string
}

var Rows = buildRows()

func buildRows() []Row {
	var rows []Row
	add := func(items ...Row) { rows = append(rows, items...) }

	for _, field := range []string{"Offset", "Length", "FileID"} {
		add(active("page.ValuePtr."+field, "value-log record", "mutable value-log segment range", "valuelog.Writer.Append", "pinned value-log file identity and FileID", "complete record end: Offset + decoded record Length", "value_vlog segment (no rename)", "value-log append/publication adapter", "value-log manager record CRC and pointer decode", "value-log GC/rewrite with segment pins"))
	}
	for _, field := range []string{"FileID", "Offset", "RecordLengthHint", "SubIndex"} {
		add(active("page.LogRecordRef."+field, "outer-leaf record", "mutable outer-leaf log segment range", "caching leaf-log appender", "pinned leaf-log file identity and FileID", "complete grouped record end from Offset and RecordLengthHint", "leaf-log segment (no rename)", "leaf-log append/publication adapter", "leaf record decode, CRC, and sub-index validation", "leaf-generation GC with segment pins"))
	}

	for _, field := range []string{"Version", "ManifestRevision", "CurrentGenerationID", "NextGenerationID", "Generations"} {
		add(active("db.leafGenerationManifest."+field, "leaf-generation manifest", "immutable manifest replacement", "saveLeafGenerationManifest", "pinned temporary file plus final manifest namespace generation", "encoded manifest digest", "temporary create then rename to manifest.json", "leaf-generation manifest publication adapter", "load and validate leaf-generation manifest", "leaf-generation manifest replacement owner"))
	}
	for _, field := range []string{"GenerationID", "State", "FileIDs", "CreatedCommitSeq", "SealedCommitSeq", "RetiredCommitSeq", "DeletedCommitSeq", "PublishedCommitSeq"} {
		add(active("db.leafGenerationRecord."+field, "leaf-generation manifest entry", "sealed leaf-generation closure", "leaf-generation pack/rewrite scheduler", "generation ID plus every referenced leaf FileID identity", "manifest digest plus sealed file digests/frontiers", "leaf generation files and manifest replacement", "leaf-generation manifest publication adapter", "manifest validation plus segment registration and CRC", "leaf-generation GC with manifest and segment pins"))
	}

	for _, field := range []string{"Kind", "Namespace", "Generation", "PartID", "FileID", "Offset", "Length", "Checksum"} {
		add(active("collections.ColumnAssetRef."+field, "column manifest asset ref", "logical immutable range in shared .tca segment", "column asset manager appenders", "namespace, generation, part, FileID, and pinned .tca identity", "Offset + Length and Checksum", "isolated column asset namespace", "column manifest publication adapter", "column asset header/schema/checksum validation", "column asset reachability GC with segment pins"))
	}

	columnConfigFields := []string{
		"Enabled", "Columns", "SortKey", "AggregateMetadata", "RetainedPayload", "RetainedPayloadEncoding", "Reconstruction",
		"AssetManager", "ManifestRoot", "ActiveManifest", "RecoveryAuthoritativeManifest", "RecoveryAuthoritativeAppliedCommandLSN",
		"PhysicalMutationParts", "ProfileSupport", "TypedColumnCompression", "TypedColumnSectionCompression", "Locator",
		"ControlRootStoragePolicy", "SchemaHash",
	}
	for _, field := range columnConfigFields {
		issue := "#3679"
		reason := "column-store catalog fields select named-root and manifest closure; external obligations enter through ColumnAssetRef and transitive value pointers"
		if field == "RecoveryAuthoritativeAppliedCommandLSN" {
			add(active("collections.ColumnStoreConfig."+field, "collection column-store catalog", "command-WAL recovery frontier", "collection root publication", "selected meta/root generation and applied command LSN", "contiguous applied command-LSN prefix", "page file and named collection roots", "ordered root publication with command-WAL coverage", "V2 command-WAL recovery and collection catalog validation", "page/root lifecycle plus covered command-WAL retention"))
			continue
		}
		add(adjacent("collections.ColumnStoreConfig."+field, "collection column-store catalog", "index/root authority metadata", issue, reason))
	}

	activeColumnKinds := []string{
		"ColumnAssetKindTCS1PartImage", "ColumnAssetKindTCS1TypedColumnPart", "ColumnAssetKindTCS1AggregateMetadata",
		"ColumnAssetKindTCS1DictionaryCodes", "ColumnAssetKindTCS1Int64Values", "ColumnAssetKindTCS1HNSWSearchPack",
	}
	for _, name := range activeColumnKinds {
		add(active("collections.ColumnAssetKind."+name, "column asset kind", "logical immutable range in shared .tca segment", "typed-column and vector asset builders", "ColumnAssetRef plus pinned backing .tca identity", "asset range end and checksum", "isolated column asset namespace", "column manifest publication adapter", "kind-specific asset decoder and checksum", "column asset reachability GC with segment pins"))
	}
	for _, name := range []string{"ColumnAssetKindQueryReadyBase", "ColumnAssetKindQueryReadyDelta", "ColumnAssetKindQueryReadyConsolidatedBase"} {
		add(nonAuthoritative("collections.ColumnAssetKind."+name, "query-ready cache asset kind", "rebuildable prepared asset", "#3677", "query-ready base, delta, and consolidated files are cache accelerators and do not select recovery state"))
	}

	for _, name := range []string{"ExternalRefValueLog", "ExternalRefLeafLog", "ExternalRefPayloadFile"} {
		add(quarantined("commitlog.ExternalRefClass."+name, "command WAL external ref", "reserved typed external resource range", "#1595", "no production producer exists; V1 and V2 reject every non-empty typed ExternalRefs section, while RawKV SetRID uses the separate active ExternalRefFenceV1 closure"))
	}
	for _, field := range []string{"Class", "Flags", "FileID", "Offset", "Length", "Digest"} {
		add(quarantined("commitlog.ExternalRef."+field, "command WAL external ref field", "reserved typed external resource range", "#1595", "the dormant typed ExternalRefs section has no complete producer, pin, sync, recovery, and deletion-owner closure"))
	}
	add(Row{
		Field: "commitlog.ExternalRef.Path", Scope: "command WAL external ref field", ResourceClass: "diagnostic DB-relative path",
		Producer: "command envelope encoder", IdentitySource: "not an identity; diagnostic display only", FrontierOrDigest: "not applicable",
		NamespaceSite: "not a namespace authority", Registrar: "must ignore Path for identity and pinning", RecoveryValidator: "resolve typed Class and FileID instead of reopening Path",
		DeletionOwner: "must ignore Path and use the pinned typed resource", ActivationState: ActivationNonAuthoritative, AdjacentIssue: "#3677",
		ExclusionReason: "paths are diagnostic only and cannot replace a pinned identity or typed FileID during sync, recovery, or deletion",
	})

	for _, field := range []string{"Version", "DurabilityClass", "LSN", "Kind", "Scope", "FeatureFlags", "CatalogEpoch", "SchemaEpoch", "BaseAppliedLSN", "PayloadFormat", "Payload", "Preconditions", "ResultAssertions"} {
		add(active("commitlog.CommandEnvelope."+field, "command WAL envelope field", "logical command and durable-horizon authority", "V2 command journal encoder", "LSN plus exact command-WAL segment identity", "length/CRC-bounded V2 frame and durability class", "pinned command-WAL segment and namespace generation", "CommandWALDependencyDebt and command journal", "V2 frame validation, durable-horizon classification, and tail repair", "covered command-WAL retention; ordinary cleanup is owned by #3682"))
	}
	add(quarantined("commitlog.CommandEnvelope.ExternalRefs", "command WAL envelope field", "reserved transitive external resource closure", "#1595", "V1 and V2 reject every non-empty typed ExternalRefs section; RawKV SetRID uses the separate active ExternalRefFenceV1 closure"))
	for _, field := range []string{"Type", "Payload"} {
		add(active("commitlog.CommandExtension."+field, "command WAL extension field", "V2 precondition/assertion extension", "V2 command-frame encoder", "owning command frame LSN and segment identity", "canonical extension bytes covered by frame length and CRC", "owning command-WAL segment namespace", "V2 command-frame validation", "critical extension validation before durable-horizon classification", "covered command-WAL retention; ordinary cleanup is owned by #3682"))
	}
	for _, field := range []string{"Count", "Digest"} {
		add(active("commitlog.ExternalRefFenceV1."+field, "external-ref fence field", "canonical RID-set fence", "RawKV SetRID V2 payload encoder", "canonical RID set plus exact pinned value-log segment identities", "RID count/digest and per-segment required record frontier", "value-log segment creation namespace", "value-log CaptureStableExternalRIDFence merged into CommandWALDependencyDebt", "V2 fence recomputation and RID existence validation through the durable horizon", "value-log GC/rewrite with command-WAL identity pins"))
	}

	add(active("commitlog.CommandJournal.Frame", "command WAL frame", "mutable command-WAL segment range", "commitlog.CommandJournal.Append", "pinned journal segment identity plus LSN", "complete encoded frame end and checksum", "wal segment (no rename)", "command journal publication adapter", "journal scan validates framing, checksum, and LSN order", "command WAL retention with segment pins"))
	add(active("dictionary.GlobalID", "value-log dictionary dependency", "transitive immutable dictionary closure", "value-log compression encoder", "stable dictdb index generation plus exact manager-owned value-log segment and dictionary ID", "dictionary definition digest plus exact index and value-log frontiers", "dictdb index namespace proof and installed value-log namespace", "dictdb CaptureDictionaryResources merged before cached stable append, raw stable rewrite, or packed promotion mutates its writer namespace", "dictionary lookup and definition decode", "online index vacuum fence plus value-log identity pin; retain forever until pin-aware dictionary GC exists"))
	add(Row{
		Field: "template.EncodedPayload.TemplateID", Scope: "value-log template dependency", ResourceClass: "transitive immutable template closure",
		Producer: "template.Engine.Encode behind forced-off runtime mode", IdentitySource: "stable templatedb index generation plus exact pointer-backed value-log segment and salt-aware template ID",
		FrontierOrDigest: "template definition digest plus exact index and value-log frontiers", NamespaceSite: "templatedb index namespace proof and installed value-log namespace",
		Registrar:         "templatedb CaptureTemplateResources is merged by the stable raw outer-leaf seam; offline ordinary-value rewrite rejects activation",
		RecoveryValidator: "template lookup, salt-aware definition identity, and definition decode", DeletionOwner: "online index vacuum fence plus value-log identity pin; retain forever because no template deletion exists",
		ActivationState: ActivationQuarantined, AdjacentIssue: "#3679",
		ExclusionReason: "public and cached runtime template compression is forced off; offline rewrite cannot activate until rewritten-root publication consumes its complete dependency union",
	})

	for _, name := range []string{"collectionTextIndexRootName", "collectionTextStateRootName", "collectionTextStatsRootName"} {
		add(adjacent("collections.TextV1Root."+name, "named text-v1 root", "index/value-log transitive closure", "#3679", "text roots are page-index closure with transitive value pointers, not independent external resource files"))
	}
	for _, name := range []string{"collectionTextV2DocIDRootName", "collectionTextV2DocMapRootName", "collectionTextV2TermsRootName", "collectionTextV2PostingBlocksRootName", "collectionTextV2NormBlocksRootName", "collectionTextV2PositionsRootName", "collectionTextV2GenerationsRootName"} {
		add(adjacent("collections.TextV2Root."+name, "named text-v2 root", "index/value-log transitive closure", "#3679", "text roots are page-index closure with transitive value pointers, not independent external resource files"))
	}

	metaAdjacent := []struct{ field, issue string }{
		{"CommitSeq", "#3679"}, {"UserRootPageID", "#3679"}, {"SystemRootPageID", "#3679"}, {"FreelistHeadID", "#3678"},
		{"TotalPages", "#3678"}, {"LastCommitHeight", "#3679"}, {"MaxEntryRevision", "#3679"},
	}
	for _, entry := range metaAdjacent {
		add(adjacent("page.MetaPageBody."+entry.field, "meta-page scalar", "page-file publication scalar", entry.issue, "owned by page/meta publication ordering rather than an external resource token"))
	}
	add(active("page.MetaPageBody.AppliedCommandLSN", "meta-page scalar", "command-WAL applied-prefix publication scalar", "root/meta publication", "selected meta-page generation", "contiguous applied command-LSN prefix", "page file", "finalizeCommit command-WAL coverage validation", "V2 recovery starts at the selected applied prefix and rejects holes through the durable horizon", "page/meta lifecycle plus covered command-WAL retention"))
	for _, field := range []string{"ActiveSlabID", "ActiveSlabTail"} {
		add(quarantined("page.MetaPageBody."+field, "legacy meta-page scalar", "removed legacy value-store namespace", "#3677", "TreeDB no longer has the legacy value-store path; these format fields are decoded only for compatibility"))
	}
	add(adjacent("collections.CollectionRoot", "named collection root", "index/value-log transitive closure", "#3679", "named roots are page-index authority, not external resource kinds"))
	add(adjacent("collections.VectorNativeRoot", "native vector root", "index/value-log transitive closure", "#3679", "native vector state is stored in named roots and value-log closure, not vector sidecar files"))
	add(nonAuthoritative("collections.LegacyVectorSidecar", "legacy vector sidecar", "rebuildable exact-search cache", "#3677", "legacy vector sidecars are validated accelerators with exact-search fallback and never select recovery state"))

	return rows
}

func active(field, scope, class, producer, identity, frontier, namespace, registrar, recovery, deletion string) Row {
	return Row{field, scope, class, producer, identity, frontier, namespace, registrar, recovery, deletion, ActivationActive, "", ""}
}

func adjacent(field, scope, class, issue, reason string) Row {
	return Row{field, scope, class, "not applicable: adjacent authority", "page/root identity", "page/root closure", "page file or named root", "adjacent issue", "adjacent issue", "adjacent issue", ActivationAdjacent, issue, reason}
}

func quarantined(field, scope, class, issue, reason string) Row {
	return Row{field, scope, class, "blocked", "not established", "not established", "not authoritative", "reject activation", "fail closed", "retain; do not delete by publication", ActivationQuarantined, issue, reason}
}

func nonAuthoritative(field, scope, class, issue, reason string) Row {
	return Row{field, scope, class, "cache builder", "cache identity only", "rebuildable checksum", "cache namespace", "never register as authority", "rebuild or fail closed", "cache lifecycle manager", ActivationNonAuthoritative, issue, reason}
}

func Validate(rows []Row) error {
	seen := make(map[string]struct{}, len(rows))
	for i, row := range rows {
		if strings.TrimSpace(row.Field) == "" {
			return fmt.Errorf("authority inventory row %d has empty Field", i)
		}
		if _, ok := seen[row.Field]; ok {
			return fmt.Errorf("authority inventory has duplicate Field %q", row.Field)
		}
		seen[row.Field] = struct{}{}
		required := map[string]string{
			"Scope": row.Scope, "ResourceClass": row.ResourceClass, "Producer": row.Producer,
			"IdentitySource": row.IdentitySource, "FrontierOrDigest": row.FrontierOrDigest,
			"NamespaceSite": row.NamespaceSite, "Registrar": row.Registrar,
			"RecoveryValidator": row.RecoveryValidator, "DeletionOwner": row.DeletionOwner,
		}
		for name, value := range required {
			if strings.TrimSpace(value) == "" {
				return fmt.Errorf("authority inventory row %q has empty %s", row.Field, name)
			}
		}
		switch row.ActivationState {
		case ActivationActive:
			if row.AdjacentIssue != "" || row.ExclusionReason != "" {
				return fmt.Errorf("active authority inventory row %q must not have adjacent issue or exclusion reason", row.Field)
			}
		case ActivationAdjacent, ActivationQuarantined, ActivationNonAuthoritative:
			if strings.TrimSpace(row.AdjacentIssue) == "" || strings.TrimSpace(row.ExclusionReason) == "" {
				return fmt.Errorf("inactive authority inventory row %q must name an owner issue and reason", row.Field)
			}
		default:
			return fmt.Errorf("authority inventory row %q has invalid activation state %q", row.Field, row.ActivationState)
		}
	}
	return nil
}

func RenderMarkdown(rows []Row) []byte {
	if err := Validate(rows); err != nil {
		panic(err)
	}
	ordered := append([]Row(nil), rows...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].Field < ordered[j].Field })
	var b bytes.Buffer
	b.WriteString("<!-- Code generated by go generate ./TreeDB/internal/authorityinventory; DO NOT EDIT. -->\n")
	b.WriteString("# Root publication authority inventory\n\n")
	b.WriteString("This table is the fail-closed source-of-truth projection for issue #3677. `active` rows must have a complete producer-to-deletion chain. `adjacent` rows remain owned by the named issue. `quarantined` and `non_authoritative` rows cannot select recovery state or satisfy publication durability.\n\n")
	b.WriteString("| Field | Scope | Resource / physical class | Producer | Identity source | Frontier or digest | Namespace site | Registrar | Recovery validator | Deletion owner | Activation state | Adjacent issue | Exclusion reason |\n")
	b.WriteString("|---|---|---|---|---|---|---|---|---|---|---|---|---|\n")
	for _, row := range ordered {
		values := []string{row.Field, row.Scope, row.ResourceClass, row.Producer, row.IdentitySource, row.FrontierOrDigest, row.NamespaceSite, row.Registrar, row.RecoveryValidator, row.DeletionOwner, string(row.ActivationState), row.AdjacentIssue, row.ExclusionReason}
		b.WriteString("|")
		for _, value := range values {
			b.WriteString(" ")
			b.WriteString(markdownCell(value))
			b.WriteString(" |")
		}
		b.WriteByte('\n')
	}
	return b.Bytes()
}

func markdownCell(value string) string {
	value = strings.ReplaceAll(value, "|", "\\|")
	return strings.ReplaceAll(value, "\n", " ")
}
