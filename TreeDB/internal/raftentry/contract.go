package raftentry

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"

	"github.com/snissn/gomap/TreeDB/internal/nativewire"
)

const (
	EntryVersionV1 = uint64(1)

	// ScopeRuleSingleGroupV1 is a schema-versioned single-group rule. It is
	// covered by CommandDigestV1 even though it is not re-encoded inside the
	// native-wire deterministic entry bytes.
	ScopeRuleSingleGroupV1 ScopeRuleV1 = "single-group-v1"

	DatabaseScopeDefaultV1 = "database/default"
	CatalogScopeDefaultV1  = "catalog/default"

	NoIdempotencyTokenV1 = "NoIdempotencyV1"

	MaxIdempotencyKeyBytesV1 = 1024
	MaxResultRecordBytesV1   = 64 << 10
	MaxProgressRecordsV1     = 1 << 20
)

const digestDomainV1 = "TreeDB/R3a/CommandDigestV1\x00"

type ScopeRuleV1 string

type ApplyEntryID struct {
	Term  uint64
	Index uint64
}

type RequestMetadataV1 struct {
	RequestID         uint64
	AckPolicy         nativewire.AckPolicy
	DeadlineUnixNanos int64
	TraceContext      []byte
	Compression       string
	OmitResultIDs     bool
	OmitResponseMeta  bool

	// ClusterRoute* fields are request-only metadata for cluster submitters.
	// They are intentionally excluded from deterministic command entry bytes,
	// but submit admission may still treat them as binding local-route guards.
	ClusterRouteKnown         bool
	ClusterRouteDatabase      string
	ClusterRouteCatalog       string
	ClusterRouteCollection    string
	ClusterRouteShape         string
	ClusterRouteGroupID       string
	ClusterRouteMembers       []string
	ClusterRouteLeaderHint    string
	ClusterRoutePlacementMode string
	ClusterRouteKey           string
	ClusterRouteTokenKnown    bool
	ClusterRouteToken         uint64
	ClusterRoutePartitionID   string
	CatalogMetaEpoch          uint64
	CatalogMetaDigest         string
}

type CommandDigestV1 [32]byte

func (d CommandDigestV1) Hex() string {
	return hex.EncodeToString(d[:])
}

type IdempotencyModeV1 string

const (
	IdempotencyRequiredV1 IdempotencyModeV1 = "required-idempotency-key-v1"
	NoIdempotencyV1       IdempotencyModeV1 = "no-idempotency-v1"
)

type ApplyStatusV1 string

const (
	ApplyStatusApplied                   ApplyStatusV1 = "applied"
	ApplyStatusAlreadyApplied            ApplyStatusV1 = "already-applied"
	ApplyStatusDeterministicGuardFailure ApplyStatusV1 = "deterministic-guard-failure"
	ApplyStatusRejectedUnsupported       ApplyStatusV1 = "rejected-unsupported"
	ApplyStatusRejectedMalformed         ApplyStatusV1 = "rejected-malformed"
	ApplyStatusRejectedConflict          ApplyStatusV1 = "rejected-conflict"
	ApplyStatusRecoveryRequired          ApplyStatusV1 = "recovery-required"
)

type DeterministicErrorCodeV1 string

const (
	ErrorNoneV1                 DeterministicErrorCodeV1 = ""
	ErrorUnsupportedCommandV1   DeterministicErrorCodeV1 = "unsupported-command"
	ErrorMalformedEntryV1       DeterministicErrorCodeV1 = "malformed-entry"
	ErrorUnsupportedVersionV1   DeterministicErrorCodeV1 = "unsupported-version"
	ErrorUnsupportedFeatureV1   DeterministicErrorCodeV1 = "unsupported-feature"
	ErrorMissingGuardV1         DeterministicErrorCodeV1 = "missing-guard"
	ErrorTargetMismatchV1       DeterministicErrorCodeV1 = "target-mismatch"
	ErrorRejectedConflictV1     DeterministicErrorCodeV1 = "rejected-conflict"
	ErrorReadOnlyV1             DeterministicErrorCodeV1 = "read-only"
	ErrorUnsafeDurabilityModeV1 DeterministicErrorCodeV1 = "unsafe-durability-mode"
	ErrorResourceExhaustedV1    DeterministicErrorCodeV1 = "resource-exhausted"
	ErrorNoIdempotencyV1        DeterministicErrorCodeV1 = "no-idempotency"
	ErrorResultReplayRequiredV1 DeterministicErrorCodeV1 = "result-replay-required"
	ErrorUnknownRequiredFieldV1 DeterministicErrorCodeV1 = "unknown-required-field"
	ErrorUnsupportedScopeRuleV1 DeterministicErrorCodeV1 = "unsupported-scope-rule"
)

type ApplyResultV1 struct {
	Status                 ApplyStatusV1
	CommandDigest          CommandDigestV1
	DeterministicErrorCode DeterministicErrorCodeV1
	AffectedCount          int64
	MatchedCount           int64
	ResultDigest           CommandDigestV1
}

type TargetIdentityV1 struct {
	ScopeRule              ScopeRuleV1
	DatabaseScope          string
	CatalogScope           string
	CommandID              nativewire.CommandID
	CommandVersion         uint64
	CollectionRef          []byte
	CollectionMeta         []byte
	ExpectedCatalogVersion []byte
}

func (t TargetIdentityV1) Equal(u TargetIdentityV1) bool {
	return t.ScopeRule == u.ScopeRule &&
		t.DatabaseScope == u.DatabaseScope &&
		t.CatalogScope == u.CatalogScope &&
		t.CommandID == u.CommandID &&
		t.CommandVersion == u.CommandVersion &&
		bytes.Equal(t.CollectionRef, u.CollectionRef) &&
		bytes.Equal(t.CollectionMeta, u.CollectionMeta) &&
		bytes.Equal(t.ExpectedCatalogVersion, u.ExpectedCatalogVersion)
}

func (t TargetIdentityV1) Clone() TargetIdentityV1 {
	t.CollectionRef = bytes.Clone(t.CollectionRef)
	t.CollectionMeta = bytes.Clone(t.CollectionMeta)
	t.ExpectedCatalogVersion = bytes.Clone(t.ExpectedCatalogVersion)
	return t
}

type DecodeOptions struct {
	Limits          nativewire.Limits
	ScopeRule       ScopeRuleV1
	DatabaseScope   string
	CatalogScope    string
	ApplyEntryID    ApplyEntryID
	RequestMetadata RequestMetadataV1
	ExpectedTarget  *TargetIdentityV1
}

type CommandEntryV1 struct {
	Bytes          []byte
	Decoded        nativewire.DeterministicEntry
	Digest         CommandDigestV1
	Target         TargetIdentityV1
	IdempotencyKey []byte
	Idempotency    IdempotencyModeV1
	Row            CommandRowV1
}

type ValidationError struct {
	Code DeterministicErrorCodeV1
	Err  error
}

func (e *ValidationError) Error() string {
	if e == nil {
		return "raftentry: <nil>"
	}
	if e.Err == nil {
		return fmt.Sprintf("raftentry: %s", e.Code)
	}
	return fmt.Sprintf("raftentry: %s: %v", e.Code, e.Err)
}

func (e *ValidationError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

func ErrorCodeOf(err error) (DeterministicErrorCodeV1, bool) {
	var e *ValidationError
	if errors.As(err, &e) {
		return e.Code, true
	}
	return "", false
}

func DecodeCommandEntryV1(src []byte, opts DecodeOptions) (CommandEntryV1, error) {
	scope, database, catalog, err := normalizeScope(opts)
	if err != nil {
		return CommandEntryV1{}, err
	}
	if max := commandEntryMaxFrameSize(opts.Limits); uint64(len(src)) > max {
		return CommandEntryV1{}, validationError(ErrorResourceExhaustedV1, fmt.Errorf("deterministic entry length %d exceeds limit %d", len(src), max))
	}
	entryBytes := bytes.Clone(src)
	decoded, err := nativewire.DecodeDeterministicEntry(entryBytes, opts.Limits)
	if err != nil {
		return CommandEntryV1{}, validationError(mapNativeWireError(entryBytes, err), err)
	}
	if decoded.Version != EntryVersionV1 {
		return CommandEntryV1{}, validationError(ErrorUnsupportedVersionV1, fmt.Errorf("entry version %d", decoded.Version))
	}
	row := ClassifyNativeWireCommandV1(decoded.CommandID)
	if !row.Known {
		return CommandEntryV1{}, validationError(ErrorUnsupportedCommandV1, fmt.Errorf("native-wire command %d", decoded.CommandID))
	}
	if row.Decision != DecisionAccepted {
		return CommandEntryV1{}, validationError(rowRejectionCodeV1(row), fmt.Errorf("%s is %s for R3a v1: %s", row.NativeWireCommand, row.Decision, row.Reason))
	}
	idempotencyKey, ok := sectionBytes(decoded.Sections, nativewire.SectionIdempotencyKey)
	if !ok || len(idempotencyKey) == 0 {
		return CommandEntryV1{}, validationError(ErrorNoIdempotencyV1, fmt.Errorf("%s missing idempotency key", row.NativeWireCommand))
	}
	if len(idempotencyKey) > MaxIdempotencyKeyBytesV1 {
		return CommandEntryV1{}, validationError(ErrorResourceExhaustedV1, fmt.Errorf("idempotency key length %d exceeds %d", len(idempotencyKey), MaxIdempotencyKeyBytesV1))
	}
	if string(idempotencyKey) == NoIdempotencyTokenV1 {
		return CommandEntryV1{}, validationError(ErrorNoIdempotencyV1, fmt.Errorf("%s is not accepted for replicated commands", NoIdempotencyTokenV1))
	}
	target := targetIdentity(decoded, scope, database, catalog)
	if opts.ExpectedTarget != nil && !target.Equal(*opts.ExpectedTarget) {
		return CommandEntryV1{}, validationError(ErrorTargetMismatchV1, fmt.Errorf("target identity mismatch"))
	}
	return CommandEntryV1{
		Bytes:          entryBytes,
		Decoded:        decoded,
		Digest:         CommandDigestV1ForBytes(entryBytes, opts),
		Target:         target,
		IdempotencyKey: bytes.Clone(idempotencyKey),
		Idempotency:    IdempotencyRequiredV1,
		Row:            row,
	}, nil
}

func CommandDigestV1ForBytes(src []byte, opts DecodeOptions) CommandDigestV1 {
	scope := opts.ScopeRule
	if scope == "" {
		scope = ScopeRuleSingleGroupV1
	}
	database := opts.DatabaseScope
	if database == "" {
		database = DatabaseScopeDefaultV1
	}
	catalog := opts.CatalogScope
	if catalog == "" {
		catalog = CatalogScopeDefaultV1
	}
	h := sha256.New()
	writeDigestField(h, "domain", []byte(digestDomainV1))
	writeDigestU64(h, "entry-version", EntryVersionV1)
	writeDigestField(h, "scope-rule", []byte(scope))
	writeDigestField(h, "database-scope", []byte(database))
	writeDigestField(h, "catalog-scope", []byte(catalog))
	writeDigestField(h, "nativewire-deterministic-entry", src)
	var out CommandDigestV1
	copy(out[:], h.Sum(nil))
	return out
}

func commandEntryMaxFrameSize(limits nativewire.Limits) uint64 {
	if limits.MaxFrameSize != 0 {
		return limits.MaxFrameSize
	}
	return nativewire.DefaultLimits().MaxFrameSize
}

func ValidateCommandDigestInputV1(src []byte, opts DecodeOptions) (CommandDigestV1, error) {
	if _, _, _, err := normalizeScope(opts); err != nil {
		return CommandDigestV1{}, err
	}
	if _, err := nativewire.DecodeDeterministicEntry(src, opts.Limits); err != nil {
		return CommandDigestV1{}, validationError(mapNativeWireError(src, err), err)
	}
	return CommandDigestV1ForBytes(src, opts), nil
}

func normalizeScope(opts DecodeOptions) (ScopeRuleV1, string, string, error) {
	scope := opts.ScopeRule
	if scope == "" {
		scope = ScopeRuleSingleGroupV1
	}
	if scope != ScopeRuleSingleGroupV1 {
		return "", "", "", validationError(ErrorUnsupportedScopeRuleV1, fmt.Errorf("scope rule %q", scope))
	}
	database := opts.DatabaseScope
	if database == "" {
		database = DatabaseScopeDefaultV1
	}
	catalog := opts.CatalogScope
	if catalog == "" {
		catalog = CatalogScopeDefaultV1
	}
	return scope, database, catalog, nil
}

func targetIdentity(entry nativewire.DeterministicEntry, scope ScopeRuleV1, database, catalog string) TargetIdentityV1 {
	return TargetIdentityV1{
		ScopeRule:              scope,
		DatabaseScope:          database,
		CatalogScope:           catalog,
		CommandID:              entry.CommandID,
		CommandVersion:         entry.CommandVersion,
		CollectionRef:          copySection(entry.Sections, nativewire.SectionCollectionRef),
		CollectionMeta:         copySection(entry.Sections, nativewire.SectionCollectionMeta),
		ExpectedCatalogVersion: copySection(entry.Sections, nativewire.SectionExpectedCatalogVersion),
	}
}

func sectionBytes(sections []nativewire.Section, id nativewire.SectionID) ([]byte, bool) {
	for _, section := range sections {
		if section.ID == id {
			return section.Bytes, true
		}
	}
	return nil, false
}

func copySection(sections []nativewire.Section, id nativewire.SectionID) []byte {
	raw, ok := sectionBytes(sections, id)
	if !ok {
		return nil
	}
	return bytes.Clone(raw)
}

func validationError(code DeterministicErrorCodeV1, err error) error {
	return &ValidationError{Code: code, Err: err}
}

func rowRejectionCodeV1(row CommandRowV1) DeterministicErrorCodeV1 {
	switch row.CommandWALStatus {
	case "read-only":
		return ErrorReadOnlyV1
	default:
		return ErrorUnsupportedCommandV1
	}
}

func mapNativeWireError(src []byte, err error) DeterministicErrorCodeV1 {
	code, ok := nativewire.ErrorCodeOf(err)
	if !ok {
		return ErrorMalformedEntryV1
	}
	switch code {
	case nativewire.ErrMalformedFrame:
		return ErrorMalformedEntryV1
	case nativewire.ErrUnsupportedVersion:
		if header, ok := deterministicEntryHeader(src); ok {
			if header.CommandVersion != 1 {
				return ErrorUnsupportedVersionV1
			}
			row := ClassifyNativeWireCommandV1(header.CommandID)
			if row.Known && row.CommandWALStatus == "read-only" {
				return ErrorReadOnlyV1
			}
			if !knownNativeWireCommandIDV1(header.CommandID) {
				return ErrorUnsupportedCommandV1
			}
		}
		return ErrorUnsupportedVersionV1
	case nativewire.ErrUnsupportedFeature:
		return ErrorUnsupportedFeatureV1
	case nativewire.ErrInvalidCommand:
		reason := nativeWireErrorReason(err)
		switch {
		case strings.Contains(reason, "missing idempotency key"):
			return ErrorNoIdempotencyV1
		case strings.Contains(reason, "idempotency_key cannot be empty"):
			return ErrorNoIdempotencyV1
		case strings.Contains(reason, "missing catalog guard"):
			return ErrorMissingGuardV1
		case strings.Contains(reason, " is not replicated"):
			if header, ok := deterministicEntryHeader(src); ok {
				return rowRejectionCodeV1(ClassifyNativeWireCommandV1(header.CommandID))
			}
			return ErrorUnsupportedCommandV1
		case strings.Contains(reason, "missing command schema"):
			return ErrorUnsupportedCommandV1
		}
		return ErrorMalformedEntryV1
	case nativewire.ErrResourceExhausted:
		return ErrorResourceExhaustedV1
	case nativewire.ErrReadOnly:
		return ErrorReadOnlyV1
	case nativewire.ErrDurabilityUnavailable:
		return ErrorUnsafeDurabilityModeV1
	default:
		return ErrorMalformedEntryV1
	}
}

func nativeWireErrorReason(err error) string {
	var e *nativewire.ProtocolError
	if errors.As(err, &e) && e != nil {
		return e.Reason
	}
	return ""
}

type deterministicEntryHeaderV1 struct {
	CommandID      nativewire.CommandID
	CommandVersion uint64
}

func deterministicEntryHeader(src []byte) (deterministicEntryHeaderV1, bool) {
	if len(src) < len(nativewire.DeterministicEntryMagic) || string(src[:len(nativewire.DeterministicEntryMagic)]) != nativewire.DeterministicEntryMagic {
		return deterministicEntryHeaderV1{}, false
	}
	off := len(nativewire.DeterministicEntryMagic)
	entryVersion, n := binary.Uvarint(src[off:])
	if n <= 0 {
		return deterministicEntryHeaderV1{}, false
	}
	if entryVersion != nativewire.DeterministicEntryVersion {
		return deterministicEntryHeaderV1{}, false
	}
	off += n
	commandID, n := binary.Uvarint(src[off:])
	if n <= 0 {
		return deterministicEntryHeaderV1{}, false
	}
	off += n
	commandVersion, n := binary.Uvarint(src[off:])
	if n <= 0 {
		return deterministicEntryHeaderV1{}, false
	}
	return deterministicEntryHeaderV1{
		CommandID:      nativewire.CommandID(commandID),
		CommandVersion: commandVersion,
	}, true
}

func knownNativeWireCommandIDV1(id nativewire.CommandID) bool {
	for _, schema := range nativewire.MustV1Registry().Schemas() {
		if schema.ID == id {
			return true
		}
	}
	return false
}

type digestWriter interface {
	Write([]byte) (int, error)
}

func writeDigestField(w digestWriter, name string, value []byte) {
	writeDigestU64(w, "field-name-len", uint64(len(name)))
	_, _ = w.Write([]byte(name))
	writeDigestU64(w, "field-value-len", uint64(len(value)))
	_, _ = w.Write(value)
}

func writeDigestU64(w digestWriter, name string, value uint64) {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], value)
	if name != "" {
		_, _ = w.Write([]byte(name))
	}
	_, _ = w.Write(buf[:])
}
