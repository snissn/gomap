package templatedb

import (
	"context"
	"crypto/sha256"
	"fmt"
	"math"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/template"
)

var (
	templateIndexPhysicalDigest    = sha256.Sum256([]byte("templatedb-index-v1"))
	templateValueLogPhysicalDigest = sha256.Sum256([]byte("templatedb-value-log-v1"))
)

type templateStablePhysicalRole string

const (
	templateStableIndexRole    templateStablePhysicalRole = "index"
	templateStableValueLogRole templateStablePhysicalRole = "value-log"
)

var addTemplateStableResourceToken = func(builder *rootpublication.StableResourceSetBuilder, token *rootpublication.StableResourceToken, _ templateStablePhysicalRole) error {
	return builder.Add(token)
}

// StablePhysicalEntry is the storage placement of one template definition in
// a stable snapshot. RecordLength is the exact encoded value-log record size.
type StablePhysicalEntry struct {
	Pointer      bool
	FileID       uint32
	Offset       uint64
	RecordLength uint64
}

// StablePhysicalSnapshot is the narrow physical-authority view required by a
// template store. NewStableIndexResourceToken transfers snapshot ownership to
// the returned token on success; otherwise the caller retains Close ownership.
type StablePhysicalSnapshot interface {
	Get([]byte) ([]byte, error)
	GetEntry([]byte) (StablePhysicalEntry, error)
	ValueLogDiagnosticPath(uint32) (string, error)
	NewStableIndexResourceToken(rootpublication.StableResourceSpec) (*rootpublication.StableResourceToken, error)
	NewStableValueLogResourceToken(uint32, rootpublication.StableResourceSpec) (*rootpublication.StableResourceToken, error)
	ReleaseCaptureLease()
	Close() error
}

// StablePhysicalCapturer is an optional KV capability. Lookup bytes alone are
// insufficient authority because they do not pin the exact index generation
// or a pointer-backed definition's value-log segment against replacement.
type StablePhysicalCapturer interface {
	AcquireStableTemplateSnapshot() (StablePhysicalSnapshot, error)
}

// CaptureTemplateResources returns the exact durable resources required to
// decode templateID. The returned set owns the stable snapshot and deletion
// pins until Release.
func (s *Store) CaptureTemplateResources(ctx context.Context, templateID uint64) (*rootpublication.StableResourceSet, error) {
	if s == nil || s.kv == nil {
		return nil, errStoreUnavailable
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if templateID == 0 {
		return nil, fmt.Errorf("templatedb: invalid template id 0")
	}
	provider, ok := s.kv.(StablePhysicalCapturer)
	if !ok {
		return nil, fmt.Errorf("%w: templatedb has no stable physical capturer", rootpublication.ErrUnresolvedResource)
	}

	// Serialize the stable read with Store-owned publication. Definitions are
	// immutable after publication, but this also prevents capture from observing
	// a partially completed id/routing update through a custom KV adapter.
	s.mu.Lock()
	defer s.mu.Unlock()

	snapshot, err := provider.AcquireStableTemplateSnapshot()
	if err != nil {
		return nil, fmt.Errorf("templatedb: establish stable template snapshot: %w", err)
	}
	if snapshot == nil {
		return nil, fmt.Errorf("%w: templatedb stable snapshot unavailable", rootpublication.ErrUnresolvedResource)
	}
	defer snapshot.ReleaseCaptureLease()
	snapshotOwned := false
	defer func() {
		if !snapshotOwned {
			_ = snapshot.Close()
		}
	}()

	key := templateKey(templateID)
	definition, err := snapshot.Get(key)
	if err != nil {
		return nil, fmt.Errorf("templatedb: read template %d: %w", templateID, err)
	}
	if definition == nil {
		return nil, fmt.Errorf("templatedb: template %d: %w", templateID, template.ErrMissingTemplate)
	}
	if len(definition) == 0 {
		return nil, fmt.Errorf("%w: templatedb template %d has empty definition", rootpublication.ErrResourceConflict, templateID)
	}
	if !templateIDMatchesDefinition(templateID, definition, NormalizeConfig(s.cfg).MaxIDAttempts) {
		return nil, fmt.Errorf("%w: templatedb template %d content id mismatch", rootpublication.ErrResourceConflict, templateID)
	}
	entry, err := snapshot.GetEntry(key)
	if err != nil {
		return nil, fmt.Errorf("templatedb: read template %d entry: %w", templateID, err)
	}

	definitionDigest := sha256.Sum256(definition)
	logical := rootpublication.StableLogicalObligation{
		Class: "template-generation", Kind: "template", Namespace: "templatedb",
		Generation: templateID, FileID: templateID, Offset: 0, Length: int64(len(definition)),
		Reachability: rootpublication.ReachabilityTemplateGeneration,
		Digest:       definitionDigest,
	}
	builder := rootpublication.NewStableResourceSetBuilder(rootpublication.ReachabilityTemplateGeneration)
	defer builder.Abandon()

	indexToken, err := snapshot.NewStableIndexResourceToken(rootpublication.StableResourceSpec{
		Kind:         rootpublication.ResourceTemplate,
		LogicalLane:  "templatedb/index",
		ResourceID:   "index",
		Digest:       templateIndexPhysicalDigest,
		Reachability: rootpublication.ReachabilityTemplateGeneration,
		LogicalObligations: []rootpublication.StableLogicalObligation{
			logical,
		},
		ContentSynced: true,
	})
	if err != nil {
		return nil, fmt.Errorf("templatedb: capture template %d index: %w", templateID, err)
	}
	// A successful index token owns the snapshot maintenance lease even if a
	// later builder/value-log operation fails.
	snapshotOwned = true
	if err := addTemplateStableResourceToken(builder, indexToken, templateStableIndexRole); err != nil {
		indexToken.Release()
		return nil, fmt.Errorf("templatedb: add template %d index: %w", templateID, err)
	}

	if entry.Pointer {
		if entry.RecordLength == 0 || entry.Offset > math.MaxUint64-entry.RecordLength {
			return nil, fmt.Errorf("%w: templatedb template %d has invalid value-log frontier", rootpublication.ErrResourceConflict, templateID)
		}
		diagnosticPath, err := snapshot.ValueLogDiagnosticPath(entry.FileID)
		if err != nil {
			return nil, fmt.Errorf("templatedb: resolve template %d value-log path: %w", templateID, err)
		}
		cleanDiagnosticPath := filepath.Clean(diagnosticPath)
		if diagnosticPath == "" || cleanDiagnosticPath == "." || filepath.IsAbs(diagnosticPath) ||
			cleanDiagnosticPath == ".." || strings.HasPrefix(cleanDiagnosticPath, ".."+string(filepath.Separator)) {
			return nil, fmt.Errorf("%w: templatedb template %d has invalid value-log path", rootpublication.ErrUnresolvedResource, templateID)
		}
		valueLogToken, err := snapshot.NewStableValueLogResourceToken(entry.FileID, rootpublication.StableResourceSpec{
			Kind:           rootpublication.ResourceTemplate,
			LogicalLane:    "templatedb/value-log",
			ResourceID:     "value-log/" + strconv.FormatUint(uint64(entry.FileID), 10),
			Generation:     uint64(entry.FileID),
			DiagnosticPath: filepath.ToSlash(diagnosticPath),
			Frontier:       rootpublication.DurableFrontier{Bytes: entry.Offset + entry.RecordLength},
			Digest:         templateValueLogPhysicalDigest,
			Reachability:   rootpublication.ReachabilityTemplateGeneration,
			LogicalObligations: []rootpublication.StableLogicalObligation{
				logical,
			},
			ContentSynced: true,
		})
		if err != nil {
			return nil, fmt.Errorf("templatedb: capture template %d value-log: %w", templateID, err)
		}
		if err := addTemplateStableResourceToken(builder, valueLogToken, templateStableValueLogRole); err != nil {
			valueLogToken.Release()
			return nil, fmt.Errorf("templatedb: add template %d value-log: %w", templateID, err)
		}
	}

	resources, err := builder.Freeze()
	if err != nil {
		return nil, fmt.Errorf("templatedb: freeze template %d resources: %w", templateID, err)
	}
	if err := validateCapturedTemplatePhysicalClosure(resources, entry.Pointer); err != nil {
		resources.Release()
		return nil, fmt.Errorf("templatedb: validate template %d physical closure: %w", templateID, err)
	}
	return resources, nil
}

func validateCapturedTemplatePhysicalClosure(resources *rootpublication.StableResourceSet, pointer bool) error {
	if resources == nil {
		return fmt.Errorf("%w: template capture returned no physical closure", rootpublication.ErrUnresolvedResource)
	}
	var index, valueLog bool
	for _, descriptor := range resources.Descriptors() {
		if descriptor.Kind() != rootpublication.ResourceTemplate {
			return fmt.Errorf("%w: template closure contains kind %q", rootpublication.ErrResourceConflict, descriptor.Kind())
		}
		switch descriptor.Digest() {
		case templateIndexPhysicalDigest:
			if index {
				return fmt.Errorf("%w: template closure contains duplicate index authority", rootpublication.ErrResourceConflict)
			}
			index = true
		case templateValueLogPhysicalDigest:
			if valueLog {
				return fmt.Errorf("%w: template closure contains duplicate value-log authority", rootpublication.ErrResourceConflict)
			}
			valueLog = true
		default:
			return fmt.Errorf("%w: template closure contains unknown physical role", rootpublication.ErrResourceConflict)
		}
	}
	if !index {
		return fmt.Errorf("%w: template closure omitted index authority", rootpublication.ErrUnresolvedResource)
	}
	if pointer && !valueLog {
		return fmt.Errorf("%w: pointer template closure omitted value-log authority", rootpublication.ErrUnresolvedResource)
	}
	if !pointer && valueLog {
		return fmt.Errorf("%w: inline template closure contains value-log authority", rootpublication.ErrResourceConflict)
	}
	return nil
}

func templateIDMatchesDefinition(templateID uint64, definition []byte, maxAttempts int) bool {
	for attempt := 0; attempt < maxAttempts; attempt++ {
		if candidate := template.TemplateID(definition, byte(attempt)); candidate != 0 && candidate == templateID {
			return true
		}
	}
	return false
}
