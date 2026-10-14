package db

import (
	"errors"
	"fmt"
	"sort"

	"github.com/snissn/gomap/TreeDB/freelist"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

// NoRecoverableMetaError retains one stable rejection reason for each physical
// meta slot. Reasons are always rendered in slot order even though candidates
// are attempted in descending commit order.
type NoRecoverableMetaError struct {
	SlotReasons [2]error
}

func (err *NoRecoverableMetaError) Error() string {
	if err == nil {
		return ErrNoRecoverableMeta.Error()
	}
	return fmt.Sprintf("%s: slot 0: %s; slot 1: %s", ErrNoRecoverableMeta, durableSlotReasonV1(err.SlotReasons[0]), durableSlotReasonV1(err.SlotReasons[1]))
}

func (err *NoRecoverableMetaError) Unwrap() error { return ErrNoRecoverableMeta }

func durableSlotReasonV1(err error) string {
	if err == nil {
		return "not selected"
	}
	return err.Error()
}

type durableRootSelectionV1 struct {
	Slot     uint64
	Meta     page.DurableMetaV1
	Record   rootpublication.DurableRootRecordV1
	Freelist *freelist.FreelistGenerationV1
	Manifest *rootpublication.DependencyManifestV1
	// SlotCommits contains only independently complete recovery generations.
	SlotCommits [2]uint64
	// SlotResources retains the exact external-resource closure for every
	// independently complete slot, including the older fallback generation.
	SlotResources [2]*rootpublication.StableResourceSet
	// SlotMetas and SlotRecords retain the bounded auxiliary-page inventory for
	// each complete slot. Publication uses the overwritten slot's inventory to
	// retire its manifest and root-record pages without scanning history.
	SlotMetas   [2]page.DurableMetaV1
	SlotRecords [2]rootpublication.DurableRootRecordV1
	resources   *rootpublication.StableResourceSet
	Primary     rootpublication.PrimaryProjectionV5
	SlotPrimary [2]rootpublication.PrimaryProjectionV5
	// Parent records are fixed one-hop frontier evidence, never lookup ancestry.
	ParentRecords         [2]rootpublication.DurablePrimaryRootRecordV5
	primaryPhysicalDigest [2][32]byte
	parentPhysicalDigest  [2][32]byte
	ParentResources       [2]*rootpublication.StableResourceSet
}

type durableManifestValidatorV1 func(*rootpublication.DependencyManifestV1) (*rootpublication.StableResourceSet, error)

type durableDirectoryValidatorV2 func(rootpublication.DurableRootRecordV1) (*rootpublication.StableResourceSet, error)

type durableMetaCandidateV1 struct {
	slot uint64
	meta page.DurableMetaV1
}

// selectDurableRootV1 performs bounded recovery selection. It deliberately
// does not recurse through the B-tree or scan value-log contents: checksummed
// COW pages and the deterministic manifest are the recovery inventory.
func selectDurableRootV1(source freelist.PageSource, physicalPageCount uint64, validateManifest durableManifestValidatorV1, directoryValidators ...durableDirectoryValidatorV2) (durableRootSelectionV1, error) {
	return selectDurableRootCandidates(source, func(candidate durableMetaCandidateV1) (durableRootSelectionV1, error) {
		return validateDurableMetaCandidateV1(source, physicalPageCount, candidate, validateManifest, directoryValidators...)
	})
}

// Both format validators use the same independent two-slot selection policy.
// Each validator retains its own exact format and physical ownership rules.
func selectDurableRootCandidates(source freelist.PageSource, validate func(durableMetaCandidateV1) (durableRootSelectionV1, error)) (durableRootSelectionV1, error) {
	if source == nil {
		return durableRootSelectionV1{}, &NoRecoverableMetaError{SlotReasons: [2]error{errors.New("meta page source unavailable"), errors.New("meta page source unavailable")}}
	}
	var reasons [2]error
	candidates := make([]durableMetaCandidateV1, 0, 2)
	for slot := uint64(0); slot < 2; slot++ {
		meta, err := readDurableMetaSlotV1(source, slot)
		if err != nil {
			reasons[slot] = err
			continue
		}
		candidates = append(candidates, durableMetaCandidateV1{slot: slot, meta: meta})
	}
	return selectDurableRootCandidateSetV1(candidates, reasons, validate)
}

// Both authorities use the same independent two-slot resource installation.
func selectDurableRootCandidateSetV1(candidates []durableMetaCandidateV1, reasons [2]error, validate func(durableMetaCandidateV1) (durableRootSelectionV1, error)) (durableRootSelectionV1, error) {
	sort.SliceStable(candidates, func(i, j int) bool {
		if candidates[i].meta.CommitSeq != candidates[j].meta.CommitSeq {
			return candidates[i].meta.CommitSeq > candidates[j].meta.CommitSeq
		}
		return candidates[i].slot < candidates[j].slot
	})
	conflictingGenerations := make(map[uint64]bool, len(candidates))
	for i := range candidates {
		for j := i + 1; j < len(candidates); j++ {
			if candidates[i].meta.CommitSeq == candidates[j].meta.CommitSeq && candidates[i].meta != candidates[j].meta {
				conflictingGenerations[candidates[i].meta.CommitSeq] = true
			}
		}
	}
	var chosen *durableRootSelectionV1
	var slotResources, parentResources [2]*rootpublication.StableResourceSet
	var slotMetas [2]page.DurableMetaV1
	var slotRecords [2]rootpublication.DurableRootRecordV1
	var slotPrimary [2]rootpublication.PrimaryProjectionV5
	var parentRecords [2]rootpublication.DurablePrimaryRootRecordV5
	var primaryPhysicalDigest, parentPhysicalDigest [2][32]byte
	seenGenerations := make(map[uint64]uint64, len(candidates))
	for _, candidate := range candidates {
		if conflictingGenerations[candidate.meta.CommitSeq] {
			reasons[candidate.slot] = fmt.Errorf("conflicting recovery generation: commit %d appears with different roots", candidate.meta.CommitSeq)
			continue
		}
		selected, err := validate(candidate)
		if err == nil {
			if priorSlot, duplicate := seenGenerations[selected.Meta.CommitSeq]; duplicate {
				selected.resources.Release()
				selected.ParentResources[candidate.slot].Release()
				reasons[candidate.slot] = fmt.Errorf("duplicate recovery generation: commit %d already selected from slot %d", selected.Meta.CommitSeq, priorSlot)
				continue
			}
			seenGenerations[selected.Meta.CommitSeq] = candidate.slot
			slotResources[candidate.slot] = selected.resources
			parentResources[candidate.slot] = selected.ParentResources[candidate.slot]
			slotMetas[candidate.slot] = selected.Meta
			slotRecords[candidate.slot] = selected.Record
			slotPrimary[candidate.slot] = selected.Primary
			parentRecords[candidate.slot] = selected.ParentRecords[candidate.slot]
			primaryPhysicalDigest[candidate.slot] = selected.primaryPhysicalDigest[candidate.slot]
			parentPhysicalDigest[candidate.slot] = selected.parentPhysicalDigest[candidate.slot]
			selected.resources = nil
			if chosen == nil {
				copy := selected
				chosen = &copy
			}
			if chosen != nil {
				chosen.SlotCommits[candidate.slot] = selected.Meta.CommitSeq
			}
			continue
		}
		reasons[candidate.slot] = err
	}
	if chosen != nil {
		chosen.SlotResources = slotResources
		chosen.ParentResources = parentResources
		chosen.SlotMetas = slotMetas
		chosen.SlotRecords = slotRecords
		chosen.SlotPrimary = slotPrimary
		chosen.ParentRecords = parentRecords
		chosen.primaryPhysicalDigest = primaryPhysicalDigest
		chosen.parentPhysicalDigest = parentPhysicalDigest
		return *chosen, nil
	}
	for _, resources := range slotResources {
		resources.Release()
	}
	for _, resources := range parentResources {
		resources.Release()
	}
	detail := &NoRecoverableMetaError{SlotReasons: reasons}
	legacy := true
	for _, reason := range reasons {
		if !errors.Is(reason, page.ErrDurableMetaLegacyFormat) {
			legacy = false
			break
		}
	}
	if legacy {
		return durableRootSelectionV1{}, errors.Join(detail, ErrLegacyFormatRebuildRequired)
	}
	return durableRootSelectionV1{}, detail
}

func readDurableMetaSlotV1(source freelist.PageSource, slot uint64) (page.DurableMetaV1, error) {
	image, err := source.ReadPage(slot)
	if err != nil {
		return page.DurableMetaV1{}, fmt.Errorf("meta page: %w", err)
	}
	if len(image) != page.PageSize {
		return page.DurableMetaV1{}, fmt.Errorf("meta page: invalid size %d", len(image))
	}
	if !page.VerifyChecksumNonMutating(image) {
		return page.DurableMetaV1{}, errors.New("meta page: checksum mismatch")
	}
	header := page.DecodeHeader(image)
	if header.PageID != slot || page.PageType(header.Flags) != page.PageTypeMeta || header.Count != 0 {
		return page.DurableMetaV1{}, errors.New("meta page: invalid header")
	}
	meta, err := page.DecodeDurableMetaV1(image[page.PageHeaderSize:])
	if err != nil {
		return page.DurableMetaV1{}, fmt.Errorf("meta page: %w", err)
	}
	return meta, nil
}

func validateDurableMetaCandidateV1(source freelist.PageSource, physicalPageCount uint64, candidate durableMetaCandidateV1, validateManifest durableManifestValidatorV1, directoryValidators ...durableDirectoryValidatorV2) (durableRootSelectionV1, error) {
	meta := candidate.meta
	recordImage, err := source.ReadPage(meta.RootRecordPageID)
	if err != nil {
		return durableRootSelectionV1{}, fmt.Errorf("root record: %w", err)
	}
	record, err := rootpublication.DecodeDurableRootRecordV1(recordImage, meta.RootRecordPageID, meta.RootRecordDigest)
	if err != nil {
		return durableRootSelectionV1{}, fmt.Errorf("root record: %w", err)
	}
	if record.CommitSeq != meta.CommitSeq || record.DurableSeq != meta.DurableSeq || record.MetaProjectionDigest != meta.MetaProjectionDigest {
		return durableRootSelectionV1{}, errors.New("root record: meta projection mismatch")
	}
	if record.TotalPages > physicalPageCount {
		return durableRootSelectionV1{}, fmt.Errorf("root record: total pages %d exceeds physical pages %d", record.TotalPages, physicalPageCount)
	}
	if err := validateDurableRootLineageV1(source, record); err != nil {
		return durableRootSelectionV1{}, fmt.Errorf("root record lineage: %w", err)
	}
	generation, err := freelist.LoadGenerationV1(source, record.Freelist)
	if err != nil {
		return durableRootSelectionV1{}, fmt.Errorf("COW freelist: %w", err)
	}
	if generation.FreeCount() != record.FreelistFreeCount || generation.RetiredCount() != record.FreelistRetiredCount {
		return durableRootSelectionV1{}, errors.New("COW freelist: count mismatch")
	}
	var manifest *rootpublication.DependencyManifestV1
	var resources *rootpublication.StableResourceSet
	if record.Directory.RootPageID != 0 {
		if record.Directory.RootPageID >= record.TotalPages || len(directoryValidators) != 1 || directoryValidators[0] == nil {
			return durableRootSelectionV1{}, errors.New("dependency directory: validator or root extent unavailable")
		}
		resources, err = directoryValidators[0](record)
	} else {
		manifest, err = rootpublication.LoadDependencyManifestV1(source, record.Manifest)
		if err == nil && validateManifest != nil {
			resources, err = validateManifest(manifest)
		}
	}
	if err != nil {
		resources.Release()
		return durableRootSelectionV1{}, fmt.Errorf("dependency metadata: %w", err)
	}
	accepted := false
	defer func() {
		if !accepted {
			resources.Release()
		}
	}()
	if err := validateDurableRootPageV1(source, record.UserRootPageID, record.TotalPages); err != nil {
		return durableRootSelectionV1{}, fmt.Errorf("user root page: %w", err)
	}
	if err := validateDurableRootPageV1(source, record.SystemRootPageID, record.TotalPages); err != nil {
		return durableRootSelectionV1{}, fmt.Errorf("system root page: %w", err)
	}
	accepted = true
	return durableRootSelectionV1{Slot: candidate.slot, Meta: meta, Record: record, Freelist: generation, Manifest: manifest, resources: resources}, nil
}

// validateDurableRootLineageV1 performs the fixed-depth lineage check promised
// by the durable-root format. A lineage anchor has no parent. Every ordinary
// successor binds the immediately preceding record by page, commit sequence,
// and digest; decoding that parent also lets recovery prove that the selected
// applied command-WAL frontier did not regress. Publication proves contiguous
// coverage before writing the child record, while this bounded read proves the
// persisted frontier belongs to the exact parent/child root lineage.
func validateDurableRootLineageV1(source freelist.PageSource, record rootpublication.DurableRootRecordV1) error {
	if record.ParentRecordPageID == 0 {
		return nil
	}
	parentImage, err := source.ReadPage(record.ParentRecordPageID)
	if err != nil {
		return fmt.Errorf("parent record: %w", err)
	}
	parent, err := rootpublication.DecodeDurableRootRecordV1(parentImage, record.ParentRecordPageID, record.ParentRecordDigest)
	if err != nil {
		return fmt.Errorf("parent record: %w", err)
	}
	if parent.CommitSeq != record.ParentCommitSeq {
		return fmt.Errorf("parent commit sequence mismatch: record=%d parent=%d", record.ParentCommitSeq, parent.CommitSeq)
	}
	if parent.DurableSeq == ^uint64(0) || parent.DurableSeq+1 != record.DurableSeq {
		return fmt.Errorf("non-contiguous durable publication sequence: parent=%d child=%d", parent.DurableSeq, record.DurableSeq)
	}
	if record.AppliedCommandLSN < parent.AppliedCommandLSN {
		return fmt.Errorf("applied command-WAL frontier regressed: parent=%d child=%d", parent.AppliedCommandLSN, record.AppliedCommandLSN)
	}
	return nil
}

func validateDurableRootPageV1(source freelist.PageSource, pageID, totalPages uint64) error {
	if pageID < 2 || pageID >= totalPages {
		return errors.New("page identity outside durable extent")
	}
	image, err := source.ReadPage(pageID)
	if err != nil {
		return err
	}
	if len(image) != page.PageSize || !page.VerifyChecksumNonMutating(image) {
		return errors.New("checksum or size mismatch")
	}
	header := page.DecodeHeader(image)
	if header.PageID != pageID {
		return errors.New("page identity mismatch")
	}
	switch node.NewNode(image).Type() {
	case page.PageTypeLeaf, page.PageTypeInternal:
		return nil
	case page.PageTypePrimaryDirectory:
		directory, err := node.DecodePrimaryDirectory(image)
		if err != nil {
			return err
		}
		base, _ := directory.Base()
		if base.Ref.Kind != page.ChildRefPage || base.Ref.Page >= totalPages {
			return node.ErrPrimaryDirectory
		}
		baseImage, err := source.ReadPage(base.Ref.Page)
		if err != nil {
			return err
		}
		baseNode := node.NewNode(baseImage)
		if !node.VerifyPrimaryOperand(base, baseImage) || page.DecodeHeader(baseImage).PageID != base.Ref.Page || (baseNode.Type() != page.PageTypeLeaf && baseNode.Type() != page.PageTypeInternal) {
			return node.ErrPrimaryDirectory
		}
		for i := 0; i < directory.Count(); i++ {
			entry, _ := directory.Entry(i)
			if entry.InlineAbsence() {
				continue
			}
			if entry.Operand.Ref.Kind != page.ChildRefPage || entry.Operand.Ref.Page >= totalPages {
				return node.ErrPrimaryDirectory
			}
			component, err := source.ReadPage(entry.Operand.Ref.Page)
			if err != nil {
				return err
			}
			if err = node.ValidatePrimaryComponent(entry, component); err != nil {
				return err
			}
		}
		return nil
	default:
		return errors.New("invalid root page type")
	}
}
