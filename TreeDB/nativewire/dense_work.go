package nativewire

import (
	"encoding/binary"
	"errors"

	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/documentservice"
	iwire "github.com/snissn/gomap/TreeDB/internal/nativewire"
)

// Only the typed v2 handler supplies this transport metadata. The original
// error chain/code is preserved; v1 never emits details from a nested service error.
type denseWorkError struct {
	error
	work documentservice.DenseSearchWork
}

func (e *denseWorkError) Unwrap() error { return e.error }

func denseWorkRouteTag(route string) uint64 {
	switch route {
	case "typed_empty":
		return 1
	case "typed_exact":
		return 2
	case "typed_hnsw":
		return 3
	}
	return 0
}

func validateDenseWork(w documentservice.DenseSearchWork) error {
	g, f, s, o := w.Graph, w.Graph.Filter, w.Graph.Snapshot, w.Output
	bad := w.Version != 1 || (g.Route != "" && denseWorkRouteTag(g.Route) == 0)
	bad = bad || (!g.Available && g != (collections.ColumnGraphQueryWork{}))
	bad = bad || (!f.Attempted && f != (collections.ColumnGraphFilterWork{}))
	bad = bad || (!s.Available && s != (collections.ColumnGraphQuerySnapshot{}))
	bad = bad || (!o.Attempted && o != (documentservice.DenseSearchOutputWork{}))
	bad = bad || (g.Completed && (!g.Available || !s.Available || g.Route == "" || (f.Attempted && !f.Completed)))
	bad = bad || o.Fetched > o.Requested || o.Missing > o.Requested-o.Fetched
	bad = bad || (w.Completed && (!g.Completed || !o.Completed || o.Missing != 0 || o.Fetched != o.Requested))
	for _, m := range []collections.ColumnGraphManifestWork{s.BaseManifest, s.CurrentManifest} {
		bad = bad || (m.Format != "" && m.Format != "tcs1")
	}
	if bad {
		return protocolError(iwire.ErrMalformedFrame, "invalid dense work proof")
	}
	return nil
}

func appendDenseWork(dst []byte, w documentservice.DenseSearchWork) ([]byte, error) {
	if err := validateDenseWork(w); err != nil {
		return nil, err
	}
	g, f, s, o := w.Graph, w.Graph.Filter, w.Graph.Snapshot, w.Output
	flags := uint64(0)
	for i, enabled := range [...]bool{w.Completed, g.Available, g.Completed, f.Attempted, f.Completed, s.Available, o.Attempted, o.Completed} {
		if enabled {
			flags |= 1 << i
		}
	}
	for _, value := range [...]uint64{w.Version, flags, denseWorkRouteTag(g.Route), g.BaseANNScored, g.BaseCandidates, g.BaseEdges, g.DeltaScored, g.ExactBaseScored, g.BaseShadowed, g.BaseResultIDs,
		f.EligibleRows, f.SourceIDs, f.SourceBytes, f.InspectedEntries, f.MappingWorkCharged, f.RetainedBytes, f.ScratchIDBytes, f.ScratchRows, f.OrdinalGrowthPeakBytes,
		s.SchemaHash, s.SchemaGeneration, s.BaseCoverageLSN, s.CurrentCoverageLSN} {
		dst = binary.AppendUvarint(dst, value)
	}
	for _, m := range [...]collections.ColumnGraphManifestWork{s.BaseManifest, s.CurrentManifest} {
		format := uint64(0)
		if m.Format == "tcs1" {
			format = 1
		}
		for _, value := range [...]uint64{m.Generation, format, uint64(m.Version), m.Checksum} {
			dst = binary.AppendUvarint(dst, value)
		}
	}
	for _, value := range [...]uint64{o.Requested, o.Fetched, o.Missing, o.OutputBytes, o.RetainedPayloadFetches, o.JSONReconstructionRows, o.TypedColumnRows} {
		dst = binary.AppendUvarint(dst, value)
	}
	return dst, nil
}

func decodeDenseWork(raw []byte) (w documentservice.DenseSearchWork, err error) {
	// Exactly 38 minimal uint64 varints; there is no variable-size owned data.
	var values [38]uint64
	off := 0
	for i := range values {
		values[i], err = readUvarintField(raw, &off, "dense work")
		if err != nil {
			return w, err
		}
	}
	if off != len(raw) || values[1] > 255 || values[2] > 3 || values[24] > 1 || values[28] > 1 || values[25] > 65535 || values[29] > 65535 {
		return w, protocolError(iwire.ErrMalformedFrame, "invalid dense work fields or length")
	}
	flags := values[1]
	w.Version, w.Completed = values[0], flags&1 != 0
	g, f, s, o := &w.Graph, &w.Graph.Filter, &w.Graph.Snapshot, &w.Output
	g.Available, g.Completed = flags&2 != 0, flags&4 != 0
	f.Attempted, f.Completed = flags&8 != 0, flags&16 != 0
	s.Available = flags&32 != 0
	o.Attempted, o.Completed = flags&64 != 0, flags&128 != 0
	g.Route = [...]string{"", "typed_empty", "typed_exact", "typed_hnsw"}[values[2]]
	g.BaseANNScored, g.BaseCandidates, g.BaseEdges = values[3], values[4], values[5]
	g.DeltaScored, g.ExactBaseScored, g.BaseShadowed, g.BaseResultIDs = values[6], values[7], values[8], values[9]
	f.EligibleRows, f.SourceIDs, f.SourceBytes, f.InspectedEntries = values[10], values[11], values[12], values[13]
	f.MappingWorkCharged, f.RetainedBytes, f.ScratchIDBytes, f.ScratchRows, f.OrdinalGrowthPeakBytes = values[14], values[15], values[16], values[17], values[18]
	s.SchemaHash, s.SchemaGeneration, s.BaseCoverageLSN, s.CurrentCoverageLSN = values[19], values[20], values[21], values[22]
	formats := [...]string{"", "tcs1"}
	s.BaseManifest = collections.ColumnGraphManifestWork{Generation: values[23], Format: formats[values[24]], Version: uint16(values[25]), Checksum: values[26]}
	s.CurrentManifest = collections.ColumnGraphManifestWork{Generation: values[27], Format: formats[values[28]], Version: uint16(values[29]), Checksum: values[30]}
	o.Requested, o.Fetched, o.Missing, o.OutputBytes = values[31], values[32], values[33], values[34]
	o.RetainedPayloadFetches, o.JSONReconstructionRows, o.TypedColumnRows = values[35], values[36], values[37]
	return w, validateDenseWork(w)
}

func decodeDenseWorkSection(sections []iwire.Section, typed bool) (documentservice.DenseSearchWork, error) {
	var unavailable documentservice.DenseSearchWork
	if typed {
		for _, section := range sections {
			switch section.ID {
			case iwire.SectionDocumentIDs, iwire.SectionDocuments, iwire.SectionDenseSearchResponse, iwire.SectionDenseSearchWork:
			default:
				if section.Flags&iwire.SectionFlagCritical != 0 {
					return unavailable, protocolError(iwire.ErrUnsupportedFeature, "unknown critical dense response section")
				}
			}
		}
	}
	raw, found, err := singletonSection(sections, iwire.SectionDenseSearchWork)
	if err != nil {
		return unavailable, err
	}
	if found != typed {
		return unavailable, protocolError(iwire.ErrMalformedFrame, "dense work section does not match command version")
	}
	if !found {
		return unavailable, nil
	}
	work, err := decodeDenseWork(raw)
	if err != nil {
		return unavailable, err
	}
	return work, nil
}

func denseServiceWork(err error) *documentservice.DenseSearchWork {
	var serviceErr *documentservice.Error
	if errors.As(err, &serviceErr) {
		return serviceErr.DenseWork
	}
	return nil
}

func validateDenseWorkResults(work documentservice.DenseSearchWork, results []DenseVectorSearchResult) error {
	bytes := uint64(0)
	for _, result := range results {
		bytes += uint64(len(result.Document))
	}
	if !work.Completed || work.Output.Fetched != uint64(len(results)) || work.Output.OutputBytes != bytes {
		return protocolError(iwire.ErrMalformedFrame, "dense work does not match response documents")
	}
	return nil
}
