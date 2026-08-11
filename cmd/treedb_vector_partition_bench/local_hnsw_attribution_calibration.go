package main

import (
	"errors"
	"runtime"
)

const localHNSWAttributionCalibrationSchemaV1 = "treedb_local_hnsw_attribution_calibration_v1"

type localHNSWAttributionCalibrationV1 struct {
	Schema   string
	Ordinals []int
	Queries  [][]float32
	Truth    [][]m8CanonicalResultV1
}

// localHNSWAttributionCalibrationV1Build creates only calibration queries and
// computes their truth from the retained authoritative FP32 source. It never
// generates or evaluates a holdout query.
func localHNSWAttributionCalibrationV1Build(source *m8ProductionMultiGroupAssetsV1, fixture fixtureManifest, ordinals []int) (localHNSWAttributionCalibrationV1, error) {
	var out localHNSWAttributionCalibrationV1
	if source == nil || source.collection == nil || len(ordinals) == 0 || fixture.Queries < 1 || fixture.Dimensions < 1 || !supportedFixtureGeneratorV1(fixture.Generator) {
		return out, errors.New("invalid local HNSW attribution calibration source")
	}
	vectors := fixtureVectors(fixture)
	if err := m8ValidateExistingAssetsFixtureV1(source.collection, source.manifest, fixture, vectors); err != nil {
		return out, err
	}
	queries64 := make([][]float64, len(ordinals))
	queries32 := make([][]float32, len(ordinals))
	var deterministicQuerySet [][]float64
	if fixture.Generator == fixtureGenerator {
		deterministicQuerySet = deterministicQueries(vectors, fixture)
	}
	for i, ordinal := range ordinals {
		if ordinal < 0 || ordinal >= fixture.Queries || i > 0 && ordinals[i-1] >= ordinal || !localHNSWCalibrationOrdinalV1(ordinal) {
			return out, errors.New("invalid local HNSW attribution calibration ordinals")
		}
		query := make([]float64, fixture.Dimensions)
		if fixture.Generator == fixtureGenerator {
			copy(query, deterministicQuerySet[ordinal])
		} else {
			qualificationVectorV1(query, fixture, uint64(ordinal), 0xd1b54a32d192ed03)
		}
		queries64[i], queries32[i] = query, m8Query32V1(query)
	}
	vectors, deterministicQuerySet = nil, nil
	runtime.GC()
	truth, err := m8ExactTruthV1(source.collection, source.manifest, queries64, 10)
	if err != nil {
		return out, err
	}
	for _, row := range truth {
		canonical := m8CanonicalResultsV1(row, 10)
		ids, scores := m8CanonicalParityV1(row, canonical)
		if len(row) != min(10, fixture.Vectors) || !ids || !scores {
			return out, errors.New("invalid local HNSW attribution calibration truth")
		}
	}
	return localHNSWAttributionCalibrationV1{Schema: localHNSWAttributionCalibrationSchemaV1, Ordinals: append([]int(nil), ordinals...), Queries: queries32, Truth: truth}, nil
}
