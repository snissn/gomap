package collections

import (
	"errors"
	"fmt"
	"math"
)

const cosineNormalizedF32V1UnitNormTolerance = 4e-6

// normalizeCosineNormalizedF32V1 owns and canonicalizes one admission vector.
// Callers must persist the returned bytes and must not normalize them again on
// replay, rebuild, or fold.
func normalizeCosineNormalizedF32V1(input []float32, dimensions int) ([]float32, error) {
	if dimensions <= 0 || len(input) != dimensions {
		return nil, fmt.Errorf("collections: normalized cosine vector dimensions=%d want %d", len(input), dimensions)
	}
	canonical := make([]float32, dimensions)
	if err := normalizeCosineNormalizedF32V1Into(canonical, input, dimensions); err != nil {
		return nil, err
	}
	return canonical, nil
}

func normalizeCosineNormalizedF32V1Into(canonical, input []float32, dimensions int) error {
	if len(canonical) != dimensions {
		return fmt.Errorf("collections: normalized cosine destination dimensions=%d want %d", len(canonical), dimensions)
	}
	invNorm, err := cosineNormalizedF32V1InputInvNorm(input, dimensions)
	if err != nil {
		return err
	}
	for dimension, value := range input {
		canonical[dimension] = float32(float64(value) * invNorm)
		if math.IsNaN(float64(canonical[dimension])) || math.IsInf(float64(canonical[dimension]), 0) {
			return fmt.Errorf("collections: normalized cosine vector component %d became non-finite", dimension)
		}
	}
	return nil
}

func cosineNormalizedF32V1InputInvNorm(input []float32, dimensions int) (float64, error) {
	if dimensions <= 0 || len(input) != dimensions {
		return 0, fmt.Errorf("collections: normalized cosine vector dimensions=%d want %d", len(input), dimensions)
	}
	var squaredNorm float64
	for dimension, value := range input {
		if math.IsNaN(float64(value)) || math.IsInf(float64(value), 0) {
			return 0, fmt.Errorf("collections: normalized cosine vector component %d is non-finite", dimension)
		}
		squaredNorm += float64(value) * float64(value)
	}
	if squaredNorm == 0 || math.IsNaN(squaredNorm) || math.IsInf(squaredNorm, 0) {
		return 0, errors.New("collections: normalized cosine vector has invalid norm")
	}
	return 1 / math.Sqrt(squaredNorm), nil
}

// validateCosineNormalizedF32V1Canonical validates bytes decoded from trusted
// durable state without changing them. The tolerance admits float32 component
// rounding from the canonical admission arithmetic.
func validateCosineNormalizedF32V1Canonical(vector []float32, dimensions int) error {
	if dimensions <= 0 || len(vector) != dimensions {
		return fmt.Errorf("collections: canonical normalized cosine vector dimensions=%d want %d", len(vector), dimensions)
	}
	var squaredNorm float64
	for dimension, value := range vector {
		if math.IsNaN(float64(value)) || math.IsInf(float64(value), 0) {
			return fmt.Errorf("collections: canonical normalized cosine vector component %d is non-finite", dimension)
		}
		squaredNorm += float64(value) * float64(value)
	}
	if math.IsNaN(squaredNorm) || math.IsInf(squaredNorm, 0) || math.Abs(squaredNorm-1) > cosineNormalizedF32V1UnitNormTolerance {
		return fmt.Errorf("collections: canonical normalized cosine vector squared norm=%g is outside tolerance", squaredNorm)
	}
	return nil
}

func cosineNormalizedF32V1Score(query, vector []float32) (float64, error) {
	if len(query) != len(vector) {
		return 0, errColumnVectorGraphNativeSearchQueryDimensionMismatch
	}
	dot := float64(vectorDotProductFloat32(query, vector))
	if math.IsNaN(dot) || math.IsInf(dot, 0) {
		return 0, errColumnVectorGraphInvNormNormInvalid
	}
	return clampCosineNormalizedF32V1Score(dot), nil
}

func clampCosineNormalizedF32V1Score(score float64) float64 {
	if score > 1 {
		return 1
	}
	if score < -1 {
		return -1
	}
	return score
}

func cosineNormalizedF32V1DefinitionForField(meta CollectionMeta, field string) (VectorIndexDefinition, bool) {
	for _, def := range meta.VectorIndexes {
		if def.Field == field && vectorIndexUsesCosineNormalizedF32V1(def) {
			return def, true
		}
	}
	return VectorIndexDefinition{}, false
}

func cosineNormalizedF32V1Definition(meta CollectionMeta) (VectorIndexDefinition, bool) {
	for _, def := range meta.VectorIndexes {
		if vectorIndexUsesCosineNormalizedF32V1(def) {
			return def, true
		}
	}
	return VectorIndexDefinition{}, false
}

// validateCosineNormalizedF32V1DeclaredRows is the non-mutating command
// boundary for replay and generic document-only updates. New vectors enter via
// typed admission, which normalizes an owned copy before this validation. A
// generic update may copy existing canonical bytes into the mutable suffix but
// must never smuggle a raw vector into durable state.
func validateCosineNormalizedF32V1DeclaredRows(meta CollectionMeta, rows []columnDeclaredRow) error {
	def, ok := cosineNormalizedF32V1Definition(meta)
	if !ok || len(rows) == 0 {
		return nil
	}
	cfg := meta.Options.ColumnStore
	if cfg == nil {
		return fmt.Errorf("collections: representation %q requires column storage", def.Representation)
	}
	vectorColumn := -1
	for i := range cfg.Columns {
		if cfg.Columns[i].Path == def.Field {
			vectorColumn = i
			break
		}
	}
	if vectorColumn < 0 {
		return fmt.Errorf("collections: representation %q vector field %q is absent", def.Representation, def.Field)
	}
	for row := range rows {
		if vectorColumn >= len(rows[row].Values) {
			return fmt.Errorf("collections: representation %q row %d has no vector field %q", def.Representation, row, def.Field)
		}
		value := rows[row].Values[vectorColumn]
		if !value.Present || value.Type != ColumnStoreValueFloat32Vector {
			return fmt.Errorf("collections: representation %q row %d has invalid vector field %q", def.Representation, row, def.Field)
		}
		if err := validateCosineNormalizedF32V1Canonical(value.Float32Vector, def.Dimensions); err != nil {
			return fmt.Errorf("collections: representation %q row %d requires canonical typed vector admission: %w", def.Representation, row, err)
		}
	}
	return nil
}
