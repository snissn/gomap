package collections

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"os"

	"github.com/snissn/gomap/TreeDB/internal/mappedresource"
)

const columnVectorGraphConstructionMatrixPattern = ".column-graph-construction-*.f32"

var errColumnVectorGraphConstructionMatrixShape = errors.New("collections: column graph construction matrix shape mismatch")

type columnVectorGraphConstructionMatrix struct {
	handle *mappedresource.Handle
	values []float32
	closed bool
}

func stageColumnVectorGraphConstructionMatrix(root string, rows []columnVectorGraphAssetRow, dimensions int) (_ *columnVectorGraphConstructionMatrix, err error) {
	if root == "" || dimensions <= 0 || len(rows) == 0 || uint64(len(rows)) > uint64(math.MaxInt64)/(uint64(dimensions)*4) {
		return nil, fmt.Errorf("%w: rows=%d dimensions=%d", errColumnVectorGraphConstructionMatrixShape, len(rows), dimensions)
	}
	file, err := os.CreateTemp(root, columnVectorGraphConstructionMatrixPattern)
	if err != nil {
		return nil, err
	}
	path := file.Name()
	fileOpen := true
	defer func() {
		if err != nil {
			if fileOpen {
				err = errors.Join(err, file.Close())
			}
			err = errors.Join(err, os.Remove(path))
		}
	}()

	writer := bufio.NewWriterSize(file, 1<<20)
	scratch := make([]byte, dimensions*4)
	for i := range rows {
		if len(rows[i].Vector) != dimensions {
			return nil, fmt.Errorf("%w: row=%d dimensions=%d want=%d", errColumnVectorGraphConstructionMatrixShape, i, len(rows[i].Vector), dimensions)
		}
		for j, value := range rows[i].Vector {
			binary.LittleEndian.PutUint32(scratch[j*4:], math.Float32bits(value))
		}
		for written := 0; written < len(scratch); {
			n, writeErr := writer.Write(scratch[written:])
			if writeErr != nil {
				return nil, writeErr
			}
			if n == 0 {
				return nil, errors.New("collections: column graph construction matrix short write")
			}
			written += n
		}
	}
	if err := writer.Flush(); err != nil {
		return nil, err
	}
	wantBytes := int64(len(rows)) * int64(dimensions) * 4
	if info, statErr := file.Stat(); statErr != nil {
		return nil, statErr
	} else if info.Size() != wantBytes {
		return nil, fmt.Errorf("collections: column graph construction matrix bytes=%d want=%d", info.Size(), wantBytes)
	}
	if err := file.Close(); err != nil {
		return nil, err
	}
	fileOpen = false

	manager := mappedresource.NewManager()
	handle, err := manager.AcquireFileRange(
		mappedresource.Key{Class: mappedresource.ClassExternalAsset, Kind: "column_graph_construction_matrix", Length: wantBytes, Encoding: "raw_float32_le"},
		mappedresource.Scope{Kind: mappedresource.ScopeMaintenance, ID: "column_graph_construction_matrix", Reason: "offline column graph rebuild"},
		path,
		mappedresource.AcquireOptions{Reason: "offline column graph rebuild", PreferMapped: true, AllowHeapCopy: false, ResourceRoot: root, ResourcePath: path},
	)
	if err != nil {
		return nil, err
	}
	values, err := manager.Float32View(handle)
	if err != nil {
		return nil, errors.Join(err, handle.Release())
	}
	if err := os.Remove(path); err != nil {
		return nil, errors.Join(err, handle.Release())
	}
	matrix := &columnVectorGraphConstructionMatrix{handle: handle, values: values}
	for i := range rows {
		start := i * dimensions
		rows[i].Vector = values[start : start+dimensions]
	}
	return matrix, nil
}

func (m *columnVectorGraphConstructionMatrix) Close() error {
	if m == nil || m.closed {
		return nil
	}
	m.closed = true
	m.values = nil
	return m.handle.Release()
}
