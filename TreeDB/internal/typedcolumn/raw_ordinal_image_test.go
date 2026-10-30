package typedcolumn

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"reflect"
	"runtime"
	"testing"
)

func rawOrdinalTestInput(kind ColumnType, n int) (Options, RawOrdinalColumnValues, ColumnPartImageOptions, Batch) {
	encoding := EncodingRawInt64
	switch kind {
	case ColumnTypeFloat32:
		encoding = EncodingRawFloat32
	case ColumnTypeUint32List:
		encoding = EncodingRawUint32OffsetsList
	case ColumnTypeBytes:
		encoding = EncodingRawBytesOffsets
	}
	opts := Options{SchemaVersion: 23, SchemaMode: ColumnSchemaFixed, Columns: []ColumnDefinition{{Name: "ordinal", Type: ColumnTypeInt64, Encoding: EncodingRawInt64, CompressionSet: true, StatsDisabled: true}, {Name: "value", Type: kind, Encoding: encoding, CompressionSet: true}}, LogicalPrimaryKey: LogicalPrimaryKey{Columns: []string{"ordinal"}}, SortKey: SortKey{Columns: []SortKeyColumn{{Column: "ordinal"}}}, PartPolicy: ColumnPartPolicy{RowsPerGranule: DefaultRowsPerGranule}}
	values := RawOrdinalColumnValues{Rows: n}
	ids := make([]int64, n)
	for i := range ids {
		ids[i] = int64(i)
	}
	batch := Batch{Rows: n, Columns: map[string][]int64{"ordinal": ids}}
	switch kind {
	case ColumnTypeInt64:
		values.Int64 = make([]int64, n)
		for i := range values.Int64 {
			values.Int64[i] = int64(i%17) - 8
		}
		batch.Columns["value"] = values.Int64
	case ColumnTypeFloat32:
		values.Float32 = make([]float32, n)
		for i := range values.Float32 {
			values.Float32[i] = math.Float32frombits(uint32(i) * 7919)
		}
		if n > 2 {
			values.Float32[0] = math.Float32frombits(0x7fc00123)
			values.Float32[1] = math.Float32frombits(0x80000000)
		}
		batch.Float32Columns = map[string][]float32{"value": values.Float32}
	case ColumnTypeUint32List:
		list := RawUint32OffsetsList{Rows: n, Offsets: make([]uint64, n+1)}
		for i := 0; i < n; i++ {
			for j := 0; j < i%4; j++ {
				list.Values = append(list.Values, uint32(i+j))
			}
			list.Offsets[i+1] = uint64(len(list.Values))
		}
		values.Uint32List = list
		batch.Uint32OffsetsLists = map[string]RawUint32OffsetsList{"value": list}
	case ColumnTypeBytes:
		list := RawBytesOffsets{Rows: n, Offsets: make([]uint64, n+1)}
		for i := 0; i < n; i++ {
			for j := 0; j < i%4; j++ {
				list.Values = append(list.Values, byte(i+j))
			}
			list.Offsets[i+1] = uint64(len(list.Values))
		}
		values.Bytes = list
		batch.BytesColumns = map[string]RawBytesOffsets{"value": list}
	}
	imageOpts := ColumnPartImageOptions{Dictionaries: map[string]map[string]int64{"adapter.metadata": {"opaque metadata": 0}}, LayoutLogicalTypes: map[string]string{"ordinal": "int64", "value": string(kind)}}
	return opts, values, imageOpts, batch
}

func TestRawOrdinalColumnPartImageGenericParity(t *testing.T) {
	for _, kind := range []ColumnType{ColumnTypeInt64, ColumnTypeFloat32, ColumnTypeUint32List, ColumnTypeBytes} {
		for _, n := range []int{0, 1, 4096, DefaultRowsPerGranule - 1, DefaultRowsPerGranule, DefaultRowsPerGranule + 1, 2*DefaultRowsPerGranule + 1} {
			for _, alignment := range []int{8, 64} {
				t.Run(fmt.Sprintf("%s/%d/%d", kind, n, alignment), func(t *testing.T) {
					opts, values, imageOpts, batch := rawOrdinalTestInput(kind, n)
					imageOpts.SectionAlignment = alignment
					part, genericErr := BuildColumnPart(91, opts, batch)
					got, err := BuildRawOrdinalColumnPartImage(91, opts, values, imageOpts)
					if genericErr != nil {
						if err == nil {
							t.Fatalf("general constructor rejected input (%v), raw accepted", genericErr)
						}
						return
					}
					if err != nil {
						t.Fatal(err)
					}
					want, err := BuildColumnPartImage(part, imageOpts)
					if err != nil {
						t.Fatal(err)
					}
					if !bytes.Equal(got.Image.Bytes, want.Bytes) {
						t.Fatalf("image byte mismatch raw=%d generic=%d", len(got.Image.Bytes), len(want.Bytes))
					}
					if !reflect.DeepEqual(got.Image.Sections, want.Sections) {
						t.Fatal("section directory mismatch")
					}
					wantAccounting := part.ByteAccountingFromImage(want)
					if !reflect.DeepEqual(got.Accounting, wantAccounting) {
						t.Fatalf("accounting mismatch\nraw=%+v\ngeneric=%+v", got.Accounting, wantAccounting)
					}
					decoded, err := ColumnPartFromImage(got.Image)
					if err != nil {
						t.Fatal(err)
					}
					wantDecoded, err := ColumnPartFromImage(want)
					if err != nil {
						t.Fatal(err)
					}
					if !reflect.DeepEqual(decoded.Descriptor, wantDecoded.Descriptor) || !reflect.DeepEqual(decoded.Columns, wantDecoded.Columns) || !reflect.DeepEqual(decoded.ColumnStats, wantDecoded.ColumnStats) || !reflect.DeepEqual(decoded.PruningMetadata, wantDecoded.PruningMetadata) {
						t.Fatal("decoded descriptor/stats/pruning mismatch")
					}
					if _, err = CertifyColumnPartLayoutContractFromImage(got.Image); err != nil {
						t.Fatal(err)
					}
					plan, err := PlanRawOrdinalColumnPartImage(opts, values, imageOpts)
					if err != nil {
						t.Fatal(err)
					}
					if plan != got.Charge {
						t.Fatal("constructor charge differs from preallocation plan")
					}
					if len(got.Image.Bytes) > plan.ImageBytesUpperBound || plan.RetainedBytes > plan.ConstructorBytes {
						t.Fatalf("invalid plan: %+v image=%d", plan, len(got.Image.Bytes))
					}
					wantG := 0
					if n > 0 {
						wantG = 1 + (n-1)/DefaultRowsPerGranule
					}
					if plan.Granules != wantG {
						t.Fatalf("granules=%d want=%d", plan.Granules, wantG)
					}
				})
			}
		}
	}
}

func TestRawOrdinalColumnPartImageStatsOverflowParity(t *testing.T) {
	opts, values, imageOpts, batch := rawOrdinalTestInput(ColumnTypeInt64, DefaultRowsPerGranule+1)
	for i := range values.Int64 {
		values.Int64[i] = math.MaxInt64
	}
	part, err := BuildColumnPart(17, opts, batch)
	if err != nil {
		t.Fatal(err)
	}
	want, err := BuildColumnPartImage(part, imageOpts)
	if err != nil {
		t.Fatal(err)
	}
	got, err := BuildRawOrdinalColumnPartImage(17, opts, values, imageOpts)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got.Image.Bytes, want.Bytes) || !reflect.DeepEqual(got.Accounting, part.ByteAccountingFromImage(want)) {
		t.Fatal("sum-overflow image/accounting differs")
	}
}

func TestRawOrdinalColumnPartImageDisabledStatsAndDictionaries(t *testing.T) {
	for _, disabled := range []bool{false, true} {
		opts, values, imageOpts, batch := rawOrdinalTestInput(ColumnTypeInt64, 9)
		opts.Columns[1].StatsDisabled = disabled
		// Exercise raw dictionary selection, multiple dictionaries and non-dense
		// codes, rather than relying only on the maintained one-entry metadata.
		imageOpts.Dictionaries = map[string]map[string]int64{"z": {"three": 3, "minus": -1}, "a": {"zero": 0, "one": 1}}
		delete(imageOpts.LayoutLogicalTypes, "ordinal") // existing fallback labels
		part, err := BuildColumnPart(5, opts, batch)
		if err != nil {
			t.Fatal(err)
		}
		want, err := BuildColumnPartImage(part, imageOpts)
		if err != nil {
			t.Fatal(err)
		}
		got, err := BuildRawOrdinalColumnPartImage(5, opts, values, imageOpts)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got.Image.Bytes, want.Bytes) || !reflect.DeepEqual(got.Accounting, part.ByteAccountingFromImage(want)) {
			t.Fatal("optional metadata image/accounting differs")
		}
	}
}

func TestRawOrdinalColumnPartImageRejectsCorruption(t *testing.T) {
	for _, kind := range []ColumnType{ColumnTypeUint32List, ColumnTypeBytes} {
		opts, values, imageOpts, _ := rawOrdinalTestInput(kind, 4)
		if kind == ColumnTypeUint32List {
			values.Uint32List.Offsets[2] = math.MaxUint64
		} else {
			values.Bytes.Offsets[2] = math.MaxUint64
		}
		if _, err := PlanRawOrdinalColumnPartImage(opts, values, imageOpts); err == nil {
			t.Fatal("corrupt offsets accepted by planner")
		}
		if _, err := BuildRawOrdinalColumnPartImage(3, opts, values, imageOpts); err == nil {
			t.Fatal("corrupt offsets accepted by constructor")
		}
	}
	for _, kind := range []ColumnType{ColumnTypeInt64, ColumnTypeFloat32, ColumnTypeUint32List, ColumnTypeBytes} {
		opts, values, imageOpts, _ := rawOrdinalTestInput(kind, 4)
		got, err := BuildRawOrdinalColumnPartImage(3, opts, values, imageOpts)
		if err != nil {
			t.Fatal(err)
		}
		for _, section := range got.Image.Sections {
			if section.Column == "value" && section.Length > 0 {
				corrupt := got.Image
				corrupt.Bytes = append([]byte(nil), got.Image.Bytes...)
				corrupt.Bytes[section.Offset] ^= 1
				if _, err = CertifyColumnPartLayoutContractFromImage(corrupt); err == nil {
					t.Fatalf("%s corruption accepted in %s", kind, section.Kind)
				}
			}
		}
	}
}

func TestRawOrdinalColumnPartImageUnsupportedDoesNotSelectGeneric(t *testing.T) {
	for _, mutate := range []func(*Options, *ColumnPartImageOptions){
		func(o *Options, _ *ColumnPartImageOptions) { o.Columns[0].Encoding = EncodingDeltaVarint },
		func(o *Options, _ *ColumnPartImageOptions) { o.Columns[0].StatsDisabled = false },
		func(o *Options, _ *ColumnPartImageOptions) { o.PartPolicy.DefaultCodecBlockRows = 2 },
		func(o *Options, _ *ColumnPartImageOptions) { o.PartPolicy.RowsPerGranule = 64 },
		func(_ *Options, i *ColumnPartImageOptions) { i.SectionCompression = CompressionLZ4 },
	} {
		opts, values, imageOpts, _ := rawOrdinalTestInput(ColumnTypeInt64, 4)
		mutate(&opts, &imageOpts)
		if _, err := PlanRawOrdinalColumnPartImage(opts, values, imageOpts); !errors.Is(err, ErrRawOrdinalImageUnsupported) {
			t.Fatalf("planner err=%v", err)
		}
		if _, err := BuildRawOrdinalColumnPartImage(3, opts, values, imageOpts); !errors.Is(err, ErrRawOrdinalImageUnsupported) {
			t.Fatalf("constructor err=%v", err)
		}
	}
	bound := rawOrdinalByteBound{}
	var total uint64
	bound.add(&total, math.MaxUint64, 1)
	if !bound.failed {
		t.Fatal("sum overflow accepted")
	}
	bound = rawOrdinalByteBound{}
	bound.product(math.MaxUint64, 2)
	if !bound.failed {
		t.Fatal("product overflow accepted")
	}
}

// This checks the promised constructor envelope against allocation cohorts,
// rather than merely comparing the final encoded byte count with its bound.
func TestRawOrdinalColumnPartImageChargeCoversAllocationCohorts(t *testing.T) {
	for _, kind := range []ColumnType{ColumnTypeInt64, ColumnTypeFloat32, ColumnTypeUint32List, ColumnTypeBytes} {
		for _, n := range []int{1, 4096, DefaultRowsPerGranule + 1} {
			opts, values, imageOpts, _ := rawOrdinalTestInput(kind, n)
			plan, err := PlanRawOrdinalColumnPartImage(opts, values, imageOpts)
			if err != nil {
				t.Fatal(err)
			}
			// Warm shared runtime/CRC/reflection state before the measured call.
			if _, err = BuildRawOrdinalColumnPartImage(1, opts, values, imageOpts); err != nil {
				t.Fatal(err)
			}
			var before, after runtime.MemStats
			runtime.ReadMemStats(&before)
			got, err := BuildRawOrdinalColumnPartImage(2, opts, values, imageOpts)
			runtime.ReadMemStats(&after)
			if err != nil {
				t.Fatal(err)
			}
			runtime.KeepAlive(got)
			if used := after.TotalAlloc - before.TotalAlloc; used > plan.ConstructorBytes {
				t.Fatalf("%s/%d allocation cohorts=%d exceed promise=%d", kind, n, used, plan.ConstructorBytes)
			}
		}
	}
}

func rawOrdinalTestShape(kind ColumnType, values RawOrdinalColumnValues) RawOrdinalColumnShape {
	shape := RawOrdinalColumnShape{Rows: values.Rows, Type: kind}
	switch kind {
	case ColumnTypeInt64:
		shape.ValueCount = uint64(len(values.Int64))
	case ColumnTypeFloat32:
		shape.ValueCount = uint64(len(values.Float32))
	case ColumnTypeUint32List, ColumnTypeAdjacencyList:
		shape.ValueCount = uint64(len(values.Uint32List.Values))
	case ColumnTypeBytes:
		shape.ValueCount = uint64(len(values.Bytes.Values))
	}
	return shape
}

func TestRawOrdinalColumnPartImageShapePlanMatchesValues(t *testing.T) {
	for _, kind := range []ColumnType{ColumnTypeInt64, ColumnTypeFloat32, ColumnTypeUint32List, ColumnTypeAdjacencyList, ColumnTypeBytes} {
		for _, n := range []int{0, 1, 4096, DefaultRowsPerGranule - 1, DefaultRowsPerGranule, DefaultRowsPerGranule + 1} {
			t.Run(fmt.Sprintf("%s/%d", kind, n), func(t *testing.T) {
				fixtureKind := kind
				if kind == ColumnTypeAdjacencyList {
					fixtureKind = ColumnTypeUint32List
				}
				opts, values, imageOpts, _ := rawOrdinalTestInput(fixtureKind, n)
				opts.Columns[1].Type = kind
				shape := rawOrdinalTestShape(kind, values)
				for _, disabled := range []bool{false, true} {
					opts.Columns[1].StatsDisabled = disabled
					for _, align := range []int{8, 64} {
						imageOpts.SectionAlignment = align
						fromShape, shapeErr := PlanRawOrdinalColumnPartImageShape(opts, shape, imageOpts)
						fromValues, valuesErr := PlanRawOrdinalColumnPartImage(opts, values, imageOpts)
						if n == 0 && (kind == ColumnTypeInt64 || kind == ColumnTypeFloat32) {
							if shapeErr == nil || valuesErr == nil {
								t.Fatal("empty scalar accepted")
							}
							continue
						}
						if shapeErr != nil || valuesErr != nil {
							t.Fatalf("shape=%v values=%v", shapeErr, valuesErr)
						}
						if fromShape != fromValues {
							t.Fatalf("shape/value promises differ: shape=%+v values=%+v", fromShape, fromValues)
						}
						built, err := BuildRawOrdinalColumnPartImage(31, opts, values, imageOpts)
						if err != nil {
							t.Fatal(err)
						}
						if built.Charge != fromShape {
							t.Fatal("pre-source promise differs from constructor charge")
						}
					}
				}
			})
		}
	}
}

func TestRawOrdinalColumnPartImageShapePlanRejectsUnsupported(t *testing.T) {
	for _, kind := range []ColumnType{ColumnTypeInt64, ColumnTypeFloat32, ColumnTypeUint32List, ColumnTypeBytes} {
		opts, values, imageOpts, _ := rawOrdinalTestInput(kind, 3)
		shape := rawOrdinalTestShape(kind, values)
		wrong := shape
		wrong.Type = ColumnType("unsupported")
		if _, err := PlanRawOrdinalColumnPartImageShape(opts, wrong, imageOpts); !errors.Is(err, ErrRawOrdinalImageUnsupported) {
			t.Fatalf("type mismatch err=%v", err)
		}
		wrong = shape
		wrong.Rows = -1
		if _, err := PlanRawOrdinalColumnPartImageShape(opts, wrong, imageOpts); err == nil {
			t.Fatal("negative rows accepted")
		}
		if kind == ColumnTypeInt64 || kind == ColumnTypeFloat32 {
			wrong = shape
			wrong.ValueCount++
			if _, err := PlanRawOrdinalColumnPartImageShape(opts, wrong, imageOpts); !errors.Is(err, ErrRawOrdinalImageUnsupported) {
				t.Fatalf("scalar count mismatch err=%v", err)
			}
		} else {
			wrong = shape
			wrong.Rows = 0
			wrong.ValueCount = 1
			if _, err := PlanRawOrdinalColumnPartImageShape(opts, wrong, imageOpts); !errors.Is(err, ErrRawOrdinalImageUnsupported) {
				t.Fatalf("nonempty zero-row list err=%v", err)
			}
			for _, count := range []uint64{math.MaxUint64, uint64(int(^uint(0) >> 1))} {
				wrong = shape
				wrong.ValueCount = count
				if _, err := PlanRawOrdinalColumnPartImageShape(opts, wrong, imageOpts); !errors.Is(err, errRawOrdinalImageCapacity) {
					t.Fatalf("unaddressable values %d err=%v", count, err)
				}
			}
		}
		opts.Columns[1].Compression = CompressionLZ4
		if _, err := PlanRawOrdinalColumnPartImageShape(opts, shape, imageOpts); !errors.Is(err, ErrRawOrdinalImageUnsupported) {
			t.Fatalf("compressed shape err=%v", err)
		}
	}
	// A row count beyond the serialized uint32 domain must be rejected before
	// any row/granule-sized accounting loop, on hosts able to express it.
	if uint64(int(^uint(0)>>1)) > math.MaxUint32 {
		opts, _, imageOpts, _ := rawOrdinalTestInput(ColumnTypeBytes, 1)
		tooMany := uint64(math.MaxUint32) + 1
		if _, err := PlanRawOrdinalColumnPartImageShape(opts, RawOrdinalColumnShape{Rows: int(tooMany), Type: ColumnTypeBytes}, imageOpts); err == nil {
			t.Fatal("unserializable row count accepted")
		}
	}
}

func TestRawOrdinalColumnPartImageShapePlanDoesNotValidateFutureOffsets(t *testing.T) {
	for _, kind := range []ColumnType{ColumnTypeUint32List, ColumnTypeBytes} {
		opts, values, imageOpts, _ := rawOrdinalTestInput(kind, 4)
		shape := rawOrdinalTestShape(kind, values)
		before, err := PlanRawOrdinalColumnPartImageShape(opts, shape, imageOpts)
		if err != nil {
			t.Fatal(err)
		}
		// The promise needs only counts. Actual source validation remains a
		// mandatory constructor step even when those counts were preplanned.
		if kind == ColumnTypeBytes {
			values.Bytes.Offsets[2] = math.MaxUint64
		} else {
			values.Uint32List.Offsets[2] = math.MaxUint64
		}
		after, err := PlanRawOrdinalColumnPartImageShape(opts, shape, imageOpts)
		if err != nil || after != before {
			t.Fatalf("shape plan depends on source data: %v", err)
		}
		if _, err = PlanRawOrdinalColumnPartImage(opts, values, imageOpts); err == nil {
			t.Fatal("actual planner accepted malformed offsets")
		}
		if _, err = BuildRawOrdinalColumnPartImage(31, opts, values, imageOpts); err == nil {
			t.Fatal("constructor accepted malformed offsets after preplanning")
		}
	}
}

func TestRawOrdinalColumnPartImageShapePlansWithoutValueCarriers(t *testing.T) {
	// Count a large potential raw carrier without constructing it. This is
	// intentionally not a cap acceptance claim or an allocation of that data.
	for _, kind := range []ColumnType{ColumnTypeUint32List, ColumnTypeBytes} {
		opts, _, imageOpts, _ := rawOrdinalTestInput(kind, 1)
		count := uint64(1) << 24
		plan, err := PlanRawOrdinalColumnPartImageShape(opts, RawOrdinalColumnShape{Rows: 4096, Type: kind, ValueCount: count}, imageOpts)
		if err != nil {
			t.Fatal(err)
		}
		payloadBytes := count
		if kind == ColumnTypeUint32List {
			payloadBytes *= 4
		}
		if uint64(plan.ImageBytesUpperBound) < payloadBytes || plan.ConstructorBytes < uint64(plan.ImageBytesUpperBound) {
			t.Fatal("large carrier omitted from promise")
		}
	}
}
