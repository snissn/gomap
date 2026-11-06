package valuelog

import (
	"encoding/binary"
	"os"
	"sync"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/transactionowner"
)

type basisDiagnosticHostileError struct{}

func (basisDiagnosticHostileError) Error() string { panic("diagnostic invoked Error") }
func (basisDiagnosticHostileError) Is(error) bool { panic("diagnostic invoked Is") }
func (basisDiagnosticHostileError) Unwrap() error { panic("diagnostic invoked Unwrap") }

func basisDiagnosticCall(t *testing.T, frozen bool) (*COWBasisRefusalObserver, COWBasisCall) {
	t.Helper()
	observer := &COWBasisRefusalObserver{}
	owner := transactionowner.New()
	if !observer.Bind(owner) {
		t.Fatal("bind")
	}
	return observer, observer.Begin(owner, frozen).Descriptor(COWBasisRoot).Side(true)
}

func TestCOWBasisRefusalScalarControls(t *testing.T) {
	t.Run("owner_and_no_entry", func(t *testing.T) {
		var observer COWBasisRefusalObserver
		owner := transactionowner.New()
		if !observer.Bind(owner) || observer.Bind(owner) || observer.Begin(transactionowner.New(), false).Enabled() {
			t.Fatal("owner binding was not exclusive")
		}
		var absent *COWBasisRefusalObserver
		if absent.Begin(owner, false).Enabled() || observer.Snapshot().First != 0 || observer.Snapshot().Calls != 0 {
			t.Fatal("absence became an observation")
		}
	})
	t.Run("first_tuple_and_unknown_shape", func(t *testing.T) {
		observer, call := basisDiagnosticCall(t, true)
		call.Refuse(COWBasisInspect, COWBasisFrameContract, COWRecordShape{Compressed: true, DictID: 91}, false)
		first := observer.Snapshot().First
		call.Refuse(COWBasisRefresh, COWBasisOtherError, COWRecordShape{}, false)
		if first != observer.Snapshot().First || first&0xffffff != 1 || first&(1<<24) == 0 || (first>>25)&7 != uint64(COWBasisRoot) || first&(1<<28) == 0 || (first>>29)&15 != uint64(COWBasisInspect) || (first>>33)&15 != uint64(COWBasisFrameContract) || first>>37 != 0 {
			t.Fatalf("tuple=%x later=%x", first, observer.Snapshot().First)
		}
	})
	t.Run("known_shape_and_read_stage", func(t *testing.T) {
		observer, call := basisDiagnosticCall(t, false)
		call.LeafRead().Refuse(COWBasisInspect, COWBasisPayloadContract, COWRecordShape{Compressed: true, DictID: 91, Codec: BlockCodecSnappy}, true)
		word := observer.Snapshot().First
		if (word>>29)&15 != uint64(COWBasisLeafRead) || (word>>37)&7 != 7 || (word>>40)&255 != uint64(BlockCodecSnappy) {
			t.Fatalf("tuple=%x", word)
		}
	})
	t.Run("no_error_methods_or_allocation", func(t *testing.T) {
		observer, call := basisDiagnosticCall(t, false)
		if n := testing.AllocsPerRun(32, func() {
			call.Enter(COWBasisInspect)
			call.RefuseMetadata(COWBasisInspect, COWBasisFrameContract, true, true, 255, true)
			COWBasisCall{}.RefuseMetadata(COWBasisInspect, COWBasisFrameContract, true, true, 255, true)
			call.Error(COWBasisInspect, basisDiagnosticHostileError{}, COWRecordShape{}, false)
			call.Finish(COWBasisInspect)
			_ = observer.Snapshot()
		}); n != 0 {
			t.Fatalf("scalar-path allocations=%g", n)
		}
	})
	t.Run("metadata_known_subset", func(t *testing.T) {
		observer, call := basisDiagnosticCall(t, false)
		// Unknown frame fields must be suppressed even if a caller supplied bits.
		call.RefuseMetadata(COWBasisInspect, COWBasisPayloadContract, false, true, 255, false)
		first := observer.Snapshot().First
		call.Error(COWBasisInspect, ErrCOWReadUnsupported, COWRecordShape{}, false)
		if (first>>37)&0x7ff != 0 || (first>>48)&7 != 1 || first>>51 != 0 || observer.Snapshot().First != first {
			t.Fatalf("metadata subset=%x later=%x", first, observer.Snapshot().First)
		}
	})
	t.Run("readback_while_borrower_active", func(t *testing.T) {
		observer, call := basisDiagnosticCall(t, true)
		var joined sync.WaitGroup
		joined.Add(1)
		go func() {
			defer joined.Done()
			for i := 0; i < 64; i++ {
				call.Enter(COWBasisInspect)
				call.RefuseMetadata(COWBasisInspect, COWBasisFrameContract, true, false, 255, true)
				call.Finish(COWBasisInspect)
			}
			call.Finish(COWBasisRefresh)
		}()
		for i := 0; i < 64; i++ {
			word := observer.Snapshot().First
			if word != 0 && ((word>>33)&15 != uint64(COWBasisFrameContract) || (word>>37)&0x7ff != 0 || (word>>48)&7 != 7 || (word>>51)&3 != 1 || (word>>53)&255 != 255) {
				t.Errorf("torn scalar tuple=%x", word)
			}
		}
		joined.Wait()
		if snapshot := observer.Snapshot(); snapshot.First == 0 || snapshot.Completed != 1 || snapshot.Entered[COWBasisInspect] != 64 || snapshot.Finished[COWBasisInspect] != 64 {
			t.Fatalf("joined controls=%+v", snapshot)
		}
	})
	t.Run("ordinal_overflow_refuses_attribution", func(t *testing.T) {
		var observer COWBasisRefusalObserver
		owner := transactionowner.New()
		if !observer.Bind(owner) {
			t.Fatal("bind")
		}
		observer.calls.Store(0xffffff)
		if observer.Begin(owner, false).Enabled() || observer.Begin(owner, false).Enabled() || !observer.Snapshot().Overflow || observer.Snapshot().First != 0 {
			t.Fatal("overflow attributed a call")
		}
	})
}

func TestCOWBasisRefusalActualInspectorControls(t *testing.T) {
	t.Run("successful_inspector_equal_and_allocation", func(t *testing.T) {
		f, ptr := cowTestLeafRecord(t, buildSparseLeafPageForPayloadTest(t), BlockCodecNone)
		observer, call := basisDiagnosticCall(t, false)
		want, wantErr := InspectCOWLeafRecord(f, ptr, cowTestLimits)
		got, gotErr := InspectCOWLeafRecordDiagnostic(f, ptr, cowTestLimits, call)
		if wantErr != nil || gotErr != nil || want != got || observer.Snapshot().First != 0 {
			t.Fatalf("shape equal=%t errors=%v/%v", want == got, wantErr, gotErr)
		}
		ordinary := testing.AllocsPerRun(16, func() {
			if _, err := InspectCOWLeafRecord(f, ptr, cowTestLimits); err != nil {
				panic(err)
			}
		})
		observed := testing.AllocsPerRun(16, func() {
			if _, err := InspectCOWLeafRecordDiagnostic(f, ptr, cowTestLimits, call); err != nil {
				panic(err)
			}
		})
		if observed != ordinary {
			t.Fatalf("inspector allocation ordinary=%g observed=%g", ordinary, observed)
		}
	})
	t.Run("actual_source_refusal", func(t *testing.T) {
		observer, call := basisDiagnosticCall(t, false)
		_, ptr := cowTestLeafRecord(t, buildSparseLeafPageForPayloadTest(t), BlockCodecNone)
		shape, err := InspectCOWLeafRecordDiagnostic(nil, ptr, cowTestLimits, call)
		word := observer.Snapshot().First
		if err != ErrCOWReadUnsupported || shape != (COWRecordShape{}) || (word>>33)&15 != uint64(COWBasisSourceContract) || word>>37 != 0 {
			t.Fatalf("source refusal=%x error=%v", word, err)
		}
	})
	for _, fixture := range []struct {
		name       string
		compressed bool
		dictionary bool
		rawCodec   uint8
	}{
		{"actual_frame_uncompressed_refusal", false, false, uint8(BlockCodecSnappy)},
		{"actual_frame_dictionary_refusal", true, true, uint8(BlockCodecSnappy)},
		{"actual_frame_codec_refusal", true, false, 255},
	} {
		t.Run(fixture.name, func(t *testing.T) {
			codec := BlockCodecNone
			if fixture.compressed {
				codec = BlockCodecSnappy
			}
			f, ptr := cowTestLeafRecord(t, buildSparseLeafPageForPayloadTest(t), codec)
			bytes, err := os.ReadFile(f.Name())
			if err != nil {
				t.Fatal(err)
			}
			bytes[HeaderSize+3] = fixture.rawCodec
			if fixture.dictionary {
				binary.LittleEndian.PutUint64(bytes[HeaderSize+4:HeaderSize+12], 91)
			}
			if err := os.WriteFile(f.Name(), bytes, 0600); err != nil {
				t.Fatal(err)
			}
			observer, call := basisDiagnosticCall(t, true)
			shape, err := InspectCOWLeafRecordDiagnostic(f, ptr, cowTestLimits, call)
			originalShape, originalErr := InspectCOWLeafRecord(f, ptr, cowTestLimits)
			word := observer.Snapshot().First
			// Match the actual caller's later empty-shape error observation.
			call.Error(COWBasisInspect, err, shape, false)
			if err != originalErr || err != ErrCOWReadUnsupported || shape != (COWRecordShape{}) || originalShape != shape || (word>>33)&15 != uint64(COWBasisFrameContract) || (word>>37)&0x7ff != 0 || (word>>48)&7 != 7 || ((word>>51)&1 != 0) != fixture.compressed || ((word>>52)&1 != 0) != fixture.dictionary || (word>>53)&255 != uint64(fixture.rawCodec) || observer.Snapshot().First != word {
				t.Fatalf("frame refusal=%x later=%x error=%v/%v", word, observer.Snapshot().First, err, originalErr)
			}
		})
	}
	t.Run("actual_payload_refusal", func(t *testing.T) {
		f, ptr := cowTestLeafRecord(t, []byte{'T', 'M', 1, 1, 1, 1}, BlockCodecNone)
		observer, call := basisDiagnosticCall(t, false)
		shape, err := InspectCOWLeafRecordDiagnostic(f, ptr, cowTestLimits, call)
		word := observer.Snapshot().First
		if err != ErrCOWReadUnsupported || shape != (COWRecordShape{}) || (word>>33)&15 != uint64(COWBasisPayloadContract) || (word>>37)&0x7ff != 0 || (word>>48)&7 != 7 || word>>51 != 0 {
			t.Fatalf("payload refusal=%x error=%v", word, err)
		}
	})
	t.Run("actual_payload_no_frame_subset", func(t *testing.T) {
		value := []byte{'T', 'M', 1, 1, 1, 1}
		f, ptr, record := cowTestRecord(t, value, false, 0)
		local, err := cowShape(record[:HeaderSize], nil, record[:HeaderSize], uint32(len(value)), ptr, cowTestLimits)
		if err != nil {
			t.Fatal(err)
		}
		observer, call := basisDiagnosticCall(t, false)
		shape, err := inspectCOWValuePrefixDiagnostic(f, ptr, local, false, record, call)
		originalShape, originalErr := inspectCOWValuePrefix(f, ptr, local, false, record)
		word := observer.Snapshot().First
		if err != originalErr || err != ErrCOWReadUnsupported || shape != (COWRecordShape{}) || originalShape != shape || (word>>33)&15 != uint64(COWBasisPayloadContract) || (word>>37)&0x7ff != 0 || (word>>48)&7 != 1 || word>>51 != 0 {
			t.Fatalf("non-frame payload=%x error=%v/%v", word, err, originalErr)
		}
	})
}
