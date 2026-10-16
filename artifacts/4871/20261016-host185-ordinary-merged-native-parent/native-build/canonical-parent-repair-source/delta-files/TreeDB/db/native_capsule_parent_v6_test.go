package db

import (
	"crypto/sha256"
	"fmt"
	"reflect"
	"testing"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/freelist"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/primaryarena"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

// The real encoder/decoder supplies both operands. No DB, allocator, owner,
// builder, retained resource, physical file or publication is constructed.
func nativeParentCodecFixtureV6(t *testing.T, seq uint64) (rootpublication.DurablePrimaryRootRecordV5, []byte) {
	t.Helper()
	baseDigest := [32]byte{1}
	image := make([]byte, page.PageSize)
	id := primaryarena.Namespace + 20
	if err := node.EncodePrimaryDirectory(image, id, 1, node.PrimaryOperand{Ref: page.PageChildRef(2), Digest: baseDigest}, nil); err != nil {
		t.Fatal(err)
	}
	return rootpublication.DurablePrimaryRootRecordV5{
		Record: rootpublication.DurableRootRecordV1{CommitSeq: seq, DurableSeq: seq, UserRootPageID: id, SystemRootPageID: 3, TotalPages: 64,
			Freelist:  freelist.GenerationRefV1{HeaderPageID: 4, GenerationID: 1, CommitSeq: 1, HighWater: 64, Digest: [32]byte{1}},
			Directory: rootpublication.DependencyDirectoryRefV2{RootPageID: primaryarena.Namespace + 8}, MetaProjectionDigest: [32]byte{1}},
		Primary: rootpublication.PrimaryProjectionV5{ArenaUUID: [16]byte{9}, ArenaHighWater: 21, DirectoryDigest: sha256.Sum256(image), BaseRootPageID: 2, BaseSequence: 1, DataCommitSeq: 1, BaseDigest: baseDigest, SystemDigest: [32]byte{2}},
	}, image
}

func TestNativeCapsuleParentRecordProjectionV6(t *testing.T) {
	for baseSlot := uint64(0); baseSlot < 2; baseSlot++ {
		t.Run(fmt.Sprintf("base-slot-%d", baseSlot), func(t *testing.T) {
			original, directory := nativeParentCodecFixtureV6(t, 1)
			child, childDirectory := nativeParentCodecFixtureV6(t, 2)
			baseImage := make([]byte, rootpublication.PrimaryCapsuleSizeV6)
			if err := rootpublication.EncodePrimaryCapsuleV6(baseImage, baseSlot, original, directory, nil, nil); err != nil {
				t.Fatal(err)
			}
			baseView, err := rootpublication.DecodePrimaryCapsuleV6(baseImage, baseSlot, original.Primary.ArenaUUID)
			if err != nil {
				t.Fatal(err)
			}
			baseOperand := baseView.Current()
			image := make([]byte, rootpublication.PrimaryCapsuleSizeV6)
			target := 1 - baseSlot
			if err := rootpublication.EncodePrimaryCapsuleV6(image, target, child, childDirectory, &baseOperand, baseView.Directory()); err != nil {
				t.Fatal(err)
			}
			view, err := rootpublication.DecodePrimaryCapsuleV6(image, target, original.Primary.ArenaUUID)
			if err != nil {
				t.Fatal(err)
			}
			base, embedded, current := baseOperand.Record, view.Parent().Record, view.Current().Record
			savedBase, savedEmbedded := base, embedded
			if base == embedded || !nativeCapsuleParentRecordMatchesV6(&base, &embedded, target) {
				t.Fatal("codec parent relocation rejected")
			}
			if current.UserRootPageID != rootpublication.PrimaryCapsulePageV6(target)+1 || embedded.UserRootPageID != rootpublication.PrimaryCapsulePageV6(target)+2 {
				t.Fatal("current/parent placement changed")
			}
			if nativeCapsuleParentRecordMatchesV6(&base, &current, target) || nativeCapsuleParentRecordMatchesV6(&base, &embedded, baseSlot) {
				t.Fatal("wrong slot/current operand accepted")
			}
			wrongAddress := embedded
			wrongAddress.UserRootPageID = rootpublication.PrimaryCapsulePageV6(target) + 1
			if nativeCapsuleParentRecordMatchesV6(&base, &wrongAddress, target) {
				t.Fatal("current directory address accepted for parent")
			}
			// Every scalar and every digest byte of the complete encoded record is
			// checked independently. Nested freelist/manifest/directory fields count too.
			var visit func(reflect.Value, string)
			visit = func(v reflect.Value, name string) {
				switch v.Kind() {
				case reflect.Struct:
					for i := 0; i < v.NumField(); i++ {
						visit(v.Field(i), name+"."+v.Type().Field(i).Name)
					}
				case reflect.Array:
					for i := 0; i < v.Len(); i++ {
						visit(v.Index(i), fmt.Sprintf("%s[%d]", name, i))
					}
				default:
					old := v.Uint()
					v.SetUint(old ^ 1)
					if nativeCapsuleParentRecordMatchesV6(&base, &embedded, target) {
						t.Errorf("accepted mismatch %s", name)
					}
					v.SetUint(old)
				}
			}
			visit(reflect.ValueOf(&embedded).Elem(), "Record")
			// Source ancestry is deliberately absent from the encoded parent. It must
			// be canonicalized, while noncanonical embedded ancestry above is rejected.
			ancestry := base
			ancestry.ParentRecordPageID, ancestry.ParentCommitSeq, ancestry.ParentRecordDigest = 17, 3, [32]byte{7}
			if !nativeCapsuleParentRecordMatchesV6(&ancestry, &embedded, target) {
				t.Fatal("source ancestry was treated as encoded authority")
			}
			if base != savedBase || embedded != savedEmbedded {
				t.Fatal("projection mutated operands")
			}
		})
	}
	t.Run("invalid-target-and-nil", func(t *testing.T) {
		base := rootpublication.DurableRootRecordV1{}
		for _, target := range []uint64{2, ^uint64(0)} {
			if nativeCapsuleParentRecordMatchesV6(&base, &base, target) {
				t.Fatal("invalid target accepted", target)
			}
		}
		if nativeCapsuleParentRecordMatchesV6(nil, &base, 0) || nativeCapsuleParentRecordMatchesV6(&base, nil, 0) {
			t.Fatal("nil record accepted")
		}
	})
	t.Run("pricing-and-pre-effect-refusal", func(t *testing.T) {
		r, b := nativeCapsuleParentRecordWorkV6()
		if r != 4 || b != 4*uint64(unsafe.Sizeof(rootpublication.DurableRootRecordV1{}))+80 {
			t.Fatal("projection traffic differs", r, b)
		}
		// The actual final-basis gate refuses before inspecting an unprepared zero
		// cursor when its initial header+projection admission cannot fit.
		c := NativeMVCCCurrentDeleteV6{}
		initialBytes := b + uint64(unsafe.Sizeof(c)) + 2*uint64(unsafe.Sizeof(rootPublicationSealV1{})) + 2*uint64(unsafe.Sizeof(rootpublication.DurableRootRecordV1{})) + 2*uint64(unsafe.Sizeof(page.DurableMetaV1{})) + 2*uint64(unsafe.Sizeof(primaryStateRootV5{})) + uint64(unsafe.Sizeof(DBState{}))
		for _, w := range []iterator.OrdinalScanWork{{RecordLimit: 4 + r, ByteLimit: initialBytes, Records: 1}, {RecordLimit: 4 + r, ByteLimit: initialBytes, Bytes: 1}} {
			before := w
			if _, ready, err := c.prepareNativeSealInstallationV6(&w); ready || err != nil || w != before {
				t.Fatal("final basis refusal changed state", ready, err, w)
			}
		}
		// Exercise the sibling's actual admission/unwind using only empty in-memory
		// state. No candidate is activated and no authority operation is reachable.
		runtime := rootPublicationRuntimeV1{db: &DB{}}
		seal := rootPublicationSealV1{}
		ar, ab, ok := checkedNativeAuthorityAdmissionV6(0, 0, 0, 1)
		ur, ub := nativeAuthorityAdmissionV6(0, 0, 0, 1)
		if !ok || ar != ur || ab != ub || ar != 17 {
			t.Fatal("authority price diverged", ar, ab, ur, ub)
		}
		for _, w := range []iterator.OrdinalScanWork{{RecordLimit: ar, ByteLimit: ab, Records: 1}, {RecordLimit: ar, ByteLimit: ab, Bytes: 1}} {
			before := w
			scope := nativeAuthorityScopeV6{}
			if ready, err := scope.begin(&runtime, &seal, &w); ready || err != nil || w != before || scope.active || scope.runtime != nil || runtime.activeSeal != nil {
				t.Fatal("authority refusal failed to unwind", ready, err, w)
			}
		}
		t.Logf("projection records=%d bytes=%d per gate; fixed F=289 R=145; no retained allocation", r, b)
	})
}
