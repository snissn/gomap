package mvcc

import (
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/freelist"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"testing"
	"unsafe"
)

// These fixed rows are diagnostic constructor admission envelopes. They are
// neither an identity registry nor an exact heap/transitive capacity census.
type selectedConstructorRowV6 struct {
	Charge             uint64
	Complete, Overflow bool
}

func (r *selectedConstructorRowV6) add(v uint64, w *iterator.OrdinalScanWork) bool {
	if w == nil || !w.Reserve(1, 32) {
		return false
	}
	if v > ^uint64(0)-r.Charge {
		r.Overflow = true
		r.Complete = false
		return true
	}
	r.Charge += v
	return true
}
func selectedLedgerQuantumV6() *iterator.OrdinalScanWork {
	return &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
}

// Fixed role order: candidate, member, cleared preparation resources, transferred
// proof, seal. Each equality is actual allocation equality observed under the
// original Set locks; values, byte charges and physical identities never dedup.
func selectedSetConstructorRowsV6(sets [5]rootpublication.NativeSetAllocationForTestV6, ready [5]bool, pairs [5][5]rootpublication.NativeSetAllocationForTestV6, pairReady [5][5]bool, transferred bool, w *iterator.OrdinalScanWork) (rows [2]selectedConstructorRowV6) {
	if w == nil || !w.Reserve(1, 2*uint64(unsafe.Sizeof(sets))+2*uint64(unsafe.Sizeof(pairs))+2*uint64(unsafe.Sizeof(rows))) {
		return rows
	}
	complete := transferred
	for i := range sets {
		if !w.Reserve(1, 2*uint64(unsafe.Sizeof(sets[i]))+8) {
			return rows
		}
		g := sets[i]
		expected := i != 2
		complete = complete && ready[i] && g.Present == expected
		if !g.Present {
			continue
		}
		complete = complete && g.Selected && g.Live && g.OriginalOwner && !g.OpaqueLogicalGraph && g.SetCharge != 0 && g.EntriesCapacity == 0 && g.KindCapacity == 0 && g.PhysicalCapacity == 0
		// These two original empty constructors have exactly one backing form.
		complete = complete && g.KindPresent != g.EmptyPresent && (g.KindPresent == (g.KindCharge != 0)) && (g.EmptyPresent == (g.EmptyCharge != 0)) && (g.EmptyPresent == (g.ExtrasRaw != 0))
		seenSet, seenKinds, seenEmpty := false, false, false
		for j := 0; j < i; j++ {
			if !w.Reserve(1, 2*uint64(unsafe.Sizeof(pairs[i][j]))+2*uint64(unsafe.Sizeof(sets[j]))+16) {
				return rows
			}
			if !sets[j].Present {
				continue
			}
			p := pairs[i][j]
			complete = complete && pairReady[i][j] && p.Present && p.SameOwner && p.SameSet == p.SameMetadata
			if p.SameMetadata {
				complete = complete && g.SetCharge == sets[j].SetCharge
				seenSet = true
			}
			if p.SameKinds {
				complete = complete && g.KindPresent && sets[j].KindPresent && g.KindCharge == sets[j].KindCharge
				seenKinds = true
			}
			if p.SameEmpty {
				complete = complete && g.EmptyPresent && sets[j].EmptyPresent && g.EmptyCharge == sets[j].EmptyCharge
				seenEmpty = true
			}
		}
		if !seenSet && !rows[0].add(g.SetCharge, w) {
			return rows
		}
		if g.KindPresent && !seenKinds && !rows[1].add(g.KindCharge, w) {
			return rows
		}
		if g.EmptyPresent && !seenEmpty && !rows[1].add(g.EmptyCharge, w) {
			return rows
		}
	}
	for i := range rows {
		rows[i].Complete = complete && !rows[i].Overflow
	}
	return rows
}

func testNativeSelectedConstructorLedgerV6(t *testing.T, c *PruneCursor) {
	t.Helper()
	admission := selectedLedgerQuantumV6()
	if c == nil || !admission.Reserve(1, 2*uint64(unsafe.Sizeof(PruneCursor{}))) {
		t.Log("selected constructor ledger incomplete: cursor observer refusal")
		return
	}
	var rows [21]selectedConstructorRowV6
	var retained [13]backenddb.NativeRetainedRowForTestV6
	var rr [13]bool
	var sets [5]rootpublication.NativeSetAllocationForTestV6
	var sr [5]bool
	var setPairs [5][5]rootpublication.NativeSetAllocationForTestV6
	var setPairReady [5][5]bool
	var indexes [2]backenddb.NativeIndexGeometryForTestV6
	var ir [2]bool
	// Each original accessor has its own unchanged Q32/1MiB diagnostic turn.
	// No cursor phase, original work/limit or production owner is replaced.
	for i := range retained {
		retained[i], rr[i] = c.preparation.MaintenanceNativeRetainedRowForTestV6(uint8(i), selectedLedgerQuantumV6())
	}
	for i := range sets {
		sets[i], sr[i] = c.preparation.MaintenanceNativeSetRowForTestV6(uint8(i), selectedLedgerQuantumV6())
	}
	for i := range sets {
		for j := 0; j < i; j++ {
			setPairs[i][j], setPairReady[i][j] = c.preparation.MaintenanceNativeSetPairForTestV6(uint8(i), uint8(j), selectedLedgerQuantumV6())
		}
	}
	for i := range indexes {
		indexes[i], ir[i] = c.preparation.MaintenanceNativeIndexGeometryForTestV6(i, 0, 0, 0, 0, selectedLedgerQuantumV6())
	}
	roles, roleReady := c.preparation.MaintenanceSelectedRoleFactsForTestV6(selectedLedgerQuantumV6())
	capture, capReady := c.capture.MaintenanceSelectedLedgerForTestV6(c.retention, selectedLedgerQuantumV6())
	capBase, capBaseReady := c.capture.MaintenanceNativeCaptureGeometryForTestV6(selectedLedgerQuantumV6())
	outer, outerReady := c.capture.MaintenanceNativeExternalGeometryForTestV6(c.retention, selectedLedgerQuantumV6())
	private, privateReady := c.preparation.MaintenanceNativePrivateLedgerForTestV6(selectedLedgerQuantumV6())
	frame, frameReady := c.preparation.MaintenanceNativeFrameGeometryForTestV6(0, freelist.NativeCOWGeometryForTestV6{}, selectedLedgerQuantumV6())
	transaction, trReady := c.preparation.MaintenanceNativeTransactionRowForTestV6(selectedLedgerQuantumV6())
	manifest, sameManifest, manifestReady := c.preparation.MaintenanceNativeManifestRowForTestV6(selectedLedgerQuantumV6())
	pair, pairReady := c.preparation.MaintenanceNativeTokenPairForTestV6(selectedLedgerQuantumV6())
	var tokens [2]rootpublication.NativeTokenLedgerForTestV6
	var tokReady [2]bool
	for i := range tokens {
		tokens[i], tokReady[i] = c.preparation.MaintenanceNativeTokenLedgerForTestV6(uint8(i), selectedLedgerQuantumV6())
	}
	// Admit the fixed local-role comparisons before examining those copies.
	comparisons := selectedLedgerQuantumV6()
	if !comparisons.Reserve(21, 2*uint64(unsafe.Sizeof(retained))+2*uint64(unsafe.Sizeof(sets))+2*uint64(unsafe.Sizeof(indexes))+2*uint64(unsafe.Sizeof(roles))+2*uint64(unsafe.Sizeof(private))+2*uint64(unsafe.Sizeof(capture))+2*uint64(unsafe.Sizeof(tokens))+2*uint64(unsafe.Sizeof(transaction))+2*uint64(unsafe.Sizeof(manifest))+1024) {
		t.Log("selected constructor ledger incomplete: comparison admission")
		return
	}
	// Prefix helpers only combine already admitted by-value facts. Every addition
	// is separately preadmitted, checked, and can leave the row incomplete.
	add := func(index int, complete bool, values ...uint64) {
		r := &rows[index]
		r.Complete = complete
		w := selectedLedgerQuantumV6()
		for _, v := range values {
			if !r.add(v, w) {
				r.Complete = false
				break
			}
		}
	}
	w := selectedLedgerQuantumV6()
	if w.Reserve(1, 2*uint64(unsafe.Sizeof(c.retainedBytes))) {
		add(0, c.retainedBytes != 0, c.retainedBytes)
	}
	add(1, outerReady && capBaseReady && capture.DistinctLeases && outer.Concrete && outer.SameBudget && !outer.Closed && capBase.LeasePresent && capBase.LeaseSameBudget, outer.HeaderCharge, capBase.WrapperLeaseHeader)
	add(2, rr[7] && retained[7].Present && !retained[7].OpaqueBacking && retained[7].Capacity == 0, retainedalloc.AllocationCharge(retained[7].Raw))
	en := capture.Enrollments
	add(3, capReady && capture.Stable && capture.ConstructorDetached && en.Complete, en.EnrollmentCharges[0], en.EnrollmentCharges[1], en.BindingCharges[0], en.BindingCharges[1], en.LeaseHeaderCharges[0], en.LeaseHeaderCharges[1])
	add(4, frameReady && frame.InputCharge != 0, frame.InputCharge)
	add(5, frameReady && frame.Charge != 0 && roleReady && roles.OutputOwnerBound && roles.OutputSameRegistry && roles.OutputPointerMapAbsent && roles.OutputScopeAbsent, frame.Charge)
	add(6, rr[5] && retained[5].SamePrimaryOwner && retained[5].Phase == 8, retained[5].Charge)
	add(7, privateReady && private.Complete, private.CloneHeader, private.AllocatedCharge, private.AbandonedCharge)
	rows[8].Complete = privateReady && private.Complete
	for _, v := range private.Controls {
		q := selectedLedgerQuantumV6()
		for _, amount := range [3]uint64{v.CurrentCharge, v.NewCharge, v.SnapshotCharge} {
			if !rows[8].add(amount, q) {
				rows[8].Complete = false
			}
		}
	}
	add(9, privateReady && private.Complete, private.SortCharges[0], private.SortCharges[1])
	add(10, trReady && transaction.Present && transaction.Native && transaction.CompletedPrepared && transaction.CandidateBound && transaction.ResourcesTransferred && transaction.TerminalPresent && transaction.SameResources && transaction.SamePromotion && transaction.SamePrimaryCertificate && transaction.PromotionIncluded && transaction.CleanupAbsent && !transaction.FloorOverflow && transaction.Charge >= transaction.ConstructorFloor && roleReady && roles.CandidateChargeTransferred, transaction.Charge)
	manifestComplete := manifestReady && sameManifest && manifest.Present && manifest.Selected && !manifest.Released && !manifest.OpaqueEntryGraph && manifest.OriginalOwner && manifest.Charge != 0
	add(11, rr[2] && retained[2].Present && retained[2].SamePrimaryOwner && retained[2].Charge != 0 && manifestComplete, retained[2].Charge, manifest.Charge)
	add(12, rr[3] && retained[3].Present && retained[3].SamePrimaryOwner && retained[3].Charge != 0, retained[3].Charge, retained[3].SecondaryCharge)
	setRows := selectedSetConstructorRowsV6(sets, sr, setPairs, setPairReady, roleReady && roles.ResourcesTransferred && roles.ProofTransferred, selectedLedgerQuantumV6())
	rows[13], rows[14] = setRows[0], setRows[1]
	selectedSets := rows[13].Complete && rows[14].Complete
	for i := 15; i < 21; i++ {
		rows[i].Complete = pairReady && pair.Present && roleReady && roles.DistinctSnapshots
	}
	for i, g := range indexes {
		q := selectedLedgerQuantumV6()
		complete := ir[i] && tokReady[i] && tokens[i].Complete && g.SameSnapshot && g.SameNativeCallback && g.SameSelectedPager && g.SameTokenOperations && g.SameRetainedCapture && g.SameMetadata && g.SameRegistry && g.SamePhysicalOwner && g.CustodyPresent && !g.CustodyComplete && rr[8+i] && retained[8+i].Present && !retained[8+i].OpaqueBacking && retained[8+i].Capacity == 0
		for j := 15; j < 21; j++ {
			rows[j].Complete = rows[j].Complete && complete
		}
		for _, v := range [3]uint64{g.WrapperCharge, g.OperationsCharge, g.CustodyCharge} {
			if !rows[15].add(v, q) {
				rows[15].Complete = false
			}
		}
		amounts := [5]uint64{g.Token.TokenCharge, tokens[i].ObligationCharge, g.Token.PhysicalCharge, g.Token.NamespaceCharge, tokens[i].PinStateCharge}
		same := [5]bool{pair.SameMetadata, pair.SameObligation, pair.SamePhysical, pair.SameNamespace, pair.SamePinState}
		for j, v := range amounts {
			if i == 0 || !same[j] {
				if !rows[16+j].add(v, q) {
					rows[16+j].Complete = false
				}
			}
		}
	}
	overall := roleReady && roles.Stable && roles.PrimaryRegistryDistinct && roles.SourceParentSame && roles.TargetDistinct && roles.TargetPresent && roles.ClaimRefundAbsent
	var total selectedConstructorRowV6
	total.Complete = overall
	for i, row := range rows {
		q := selectedLedgerQuantumV6()
		if !total.add(row.Charge, q) {
			total.Complete = false
		}
		total.Complete = total.Complete && row.Complete && !row.Overflow
		t.Logf("native-observer group=4 domain=selected-empty-constructor-admission-envelope row=P%02d geometry=%+v", i+1, row)
	}
	named := total
	for _, role := range [2]uint8{0, 2} {
		bank, source, parent, ready := c.preparation.MaintenanceNativeBankGeometryForTestV6(role, selectedLedgerQuantumV6())
		complete := ready && bank.Valid && bank.Sealed && bank.References > 0 && bank.ConstructorCovered && !bank.TableBackingOpaque
		if role == 0 {
			complete = complete && source && parent
		} else {
			complete = complete && !source && !parent
		}
		named.Complete = named.Complete && complete
		if !named.add(bank.MetadataCharge, selectedLedgerQuantumV6()) {
			named.Complete = false
		}
		t.Logf("native-observer group=4 domain=named-root-envelope row=H01 role=%d geometry=%+v complete=%v", role, bank, complete)
	}
	t.Logf("native-observer group=4 domain=selected-empty-constructor-admission-envelope P=%d PComplete=%v overflow=%v P_named_roots=%d namedComplete=%v namedOverflow=%v H02_H03Excluded=true noOwnerPayloadSum=true noExactHeapOrTransitiveClaim=true noAllowance=true manifestSubrow=%d manifestTransferred=%v private=%+v capture=%+v roleFacts=%+v tokenPair=%+v tokens=%+v", total.Charge, total.Complete, total.Overflow, named.Charge, named.Complete, named.Overflow, manifest.Charge, sameManifest, private, capture, roles, pair, tokens)
	// Complete means every specifically selected empty prerequisite was proven;
	// unsupported observed roles remain explicitly incomplete, never zero.
	if !total.Complete || !named.Complete {
		t.Fatal("expected selected-empty constructor ledger incomplete; see explicit row and role facts")
	}
	if total.Complete && (!private.Complete || !selectedSets || !manifestComplete || !tokens[0].Complete || !tokens[1].Complete) {
		t.Fatal("selected ledger completeness contradiction")
	}
}

func TestNativeSelectedConstructorLedgerCheckedArithmeticV6(t *testing.T) {
	r := selectedConstructorRowV6{Charge: 7, Complete: true}
	if r.add(1, &iterator.OrdinalScanWork{}) || r.Charge != 7 {
		t.Fatal("refusal mutated row")
	}
	if !r.add(9, selectedLedgerQuantumV6()) || r.Charge != 16 || !r.Complete {
		t.Fatal("admitted arithmetic")
	}
	r.Charge = ^uint64(0)
	if !r.add(1, selectedLedgerQuantumV6()) || !r.Overflow || r.Complete || r.Charge != ^uint64(0) {
		t.Fatal("overflow was accepted/wrapped")
	}
}

func TestNativeSelectedSetLedgerCombinationsV6(t *testing.T) {
	// These are already admitted by-value facts, not fake publication objects.
	// Original constructor identity production is covered in rootpublication/db.
	for _, empty := range []bool{false, true} {
		name := "kind-backing"
		if empty {
			name = "empty-backing"
		}
		t.Run(name, func(t *testing.T) {
			var sets [5]rootpublication.NativeSetAllocationForTestV6
			var ready [5]bool
			var pairs [5][5]rootpublication.NativeSetAllocationForTestV6
			var pairReady [5][5]bool
			// 0/1/4 share one backing, proof3 has distinct backing with equal bytes.
			backing := [5]int{1, 1, 0, 2, 1}
			for i := range sets {
				ready[i] = true
				if i == 2 {
					continue
				}
				g := &sets[i]
				g.Present = true
				g.Selected = true
				g.Live = true
				g.OriginalOwner = true
				g.SetCharge = 10
				if empty {
					g.EmptyPresent = true
					g.EmptyCharge = 20
					g.ExtrasRaw = 8
				} else {
					g.KindPresent = true
					g.KindCharge = 20
				}
				for j := 0; j < i; j++ {
					pairReady[i][j] = true
					if j == 2 {
						continue
					}
					pairs[i][j] = rootpublication.NativeSetAllocationForTestV6{Present: true, SameOwner: true, SameKinds: !empty && backing[i] == backing[j], SameEmpty: empty && backing[i] == backing[j]}
				}
			}
			evaluate := func(s [5]rootpublication.NativeSetAllocationForTestV6, r [5]bool, p [5][5]rootpublication.NativeSetAllocationForTestV6, pr [5][5]bool, transfer bool) [2]selectedConstructorRowV6 {
				return selectedSetConstructorRowsV6(s, r, p, pr, transfer, selectedLedgerQuantumV6())
			}
			rows := evaluate(sets, ready, pairs, pairReady, true)
			if !rows[0].Complete || !rows[1].Complete || rows[0].Charge != 40 || rows[1].Charge != 40 {
				t.Fatal("supported distinct wrappers/shared and independent backing", rows)
			}
			if rows = selectedSetConstructorRowsV6(sets, ready, pairs, pairReady, true, &iterator.OrdinalScanWork{}); rows[0].Complete || rows[1].Complete || rows[0].Charge != 0 || rows[1].Charge != 0 {
				t.Fatal("refusal accepted", rows)
			}
			checks := []string{"missing-proof", "unexpected-prep", "opaque", "no-owner", "no-live", "missing-backing", "unavailable-row", "unavailable-pair", "metadata-contradiction", "unequal-shared-charge", "transfer-incomplete", "overflow"}
			for _, which := range checks {
				t.Run(which, func(t *testing.T) {
					s, r, p, pr := sets, ready, pairs, pairReady
					transfer := true
					switch which {
					case "missing-proof":
						s[3] = rootpublication.NativeSetAllocationForTestV6{}
					case "unexpected-prep":
						s[2] = sets[0]
					case "opaque":
						s[3].OpaqueLogicalGraph = true
					case "no-owner":
						s[3].OriginalOwner = false
					case "no-live":
						s[3].Live = false
					case "missing-backing":
						s[3].KindPresent = false
						s[3].EmptyPresent = false
						s[3].KindCharge = 0
						s[3].EmptyCharge = 0
						s[3].ExtrasRaw = 0
					case "unavailable-row":
						r[3] = false
					case "unavailable-pair":
						pr[4][3] = false
					case "metadata-contradiction":
						p[4][1].SameMetadata = true
					case "unequal-shared-charge":
						if empty {
							s[4].EmptyCharge++
						} else {
							s[4].KindCharge++
						}
					case "transfer-incomplete":
						transfer = false
					case "overflow":
						s[3].SetCharge = ^uint64(0)
					}
					rows := evaluate(s, r, p, pr, transfer)
					if rows[0].Complete && rows[1].Complete {
						t.Fatal("unsupported shape accepted", rows)
					}
					if which == "overflow" && !rows[0].Overflow {
						t.Fatal("overflow not reported", rows)
					}
				})
			}
		})
	}
}
