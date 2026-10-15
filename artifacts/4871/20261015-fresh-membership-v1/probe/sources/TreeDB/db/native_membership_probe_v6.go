//go:build treedb_test

package db

import (
	"crypto/sha256"
	"errors"
	"github.com/snissn/gomap/TreeDB/freelist"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/primaryarena"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

// This diagnostic deliberately uses unlimited ordinary allocation/sealing. It
// proves producer incorporation, NOT bounded storage or the Q32 final turn.
// The retained Snapshot and original live allocator transaction own the page.
func (s *Snapshot) AllocateMembershipLeafForTest() (uint64, error) {
	if err := s.beginRead(); err != nil {
		return 0, err
	}
	defer s.endRead()
	d := s.db
	d.writeMu.Lock()
	defer d.writeMu.Unlock()
	d.durablePublishMu.Lock()
	defer d.durablePublishMu.Unlock()
	if d.idx.Load() != s.idx {
		return 0, ErrClosed
	}
	id, err := s.idx.allocator.AllocAppend()
	if err != nil {
		return 0, err
	}
	image := make([]byte, page.PageSize)
	b := node.NewBuilderWithOptions(image, page.PageTypeLeaf, node.BuilderOptions{EntryRevisions: true})
	b.SetPageID(id)
	if err = b.AddLeafEntryWithRevision([]byte("private"), []byte("survivor"), 0, page.ValuePtr{}, 1); err != nil {
		return 0, err
	}
	b.FinishNoNode()
	return id, s.idx.pager.Write(id, image)
}

// BindMembershipLeafForTest prepares and aborts a fresh ORIGINAL allocator
// candidate within one original writer turn, after the public writer has run.
// No prepared candidate is retained across a public call or yield.
func (s *Snapshot) BindMembershipLeafForTest(id uint64, stale, corrupt bool) (generation uint64, records uint64, err error) {
	if err = s.beginRead(); err != nil {
		return
	}
	defer s.endRead()
	d := s.db
	d.writeMu.Lock()
	defer d.writeMu.Unlock()
	d.durablePublishMu.Lock()
	defer d.durablePublishMu.Unlock()
	if d.idx.Load() != s.idx {
		err = ErrClosed
		return
	}
	a := s.idx.allocator
	live := a.COWGenerationV1()
	cap, e := d.durableRootReuseCapabilityV1(d.durableRoot)
	if e != nil {
		err = e
		return
	}
	seq := d.meta.CommitSeq + 1
	candidate, e := a.PrepareCOWCandidateV1(live.GenerationID()+1, seq, freelist.CandidateIDV1{0xfc, byte(seq), byte(seq >> 8)}, cap, 0, freelist.NewCandidatePageSinkV1())
	if e != nil {
		err = e
		return
	}
	defer func() { err = errors.Join(err, a.AbortCOWCandidateV1(candidate)) }()
	g := candidate.Candidate().Generation()
	generation = g.GenerationID()
	w := &iterator.OrdinalScanWork{RecordLimit: ^uint64(0), ByteLimit: ^uint64(0)}
	constructor, ready, e := rootpublication.NewPrimaryConstructionTransactionV6(s.idx.primary, w)
	if !ready || e != nil {
		err = errors.Join(e, rootpublication.ErrInvalidCandidate)
		return
	}
	txn := constructor.(*rootpublication.DurableRootTransaction)
	defer func() { err = errors.Join(err, txn.Abort()) }()
	image, e := s.idx.pager.ReadPage(id)
	if e != nil {
		err = e
		return
	}
	directory := make([]byte, page.PageSize)
	ref := txn.PrimaryConstructionReferenceV6()
	if e = node.EncodePrimaryDirectory(directory, ref.PageID, seq, node.PrimaryOperand{Ref: page.PageChildRef(id), Digest: sha256.Sum256(image)}, nil); e != nil {
		err = e
		return
	}
	if ready, e = txn.SealPrimaryConstructionV6(directory, [1]*primaryarena.Group{}, w); !ready || e != nil {
		err = errors.Join(e, rootpublication.ErrInvalidCandidate)
		return
	}
	if stale {
		g = live
	}
	if corrupt {
		damaged := append([]byte(nil), image...)
		damaged[len(damaged)-1] ^= 1
		if e = s.idx.pager.Write(id, damaged); e != nil {
			err = e
			return
		}
		defer func() { err = errors.Join(err, s.idx.pager.Write(id, image)) }()
	}
	proofWork := &iterator.OrdinalScanWork{RecordLimit: ^uint64(0), ByteLimit: ^uint64(0)}
	prepared, ready, e := rootpublication.NewPreparedPrimaryReadRootCertificateV6(txn.PrimaryConstructionCertificateV6(), g, candidate, rootpublication.NewFrontier(seq, ref.PageID, d.meta.SystemRootPageID, 0, 1), s.idx.pager, nil, proofWork)
	records = proofWork.Records
	if e != nil {
		err = e
		return
	}
	if !ready || prepared == nil {
		err = rootpublication.ErrInvalidCandidate
		return
	}
	return
}

func (s *Snapshot) RetireMembershipLeafForTest(id uint64) error {
	if err := s.beginRead(); err != nil {
		return err
	}
	defer s.endRead()
	d := s.db
	d.writeMu.Lock()
	defer d.writeMu.Unlock()
	d.durablePublishMu.Lock()
	defer d.durablePublishMu.Unlock()
	return s.idx.allocator.RetireCOWV1([]uint64{id}, d.meta.CommitSeq)
}
