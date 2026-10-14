package db

import (
	"github.com/snissn/gomap/TreeDB/internal/primaryarena"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

// PrimaryReadRootCertificateV6 borrows the producer's exact immutable bank.
// The caller must retain this Snapshot throughout every use of the certificate,
// its image and directory. This tuple check grants no durability, mutation or
// fresh content-integrity authority; bounded consumers validate the SAME image.
func (s *Snapshot) PrimaryReadRootCertificateV6() (*primaryarena.ReadRootCertificateV6, error) {
	if s == nil {
		return nil, ErrClosed
	}
	if err := s.beginRead(); err != nil {
		return nil, err
	}
	defer s.endRead()
	root := s.primaryRoot
	if root == nil || !root.arena.CapsuleFormatV6() {
		return nil, nil
	}
	cert := root.certificate
	if cert == nil || cert.Arena() != root.arena || cert.Reference() != root.ref {
		return nil, rootpublication.ErrResourceOwnership
	}
	return cert, nil
}
