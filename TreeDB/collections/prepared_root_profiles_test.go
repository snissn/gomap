package collections

import (
	"errors"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/freelist"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
)

func acceptedPreparedPublicationBaseProfile() backenddb.PreparedRootPublicationBaseProfile {
	return backenddb.PreparedRootPublicationBaseProfile{
		FreelistCOW:  freelist.COWPrepareProfileV1{Valid: true, HighWater: 1},
		ValueLogRead: valuelog.PreparedReadProfile{MaxRecordBytes: 1},
	}
}

func TestPreparedInsertPublicationBaseRejectsHistoricalLeafCounts(t *testing.T) {
	for name, mutate := range map[string]func(*backenddb.PreparedRootPublicationBaseProfile){
		"generations": func(p *backenddb.PreparedRootPublicationBaseProfile) {
			p.LeafGenerations = preparedInsertPublisherMaxLeafGenerations + 1
		},
		"file IDs": func(p *backenddb.PreparedRootPublicationBaseProfile) {
			p.LeafGenerationFileIDs = preparedInsertPublisherMaxLeafFileIDs + 1
		},
	} {
		t.Run(name, func(t *testing.T) {
			profile := acceptedPreparedPublicationBaseProfile()
			mutate(&profile)
			if err := checkPreparedInsertPublicationBase(profile); !errors.Is(err, ErrPreparedInsertResourceLimit) {
				t.Fatalf("profile=%+v error=%v, want prepared resource limit", profile, err)
			}
		})
	}
}

func TestPreparedInsertPublicationBaseRejectsEveryCOWCount(t *testing.T) {
	over := preparedInsertPublisherMaxBasePages + 1
	for name, mutate := range map[string]func(*freelist.COWPrepareProfileV1){
		"high water":                  func(p *freelist.COWPrepareProfileV1) { p.HighWater = over },
		"retired pages":               func(p *freelist.COWPrepareProfileV1) { p.RetiredPages = over },
		"allocated pages":             func(p *freelist.COWPrepareProfileV1) { p.AllocatedPages = over },
		"abandoned append extents":    func(p *freelist.COWPrepareProfileV1) { p.AbandonedAppendExtents = over },
		"changed chunks":              func(p *freelist.COWPrepareProfileV1) { p.ChangedChunks = over },
		"replaced metadata pages":     func(p *freelist.COWPrepareProfileV1) { p.ReplacedMetadataPages = over },
		"base reservation extents":    func(p *freelist.COWPrepareProfileV1) { p.BaseReservationExtents = over },
		"base metadata pages":         func(p *freelist.COWPrepareProfileV1) { p.BaseMetadataPages = over },
		"ledger owners":               func(p *freelist.COWPrepareProfileV1) { p.LedgerOwners = over },
		"ledger candidates":           func(p *freelist.COWPrepareProfileV1) { p.LedgerCandidates = over },
		"ledger burned tail ranges":   func(p *freelist.COWPrepareProfileV1) { p.LedgerBurnedTailRanges = over },
		"ledger highest reserved end": func(p *freelist.COWPrepareProfileV1) { p.LedgerHighestReservedEnd = over },
	} {
		t.Run(name, func(t *testing.T) {
			profile := acceptedPreparedPublicationBaseProfile()
			mutate(&profile.FreelistCOW)
			if err := checkPreparedInsertPublicationBase(profile); !errors.Is(err, ErrPreparedInsertResourceLimit) {
				t.Fatalf("profile=%+v error=%v, want prepared resource limit", profile, err)
			}
		})
	}
}
