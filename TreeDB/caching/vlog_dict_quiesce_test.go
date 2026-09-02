package caching

import (
	"context"
	"sync"
	"testing"

	"github.com/snissn/compress/zstd"
	"github.com/snissn/gomap/TreeDB/internal/compression"
)

type quiesceValueLogDictStore struct{}

func (quiesceValueLogDictStore) GetCurrent(context.Context) (uint64, error) {
	return 1, nil
}

func (quiesceValueLogDictStore) GetDictBytes(context.Context, uint64) ([]byte, error) {
	return []byte("published-dictionary"), nil
}

func TestQuiesceValueLogDictTrainingDetachesAndPreventsRestart(t *testing.T) {
	trainer := compression.NewTrainer(compression.TrainConfig{
		TrainBytes:     1 << 20,
		MinRecords:     100,
		MaxRecordBytes: 1024,
	}, compression.Config{Kind: compression.KindZSTD, Level: zstd.SpeedFastest}, false, false)
	if trainer == nil {
		t.Fatal("NewTrainer returned nil")
	}
	database := &DB{
		dictStore:           quiesceValueLogDictStore{},
		valueLogDictTrain:   compression.TrainConfig{TrainBytes: 1 << 20},
		valueLogDictTrainer: trainer,
	}
	database.valueLogDictTrainerByClass[vlogDictClassSingleValue] = trainer
	if !database.valueLogDictTrainingEnabled() {
		t.Fatal("test DB did not enable dictionary training")
	}

	var callers sync.WaitGroup
	for i := 0; i < 8; i++ {
		callers.Add(1)
		go func() {
			defer callers.Done()
			database.QuiesceValueLogDictTraining()
		}()
	}
	callers.Wait()
	if !database.valueLogDictQuiesced.Load() {
		t.Fatal("dictionary training was not marked quiesced")
	}
	if database.valueLogDictTrainer != nil || database.valueLogDictTrainerByClass[vlogDictClassSingleValue] != nil {
		t.Fatal("quiesced DB retained a dictionary trainer")
	}
	if trainer.ShouldCollect() {
		t.Fatal("detached dictionary trainer still collects samples")
	}

	database.ensureValueLogDictTrainer()
	if database.valueLogDictTrainer != nil || database.valueLogDictTrainerByClass[vlogDictClassSingleValue] != nil {
		t.Fatal("quiesced DB recreated a dictionary trainer")
	}
	// The lifecycle boundary is idempotent.
	database.QuiesceValueLogDictTraining()
}
