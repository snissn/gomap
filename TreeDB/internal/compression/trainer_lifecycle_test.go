package compression

import (
	"testing"
	"time"

	"github.com/snissn/compress/zstd"
)

func TestTrainerWaitDrainsClosedWorker(t *testing.T) {
	trainer := NewTrainer(TrainConfig{
		TrainBytes:     1 << 20,
		MinRecords:     100,
		MaxRecordBytes: 1024,
	}, Config{Kind: KindZSTD, Level: zstd.SpeedFastest}, false, false)
	if trainer == nil {
		t.Fatal("NewTrainer returned nil")
	}
	for i := 0; i < 32; i++ {
		trainer.Collect([]byte("authoritative-resource-trainer-drain"))
	}
	trainer.Close()
	done := make(chan struct{})
	go func() {
		trainer.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Trainer.Wait did not observe worker shutdown")
	}
	if trainer.ShouldCollect() {
		t.Fatal("closed trainer still collects samples")
	}
}

func TestTrainerWaitIncludesSpawnedTrainingJobs(t *testing.T) {
	trainer := NewTrainer(TrainConfig{
		TrainBytes:     1 << 20,
		MinRecords:     100,
		MaxRecordBytes: 1024,
	}, Config{Kind: KindZSTD, Level: zstd.SpeedFastest}, false, false)
	if trainer == nil {
		t.Fatal("NewTrainer returned nil")
	}

	// Model a job already spawned by appendSample. The queue worker owns Add,
	// so closing and draining sampleCh guarantees there can be no later Add when
	// run reaches its Wait.
	jobRelease := make(chan struct{})
	trainer.trainWG.Add(1)
	go func() {
		defer trainer.trainWG.Done()
		<-jobRelease
	}()
	trainer.Close()

	waitDone := make(chan struct{})
	go func() {
		trainer.Wait()
		close(waitDone)
	}()
	select {
	case <-waitDone:
		t.Fatal("Trainer.Wait returned while a spawned training job was active")
	case <-time.After(20 * time.Millisecond):
	}
	close(jobRelease)
	select {
	case <-waitDone:
	case <-time.After(5 * time.Second):
		t.Fatal("Trainer.Wait did not observe spawned training job completion")
	}
}
