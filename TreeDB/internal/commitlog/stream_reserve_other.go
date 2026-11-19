//go:build !linux

package commitlog

type generationStreamReserve struct{}

func (target *CapturedSync) SyncGenerationWAL() error {
	return target.Sync()
}
