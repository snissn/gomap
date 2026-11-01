//go:build linux

package ycsbevidence

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"syscall"
)

// Validate a phase caller before reserving evidence or observing its owner.
// Read start ticks again after hashing to reject PID reuse during the check.
func validateLiveProcess(p Process) error {
	if p.PID <= 0 || p.StartTicks == 0 || !lowerHex(p.EXESHA256, 64) {
		return errors.New("phase stats client identity mismatch")
	}
	statPath := fmt.Sprintf("/proc/%d/stat", p.PID)
	readTicks := func() (uint64, error) {
		raw, err := os.ReadFile(statPath)
		if err != nil {
			return 0, err
		}
		end := strings.LastIndexByte(string(raw), ')')
		if end < 0 {
			return 0, errors.New("malformed phase stats client procfs stat")
		}
		fields := strings.Fields(string(raw[end+1:]))
		if len(fields) <= 19 {
			return 0, errors.New("incomplete phase stats client procfs stat")
		}
		return strconv.ParseUint(fields[19], 10, 64)
	}
	ticks, err := readTicks()
	if err != nil || ticks != p.StartTicks {
		return errors.New("stale phase stats client process")
	}
	f, err := os.Open(fmt.Sprintf("/proc/%d/exe", p.PID))
	if err != nil {
		return err
	}
	h := sha256.New()
	_, readErr := io.Copy(h, f)
	if err = errors.Join(readErr, f.Close()); err != nil {
		return err
	}
	if hex.EncodeToString(h.Sum(nil)) != p.EXESHA256 {
		return errors.New("phase stats client executable mismatch")
	}
	ticks, err = readTicks()
	if err != nil || ticks != p.StartTicks {
		return errors.New("phase stats client process changed during validation")
	}
	return nil
}

func processIdentity(dir string) (Process, uint64, uint64, error) {
	raw, err := os.ReadFile("/proc/self/stat")
	if err != nil {
		return Process{}, 0, 0, err
	}
	fields := strings.Fields(string(raw[strings.LastIndexByte(string(raw), ')')+1:]))
	ticks, err := strconv.ParseUint(fields[19], 10, 64)
	if err != nil {
		return Process{}, 0, 0, err
	}
	hash, err := hashExe()
	if err != nil {
		return Process{}, 0, 0, err
	}
	var stat syscall.Stat_t
	if err = syscall.Stat(dir, &stat); err != nil {
		return Process{}, 0, 0, err
	}
	_, set := os.LookupEnv("GOMAXPROCS")
	return Process{os.Getpid(), ticks, hash, !set}, uint64(stat.Dev), stat.Ino, nil
}
