package main

import (
	"os/exec"
	"runtime"
)

func vectorPartitionCloneTreeCommandV1(source, target string) *exec.Cmd {
	sourceContents := source + "/."
	if runtime.GOOS == "darwin" {
		return exec.Command("cp", "-ac", sourceContents, target)
	}
	return exec.Command("cp", "-a", "--reflink=auto", sourceContents, target)
}
