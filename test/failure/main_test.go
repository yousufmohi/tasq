//go:build failure

// Package failure injects real failures (SIGKILLed worker processes, Redis
// restarts, network faults via Toxiproxy) and checks after each one that no
// acknowledged job was lost, reporting the duplicate count.
package failure

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/yousufmo27/taskq/internal/testutil"
)

// workerBin is the race-enabled taskq-worker binary built for this run.
var workerBin string

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "taskq-failure-")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	workerBin = filepath.Join(dir, "taskq-worker")
	build := exec.Command("go", "build", "-race", "-o", workerBin, "./cmd/taskq-worker")
	build.Dir = "../.."
	build.Stdout, build.Stderr = os.Stdout, os.Stderr
	if err := build.Run(); err != nil {
		fmt.Fprintln(os.Stderr, "build taskq-worker:", err)
		os.Exit(1)
	}
	defer os.RemoveAll(dir)
	testutil.Main(m)
}
