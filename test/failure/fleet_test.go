//go:build failure

package failure

import (
	"bufio"
	"fmt"
	"math/rand/v2"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"
)

// proc is one taskq-worker OS process.
type proc struct {
	cmd  *exec.Cmd
	log  string
	done chan struct{}
}

func startProc(t *testing.T, logDir string, n int, args []string) *proc {
	t.Helper()
	logPath := filepath.Join(logDir, fmt.Sprintf("worker-%03d.log", n))
	f, err := os.Create(logPath)
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(workerBin, args...)
	cmd.Stdout, cmd.Stderr = f, f
	if err := cmd.Start(); err != nil {
		f.Close()
		t.Fatal(err)
	}
	p := &proc{cmd: cmd, log: logPath, done: make(chan struct{})}
	go func() {
		_ = cmd.Wait()
		f.Close()
		close(p.done)
	}()
	return p
}

func (p *proc) exited() bool {
	select {
	case <-p.done:
		return true
	default:
		return false
	}
}

// fleet keeps n worker processes alive, restarting any that exit (killed
// by the test or by a poison-pill job), until stop.
type fleet struct {
	t      *testing.T
	args   []string
	logDir string

	mu      sync.Mutex
	procs   []*proc
	all     []*proc
	started int
	kills   int
	stopped bool
	quit    chan struct{}
	wg      sync.WaitGroup
}

func newFleet(t *testing.T, n int, args ...string) *fleet {
	f := &fleet{t: t, args: args, logDir: t.TempDir(), quit: make(chan struct{})}
	for range n {
		f.spawn()
	}
	f.wg.Add(1)
	go f.supervise()
	t.Cleanup(f.stop)
	return f
}

func (f *fleet) spawn() {
	f.mu.Lock()
	defer f.mu.Unlock()
	p := startProc(f.t, f.logDir, f.started, f.args)
	f.started++
	f.procs = append(f.procs, p)
	f.all = append(f.all, p)
}

func (f *fleet) supervise() {
	defer f.wg.Done()
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-f.quit:
			return
		case <-tick.C:
		}
		f.mu.Lock()
		var dead int
		live := f.procs[:0]
		for _, p := range f.procs {
			if p.exited() {
				dead++
			} else {
				live = append(live, p)
			}
		}
		f.procs = live
		f.mu.Unlock()
		for range dead {
			f.spawn()
		}
	}
}

// killRandom SIGKILLs one live worker and reports how many there were.
func (f *fleet) killRandom() {
	f.mu.Lock()
	defer f.mu.Unlock()
	var live []*proc
	for _, p := range f.procs {
		if !p.exited() {
			live = append(live, p)
		}
	}
	if len(live) == 0 {
		return
	}
	p := live[rand.IntN(len(live))]
	if err := p.cmd.Process.Signal(syscall.SIGKILL); err == nil {
		f.kills++
	}
}

// stop ends supervision, SIGTERMs every process and waits for them; any
// still running after 30s is killed. On test failure it dumps log tails.
func (f *fleet) stop() {
	f.mu.Lock()
	if f.stopped {
		f.mu.Unlock()
		return
	}
	f.stopped = true
	f.mu.Unlock()
	close(f.quit)
	f.wg.Wait()

	f.mu.Lock()
	procs := append([]*proc(nil), f.procs...)
	all := append([]*proc(nil), f.all...)
	f.mu.Unlock()
	for _, p := range procs {
		_ = p.cmd.Process.Signal(syscall.SIGTERM)
	}
	for _, p := range procs {
		select {
		case <-p.done:
		case <-time.After(30 * time.Second):
			f.t.Errorf("worker pid %d did not exit on SIGTERM; killing", p.cmd.Process.Pid)
			_ = p.cmd.Process.Kill()
			<-p.done
		}
	}
	if f.t.Failed() {
		for _, p := range all {
			f.t.Logf("--- %s (tail) ---\n%s", filepath.Base(p.log), tail(p.log, 15))
		}
	}
}

func tail(path string, n int) string {
	fh, err := os.Open(path)
	if err != nil {
		return err.Error()
	}
	defer fh.Close()
	var lines []string
	s := bufio.NewScanner(fh)
	s.Buffer(make([]byte, 1<<20), 1<<20)
	for s.Scan() {
		lines = append(lines, s.Text())
		if len(lines) > n {
			lines = lines[1:]
		}
	}
	out := ""
	for _, l := range lines {
		out += l + "\n"
	}
	return out
}
