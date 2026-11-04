package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
)

// CPU is the CPU use measured during one phase of a run. Every value is in
// cores: CPU-seconds consumed divided by wall-clock seconds, so 1.0 means one
// core kept fully busy.
//
//   - redis_main_thread: the Redis thread that runs commands and scripts (from
//     INFO cpu used_cpu_{user,sys}_main_thread). It can't exceed 1.0; a value
//     near 1.0 means Redis is saturated.
//   - redis_process: every Redis thread (main + background I/O and fsync).
//   - workers: all worker processes together (/proc/<pid>/stat).
//   - client: this benchmark process (producer, Stats polling).
//   - host_busy: the whole WSL2 VM (/proc/stat), which also runs Docker
//     Desktop's containers, out of host_cores.
//
// Mean values cover the whole phase. Median values are the median of the
// per-interval values, which is less affected by start-up and the final
// polling interval. Samples are the per-interval values themselves.
type CPU struct {
	WindowS   float64     `json:"window_s"`
	HostCores int         `json:"host_cores"`
	Mean      CPUCores    `json:"mean"`
	Median    CPUCores    `json:"median"`
	IntervalS float64     `json:"interval_s"`
	Samples   []CPUSample `json:"samples"`
}

// CPUCores holds one set of utilisation values in cores.
type CPUCores struct {
	RedisMainThread float64 `json:"redis_main_thread"`
	RedisProcess    float64 `json:"redis_process"`
	Workers         float64 `json:"workers"`
	Client          float64 `json:"client"`
	HostBusy        float64 `json:"host_busy"`
}

// CPUSample is one interval's utilisation, starting at T seconds into the
// phase.
type CPUSample struct {
	T float64 `json:"t_s"`
	CPUCores
}

// snap is a cumulative reading of every CPU counter.
type snap struct {
	at                    time.Time
	redisMain, redisTotal float64 // seconds
	workers, client       float64 // seconds
	hostBusy, hostAll     uint64  // jiffies
}

type sampler struct {
	rdb      *redis.Client
	pids     func() []int
	interval time.Duration
	tck      float64

	seen map[int]float64 // last CPU seconds of every worker pid ever seen

	mu    sync.Mutex
	snaps []snap
	err   error
	stop  chan struct{}
	done  chan struct{}
}

func clockTicks() float64 {
	out, err := exec.Command("getconf", "CLK_TCK").Output()
	if err == nil {
		if n, err := strconv.ParseFloat(strings.TrimSpace(string(out)), 64); err == nil && n > 0 {
			return n
		}
	}
	return 100
}

func startSampler(rdb *redis.Client, pids func() []int, interval time.Duration) *sampler {
	s := &sampler{rdb: rdb, pids: pids, interval: interval, tck: clockTicks(), seen: map[int]float64{},
		stop: make(chan struct{}), done: make(chan struct{})}
	s.take()
	go func() {
		defer close(s.done)
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-s.stop:
				s.take()
				return
			case <-t.C:
				s.take()
			}
		}
	}()
	return s
}

func (s *sampler) take() {
	sn, err := s.read()
	s.mu.Lock()
	defer s.mu.Unlock()
	if err != nil {
		if s.err == nil {
			s.err = err
		}
		return
	}
	s.snaps = append(s.snaps, sn)
}

func (s *sampler) read() (snap, error) {
	sn := snap{at: time.Now()}
	info, err := s.rdb.Info(context.Background(), "cpu").Result()
	if err != nil {
		return sn, err
	}
	f := map[string]float64{}
	for _, line := range strings.Split(info, "\n") {
		k, v, ok := strings.Cut(strings.TrimSpace(line), ":")
		if !ok {
			continue
		}
		if x, err := strconv.ParseFloat(v, 64); err == nil {
			f[k] = x
		}
	}
	if _, ok := f["used_cpu_user_main_thread"]; !ok {
		return sn, fmt.Errorf("INFO cpu has no used_cpu_user_main_thread")
	}
	sn.redisMain = f["used_cpu_user_main_thread"] + f["used_cpu_sys_main_thread"]
	sn.redisTotal = f["used_cpu_user"] + f["used_cpu_sys"]
	// A worker that exits (or is SIGKILLed) keeps its last reading, so its
	// CPU doesn't vanish from the total.
	for _, pid := range s.pids() {
		if v, ok := s.procCPU(pid); ok {
			s.seen[pid] = v
		}
	}
	for _, v := range s.seen {
		sn.workers += v
	}
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err == nil {
		sn.client = tv(ru.Utime) + tv(ru.Stime)
	}
	sn.hostBusy, sn.hostAll = hostJiffies()
	return sn, nil
}

func tv(t syscall.Timeval) float64 { return float64(t.Sec) + float64(t.Usec)/1e6 }

// procCPU returns utime+stime of pid in seconds; ok is false once the
// process is gone.
func (s *sampler) procCPU(pid int) (float64, bool) {
	b, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return 0, false
	}
	// The command name (field 2) may contain spaces; fields after the
	// closing parenthesis are fixed. utime and stime are fields 14 and 15.
	i := strings.LastIndexByte(string(b), ')')
	if i < 0 {
		return 0, false
	}
	fs := strings.Fields(string(b)[i+1:])
	if len(fs) < 13 || fs[0] == "Z" { // a zombie's counters are final but it is gone
		return 0, false
	}
	u, _ := strconv.ParseFloat(fs[11], 64)
	st, _ := strconv.ParseFloat(fs[12], 64)
	return (u + st) / s.tck, true
}

// hostJiffies returns busy and total jiffies from the first line of
// /proc/stat (busy excludes idle and iowait).
func hostJiffies() (busy, all uint64) {
	b, err := os.ReadFile("/proc/stat")
	if err != nil {
		return 0, 0
	}
	line, _, _ := strings.Cut(string(b), "\n")
	fs := strings.Fields(line)
	if len(fs) < 8 || fs[0] != "cpu" {
		return 0, 0
	}
	var idle uint64
	for i, v := range fs[1:] {
		if i >= 8 { // guest time is already counted in user
			break
		}
		n, _ := strconv.ParseUint(v, 10, 64)
		all += n
		if i == 3 || i == 4 { // idle, iowait
			idle += n
		}
	}
	return all - idle, all
}

func cores(a, b snap, tck float64, hostCores int) CPUCores {
	w := b.at.Sub(a.at).Seconds()
	if w <= 0 {
		return CPUCores{}
	}
	c := CPUCores{
		RedisMainThread: (b.redisMain - a.redisMain) / w,
		RedisProcess:    (b.redisTotal - a.redisTotal) / w,
		Workers:         (b.workers - a.workers) / w,
		Client:          (b.client - a.client) / w,
	}
	if all := b.hostAll - a.hostAll; all > 0 {
		c.HostBusy = float64(b.hostBusy-a.hostBusy) / float64(all) * float64(hostCores)
	}
	return c
}

// finish stops sampling and summarises the phase.
func (s *sampler) finish() (*CPU, error) {
	close(s.stop)
	<-s.done
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return nil, s.err
	}
	if len(s.snaps) < 2 {
		return nil, fmt.Errorf("cpu sampler: only %d readings", len(s.snaps))
	}
	host := hostCPUs()
	first, last := s.snaps[0], s.snaps[len(s.snaps)-1]
	c := &CPU{
		WindowS:   last.at.Sub(first.at).Seconds(),
		HostCores: host,
		IntervalS: s.interval.Seconds(),
		Mean:      cores(first, last, s.tck, host),
	}
	var m [5][]float64
	for i := 1; i < len(s.snaps); i++ {
		v := cores(s.snaps[i-1], s.snaps[i], s.tck, host)
		c.Samples = append(c.Samples, CPUSample{T: round3(s.snaps[i-1].at.Sub(first.at).Seconds()), CPUCores: v.rounded()})
		for k, x := range []float64{v.RedisMainThread, v.RedisProcess, v.Workers, v.Client, v.HostBusy} {
			m[k] = append(m[k], x)
		}
	}
	c.Median = CPUCores{median(m[0]), median(m[1]), median(m[2]), median(m[3]), median(m[4])}.rounded()
	c.Mean = c.Mean.rounded()
	c.WindowS = round3(c.WindowS)
	return c, nil
}

func hostCPUs() int {
	b, err := os.ReadFile("/proc/stat")
	if err != nil {
		return 0
	}
	n := 0
	for _, line := range strings.Split(string(b), "\n") {
		if strings.HasPrefix(line, "cpu") && len(line) > 3 && line[3] >= '0' && line[3] <= '9' {
			n++
		}
	}
	return n
}

func median(v []float64) float64 {
	if len(v) == 0 {
		return 0
	}
	v = slices.Clone(v)
	slices.Sort(v)
	if len(v)%2 == 1 {
		return v[len(v)/2]
	}
	return (v[len(v)/2-1] + v[len(v)/2]) / 2
}

func round3(x float64) float64 { return float64(int64(x*1000+0.5)) / 1000 }

func (c CPUCores) rounded() CPUCores {
	return CPUCores{round3(c.RedisMainThread), round3(c.RedisProcess), round3(c.Workers), round3(c.Client), round3(c.HostBusy)}
}
