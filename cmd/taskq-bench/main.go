// Command taskq-bench runs reproducible load tests against a dedicated
// Redis and writes a JSON summary plus raw per-job timings.
//
// Modes:
//
//	drain     enqueue -jobs first, then start -workers processes and measure
//	          how fast they drain the backlog (consume throughput).
//	latency   start the workers, then enqueue open-loop at -rate jobs/s for
//	          -duration; report end-to-end latency percentiles and the
//	          producer's schedule lag (to show there was no coordinated
//	          omission).
//	recovery  like latency, but SIGKILL -kill workers after -warmup and
//	          measure how long their orphaned jobs take to finish.
//	summarize print a Markdown table of every summary JSON under -out.
//	rtt       print the PING round-trip time to -redis as JSON.
//
// Every timestamp used in a result comes from Redis (job created/started/
// finished), so the producer's and workers' clocks never mix.
package main

import (
	"compress/gzip"
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/yousufmohi/tasq"
	"github.com/yousufmohi/tasq/internal/demo"
	"github.com/yousufmohi/tasq/internal/keys"
)

// Config is recorded verbatim in every result.
type Config struct {
	Name        string  `json:"name"`
	Mode        string  `json:"mode"`
	Workers     int     `json:"workers"`
	Concurrency int     `json:"concurrency"`
	Jobs        int     `json:"jobs,omitempty"`
	Rate        float64 `json:"rate,omitempty"`
	DurationS   float64 `json:"duration_s,omitempty"`
	SleepMS     int     `json:"handler_sleep_ms"`
	Visibility  string  `json:"visibility_timeout"`
	Kill        int     `json:"kill,omitempty"`
	WarmupS     float64 `json:"warmup_s,omitempty"`
	Rep         int     `json:"rep"`
}

// Pct holds latency percentiles in milliseconds (nearest-rank, from every
// raw sample; no histogram buckets).
type Pct struct {
	N    int     `json:"n"`
	P50  float64 `json:"p50_ms"`
	P95  float64 `json:"p95_ms"`
	P99  float64 `json:"p99_ms"`
	Max  float64 `json:"max_ms"`
	Mean float64 `json:"mean_ms"`
}

// Result is one run's summary.
type Result struct {
	Config     Config  `json:"config"`
	Succeeded  int     `json:"succeeded"`
	NotDone    int     `json:"not_done"`
	Throughput float64 `json:"throughput_jobs_per_s"`
	// EndToEnd: finished - created. QueueWait: started - run_at.
	EndToEnd  *Pct `json:"end_to_end,omitempty"`
	QueueWait *Pct `json:"queue_wait,omitempty"`
	// Open-loop producer health.
	OfferedRate      float64   `json:"offered_rate,omitempty"`
	AchievedEnqueue  float64   `json:"achieved_enqueue_rate,omitempty"`
	ProducerMaxLagMS float64   `json:"producer_max_schedule_lag_ms,omitempty"`
	ProducerP99LagMS float64   `json:"producer_p99_schedule_lag_ms,omitempty"`
	EnqueueErrors    int       `json:"enqueue_errors"`
	DuplicateStarts  int       `json:"jobs_started_more_than_once"`
	Recovery         *Recovery `json:"recovery,omitempty"`
	RawFile          string    `json:"raw_file"`
	RecordedAt       string    `json:"recorded_at"`
}

// Recovery describes a recovery-mode run.
type Recovery struct {
	Killed   int `json:"workers_killed"`
	Orphaned int `json:"orphaned_jobs"`
	// TimeMS: from the kill (Redis clock) until the last orphaned job
	// finished. Expected ≈ visibility timeout + reclaim interval + work.
	TimeMS float64 `json:"recovery_time_ms"`
	// Orphan end-to-end latency percentiles.
	OrphanEndToEnd *Pct `json:"orphan_end_to_end,omitempty"`
}

type sample struct {
	id                                string
	created, runAt, started, finished int64
	attempts                          int
	state                             string
}

func main() {
	var (
		redisAddr = flag.String("redis", "localhost:6390", "dedicated benchmark Redis")
		workerBin = flag.String("worker-bin", "bench/bin/taskq-worker", "taskq-worker binary (build without -race)")
		out       = flag.String("out", "bench/results/dev", "output directory")
		c         Config
		duration  time.Duration
		warmup    time.Duration
		vis       time.Duration
	)
	flag.StringVar(&c.Mode, "mode", "drain", "drain | latency | recovery | summarize | rtt")
	flag.StringVar(&c.Name, "name", "", "result name (default derived from flags)")
	flag.IntVar(&c.Workers, "workers", 1, "worker processes")
	flag.IntVar(&c.Concurrency, "concurrency", 16, "jobs in flight per worker")
	flag.IntVar(&c.Jobs, "jobs", 20000, "drain: jobs to enqueue")
	flag.Float64Var(&c.Rate, "rate", 1000, "latency/recovery: offered jobs per second")
	flag.DurationVar(&duration, "duration", 20*time.Second, "latency/recovery: producer duration")
	flag.IntVar(&c.SleepMS, "sleep-ms", 0, "handler work per job (ms)")
	flag.DurationVar(&vis, "visibility", 5*time.Second, "worker visibility timeout")
	flag.IntVar(&c.Kill, "kill", 1, "recovery: workers to SIGKILL")
	flag.DurationVar(&warmup, "warmup", 5*time.Second, "recovery: time before the kill")
	flag.IntVar(&c.Rep, "rep", 1, "repetition number (recorded)")
	flag.Parse()
	c.DurationS, c.WarmupS, c.Visibility = duration.Seconds(), warmup.Seconds(), vis.String()

	if c.Mode == "summarize" {
		if err := summarize(*out); err != nil {
			fail(err)
		}
		return
	}
	if c.Mode == "rtt" {
		if err := rtt(*redisAddr); err != nil {
			fail(err)
		}
		return
	}
	if c.Name == "" {
		c.Name = fmt.Sprintf("%s_w%d_c%d_s%d", c.Mode, c.Workers, c.Concurrency, c.SleepMS)
		if c.Mode != "drain" {
			c.Name += fmt.Sprintf("_r%g", c.Rate)
		}
		if c.Mode == "recovery" {
			c.Name += "_vt" + vis.String()
		}
		c.Name += fmt.Sprintf("_rep%d", c.Rep)
	}
	b := &bench{cfg: c, addr: *redisAddr, bin: *workerBin, vis: vis}
	b.rdb = redis.NewClient(&redis.Options{Addr: *redisAddr, PoolSize: 128})
	defer b.rdb.Close()
	b.client = taskq.NewClient(b.rdb)
	b.q = fmt.Sprintf("bench-%s-%d", strings.ReplaceAll(c.Name, ".", "_"), time.Now().UnixNano())

	var res Result
	var err error
	switch c.Mode {
	case "drain":
		res, err = b.drain()
	case "latency":
		res, err = b.openLoop(duration, 0, 0)
	case "recovery":
		res, err = b.openLoop(duration, warmup, c.Kill)
	default:
		err = fmt.Errorf("unknown mode %q", c.Mode)
	}
	b.stopWorkers()
	if err != nil {
		fail(err)
	}
	res.Config = c
	res.RecordedAt = time.Now().UTC().Format(time.RFC3339)
	if err := b.write(*out, &res); err != nil {
		fail(err)
	}
	j, _ := json.MarshalIndent(res, "", "  ")
	fmt.Println(string(j))
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "taskq-bench:", err)
	os.Exit(1)
}

type bench struct {
	cfg     Config
	addr    string
	bin     string
	vis     time.Duration
	rdb     *redis.Client
	client  *taskq.Client
	q       string
	procs   []*exec.Cmd
	ids     []string
	idsMu   sync.Mutex
	samples []sample
}

func (b *bench) payload() []byte { return demo.Payload{SleepMS: b.cfg.SleepMS}.Encode() }

func (b *bench) startWorkers() error {
	for range b.cfg.Workers {
		cmd := exec.Command(b.bin,
			"-redis", b.addr, "-queue", b.q,
			"-concurrency", strconv.Itoa(b.cfg.Concurrency),
			"-visibility", b.vis.String(),
			"-metrics-addr", "", "-log-level", "error")
		cmd.Stderr = os.Stderr
		if err := cmd.Start(); err != nil {
			return err
		}
		b.procs = append(b.procs, cmd)
	}
	// Ready when every worker has registered a consumer on the group.
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		cs, err := b.rdb.XInfoConsumers(context.Background(), keys.Stream(b.q, keys.DefaultPriority), keys.Group).Result()
		if err == nil && len(cs) >= b.cfg.Workers {
			return nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	return errors.New("workers did not register within 15s")
}

func (b *bench) stopWorkers() {
	for _, p := range b.procs {
		_ = p.Process.Signal(syscall.SIGTERM)
	}
	for _, p := range b.procs {
		_ = p.Wait()
	}
	b.procs = nil
}

func (b *bench) record(id string) {
	b.idsMu.Lock()
	b.ids = append(b.ids, id)
	b.idsMu.Unlock()
}

// drain: backlog first, then measure consume throughput on the Redis clock
// from the first start to the last finish.
func (b *bench) drain() (Result, error) {
	ctx := context.Background()
	var wg sync.WaitGroup
	var errs int
	var mu sync.Mutex
	per := (b.cfg.Jobs + 63) / 64
	for g := range 64 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := g * per; i < min((g+1)*per, b.cfg.Jobs); i++ {
				id, err := b.client.Enqueue(ctx, b.q, taskq.EnqueueOptions{Type: "bench", Payload: b.payload()})
				if err != nil {
					mu.Lock()
					errs++
					mu.Unlock()
					continue
				}
				b.record(id)
			}
		}()
	}
	wg.Wait()
	if err := b.startWorkers(); err != nil {
		return Result{}, err
	}
	if err := b.waitDone(10 * time.Minute); err != nil {
		return Result{}, err
	}
	if err := b.collect(); err != nil {
		return Result{}, err
	}
	res := b.summarise()
	res.EnqueueErrors = errs
	var first, last int64 = math.MaxInt64, 0
	for _, s := range b.samples {
		if s.state == "succeeded" {
			first = min(first, s.started)
			last = max(last, s.finished)
		}
	}
	if last > first {
		res.Throughput = float64(res.Succeeded) / (float64(last-first) / 1000)
	}
	// End-to-end in drain mode is dominated by backlog position; keep only
	// queue wait as a sanity value and drop e2e to avoid misreading.
	res.EndToEnd = nil
	return res, nil
}

// openLoop offers jobs at a fixed rate regardless of how fast they complete
// (open-loop), optionally killing workers mid-run.
func (b *bench) openLoop(duration, warmup time.Duration, kill int) (Result, error) {
	ctx := context.Background()
	if err := b.startWorkers(); err != nil {
		return Result{}, err
	}
	interval := time.Duration(float64(time.Second) / b.cfg.Rate)
	total := int(duration.Seconds() * b.cfg.Rate)
	type tick struct{ due time.Time }
	ticks := make(chan tick, total)
	var lagMu sync.Mutex
	lags := make([]float64, 0, total)
	var errs int

	start := time.Now()
	var wg sync.WaitGroup
	for range 128 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for t := range ticks {
				if d := time.Until(t.due); d > 0 {
					time.Sleep(d)
				}
				lag := time.Since(t.due)
				id, err := b.client.Enqueue(ctx, b.q, taskq.EnqueueOptions{Type: "bench", Payload: b.payload()})
				lagMu.Lock()
				lags = append(lags, float64(lag.Microseconds())/1000)
				if err != nil {
					errs++
				}
				lagMu.Unlock()
				if err == nil {
					b.record(id)
				}
			}
		}()
	}
	for i := range total {
		ticks <- tick{due: start.Add(time.Duration(i) * interval)}
	}
	close(ticks)

	var rec *Recovery
	var killMS int64
	var victims []int
	if kill > 0 {
		time.Sleep(time.Until(start.Add(warmup)))
		for i := range min(kill, len(b.procs)) {
			p := b.procs[i]
			_ = p.Process.Signal(syscall.SIGKILL)
			victims = append(victims, p.Process.Pid)
		}
		t, err := b.rdb.Time(ctx).Result()
		if err != nil {
			return Result{}, err
		}
		killMS = t.UnixMilli()
		for _, p := range b.procs[:len(victims)] {
			_ = p.Wait()
		}
		b.procs = b.procs[len(victims):]
	}
	var orphans map[string]bool
	if kill > 0 {
		var err error
		if orphans, err = b.orphansOf(victims); err != nil {
			return Result{}, err
		}
	}
	wg.Wait()
	produceSpan := time.Since(start)
	if err := b.waitDone(10 * time.Minute); err != nil {
		return Result{}, err
	}
	if err := b.collect(); err != nil {
		return Result{}, err
	}
	res := b.summarise()
	res.EnqueueErrors = errs
	res.OfferedRate = b.cfg.Rate
	res.AchievedEnqueue = float64(len(b.ids)) / produceSpan.Seconds()
	sort.Float64s(lags)
	if len(lags) > 0 {
		res.ProducerMaxLagMS = lags[len(lags)-1]
		res.ProducerP99LagMS = nearestRank(lags, 0.99)
	}
	var first, last int64 = math.MaxInt64, 0
	for _, s := range b.samples {
		if s.state == "succeeded" {
			first = min(first, s.created)
			last = max(last, s.finished)
		}
	}
	if last > first {
		res.Throughput = float64(res.Succeeded) / (float64(last-first) / 1000)
	}
	if kill > 0 {
		rec = &Recovery{Killed: len(victims), Orphaned: len(orphans)}
		var e2e []float64
		var lastOrphan int64
		for _, s := range b.samples {
			if orphans[s.id] && s.state == "succeeded" {
				lastOrphan = max(lastOrphan, s.finished)
				e2e = append(e2e, float64(s.finished-s.created))
			}
		}
		if lastOrphan > 0 {
			rec.TimeMS = float64(lastOrphan - killMS)
		}
		rec.OrphanEndToEnd = pct(e2e)
		res.Recovery = rec
	}
	return res, nil
}

// orphansOf returns the job IDs whose stream entries were in the PEL of the
// killed processes' consumers at the time of the kill.
func (b *bench) orphansOf(pids []int) (map[string]bool, error) {
	ctx := context.Background()
	out := map[string]bool{}
	for _, s := range keys.Streams(b.q) {
		cs, err := b.rdb.XInfoConsumers(ctx, s, keys.Group).Result()
		if err != nil {
			continue
		}
		for _, c := range cs {
			victim := false
			for _, pid := range pids {
				if strings.Contains(c.Name, fmt.Sprintf("-%d-", pid)) {
					victim = true
				}
			}
			if !victim || c.Pending == 0 {
				continue
			}
			pend, err := b.rdb.XPendingExt(ctx, &redis.XPendingExtArgs{
				Stream: s, Group: keys.Group, Start: "-", End: "+", Count: 100000, Consumer: c.Name,
			}).Result()
			if err != nil {
				return nil, err
			}
			for _, p := range pend {
				msgs, err := b.rdb.XRange(ctx, s, p.ID, p.ID).Result()
				if err != nil || len(msgs) == 0 {
					continue
				}
				if id, ok := msgs[0].Values["id"].(string); ok {
					out[id] = true
				}
			}
		}
	}
	return out, nil
}

func (b *bench) waitDone(timeout time.Duration) error {
	ctx := context.Background()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		st, err := b.client.Stats(ctx, b.q)
		if err != nil {
			return err
		}
		busy := st.Scheduled
		for _, p := range st.Priorities {
			busy += p.Waiting + p.InFlight
		}
		if busy == 0 {
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("queue not drained within %v", timeout)
}

func (b *bench) collect() error {
	ctx := context.Background()
	b.samples = make([]sample, 0, len(b.ids))
	for i := 0; i < len(b.ids); i += 1000 {
		batch := b.ids[i:min(i+1000, len(b.ids))]
		pipe := b.rdb.Pipeline()
		cmds := make([]*redis.SliceCmd, len(batch))
		for k, id := range batch {
			cmds[k] = pipe.HMGet(ctx, keys.Job(b.q, id), "created_ms", "run_at_ms", "started_ms", "finished_ms", "attempts", "state")
		}
		if _, err := pipe.Exec(ctx); err != nil {
			return err
		}
		for k, id := range batch {
			v := cmds[k].Val()
			s := sample{id: id, created: i64(v[0]), runAt: i64(v[1]), started: i64(v[2]),
				finished: i64(v[3]), attempts: int(i64(v[4]))}
			s.state, _ = v[5].(string)
			b.samples = append(b.samples, s)
		}
	}
	return nil
}

func i64(v any) int64 {
	s, _ := v.(string)
	n, _ := strconv.ParseInt(s, 10, 64)
	return n
}

func (b *bench) summarise() Result {
	var res Result
	var e2e, wait []float64
	for _, s := range b.samples {
		if s.state != "succeeded" {
			res.NotDone++
			continue
		}
		res.Succeeded++
		if s.attempts > 1 {
			res.DuplicateStarts++
		}
		e2e = append(e2e, float64(s.finished-s.created))
		wait = append(wait, float64(s.started-s.runAt))
	}
	res.EndToEnd = pct(e2e)
	res.QueueWait = pct(wait)
	return res
}

func pct(v []float64) *Pct {
	if len(v) == 0 {
		return nil
	}
	slices.Sort(v)
	var sum float64
	for _, x := range v {
		sum += x
	}
	return &Pct{N: len(v), P50: nearestRank(v, 0.50), P95: nearestRank(v, 0.95),
		P99: nearestRank(v, 0.99), Max: v[len(v)-1], Mean: sum / float64(len(v))}
}

// nearestRank: the smallest value with at least p of the samples at or
// below it. v must be sorted.
func nearestRank(v []float64, p float64) float64 {
	i := int(math.Ceil(p*float64(len(v)))) - 1
	return v[max(0, min(i, len(v)-1))]
}

func (b *bench) write(dir string, res *Result) error {
	if err := os.MkdirAll(filepath.Join(dir, "raw"), 0o755); err != nil {
		return err
	}
	raw := filepath.Join("raw", res.Config.Name+".csv.gz")
	f, err := os.Create(filepath.Join(dir, raw))
	if err != nil {
		return err
	}
	defer f.Close()
	gz := gzip.NewWriter(f)
	w := csv.NewWriter(gz)
	_ = w.Write([]string{"job_id", "created_ms", "run_at_ms", "started_ms", "finished_ms", "attempts", "state"})
	for _, s := range b.samples {
		_ = w.Write([]string{s.id, strconv.FormatInt(s.created, 10), strconv.FormatInt(s.runAt, 10),
			strconv.FormatInt(s.started, 10), strconv.FormatInt(s.finished, 10),
			strconv.Itoa(s.attempts), s.state})
	}
	w.Flush()
	if err := w.Error(); err != nil {
		return err
	}
	if err := gz.Close(); err != nil {
		return err
	}
	res.RawFile = raw
	j, _ := json.MarshalIndent(res, "", "  ")
	return os.WriteFile(filepath.Join(dir, res.Config.Name+".json"), append(j, '\n'), 0o644)
}

// summarize prints Markdown tables of every result in dir.
func summarize(dir string) error {
	files, err := filepath.Glob(filepath.Join(dir, "*.json"))
	if err != nil {
		return err
	}
	var rs []Result
	for _, f := range files {
		b, err := os.ReadFile(f)
		if err != nil {
			return err
		}
		var r Result
		if err := json.Unmarshal(b, &r); err != nil || r.Config.Mode == "" {
			continue // env.json and other files
		}
		rs = append(rs, r)
	}
	sort.Slice(rs, func(i, j int) bool { return rs[i].Config.Name < rs[j].Config.Name })
	f := func(p *Pct, g func(*Pct) float64) string {
		if p == nil {
			return "-"
		}
		return fmt.Sprintf("%.1f", g(p))
	}
	fmt.Println("### drain (consume throughput)")
	fmt.Println("| run | workers | conc | sleep ms | jobs | jobs/s | wait p50 ms | wait p99 ms |")
	fmt.Println("|---|---|---|---|---|---|---|---|")
	for _, r := range rs {
		if r.Config.Mode == "drain" {
			fmt.Printf("| %s | %d | %d | %d | %d | %.0f | %s | %s |\n", r.Config.Name, r.Config.Workers, r.Config.Concurrency,
				r.Config.SleepMS, r.Succeeded, r.Throughput,
				f(r.QueueWait, func(p *Pct) float64 { return p.P50 }), f(r.QueueWait, func(p *Pct) float64 { return p.P99 }))
		}
	}
	fmt.Println("\n### latency (open loop)")
	fmt.Println("| run | workers | conc | offered/s | achieved/s | e2e p50 ms | e2e p95 ms | e2e p99 ms | e2e max ms | producer p99 lag ms |")
	fmt.Println("|---|---|---|---|---|---|---|---|---|---|")
	for _, r := range rs {
		if r.Config.Mode == "latency" {
			fmt.Printf("| %s | %d | %d | %.0f | %.0f | %s | %s | %s | %s | %.1f |\n", r.Config.Name, r.Config.Workers, r.Config.Concurrency,
				r.OfferedRate, r.Throughput,
				f(r.EndToEnd, func(p *Pct) float64 { return p.P50 }), f(r.EndToEnd, func(p *Pct) float64 { return p.P95 }),
				f(r.EndToEnd, func(p *Pct) float64 { return p.P99 }), f(r.EndToEnd, func(p *Pct) float64 { return p.Max }),
				r.ProducerP99LagMS)
		}
	}
	fmt.Println("\n### recovery (SIGKILL under load)")
	fmt.Println("| run | workers | killed | visibility | orphaned | recovery ms | orphan e2e p99 ms | jobs started >1 time |")
	fmt.Println("|---|---|---|---|---|---|---|---|")
	for _, r := range rs {
		if r.Config.Mode == "recovery" && r.Recovery != nil {
			fmt.Printf("| %s | %d | %d | %s | %d | %.0f | %s | %d |\n", r.Config.Name, r.Config.Workers, r.Recovery.Killed,
				r.Config.Visibility, r.Recovery.Orphaned, r.Recovery.TimeMS,
				f(r.Recovery.OrphanEndToEnd, func(p *Pct) float64 { return p.P99 }), r.DuplicateStarts)
		}
	}
	return nil
}

// rtt measures the client-to-Redis round trip (sequential PINGs), which
// bounds per-job latency and differs between machines and Docker setups.
func rtt(addr string) error {
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	defer rdb.Close()
	ctx := context.Background()
	for range 200 { // warm up
		if err := rdb.Ping(ctx).Err(); err != nil {
			return err
		}
	}
	v := make([]float64, 0, 5000)
	for range 5000 {
		t0 := time.Now()
		if err := rdb.Ping(ctx).Err(); err != nil {
			return err
		}
		v = append(v, float64(time.Since(t0).Microseconds())/1000)
	}
	j, _ := json.Marshal(pct(v))
	fmt.Println(string(j))
	return nil
}
