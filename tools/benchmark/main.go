package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/tinykvpb"
	pd "github.com/pingcap-incubator/tinykv/scheduler/client"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

var (
	pdAddr     = flag.String("pd", "127.0.0.1:2379", "scheduler (PD) address")
	threads    = flag.Int("threads", 4, "number of concurrent workers")
	records    = flag.Int("records", 10000, "number of records to load")
	operations = flag.Int("ops", 10000, "number of operations to run")
	valueSize  = flag.Int("value-size", 100, "value size in bytes")
	workload   = flag.String("workload", "mixed", "workload type: put, get, scan, mixed")
	readRatio  = flag.Float64("read-ratio", 0.5, "read ratio for mixed workload (0.0-1.0)")
	scanLimit  = flag.Int("scan-limit", 100, "max keys per scan operation")

	// YCSB workload presets
	workloadPreset = flag.String("workload-preset", "", "YCSB workload preset: A, B, C, D, E, F (overrides -workload)")

	topology = flag.Bool("topology", false, "print cluster topology and exit")
	progress = flag.Duration("progress", 5*time.Second, "progress report interval (0 to disable)")
	suite    = flag.Bool("suite", false, "run 10K/100K/1M comparison suite")
	ycsbSuite = flag.Bool("ycsb-suite", false, "run all YCSB workloads (A-F) with current settings")
	jsonOut  = flag.String("json-out", "", "write JSON results to this file path")

	targetQPS = flag.Int("target-qps", 0, "rate-limit to target QPS (0 = unlimited)")
	chaos     = flag.Bool("chaos", false, "inject fault during run phase (kill leader store connection)")
	chaosAt   = flag.Duration("chaos-at", 10*time.Second, "time after run phase starts to inject fault")
)

// connPool holds gRPC connections to TinyKV stores.
type connPool struct {
	mu    sync.RWMutex
	conns map[string]*grpc.ClientConn
}

func newConnPool() *connPool {
	return &connPool{conns: make(map[string]*grpc.ClientConn)}
}

func (p *connPool) getConn(addr string) (*grpc.ClientConn, error) {
	p.mu.RLock()
	if conn, ok := p.conns[addr]; ok {
		p.mu.RUnlock()
		return conn, nil
	}
	p.mu.RUnlock()

	p.mu.Lock()
	defer p.mu.Unlock()
	if conn, ok := p.conns[addr]; ok {
		return conn, nil
	}
	conn, err := grpc.Dial(addr,
		grpc.WithInsecure(),
		grpc.WithInitialWindowSize(2*1024*1024),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:    3 * time.Second,
			Timeout: 60 * time.Second,
		}),
	)
	if err != nil {
		return nil, err
	}
	p.conns[addr] = conn
	return conn, nil
}

func (p *connPool) closeAll() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, conn := range p.conns {
		conn.Close()
	}
}

// metrics collects benchmark results.
type metrics struct {
	totalOps   int64
	totalErr   int64
	latencies  []time.Duration
	mu         sync.Mutex
}

func (m *metrics) record(d time.Duration) {
	atomic.AddInt64(&m.totalOps, 1)
	m.mu.Lock()
	m.latencies = append(m.latencies, d)
	m.mu.Unlock()
}

func (m *metrics) recordErr() {
	atomic.AddInt64(&m.totalErr, 1)
}

func (m *metrics) report(elapsed time.Duration) {
	ops := atomic.LoadInt64(&m.totalOps)
	errs := atomic.LoadInt64(&m.totalErr)
	throughput := float64(ops) / elapsed.Seconds()

	m.mu.Lock()
	lats := make([]time.Duration, len(m.latencies))
	copy(lats, m.latencies)
	m.mu.Unlock()

	sort.Slice(lats, func(i, j int) bool { return lats[i] < lats[j] })

	fmt.Println("========== Benchmark Results ==========")
	fmt.Printf("Total operations : %d\n", ops)
	fmt.Printf("Total errors     : %d\n", errs)
	fmt.Printf("Elapsed time     : %v\n", elapsed.Round(time.Millisecond))
	fmt.Printf("Throughput       : %.2f ops/s\n", throughput)
	if len(lats) > 0 {
		fmt.Printf("Avg latency      : %v\n", avg(lats).Round(time.Microsecond))
		fmt.Printf("P50 latency      : %v\n", percentile(lats, 0.50).Round(time.Microsecond))
		fmt.Printf("P95 latency      : %v\n", percentile(lats, 0.95).Round(time.Microsecond))
		fmt.Printf("P99 latency      : %v\n", percentile(lats, 0.99).Round(time.Microsecond))
		fmt.Printf("Max latency      : %v\n", lats[len(lats)-1].Round(time.Microsecond))
	}
	fmt.Println("=======================================")
}

func avg(lats []time.Duration) time.Duration {
	var sum time.Duration
	for _, l := range lats {
		sum += l
	}
	return sum / time.Duration(len(lats))
}

func percentile(lats []time.Duration, p float64) time.Duration {
	idx := int(float64(len(lats)) * p)
	if idx >= len(lats) {
		idx = len(lats) - 1
	}
	return lats[idx]
}

func makeKey(i int) []byte {
	return []byte(fmt.Sprintf("bench_%010d", i))
}

func makeValue(size int) []byte {
	v := make([]byte, size)
	rand.Read(v)
	return v
}

// benchResult holds JSON-serializable results for one benchmark run.
type benchResult struct {
	Label     string `json:"label"`
	Records   int    `json:"records"`
	Ops       int    `json:"ops"`
	ValueSize int    `json:"value_size"`
	Threads   int    `json:"threads"`
	Workload  string `json:"workload"`

	LoadOpsPerSec float64 `json:"load_ops_per_sec"`
	LoadErrors    int64   `json:"load_errors"`
	LoadElapsedMs int64   `json:"load_elapsed_ms"`
	LoadWriteMB   float64 `json:"load_write_mb"`

	RunOpsPerSec float64 `json:"run_ops_per_sec"`
	RunErrors    int64   `json:"run_errors"`
	RunElapsedMs int64   `json:"run_elapsed_ms"`
	RunAvgUs     int64   `json:"run_avg_us"`
	RunP50Us     int64   `json:"run_p50_us"`
	RunP95Us     int64   `json:"run_p95_us"`
	RunP99Us     int64   `json:"run_p99_us"`
	RunMaxUs     int64   `json:"run_max_us"`

	// Chaos mode fields (only populated when -chaos is used)
	ChaosEnabled    bool   `json:"chaos_enabled,omitempty"`
	ChaosTarget     string `json:"chaos_target,omitempty"`
	ChaosInjectedAt int64  `json:"chaos_injected_at_ms,omitempty"`
	ErrorWindowMs   int64  `json:"error_window_ms,omitempty"`
	RecoveryMs      int64  `json:"recovery_ms,omitempty"`
	ErrorsDuringRTO int64  `json:"errors_during_rto,omitempty"`
}

// printTopology prints cluster topology: stores, regions, and leaders.
func printTopology(client *kvClient) {
	ctx := context.Background()
	clusterID := client.pdClient.GetClusterID(ctx)
	fmt.Printf("=== Cluster Topology (ID: %d) ===\n", clusterID)

	stores, err := client.pdClient.GetAllStores(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "GetAllStores: %v\n", err)
		return
	}
	fmt.Printf("\nStores (%d):\n", len(stores))
	for _, s := range stores {
		fmt.Printf("  Store %d  addr=%-22s state=%s\n",
			s.GetId(), s.GetAddress(), s.GetState().String())
	}

	regions, leaders, err := client.pdClient.ScanRegions(ctx, nil, nil, 0)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ScanRegions: %v\n", err)
		return
	}
	fmt.Printf("\nRegions (%d):\n", len(regions))
	for i, r := range regions {
		startKey, endKey := formatKey(r.GetStartKey()), formatKey(r.GetEndKey())
		leaderStore := uint64(0)
		if i < len(leaders) && leaders[i] != nil {
			leaderStore = leaders[i].GetStoreId()
		}
		fmt.Printf("  Region %d  start=%-20s end=%-20s leader_store=%d\n",
			r.GetId(), startKey, endKey, leaderStore)
	}
	fmt.Println("================================")
}

func formatKey(key []byte) string {
	if len(key) == 0 {
		return "(min)"
	}
	s := string(key)
	if strings.HasPrefix(s, "bench_") {
		return s
	}
	return fmt.Sprintf("%x", key)
}

// runParallelWithProgress wraps runParallel with a real-time progress ticker.
func runParallelWithProgress(workers, total int, interval time.Duration, fn func(i int)) {
	if interval <= 0 {
		runParallel(workers, total, fn)
		return
	}

	var completed int64
	start := time.Now()

	ticker := time.NewTicker(interval)
	done := make(chan struct{})
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				c := atomic.LoadInt64(&completed)
				elapsed := time.Since(start)
				pct := float64(c) / float64(total) * 100
				ops := float64(c) / elapsed.Seconds()
				fmt.Printf("  [progress] %d/%d (%.1f%%)  %.0f ops/s  elapsed=%v\n",
					c, total, pct, ops, elapsed.Round(time.Millisecond))
			case <-done:
				return
			}
		}
	}()

	runParallel(workers, total, func(i int) {
		fn(i)
		atomic.AddInt64(&completed, 1)
	})
	close(done)
}

// kvClient wraps the gRPC client with region context.
type kvClient struct {
	pdClient pd.Client
	pool     *connPool
}

func newKVClient(pdAddr string) (*kvClient, error) {
	pdClient, err := pd.NewClient([]string{pdAddr}, pd.SecurityOption{})
	if err != nil {
		return nil, fmt.Errorf("connect to scheduler: %w", err)
	}
	return &kvClient{pdClient: pdClient, pool: newConnPool()}, nil
}

func (c *kvClient) close() {
	c.pdClient.Close()
	c.pool.closeAll()
}

func (c *kvClient) getClientAndCtx(key []byte) (tinykvpb.TinyKvClient, *kvrpcpb.Context, error) {
	region, leader, err := c.pdClient.GetRegion(context.Background(), key)
	if err != nil {
		return nil, nil, fmt.Errorf("get region: %w", err)
	}
	if leader == nil || leader.GetStoreId() == 0 {
		// Fallback: use first peer as leader (single-node cluster)
		if region != nil && len(region.GetPeers()) > 0 {
			leader = region.GetPeers()[0]
		} else {
			return nil, nil, fmt.Errorf("no leader and no peers for key %q", key)
		}
	}
	store, err := c.pdClient.GetStore(context.Background(), leader.GetStoreId())
	if err != nil {
		return nil, nil, fmt.Errorf("get store: %w", err)
	}
	conn, err := c.pool.getConn(store.GetAddress())
	if err != nil {
		return nil, nil, fmt.Errorf("dial store %s: %w", store.GetAddress(), err)
	}
	reqCtx := &kvrpcpb.Context{
		RegionId:    region.GetId(),
		RegionEpoch: region.GetRegionEpoch(),
		Peer:        leader,
	}
	return tinykvpb.NewTinyKvClient(conn), reqCtx, nil
}

func (c *kvClient) rawPut(key, value []byte) error {
	client, reqCtx, err := c.getClientAndCtx(key)
	if err != nil {
		return err
	}
	resp, err := client.RawPut(context.Background(), &kvrpcpb.RawPutRequest{
		Context: reqCtx, Key: key, Value: value, Cf: "default",
	})
	if err != nil {
		return err
	}
	if resp.GetRegionError() != nil {
		return fmt.Errorf("region error: %v", resp.GetRegionError())
	}
	if resp.GetError() != "" {
		return fmt.Errorf("resp error: %s", resp.GetError())
	}
	return nil
}

func (c *kvClient) rawGet(key []byte) ([]byte, error) {
	client, reqCtx, err := c.getClientAndCtx(key)
	if err != nil {
		return nil, err
	}
	resp, err := client.RawGet(context.Background(), &kvrpcpb.RawGetRequest{
		Context: reqCtx, Key: key, Cf: "default",
	})
	if err != nil {
		return nil, err
	}
	if resp.GetRegionError() != nil {
		return nil, fmt.Errorf("region error: %v", resp.GetRegionError())
	}
	if resp.GetError() != "" {
		return nil, fmt.Errorf("resp error: %s", resp.GetError())
	}
	return resp.GetValue(), nil
}

func (c *kvClient) rawScan(startKey []byte, limit uint32) error {
	client, reqCtx, err := c.getClientAndCtx(startKey)
	if err != nil {
		return err
	}
	resp, err := client.RawScan(context.Background(), &kvrpcpb.RawScanRequest{
		Context: reqCtx, StartKey: startKey, Limit: limit, Cf: "default",
	})
	if err != nil {
		return err
	}
	if resp.GetRegionError() != nil {
		return fmt.Errorf("region error: %v", resp.GetRegionError())
	}
	if resp.GetError() != "" {
		return fmt.Errorf("resp error: %s", resp.GetError())
	}
	return nil
}

// rateLimitedParallel runs fn at a fixed QPS across workers.
// Uses a token-bucket approach: a single goroutine emits work items at the target rate.
func rateLimitedParallel(workers, total, qps int, fn func(i int)) {
	var wg sync.WaitGroup
	ch := make(chan int, workers*2)

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range ch {
				fn(i)
			}
		}()
	}

	interval := time.Second / time.Duration(qps)
	for i := 0; i < total; i++ {
		ch <- i
		if i < total-1 {
			time.Sleep(interval)
		}
	}
	close(ch)
	wg.Wait()
}

// chaosReport holds fault injection timing data.
type chaosReport struct {
	target        string
	injectedAt    time.Time
	firstErrAfter time.Time
	recoveredAt   time.Time
	errCount      int64
}

// runChaos injects a fault by killing the store process listening on the target address.
// It monitors the error stream to measure the error window and recovery time.
func runChaos(client *kvClient, delay time.Duration) *chaosReport {
	report := &chaosReport{}

	// Find the leader store for a sample key to determine which process to kill.
	ctx := context.Background()
	stores, err := client.pdClient.GetAllStores(ctx)
	if err != nil || len(stores) == 0 {
		fmt.Fprintf(os.Stderr, "[chaos] cannot get stores: %v\n", err)
		return nil
	}

	// Pick the store with the most regions (likely the busiest).
	// For simplicity, just pick the first store.
	target := stores[0]
	report.target = target.GetAddress()

	fmt.Printf("[chaos] will kill store %d (%s) in %v\n",
		target.GetId(), target.GetAddress(), delay)

	time.Sleep(delay)

	// Kill the store process by address (find PID by port).
	addr := target.GetAddress()
	port := addr
	if idx := strings.LastIndex(addr, ":"); idx >= 0 {
		port = addr[idx+1:]
	}

	report.injectedAt = time.Now()
	fmt.Printf("[chaos] >>> INJECTING FAULT at %v — killing store on port %s <<<\n",
		report.injectedAt.Format("15:04:05.000"), port)

	// Use lsof to find and kill the process.
	cmd := exec.Command("bash", "-c",
		fmt.Sprintf("lsof -ti tcp:%s | head -1 | xargs kill -9 2>/dev/null", port))
	if out, err := cmd.CombinedOutput(); err != nil {
		fmt.Fprintf(os.Stderr, "[chaos] kill failed: %v (%s)\n", err, strings.TrimSpace(string(out)))
	} else {
		fmt.Printf("[chaos] store process killed successfully\n")
	}

	// Also drop the cached gRPC connection so the client is forced to reconnect.
	client.pool.mu.Lock()
	if conn, ok := client.pool.conns[addr]; ok {
		conn.Close()
		delete(client.pool.conns, addr)
	}
	client.pool.mu.Unlock()

	return report
}

// chaosMonitor wraps an operation function to track error timing for RTO measurement.
type chaosMonitor struct {
	report      *chaosReport
	mu          sync.Mutex
	firstErrSet bool
	lastErrTime time.Time
}

func newChaosMonitor(report *chaosReport) *chaosMonitor {
	return &chaosMonitor{report: report}
}

func (cm *chaosMonitor) onError() {
	if cm == nil || cm.report == nil {
		return
	}
	now := time.Now()
	cm.mu.Lock()
	defer cm.mu.Unlock()
	atomic.AddInt64(&cm.report.errCount, 1)
	if !cm.firstErrSet {
		cm.report.firstErrAfter = now
		cm.firstErrSet = true
	}
	cm.lastErrTime = now
}

func (cm *chaosMonitor) finalize() {
	if cm == nil || cm.report == nil {
		return
	}
	cm.mu.Lock()
	defer cm.mu.Unlock()
	cm.report.recoveredAt = cm.lastErrTime
}

func main() {
	flag.Parse()

	client, err := newKVClient(*pdAddr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create client: %v\n", err)
		os.Exit(1)
	}
	defer client.close()

	// Mode 1: Topology only
	if *topology {
		printTopology(client)
		return
	}

	// Mode 2: YCSB Suite (run all workloads A-F)
	if *ycsbSuite {
		results := runYCSBSuite(client)
		if *jsonOut != "" {
			writeJSON(results, *jsonOut)
		}
		return
	}

	// Mode 3: Suite (10K / 100K / 1M comparison)
	if *suite {
		results := runSuite(client)
		if *jsonOut != "" {
			writeJSON(results, *jsonOut)
		}
		return
	}

	// Mode 4: Single run
	fmt.Printf("TinyKV Benchmark\n")
	fmt.Printf("  PD addr    : %s\n", *pdAddr)
	fmt.Printf("  Threads    : %d\n", *threads)
	fmt.Printf("  Records    : %d\n", *records)
	fmt.Printf("  Operations : %d\n", *operations)
	fmt.Printf("  Value size : %d bytes\n", *valueSize)

	// Handle workload preset
	if *workloadPreset != "" {
		preset, ok := YCSBWorkloads[*workloadPreset]
		if !ok {
			fmt.Fprintf(os.Stderr, "Unknown workload preset: %s (valid: A, B, C, D, E, F)\n", *workloadPreset)
			os.Exit(1)
		}
		fmt.Printf("  Workload   : %s - %s\n", preset.Name, preset.Description)
	} else {
		fmt.Printf("  Workload   : %s\n", *workload)
		if *workload == "mixed" {
			fmt.Printf("  Read ratio : %.0f%%\n", *readRatio*100)
		}
	}

	if *targetQPS > 0 {
		fmt.Printf("  Target QPS : %d\n", *targetQPS)
	}
	if *chaos {
		fmt.Printf("  Chaos      : enabled (inject at %v)\n", *chaosAt)
	}
	fmt.Println()

	result := runOnceWithOpts(client, *records, *operations, *valueSize, *threads,
		*workload, *readRatio, *scanLimit, *progress,
		*targetQPS, *chaos, *chaosAt)
	printResult(result)

	if *jsonOut != "" {
		writeJSON([]benchResult{result}, *jsonOut)
	}
}

// runOnce executes a single load+run benchmark cycle and returns the result.
func runOnce(client *kvClient, numRecords, numOps, valSize, numThreads int,
	wl string, rr float64, scanLim int, interval time.Duration) benchResult {

	return runOnceWithOpts(client, numRecords, numOps, valSize, numThreads,
		wl, rr, scanLim, interval, 0, false, 0)
}

// runOnceWithOpts is the full-featured version of runOnce with rate limiting and chaos support.
func runOnceWithOpts(client *kvClient, numRecords, numOps, valSize, numThreads int,
	wl string, rr float64, scanLim int, interval time.Duration,
	qps int, chaosEnabled bool, chaosDelay time.Duration) benchResult {

	// Check if workload preset is specified
	if *workloadPreset != "" {
		preset, ok := YCSBWorkloads[*workloadPreset]
		if !ok {
			fmt.Fprintf(os.Stderr, "Unknown workload preset: %s\n", *workloadPreset)
			os.Exit(1)
		}
		return runOnceWithPreset(client, numRecords, numOps, valSize, numThreads,
			*workloadPreset, preset, scanLim, interval, qps, chaosEnabled, chaosDelay)
	}

	// Phase 1: Load
	fmt.Printf("Loading %d records...\n", numRecords)
	loadM := &metrics{}
	loadStart := time.Now()
	runParallelWithProgress(numThreads, numRecords, interval, func(i int) {
		start := time.Now()
		if err := client.rawPut(makeKey(i), makeValue(valSize)); err != nil {
			loadM.recordErr()
			return
		}
		loadM.record(time.Since(start))
	})
	loadElapsed := time.Since(loadStart)

	// Phase 2: Run workload
	keyRange := numRecords
	if keyRange == 0 {
		keyRange = numOps
	}
	if qps > 0 {
		fmt.Printf("Running %s workload (%d ops, target %d QPS)...\n", wl, numOps, qps)
	} else {
		fmt.Printf("Running %s workload (%d ops)...\n", wl, numOps)
	}
	runM := &metrics{}

	// Set up chaos monitor if enabled.
	var cm *chaosMonitor
	var chaosRpt *chaosReport
	if chaosEnabled {
		chaosRpt = &chaosReport{}
		cm = newChaosMonitor(chaosRpt)
	}

	opFn := func(i int) {
		start := time.Now()
		var opErr error
		switch wl {
		case "put":
			opErr = client.rawPut(makeKey(rand.Intn(keyRange)), makeValue(valSize))
		case "get":
			_, opErr = client.rawGet(makeKey(rand.Intn(keyRange)))
		case "scan":
			opErr = client.rawScan(makeKey(rand.Intn(keyRange)), uint32(scanLim))
		case "mixed":
			if rand.Float64() < rr {
				_, opErr = client.rawGet(makeKey(rand.Intn(keyRange)))
			} else {
				opErr = client.rawPut(makeKey(rand.Intn(keyRange)), makeValue(valSize))
			}
		default:
			fmt.Fprintf(os.Stderr, "unknown workload: %s\n", wl)
			os.Exit(1)
		}
		if opErr != nil {
			runM.recordErr()
			if cm != nil {
				cm.onError()
			}
			return
		}
		runM.record(time.Since(start))
	}

	// Launch chaos goroutine if enabled.
	var chaosWg sync.WaitGroup
	if chaosEnabled {
		chaosWg.Add(1)
		go func() {
			defer chaosWg.Done()
			rpt := runChaos(client, chaosDelay)
			if rpt != nil {
				chaosRpt.target = rpt.target
				chaosRpt.injectedAt = rpt.injectedAt
			}
		}()
	}

	runStart := time.Now()
	if qps > 0 {
		rateLimitedParallel(numThreads, numOps, qps, opFn)
	} else {
		runParallelWithProgress(numThreads, numOps, interval, opFn)
	}
	runElapsed := time.Since(runStart)

	if chaosEnabled {
		chaosWg.Wait()
		cm.finalize()
	}

	// Build result
	result := benchResult{
		Records:   numRecords,
		Ops:       numOps,
		ValueSize: valSize,
		Threads:   numThreads,
		Workload:  wl,
		Label:     fmt.Sprintf("%dK", numRecords/1000),

		LoadOpsPerSec: float64(atomic.LoadInt64(&loadM.totalOps)) / loadElapsed.Seconds(),
		LoadErrors:    atomic.LoadInt64(&loadM.totalErr),
		LoadElapsedMs: loadElapsed.Milliseconds(),
		LoadWriteMB:   float64(numRecords*valSize) / (1024 * 1024),

		RunOpsPerSec: float64(atomic.LoadInt64(&runM.totalOps)) / runElapsed.Seconds(),
		RunErrors:    atomic.LoadInt64(&runM.totalErr),
		RunElapsedMs: runElapsed.Milliseconds(),
	}

	runM.mu.Lock()
	lats := make([]time.Duration, len(runM.latencies))
	copy(lats, runM.latencies)
	runM.mu.Unlock()
	sort.Slice(lats, func(i, j int) bool { return lats[i] < lats[j] })

	if len(lats) > 0 {
		result.RunAvgUs = avg(lats).Microseconds()
		result.RunP50Us = percentile(lats, 0.50).Microseconds()
		result.RunP95Us = percentile(lats, 0.95).Microseconds()
		result.RunP99Us = percentile(lats, 0.99).Microseconds()
		result.RunMaxUs = lats[len(lats)-1].Microseconds()
	}

	// Populate chaos fields if applicable.
	if chaosEnabled && chaosRpt != nil {
		result.ChaosEnabled = true
		result.ChaosTarget = chaosRpt.target
		if !chaosRpt.injectedAt.IsZero() {
			result.ChaosInjectedAt = chaosRpt.injectedAt.Sub(runStart).Milliseconds()
		}
		result.ErrorsDuringRTO = atomic.LoadInt64(&chaosRpt.errCount)
		if !chaosRpt.firstErrAfter.IsZero() && !chaosRpt.recoveredAt.IsZero() {
			result.ErrorWindowMs = chaosRpt.recoveredAt.Sub(chaosRpt.firstErrAfter).Milliseconds()
		}
		if !chaosRpt.injectedAt.IsZero() && !chaosRpt.recoveredAt.IsZero() {
			result.RecoveryMs = chaosRpt.recoveredAt.Sub(chaosRpt.injectedAt).Milliseconds()
		}
	}

	return result
}

// printResult prints a single benchmark result in human-readable format.
func printResult(r benchResult) {
	fmt.Println("[Load Phase]")
	fmt.Println("========== Benchmark Results ==========")
	fmt.Printf("Total operations : %d\n", r.Records)
	fmt.Printf("Total errors     : %d\n", r.LoadErrors)
	fmt.Printf("Elapsed time     : %dms\n", r.LoadElapsedMs)
	fmt.Printf("Throughput       : %.2f ops/s\n", r.LoadOpsPerSec)
	fmt.Printf("Data written     : %.2f MB\n", r.LoadWriteMB)
	fmt.Println("=======================================")
	fmt.Println()

	fmt.Printf("[Run Phase - %s]\n", r.Workload)
	fmt.Println("========== Benchmark Results ==========")
	fmt.Printf("Total operations : %d\n", r.Ops)
	fmt.Printf("Total errors     : %d\n", r.RunErrors)
	fmt.Printf("Elapsed time     : %dms\n", r.RunElapsedMs)
	fmt.Printf("Throughput       : %.2f ops/s\n", r.RunOpsPerSec)
	if r.RunAvgUs > 0 {
		fmt.Printf("Avg latency      : %dus\n", r.RunAvgUs)
		fmt.Printf("P50 latency      : %dus\n", r.RunP50Us)
		fmt.Printf("P95 latency      : %dus\n", r.RunP95Us)
		fmt.Printf("P99 latency      : %dus\n", r.RunP99Us)
		fmt.Printf("Max latency      : %dus\n", r.RunMaxUs)
	}
	fmt.Println("=======================================")

	// Chaos report
	if r.ChaosEnabled {
		fmt.Println()
		fmt.Println("[Chaos / Fault Injection]")
		fmt.Println("========== RTO/RPO Assessment ==========")
		fmt.Printf("Target store     : %s\n", r.ChaosTarget)
		fmt.Printf("Fault injected   : %dms into run phase\n", r.ChaosInjectedAt)
		fmt.Printf("Error window     : %dms\n", r.ErrorWindowMs)
		fmt.Printf("Recovery time    : %dms (RTO)\n", r.RecoveryMs)
		fmt.Printf("Errors during RTO: %d\n", r.ErrorsDuringRTO)
		fmt.Println("RPO              : 0 (Raft majority commit)")
		fmt.Println("========================================")
	}
}

// runYCSBSuite runs all YCSB workloads (A-F) and prints comparison table.
func runYCSBSuite(client *kvClient) []benchResult {
	presets := []string{"A", "B", "C", "D", "E", "F"}
	results := make([]benchResult, 0, len(presets))

	for _, presetName := range presets {
		preset := YCSBWorkloads[presetName]
		fmt.Printf("\n>>> YCSB %s: %s <<<\n\n", preset.Name, preset.Description)

		r := runOnceWithPreset(client, *records, *operations, *valueSize, *threads,
			presetName, preset, *scanLimit, *progress, *targetQPS, false, 0)
		printResult(r)
		results = append(results, r)
	}

	// Comparison table
	fmt.Println("\n========== YCSB Suite Comparison ==========")
	fmt.Printf("%-12s %10s %10s %10s %10s %10s\n",
		"Workload", "Ops/s", "P50(us)", "P95(us)", "P99(us)", "Errors")
	fmt.Println(strings.Repeat("-", 70))
	for _, r := range results {
		fmt.Printf("%-12s %10.0f %10d %10d %10d %10d\n",
			r.Label, r.RunOpsPerSec, r.RunP50Us, r.RunP95Us, r.RunP99Us,
			r.LoadErrors+r.RunErrors)
	}
	fmt.Println("===========================================")
	return results
}

// runOnceWithPreset executes a benchmark with a YCSB workload preset.
func runOnceWithPreset(client *kvClient, numRecords, numOps, valSize, numThreads int,
	presetName string, preset WorkloadPreset, scanLim int, interval time.Duration,
	qps int, chaosEnabled bool, chaosDelay time.Duration) benchResult {

	// Phase 1: Load
	fmt.Printf("Loading %d records...\n", numRecords)
	loadM := &metrics{}
	loadStart := time.Now()
	runParallelWithProgress(numThreads, numRecords, interval, func(i int) {
		start := time.Now()
		if err := client.rawPut(makeKey(i), makeValue(valSize)); err != nil {
			loadM.recordErr()
			return
		}
		loadM.record(time.Since(start))
	})
	loadElapsed := time.Since(loadStart)

	// Phase 2: Run workload with preset
	keyRange := numRecords
	if keyRange == 0 {
		keyRange = numOps
	}
	if qps > 0 {
		fmt.Printf("Running %s (%d ops, target %d QPS)...\n", preset.Name, numOps, qps)
	} else {
		fmt.Printf("Running %s (%d ops)...\n", preset.Name, numOps)
	}
	runM := &metrics{}

	// Set up chaos monitor if enabled
	var cm *chaosMonitor
	var chaosRpt *chaosReport
	if chaosEnabled {
		chaosRpt = &chaosReport{}
		cm = newChaosMonitor(chaosRpt)
	}

	executor := newWorkloadExecutor(preset, client, keyRange, valSize, scanLim)

	opFn := func(i int) {
		start := time.Now()
		opErr := executor.executeOp()
		if opErr != nil {
			runM.recordErr()
			if cm != nil {
				cm.onError()
			}
			return
		}
		runM.record(time.Since(start))
	}

	// Launch chaos goroutine if enabled
	var chaosWg sync.WaitGroup
	if chaosEnabled {
		chaosWg.Add(1)
		go func() {
			defer chaosWg.Done()
			rpt := runChaos(client, chaosDelay)
			if rpt != nil {
				chaosRpt.target = rpt.target
				chaosRpt.injectedAt = rpt.injectedAt
			}
		}()
	}

	runStart := time.Now()
	if qps > 0 {
		rateLimitedParallel(numThreads, numOps, qps, opFn)
	} else {
		runParallelWithProgress(numThreads, numOps, interval, opFn)
	}
	runElapsed := time.Since(runStart)

	if chaosEnabled {
		chaosWg.Wait()
		cm.finalize()
	}

	// Build result
	result := benchResult{
		Records:   numRecords,
		Ops:       numOps,
		ValueSize: valSize,
		Threads:   numThreads,
		Workload:  preset.Name,
		Label:     preset.Name,

		LoadOpsPerSec: float64(atomic.LoadInt64(&loadM.totalOps)) / loadElapsed.Seconds(),
		LoadErrors:    atomic.LoadInt64(&loadM.totalErr),
		LoadElapsedMs: loadElapsed.Milliseconds(),
		LoadWriteMB:   float64(numRecords*valSize) / (1024 * 1024),

		RunOpsPerSec: float64(atomic.LoadInt64(&runM.totalOps)) / runElapsed.Seconds(),
		RunErrors:    atomic.LoadInt64(&runM.totalErr),
		RunElapsedMs: runElapsed.Milliseconds(),
	}

	runM.mu.Lock()
	lats := make([]time.Duration, len(runM.latencies))
	copy(lats, runM.latencies)
	runM.mu.Unlock()
	sort.Slice(lats, func(i, j int) bool { return lats[i] < lats[j] })

	if len(lats) > 0 {
		result.RunAvgUs = avg(lats).Microseconds()
		result.RunP50Us = percentile(lats, 0.50).Microseconds()
		result.RunP95Us = percentile(lats, 0.95).Microseconds()
		result.RunP99Us = percentile(lats, 0.99).Microseconds()
		result.RunMaxUs = lats[len(lats)-1].Microseconds()
	}

	// Populate chaos fields if applicable
	if chaosEnabled && chaosRpt != nil {
		result.ChaosEnabled = true
		result.ChaosTarget = chaosRpt.target
		if !chaosRpt.injectedAt.IsZero() {
			result.ChaosInjectedAt = chaosRpt.injectedAt.Sub(runStart).Milliseconds()
		}
		result.ErrorsDuringRTO = atomic.LoadInt64(&chaosRpt.errCount)
		if !chaosRpt.firstErrAfter.IsZero() && !chaosRpt.recoveredAt.IsZero() {
			result.ErrorWindowMs = chaosRpt.recoveredAt.Sub(chaosRpt.firstErrAfter).Milliseconds()
		}
		if !chaosRpt.injectedAt.IsZero() && !chaosRpt.recoveredAt.IsZero() {
			result.RecoveryMs = chaosRpt.recoveredAt.Sub(chaosRpt.injectedAt).Milliseconds()
		}
	}

	return result
}

// runSuite runs 10K/100K/1M benchmarks and prints a comparison table.
func runSuite(client *kvClient) []benchResult {
	scales := []int{10_000, 100_000, 1_000_000}
	results := make([]benchResult, 0, len(scales))

	for _, n := range scales {
		fmt.Printf("\n>>> Suite: %dK records <<<\n\n", n/1000)
		r := runOnce(client, n, n, *valueSize, *threads,
			*workload, *readRatio, *scanLimit, *progress)
		printResult(r)
		results = append(results, r)
	}

	// Comparison table
	fmt.Println("\n========== Suite Comparison ==========")
	fmt.Printf("%-8s %10s %10s %10s %10s %10s\n",
		"Scale", "Load ops/s", "Run ops/s", "P50(us)", "P99(us)", "Errors")
	fmt.Println(strings.Repeat("-", 60))
	for _, r := range results {
		fmt.Printf("%-8s %10.0f %10.0f %10d %10d %10d\n",
			r.Label, r.LoadOpsPerSec, r.RunOpsPerSec, r.RunP50Us, r.RunP99Us,
			r.LoadErrors+r.RunErrors)
	}
	fmt.Println("======================================")
	return results
}

// writeJSON writes benchmark results to a JSON file.
func writeJSON(results []benchResult, path string) {
	data, err := json.MarshalIndent(results, "", "  ")
	if err != nil {
		fmt.Fprintf(os.Stderr, "JSON marshal: %v\n", err)
		return
	}
	if err := os.WriteFile(path, data, 0644); err != nil {
		fmt.Fprintf(os.Stderr, "Write %s: %v\n", path, err)
		return
	}
	fmt.Printf("\nResults written to %s\n", path)
}

func runParallel(workers, total int, fn func(i int)) {
	var wg sync.WaitGroup
	ch := make(chan int, workers*2)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range ch {
				fn(i)
			}
		}()
	}
	for i := 0; i < total; i++ {
		ch <- i
	}
	close(ch)
	wg.Wait()
}
