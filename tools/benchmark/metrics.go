package main

import (
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// enhancedMetrics extends metrics with detailed percentiles and error tracking.
type enhancedMetrics struct {
	totalOps   int64
	totalErr   int64
	latencies  []time.Duration
	errorTypes map[string]int64
	mu         sync.Mutex

	// Throughput over time tracking
	throughputSamples []throughputSample
	sampleInterval    time.Duration
	startTime         time.Time
}

type throughputSample struct {
	timestamp time.Time
	ops       int64
}

func newEnhancedMetrics(sampleInterval time.Duration) *enhancedMetrics {
	return &enhancedMetrics{
		errorTypes:     make(map[string]int64),
		sampleInterval: sampleInterval,
		startTime:      time.Now(),
	}
}

func (m *enhancedMetrics) record(d time.Duration) {
	atomic.AddInt64(&m.totalOps, 1)
	m.mu.Lock()
	m.latencies = append(m.latencies, d)
	m.mu.Unlock()
}

func (m *enhancedMetrics) recordErrWithType(errType string) {
	atomic.AddInt64(&m.totalErr, 1)
	m.mu.Lock()
	m.errorTypes[errType]++
	m.mu.Unlock()
}

func (m *enhancedMetrics) recordErr() {
	m.recordErrWithType("unknown")
}

func (m *enhancedMetrics) sampleThroughput() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.throughputSamples = append(m.throughputSamples, throughputSample{
		timestamp: time.Now(),
		ops:       atomic.LoadInt64(&m.totalOps),
	})
}

// getPercentiles returns P50, P95, P99, P999 latencies.
func (m *enhancedMetrics) getPercentiles() (p50, p95, p99, p999 time.Duration) {
	m.mu.Lock()
	lats := make([]time.Duration, len(m.latencies))
	copy(lats, m.latencies)
	m.mu.Unlock()

	if len(lats) == 0 {
		return 0, 0, 0, 0
	}

	sort.Slice(lats, func(i, j int) bool { return lats[i] < lats[j] })
	return percentile(lats, 0.50),
		percentile(lats, 0.95),
		percentile(lats, 0.99),
		percentile(lats, 0.999)
}

// enhancedBenchResult extends benchResult with additional metrics.
type enhancedBenchResult struct {
	benchResult

	// Additional percentiles
	RunP999Us int64 `json:"run_p999_us"`

	// Error breakdown
	ErrorTypes map[string]int64 `json:"error_types,omitempty"`

	// Throughput over time
	ThroughputSamples []throughputPoint `json:"throughput_samples,omitempty"`

	// Workload preset info
	WorkloadPreset string `json:"workload_preset,omitempty"`
}

type throughputPoint struct {
	ElapsedMs int64   `json:"elapsed_ms"`
	OpsPerSec float64 `json:"ops_per_sec"`
}
