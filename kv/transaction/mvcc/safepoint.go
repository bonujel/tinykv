package mvcc

import (
	"math"
	"sync"
	"time"

	"github.com/pingcap-incubator/tinykv/kv/metrics"
)

// SafePointManager manages the GC safe point for MVCC garbage collection.
// SafePoint is the minimum StartTS of all active transactions.
// Versions with CommitTS < SafePoint can be safely deleted.
type SafePointManager struct {
	mu         sync.RWMutex
	safePoint  uint64
	activeTxns map[uint64]bool // StartTS -> active
}

// NewSafePointManager creates a new SafePointManager.
func NewSafePointManager() *SafePointManager {
	return &SafePointManager{
		safePoint:  0,
		activeTxns: make(map[uint64]bool),
	}
}

// UpdateSafePoint calculates and updates the SafePoint.
// Returns the new SafePoint value.
func (m *SafePointManager) UpdateSafePoint() uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()

	if len(m.activeTxns) == 0 {
		// No active transactions, use current timestamp
		m.safePoint = getCurrentTS()
		metrics.SafePointGauge.Set(float64(m.safePoint))
		return m.safePoint
	}

	// Find the minimum active transaction StartTS
	minStartTS := uint64(math.MaxUint64)
	for startTS := range m.activeTxns {
		if startTS < minStartTS {
			minStartTS = startTS
		}
	}

	m.safePoint = minStartTS
	metrics.SafePointGauge.Set(float64(m.safePoint))
	return m.safePoint
}

// RegisterTxn registers an active transaction.
func (m *SafePointManager) RegisterTxn(startTS uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.activeTxns[startTS] = true
}

// UnregisterTxn unregisters a transaction.
func (m *SafePointManager) UnregisterTxn(startTS uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.activeTxns, startTS)
}

// GetSafePoint returns the current SafePoint.
func (m *SafePointManager) GetSafePoint() uint64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.safePoint
}

// GetActiveTxnCount returns the number of active transactions.
func (m *SafePointManager) GetActiveTxnCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.activeTxns)
}

// getCurrentTS returns the current timestamp in TinyKV format.
// Format: physical timestamp (milliseconds) << 18 | logical counter
func getCurrentTS() uint64 {
	physical := uint64(time.Now().UnixNano() / int64(time.Millisecond))
	return physical << 18 // Shift by 18 bits, logical part is 0
}
