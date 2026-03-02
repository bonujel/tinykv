package mvcc

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSafePointManager_NoActiveTxns(t *testing.T) {
	mgr := NewSafePointManager()

	// Test with no active transactions
	sp := mgr.UpdateSafePoint()
	assert.Greater(t, sp, uint64(0), "SafePoint should be greater than 0")

	// Verify GetSafePoint returns the same value
	assert.Equal(t, sp, mgr.GetSafePoint())
}

func TestSafePointManager_SingleTxn(t *testing.T) {
	mgr := NewSafePointManager()

	// Register a single transaction
	startTS := uint64(100)
	mgr.RegisterTxn(startTS)

	// SafePoint should be the transaction's StartTS
	sp := mgr.UpdateSafePoint()
	assert.Equal(t, startTS, sp, "SafePoint should equal the single transaction's StartTS")

	// Verify active transaction count
	assert.Equal(t, 1, mgr.GetActiveTxnCount())
}

func TestSafePointManager_MultipleTxns(t *testing.T) {
	mgr := NewSafePointManager()

	// Register multiple transactions
	mgr.RegisterTxn(200)
	mgr.RegisterTxn(150)
	mgr.RegisterTxn(100)

	// SafePoint should be the minimum StartTS
	sp := mgr.UpdateSafePoint()
	assert.Equal(t, uint64(100), sp, "SafePoint should be the minimum StartTS")

	// Verify active transaction count
	assert.Equal(t, 3, mgr.GetActiveTxnCount())
}

func TestSafePointManager_UnregisterTxn(t *testing.T) {
	mgr := NewSafePointManager()

	// Register multiple transactions
	mgr.RegisterTxn(100)
	mgr.RegisterTxn(200)
	mgr.RegisterTxn(150)

	// Initial SafePoint
	sp := mgr.UpdateSafePoint()
	assert.Equal(t, uint64(100), sp)

	// Unregister the minimum transaction
	mgr.UnregisterTxn(100)
	sp = mgr.UpdateSafePoint()
	assert.Equal(t, uint64(150), sp, "SafePoint should update to next minimum after unregister")

	// Verify active transaction count
	assert.Equal(t, 2, mgr.GetActiveTxnCount())
}

func TestSafePointManager_AllTxnsUnregistered(t *testing.T) {
	mgr := NewSafePointManager()

	// Register and then unregister all transactions
	mgr.RegisterTxn(100)
	mgr.RegisterTxn(200)

	mgr.UnregisterTxn(100)
	mgr.UnregisterTxn(200)

	// SafePoint should use current timestamp
	sp := mgr.UpdateSafePoint()
	assert.Greater(t, sp, uint64(200), "SafePoint should be current timestamp after all txns unregistered")

	// Verify no active transactions
	assert.Equal(t, 0, mgr.GetActiveTxnCount())
}

func TestSafePointManager_ConcurrentAccess(t *testing.T) {
	mgr := NewSafePointManager()

	// Concurrent registration and unregistration
	var wg sync.WaitGroup
	numGoroutines := 100

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			startTS := uint64(id * 100)
			mgr.RegisterTxn(startTS)
			mgr.UpdateSafePoint()
			mgr.GetSafePoint()
			mgr.UnregisterTxn(startTS)
		}(i)
	}

	wg.Wait()

	// All transactions should be unregistered
	assert.Equal(t, 0, mgr.GetActiveTxnCount())
}

func TestSafePointManager_DuplicateRegistration(t *testing.T) {
	mgr := NewSafePointManager()

	// Register the same transaction multiple times
	startTS := uint64(100)
	mgr.RegisterTxn(startTS)
	mgr.RegisterTxn(startTS)
	mgr.RegisterTxn(startTS)

	// Should only count as one active transaction (map behavior)
	assert.Equal(t, 1, mgr.GetActiveTxnCount())

	sp := mgr.UpdateSafePoint()
	assert.Equal(t, startTS, sp)
}

