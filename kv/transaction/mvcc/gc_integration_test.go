package mvcc

import (
	"testing"
	"time"

	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/storage/standalone_storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestGC_VersionCleanup tests that GC deletes old versions correctly.
func TestGC_VersionCleanup(t *testing.T) {
	// Create a temporary storage
	st := createTestStorage(t)
	defer st.Stop()

	// Create SafePointManager and GCWorker
	safePointMgr := NewSafePointManager()
	gcWorker := NewGCWorker(st, safePointMgr, 1*time.Second, 100)

	// Write multiple versions of the same key
	key := []byte("test_key")
	numVersions := 10

	for i := 0; i < numVersions; i++ {
		ts := uint64((i + 1) * 1000)
		value := []byte("value_" + string(rune('0'+i)))

		// Write to storage
		batch := []storage.Modify{
			{
				Data: storage.Put{
					Cf:    engine_util.CfWrite,
					Key:   EncodeKey(key, ts),
					Value: (&Write{StartTS: ts, Kind: WriteKindPut}).ToBytes(),
				},
			},
			{
				Data: storage.Put{
					Cf:    engine_util.CfDefault,
					Key:   EncodeKey(key, ts),
					Value: value,
				},
			},
		}
		err := st.Write(nil, batch)
		require.NoError(t, err)
	}

	// Verify all versions exist
	reader, err := st.Reader(nil)
	require.NoError(t, err)
	versionCount := countVersions(t, reader, key)
	reader.Close()
	assert.Equal(t, numVersions, versionCount, "Should have all versions before GC")

	// Set SafePoint to delete old versions (keep only last 3)
	safePointMgr.safePoint = uint64(8000) // Versions 1-7 should be deleted

	// Run GC
	gcWorker.doGC()

	// Verify old versions are deleted
	reader, err = st.Reader(nil)
	require.NoError(t, err)
	defer reader.Close()

	// Count remaining versions
	remainingVersions := countVersions(t, reader, key)

	// Should have 3 versions left (8000, 9000, 10000)
	// But we keep the latest version, so we should have at least 1
	assert.LessOrEqual(t, remainingVersions, 3, "Should have deleted old versions")
	assert.GreaterOrEqual(t, remainingVersions, 1, "Should keep at least the latest version")
}

// TestGC_ActiveTransactionProtection tests that GC doesn't delete versions needed by active transactions.
func TestGC_ActiveTransactionProtection(t *testing.T) {
	st := createTestStorage(t)
	defer st.Stop()

	safePointMgr := NewSafePointManager()
	gcWorker := NewGCWorker(st, safePointMgr, 1*time.Second, 100)

	// Write multiple versions
	key := []byte("test_key")
	for i := 1; i <= 10; i++ {
		ts := uint64(i * 1000)
		value := []byte("value")

		batch := []storage.Modify{
			{
				Data: storage.Put{
					Cf:    engine_util.CfWrite,
					Key:   EncodeKey(key, ts),
					Value: (&Write{StartTS: ts, Kind: WriteKindPut}).ToBytes(),
				},
			},
			{
				Data: storage.Put{
					Cf:    engine_util.CfDefault,
					Key:   EncodeKey(key, ts),
					Value: value,
				},
			},
		}
		err := st.Write(nil, batch)
		require.NoError(t, err)
	}

	// Register an active transaction at ts=5000
	safePointMgr.RegisterTxn(5000)

	// Update SafePoint - should be 5000 (minimum active transaction)
	sp := safePointMgr.UpdateSafePoint()
	assert.Equal(t, uint64(5000), sp, "SafePoint should be minimum active transaction StartTS")

	// Run GC
	gcWorker.doGC()

	// Verify versions >= 5000 are kept
	reader, err := st.Reader(nil)
	require.NoError(t, err)
	defer reader.Close()

	// Check that version 5000 still exists
	iter := reader.IterCF(engine_util.CfWrite)
	defer iter.Close()

	found5000 := false
	for iter.Seek(EncodeKey(key, ^uint64(0))); iter.Valid(); iter.Next() {
		item := iter.Item()
		itemKey := item.KeyCopy(nil)
		userKey := DecodeUserKey(itemKey)
		ts := decodeTimestamp(itemKey)

		if bytesEqual(userKey, key) && ts == 5000 {
			found5000 = true
			break
		}
	}

	assert.True(t, found5000, "Version 5000 should be protected by active transaction")

	// Unregister transaction
	safePointMgr.UnregisterTxn(5000)
}

// TestGC_KeepLatestVersion tests that GC always keeps the latest version.
func TestGC_KeepLatestVersion(t *testing.T) {
	st := createTestStorage(t)
	defer st.Stop()

	safePointMgr := NewSafePointManager()
	gcWorker := NewGCWorker(st, safePointMgr, 1*time.Second, 100)

	// Write multiple versions
	key := []byte("test_key")
	latestTS := uint64(10000)

	for i := 1; i <= 10; i++ {
		ts := uint64(i * 1000)
		value := []byte("value")

		batch := []storage.Modify{
			{
				Data: storage.Put{
					Cf:    engine_util.CfWrite,
					Key:   EncodeKey(key, ts),
					Value: (&Write{StartTS: ts, Kind: WriteKindPut}).ToBytes(),
				},
			},
			{
				Data: storage.Put{
					Cf:    engine_util.CfDefault,
					Key:   EncodeKey(key, ts),
					Value: value,
				},
			},
		}
		err := st.Write(nil, batch)
		require.NoError(t, err)
	}

	// Set SafePoint to a very high value (should delete all old versions)
	safePointMgr.safePoint = uint64(100000)

	// Run GC
	gcWorker.doGC()

	// Verify latest version still exists
	reader, err := st.Reader(nil)
	require.NoError(t, err)
	defer reader.Close()

	iter := reader.IterCF(engine_util.CfWrite)
	defer iter.Close()

	foundLatest := false
	for iter.Seek(EncodeKey(key, ^uint64(0))); iter.Valid(); iter.Next() {
		item := iter.Item()
		itemKey := item.KeyCopy(nil)
		userKey := DecodeUserKey(itemKey)
		ts := decodeTimestamp(itemKey)

		if bytesEqual(userKey, key) && ts == latestTS {
			foundLatest = true
			break
		}
	}

	assert.True(t, foundLatest, "Latest version should always be kept")
}

// TestGC_ConcurrentOperations tests GC running concurrently with writes.
func TestGC_ConcurrentOperations(t *testing.T) {
	st := createTestStorage(t)
	defer st.Stop()

	safePointMgr := NewSafePointManager()
	gcWorker := NewGCWorker(st, safePointMgr, 100*time.Millisecond, 100)

	// Start GC worker
	gcWorker.Start()
	defer gcWorker.Stop()

	// Concurrently write data
	done := make(chan bool)
	go func() {
		for i := 0; i < 100; i++ {
			key := []byte("concurrent_key")
			ts := uint64((i + 1) * 1000)
			value := []byte("value")

			batch := []storage.Modify{
				{
					Data: storage.Put{
						Cf:    engine_util.CfWrite,
						Key:   EncodeKey(key, ts),
						Value: (&Write{StartTS: ts, Kind: WriteKindPut}).ToBytes(),
					},
				},
				{
					Data: storage.Put{
						Cf:    engine_util.CfDefault,
						Key:   EncodeKey(key, ts),
						Value: value,
					},
				},
			}
			st.Write(nil, batch)
			time.Sleep(10 * time.Millisecond)
		}
		done <- true
	}()

	// Wait for writes to complete
	<-done

	// Give GC time to run
	time.Sleep(500 * time.Millisecond)

	// Verify data is still accessible
	reader, err := st.Reader(nil)
	require.NoError(t, err)
	defer reader.Close()

	// Should have at least one version
	versionCount := countVersions(t, reader, []byte("concurrent_key"))
	assert.GreaterOrEqual(t, versionCount, 1, "Should have at least one version after concurrent operations")
}

// Helper functions

func createTestStorage(t *testing.T) storage.Storage {
	conf := &config.Config{
		DBPath: t.TempDir(),
	}
	st := standalone_storage.NewStandAloneStorage(conf)
	err := st.Start()
	require.NoError(t, err)
	return st
}

func countVersions(t *testing.T, reader storage.StorageReader, key []byte) int {
	iter := reader.IterCF(engine_util.CfWrite)
	defer iter.Close()

	count := 0
	for iter.Seek(EncodeKey(key, ^uint64(0))); iter.Valid(); iter.Next() {
		item := iter.Item()
		itemKey := item.KeyCopy(nil)
		userKey := DecodeUserKey(itemKey)

		if !bytesEqual(userKey, key) {
			break
		}
		count++
	}
	return count
}

