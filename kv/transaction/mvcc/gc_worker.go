package mvcc

import (
	"time"

	"github.com/pingcap-incubator/tinykv/kv/metrics"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/log"
)

// GCWorker performs garbage collection on old MVCC versions.
type GCWorker struct {
	storage      storage.Storage
	safePointMgr *SafePointManager
	gcInterval   time.Duration
	batchSize    int
	stopCh       chan struct{}
}

// NewGCWorker creates a new GC worker.
func NewGCWorker(storage storage.Storage, mgr *SafePointManager, gcInterval time.Duration, batchSize int) *GCWorker {
	if gcInterval == 0 {
		gcInterval = 10 * time.Minute // Default: 10 minutes
	}
	if batchSize == 0 {
		batchSize = 1000 // Default: 1000 keys per batch
	}

	return &GCWorker{
		storage:      storage,
		safePointMgr: mgr,
		gcInterval:   gcInterval,
		batchSize:    batchSize,
		stopCh:       make(chan struct{}),
	}
}

// Start starts the GC worker in a background goroutine.
func (w *GCWorker) Start() {
	go w.run()
}

// Stop stops the GC worker.
func (w *GCWorker) Stop() {
	close(w.stopCh)
}

func (w *GCWorker) run() {
	ticker := time.NewTicker(w.gcInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			w.doGC()
		case <-w.stopCh:
			return
		}
	}
}

// doGC performs a single GC cycle.
func (w *GCWorker) doGC() {
	start := time.Now()
	defer func() {
		duration := time.Since(start).Seconds()
		metrics.GCDuration.Observe(duration)
		log.Infof("GC completed in %.2f seconds", duration)
	}()

	// Update SafePoint
	safePoint := w.safePointMgr.UpdateSafePoint()
	log.Infof("Starting GC with SafePoint: %d", safePoint)

	// Scan and delete old versions
	deletedVersions := 0
	deletedBytes := 0

	reader, err := w.storage.Reader(nil)
	if err != nil {
		log.Errorf("Failed to create storage reader: %v", err)
		return
	}
	defer reader.Close()

	// Iterate over the Write CF to find old versions
	iter := reader.IterCF(engine_util.CfWrite)
	defer iter.Close()

	batch := []storage.Modify{}
	for iter.Seek([]byte{}); iter.Valid(); iter.Next() {
		item := iter.Item()
		key := item.KeyCopy(nil)

		// Decode the key to get user key and commit timestamp
		userKey := DecodeUserKey(key)
		commitTS := decodeTimestamp(key)

		// If CommitTS < SafePoint, this version can be deleted
		if commitTS < safePoint {
			// Check if this is the latest version for this key
			if !w.isLatestVersion(reader, userKey, commitTS) {
				// Add to batch for deletion
				batch = append(batch, storage.Modify{
					Data: storage.Delete{
						Cf:  engine_util.CfWrite,
						Key: key,
					},
				})
				deletedVersions++
				deletedBytes += len(key)

				// Also need to delete corresponding data in Default CF
				dataKey := EncodeKey(userKey, commitTS)
				batch = append(batch, storage.Modify{
					Data: storage.Delete{
						Cf:  engine_util.CfDefault,
						Key: dataKey,
					},
				})
				deletedBytes += len(dataKey)
			}
		}

		// Batch write when reaching batch size
		if len(batch) >= w.batchSize {
			if err := w.storage.Write(nil, batch); err != nil {
				log.Errorf("Failed to write GC batch: %v", err)
			}
			batch = []storage.Modify{}
		}
	}

	// Write remaining batch
	if len(batch) > 0 {
		if err := w.storage.Write(nil, batch); err != nil {
			log.Errorf("Failed to write final GC batch: %v", err)
		}
	}

	log.Infof("GC completed: deleted %d versions, %d bytes", deletedVersions, deletedBytes)
	metrics.GCDeletedVersions.Add(float64(deletedVersions))
	metrics.GCDeletedBytes.Add(float64(deletedBytes))
}

// isLatestVersion checks if the given version is the latest version for the key.
// Returns true if there are no newer versions (with higher commitTS).
func (w *GCWorker) isLatestVersion(reader storage.StorageReader, userKey []byte, commitTS uint64) bool {
	iter := reader.IterCF(engine_util.CfWrite)
	defer iter.Close()

	// Seek to the start of this key's versions (highest timestamp first)
	iter.Seek(EncodeKey(userKey, ^uint64(0)))

	if !iter.Valid() {
		return true
	}

	// Check if the first version we find is the one we're checking
	item := iter.Item()
	key := item.KeyCopy(nil)
	foundUserKey := DecodeUserKey(key)
	foundCommitTS := decodeTimestamp(key)

	// If the keys don't match, this version doesn't exist
	if !bytesEqual(foundUserKey, userKey) {
		return true
	}

	// This is the latest version if the timestamps match
	return foundCommitTS == commitTS
}

// bytesEqual compares two byte slices for equality.
func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}


