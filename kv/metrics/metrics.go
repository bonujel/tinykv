package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// Raft Layer Metrics

	// RaftProposalTotal tracks the total number of Raft proposals
	RaftProposalTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "tinykv_raft_proposal_total",
			Help: "Total number of Raft proposals",
		},
		[]string{"status"}, // success, error
	)

	// RaftProposalDuration tracks the latency of Raft proposals
	RaftProposalDuration = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "tinykv_raft_proposal_duration_seconds",
			Help:    "Duration of Raft proposal processing",
			Buckets: prometheus.DefBuckets,
		},
	)

	// RaftLeaderChanges tracks the number of leader elections
	RaftLeaderChanges = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "tinykv_raft_leader_changes_total",
			Help: "Total number of Raft leader changes",
		},
	)

	// RaftLeaderGauge indicates which node is the leader (1 = leader, 0 = follower)
	RaftLeaderGauge = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "tinykv_raft_leader",
			Help: "Indicates if this node is the Raft leader (1 = leader, 0 = follower)",
		},
		[]string{"node_id"},
	)

	// RaftLogReplicationLag tracks the log replication lag per peer
	RaftLogReplicationLag = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "tinykv_raft_log_replication_lag",
			Help: "Number of log entries behind the leader per peer",
		},
		[]string{"peer_id"},
	)

	// RaftHeartbeatInterval tracks the time between heartbeats
	RaftHeartbeatInterval = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "tinykv_raft_heartbeat_interval_seconds",
			Help:    "Time between Raft heartbeats",
			Buckets: prometheus.DefBuckets,
		},
	)

	// Transaction Layer Metrics

	// TxnCommitTotal tracks the total number of transaction commits
	TxnCommitTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "tinykv_txn_commit_total",
			Help: "Total number of transaction commits",
		},
		[]string{"status"}, // success, error
	)

	// TxnCommitDuration tracks the latency of transaction commits
	TxnCommitDuration = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "tinykv_txn_commit_duration_seconds",
			Help:    "Duration of transaction commit operations",
			Buckets: prometheus.DefBuckets,
		},
	)

	// TxnLockWaitDuration tracks the time spent waiting for locks
	TxnLockWaitDuration = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "tinykv_txn_lock_wait_duration_seconds",
			Help:    "Duration of lock wait time in transactions",
			Buckets: prometheus.DefBuckets,
		},
	)

	// MvccVersions tracks the distribution of MVCC version chain lengths
	MvccVersions = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "tinykv_mvcc_versions",
			Help:    "Distribution of MVCC version chain lengths",
			Buckets: []float64{1, 2, 5, 10, 20, 50, 100},
		},
	)

	// TxnPrewriteDuration tracks the latency of prewrite operations
	TxnPrewriteDuration = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "tinykv_txn_prewrite_duration_seconds",
			Help:    "Duration of transaction prewrite operations",
			Buckets: prometheus.DefBuckets,
		},
	)

	// Storage Layer Metrics

	// StorageReadBytes tracks the total bytes read from storage
	StorageReadBytes = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "tinykv_storage_read_bytes_total",
			Help: "Total bytes read from storage engine",
		},
	)

	// StorageWriteBytes tracks the total bytes written to storage
	StorageWriteBytes = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "tinykv_storage_write_bytes_total",
			Help: "Total bytes written to storage engine",
		},
	)

	// StorageCompactionDuration tracks the duration of compaction operations
	StorageCompactionDuration = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "tinykv_storage_compaction_duration_seconds",
			Help:    "Duration of storage compaction operations",
			Buckets: prometheus.DefBuckets,
		},
	)

	// StorageCacheHitRate tracks the cache hit rate percentage
	StorageCacheHitRate = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "tinykv_storage_cache_hit_rate",
			Help: "Storage cache hit rate (0-1)",
		},
	)

	// StorageReadOps tracks the number of read operations
	StorageReadOps = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "tinykv_storage_read_ops_total",
			Help: "Total number of storage read operations",
		},
	)

	// StorageWriteOps tracks the number of write operations
	StorageWriteOps = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "tinykv_storage_write_ops_total",
			Help: "Total number of storage write operations",
		},
	)

	// StorageReadDuration tracks the latency of read operations
	StorageReadDuration = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "tinykv_storage_read_duration_seconds",
			Help:    "Duration of storage read operations",
			Buckets: prometheus.DefBuckets,
		},
	)

	// StorageWriteDuration tracks the latency of write operations
	StorageWriteDuration = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "tinykv_storage_write_duration_seconds",
			Help:    "Duration of storage write operations",
			Buckets: prometheus.DefBuckets,
		},
	)

	// GC Metrics

	// GCDuration tracks the duration of GC operations
	GCDuration = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "tinykv_gc_duration_seconds",
			Help:    "Duration of GC operations",
			Buckets: prometheus.DefBuckets,
		},
	)

	// GCDeletedVersions tracks the total number of versions deleted by GC
	GCDeletedVersions = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "tinykv_gc_deleted_versions_total",
			Help: "Total number of versions deleted by GC",
		},
	)

	// GCDeletedBytes tracks the total bytes deleted by GC
	GCDeletedBytes = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "tinykv_gc_deleted_bytes_total",
			Help: "Total bytes deleted by GC",
		},
	)

	// SafePointGauge tracks the current GC SafePoint timestamp
	SafePointGauge = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "tinykv_gc_safepoint",
			Help: "Current GC SafePoint timestamp",
		},
	)
)

