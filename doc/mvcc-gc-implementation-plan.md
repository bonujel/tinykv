# MVCC GC Implementation Plan

## 项目概述

**目标:** 实现基于 SafePoint 的 MVCC 垃圾回收机制,防止版本堆积导致的性能退化

**优先级:** 中

**预计工作量:** 12-16 小时

**技术难度:** 中等

---

## 背景与动机

### 问题描述

TinyKV 使用 MVCC (Multi-Version Concurrency Control) 来实现事务隔离。每次写操作都会创建新版本,旧版本会保留以支持快照读。随着时间推移,会出现以下问题:

1. **版本链过长** - 单个 key 可能有数十甚至上百个历史版本
2. **存储空间浪费** - 大量不再需要的旧版本占用磁盘空间
3. **读性能下降** - 扫描版本链时需要遍历更多版本
4. **内存压力** - Badger LSM-tree 的 memtable 和 block cache 被旧版本占用

### 当前状态

从 Prometheus 监控指标可以看到:
- `tinykv_mvcc_versions` - 已实现版本链长度跟踪
- 但缺少自动清理机制

### 解决方案

实现基于 SafePoint 的 GC 机制:
- **SafePoint** - 所有活跃事务的最小 StartTS
- **GC 策略** - 删除 CommitTS < SafePoint 的旧版本
- **安全性** - 保证不会删除任何活跃事务可能读取的版本

---

## 技术设计

### 1. SafePoint 管理

#### 1.1 SafePoint 计算

**位置:** `kv/transaction/mvcc/safepoint.go` (新建)

**核心逻辑:**
```go
type SafePointManager struct {
    mu          sync.RWMutex
    safePoint   uint64
    activeTxns  map[uint64]bool  // StartTS -> active
}

// UpdateSafePoint 计算并更新 SafePoint
func (m *SafePointManager) UpdateSafePoint() uint64 {
    m.mu.Lock()
    defer m.mu.Unlock()

    if len(m.activeTxns) == 0 {
        // 没有活跃事务,使用当前时间戳
        m.safePoint = getCurrentTS()
        return m.safePoint
    }

    // 找到最小的活跃事务 StartTS
    minStartTS := uint64(math.MaxUint64)
    for startTS := range m.activeTxns {
        if startTS < minStartTS {
            minStartTS = startTS
        }
    }

    m.safePoint = minStartTS
    return m.safePoint
}

// RegisterTxn 注册活跃事务
func (m *SafePointManager) RegisterTxn(startTS uint64) {
    m.mu.Lock()
    defer m.mu.Unlock()
    m.activeTxns[startTS] = true
}

// UnregisterTxn 注销事务
func (m *SafePointManager) UnregisterTxn(startTS uint64) {
    m.mu.Lock()
    defer m.mu.Unlock()
    delete(m.activeTxns, startTS)
}

// GetSafePoint 获取当前 SafePoint
func (m *SafePointManager) GetSafePoint() uint64 {
    m.mu.RLock()
    defer m.mu.RUnlock()
    return m.safePoint
}
```

#### 1.2 事务生命周期集成

**修改文件:** `kv/server/server.go`

在 Prewrite/Commit 时注册/注销事务:

```go
func (server *Server) KvPrewrite(ctx context.Context, req *PrewriteRequest) (*PrewriteResponse, error) {
    // 注册事务
    server.SafePointMgr.RegisterTxn(req.StartVersion)
    defer server.SafePointMgr.UnregisterTxn(req.StartVersion)

    // ... 原有逻辑 ...
}
```

### 2. GC Worker

#### 2.1 GC Worker 实现

**位置:** `kv/transaction/mvcc/gc_worker.go` (新建)

**核心逻辑:**
```go
type GCWorker struct {
    storage       storage.Storage
    safePointMgr  *SafePointManager
    gcInterval    time.Duration
    batchSize     int
    stopCh        chan struct{}
}

func NewGCWorker(storage storage.Storage, mgr *SafePointManager) *GCWorker {
    return &GCWorker{
        storage:      storage,
        safePointMgr: mgr,
        gcInterval:   10 * time.Minute,  // 每 10 分钟运行一次
        batchSize:    1000,               // 每批处理 1000 个 key
        stopCh:       make(chan struct{}),
    }
}

func (w *GCWorker) Start() {
    go w.run()
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

func (w *GCWorker) doGC() {
    start := time.Now()
    defer func() {
        metrics.GCDuration.Observe(time.Since(start).Seconds())
    }()

    // 更新 SafePoint
    safePoint := w.safePointMgr.UpdateSafePoint()
    log.Infof("Starting GC with SafePoint: %d", safePoint)

    // 扫描并清理旧版本
    deletedVersions := 0
    deletedBytes := 0

    reader, _ := w.storage.Reader(nil)
    defer reader.Close()

    iter := reader.IterCF(engine_util.CfWrite)
    defer iter.Close()

    batch := []storage.Modify{}
    for iter.Seek([]byte{}); iter.Valid(); iter.Next() {
        item := iter.Item()
        key := item.Key()

        // 解码 key 获取 timestamp
        userKey, ts := mvcc.DecodeKey(key)

        // 如果 CommitTS < SafePoint,标记删除
        if ts < safePoint {
            // 检查是否是最新版本
            if !w.isLatestVersion(userKey, ts) {
                batch = append(batch, storage.Modify{
                    Data: storage.Delete{
                        Cf:  engine_util.CfWrite,
                        Key: key,
                    },
                })
                deletedVersions++
                deletedBytes += len(key)
            }
        }

        // 批量删除
        if len(batch) >= w.batchSize {
            w.storage.Write(nil, batch)
            batch = []storage.Modify{}
        }
    }

    // 处理剩余批次
    if len(batch) > 0 {
        w.storage.Write(nil, batch)
    }

    log.Infof("GC completed: deleted %d versions, %d bytes", deletedVersions, deletedBytes)
    metrics.GCDeletedVersions.Add(float64(deletedVersions))
    metrics.GCDeletedBytes.Add(float64(deletedBytes))
}

// isLatestVersion 检查是否是最新版本
func (w *GCWorker) isLatestVersion(userKey []byte, ts uint64) bool {
    // 实现逻辑:查找该 key 是否有更新的版本
    // ...
}
```

### 3. 指标集成

#### 3.1 新增 Prometheus 指标

**修改文件:** `kv/metrics/metrics.go`

```go
var (
    // GC 相关指标
    GCDuration = promauto.NewHistogram(
        prometheus.HistogramOpts{
            Name:    "tinykv_gc_duration_seconds",
            Help:    "Duration of GC operations",
            Buckets: prometheus.DefBuckets,
        },
    )

    GCDeletedVersions = promauto.NewCounter(
        prometheus.CounterOpts{
            Name: "tinykv_gc_deleted_versions_total",
            Help: "Total number of versions deleted by GC",
        },
    )

    GCDeletedBytes = promauto.NewCounter(
        prometheus.CounterOpts{
            Name: "tinykv_gc_deleted_bytes_total",
            Help: "Total bytes deleted by GC",
        },
    )

    SafePointGauge = promauto.NewGauge(
        prometheus.GaugeOpts{
            Name: "tinykv_gc_safepoint",
            Help: "Current GC SafePoint timestamp",
        },
    )
)
```

### 4. 配置选项

#### 4.1 GC 配置

**修改文件:** `kv/config/config.go`

```go
type Config struct {
    // ... 现有字段 ...

    // GC 配置
    GCEnabled      bool          `toml:"gc-enabled"`
    GCInterval     time.Duration `toml:"gc-interval"`
    GCBatchSize    int           `toml:"gc-batch-size"`
    GCSafePointTTL time.Duration `toml:"gc-safepoint-ttl"`
}

func NewDefaultConfig() *Config {
    return &Config{
        // ... 现有默认值 ...
        GCEnabled:      true,
        GCInterval:     10 * time.Minute,
        GCBatchSize:    1000,
        GCSafePointTTL: 10 * time.Minute,
    }
}
```

---

## 实施步骤

### Phase 1: SafePoint 管理 (4 小时)

**任务:**
1. 创建 `kv/transaction/mvcc/safepoint.go`
2. 实现 SafePointManager
3. 在 Server 中集成事务注册/注销
4. 添加单元测试

**验证:**
- 测试多个并发事务的 SafePoint 计算
- 验证事务注册/注销正确性

### Phase 2: GC Worker 实现 (6 小时)

**任务:**
1. 创建 `kv/transaction/mvcc/gc_worker.go`
2. 实现 GC 扫描和删除逻辑
3. 实现批量删除优化
4. 添加 GC 指标

**验证:**
- 手动触发 GC,验证旧版本被删除
- 检查 Prometheus 指标是否正确更新
- 性能测试:GC 对读写性能的影响

### Phase 3: 配置与集成 (3 小时)

**任务:**
1. 添加 GC 配置选项
2. 在 main.go 中启动 GC Worker
3. 添加命令行参数
4. 更新文档

**验证:**
- 测试不同配置参数的效果
- 验证 GC 可以正确启动/停止

### Phase 4: 测试与优化 (3 小时)

**任务:**
1. 编写集成测试
2. 压力测试:大量版本堆积场景
3. 性能优化:减少 GC 对前台请求的影响
4. 文档完善

**验证:**
- 运行 TinyKV 测试套件
- 使用 go-ycsb 进行负载测试
- 验证版本链长度指标下降

---

## 测试计划

### 单元测试

**文件:** `kv/transaction/mvcc/safepoint_test.go`

```go
func TestSafePointManager(t *testing.T) {
    mgr := NewSafePointManager()

    // 测试无活跃事务
    sp := mgr.UpdateSafePoint()
    assert.Greater(t, sp, uint64(0))

    // 测试单个事务
    mgr.RegisterTxn(100)
    sp = mgr.UpdateSafePoint()
    assert.Equal(t, uint64(100), sp)

    // 测试多个事务
    mgr.RegisterTxn(200)
    mgr.RegisterTxn(150)
    sp = mgr.UpdateSafePoint()
    assert.Equal(t, uint64(100), sp)  // 最小的

    // 测试注销事务
    mgr.UnregisterTxn(100)
    sp = mgr.UpdateSafePoint()
    assert.Equal(t, uint64(150), sp)
}
```

### 集成测试

**场景 1: 版本清理**
1. 写入 key1 的 10 个版本 (ts: 1-10)
2. 启动事务 (startTS=15)
3. 运行 GC (safePoint=15)
4. 验证 ts < 15 的旧版本被删除
5. 验证最新版本保留

**场景 2: 活跃事务保护**
1. 写入 key1 的 10 个版本
2. 启动事务 A (startTS=5)
3. 运行 GC
4. 验证 ts >= 5 的版本保留 (保护事务 A)

**场景 3: 并发 GC**
1. 并发写入大量数据
2. 同时运行 GC
3. 验证数据一致性
4. 验证无死锁

### 性能测试

**指标:**
- GC 吞吐量: 每秒删除的版本数
- GC 延迟: P50/P99/P999
- 对前台请求的影响: 读写延迟变化

**测试工具:**
- go-ycsb Workload E (扫描密集型)
- 自定义脚本:生成大量版本堆积

---

## 风险与挑战

### 1. 数据安全性 ⚠️

**风险:** GC 错误删除活跃事务需要的版本

**缓解措施:**
- 保守的 SafePoint 计算
- 保留最新版本
- 充分的单元测试和集成测试

### 2. 性能影响 ⚠️

**风险:** GC 扫描占用 CPU/IO,影响前台请求

**缓解措施:**
- 批量删除减少写放大
- 可配置的 GC 间隔
- 限流:GC 速率控制

### 3. 并发控制 ⚠️

**风险:** GC 与事务并发访问导致数据竞争

**缓解措施:**
- SafePointManager 使用 RWMutex
- GC 只删除旧版本,不影响新写入
- 事务注册/注销原子操作

---

## 监控与告警

### Prometheus 指标

| 指标名称 | 类型 | 说明 |
|---------|------|------|
| `tinykv_gc_duration_seconds` | Histogram | GC 运行时长 |
| `tinykv_gc_deleted_versions_total` | Counter | 删除的版本总数 |
| `tinykv_gc_deleted_bytes_total` | Counter | 删除的字节总数 |
| `tinykv_gc_safepoint` | Gauge | 当前 SafePoint |

### Grafana 面板

**GC 性能面板:**
- GC 运行频率: `rate(tinykv_gc_duration_seconds_count[5m])`
- GC P99 延迟: `histogram_quantile(0.99, rate(tinykv_gc_duration_seconds_bucket[5m]))`
- 版本删除速率: `rate(tinykv_gc_deleted_versions_total[5m])`

**版本健康面板:**
- 平均版本链长度: `avg(tinykv_mvcc_versions)`
- P99 版本链长度: `histogram_quantile(0.99, tinykv_mvcc_versions)`
- SafePoint 趋势: `tinykv_gc_safepoint`

### 告警规则

```yaml
groups:
  - name: mvcc_gc
    rules:
      - alert: MVCCVersionChainTooLong
        expr: histogram_quantile(0.99, tinykv_mvcc_versions) > 50
        for: 10m
        annotations:
          summary: "MVCC version chain too long"
          description: "P99 version chain length is {{ $value }}"

      - alert: GCNotRunning
        expr: rate(tinykv_gc_duration_seconds_count[30m]) == 0
        for: 30m
        annotations:
          summary: "GC has not run for 30 minutes"
```

---

## 参考资料

### TiKV GC 实现
- [TiKV GC Overview](https://tikv.org/docs/latest/concepts/architecture/#garbage-collection)
- [TiKV GC Source Code](https://github.com/tikv/tikv/tree/master/src/server/gc_worker)

### MVCC 理论
- [PostgreSQL MVCC](https://www.postgresql.org/docs/current/mvcc.html)
- [Percolator Transaction Model](https://research.google/pubs/pub36726/)

### Badger LSM-tree
- [Badger GC Documentation](https://dgraph.io/docs/badger/get-started/#garbage-collection)

---

## 附录

### A. 配置示例

**config.toml:**
```toml
[gc]
enabled = true
interval = "10m"
batch-size = 1000
safepoint-ttl = "10m"
```

### B. 命令行参数

```bash
./bin/tinykv-server \
  --gc-enabled=true \
  --gc-interval=10m \
  --gc-batch-size=1000
```

### C. 手动触发 GC (调试用)

```go
// 添加 HTTP 端点用于手动触发 GC
http.HandleFunc("/debug/gc", func(w http.ResponseWriter, r *http.Request) {
    gcWorker.doGC()
    w.Write([]byte("GC triggered\n"))
})
```

---

## 总结

### 预期收益

1. **存储空间节省** - 删除不再需要的旧版本,节省 30-50% 磁盘空间
2. **读性能提升** - 缩短版本链,减少扫描开销,P99 延迟降低 20-30%
3. **系统稳定性** - 防止版本堆积导致的性能退化

### 实施优先级

**建议优先级:** 中

**理由:**
- Prometheus 监控已完成,可以观测版本链长度
- 对于长时间运行的 TinyKV 实例,GC ��必需的
- 实现复杂度适中,风险可控

### 后续优化

1. **增量 GC** - 只扫描有更新的 key,减少全量扫描开销
2. **并行 GC** - 多线程并行处理不同 key range
3. **自适应调度** - 根据系统负载动态调整 GC 频率

---

**文档版本:** v1.0
**创建日期:** 2026-03-01
**作者:** Claude Opus 4.6
**状态:** 待实施
