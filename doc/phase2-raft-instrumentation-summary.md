# Phase 2 完成总结

## ✅ Raft 层插桩完成

### 修改的文件
- `raft/raft.go` - 在 Raft 共识算法的关键路径上添加了指标收集

### 添加的插桩点

#### 1. Leader 选举跟踪
**位置:** `becomeLeader()` 和 `becomeFollower()` 函数

**指标:**
- `tinykv_raft_leader_changes_total` - Leader 选举次数计数器
- `tinykv_raft_leader{node_id}` - 当前 Leader 状态 (1=leader, 0=follower)

**代码示例:**
```go
func (r *Raft) becomeLeader() {
    // ... 状态转换代码 ...

    // 记录 Leader 选举
    metrics.RaftLeaderChanges.Inc()
    metrics.RaftLeaderGauge.WithLabelValues(fmt.Sprintf("%d", r.id)).Set(1)
}

func (r *Raft) becomeFollower(term uint64, lead uint64) {
    // ... 状态转换代码 ...

    // 清除 Leader 状态
    metrics.RaftLeaderGauge.WithLabelValues(fmt.Sprintf("%d", r.id)).Set(0)
}
```

#### 2. 提案处理跟踪
**位置:** `Step()` 函数

**指标:**
- `tinykv_raft_proposal_duration_seconds` - 提案处理延迟直方图
- `tinykv_raft_proposal_total{status}` - 提案总数计数器,按状态分类:
  - `success` - 成功接受
  - `dropped` - Leader 转移期间丢弃
  - `not_leader` - 非 Leader 节点拒绝

**代码示例:**
```go
func (r *Raft) Step(m pb.Message) error {
    // 跟踪提案处理时间
    start := time.Now()
    defer func() {
        if m.MsgType == pb.MessageType_MsgPropose {
            metrics.RaftProposalDuration.Observe(time.Since(start).Seconds())
        }
    }()

    // ... 消息处理逻辑 ...

    case pb.MessageType_MsgPropose:
        if r.State == StateLeader {
            if r.leadTransferee != None {
                metrics.RaftProposalTotal.WithLabelValues("dropped").Inc()
                return ErrProposalDropped
            }
            // ... 处理提案 ...
            metrics.RaftProposalTotal.WithLabelValues("success").Inc()
        } else {
            metrics.RaftProposalTotal.WithLabelValues("not_leader").Inc()
        }
}
```

#### 3. 日志复制延迟跟踪
**位置:** `sendAppend()` 函数

**指标:**
- `tinykv_raft_log_replication_lag{peer_id}` - 每个 Follower 落后的日志条目数

**代码示例:**
```go
func (r *Raft) sendAppend(to uint64) bool {
    pr := r.Prs[to]

    // 跟踪复制延迟
    if r.State == StateLeader {
        lag := r.RaftLog.LastIndex() - pr.Match
        metrics.RaftLogReplicationLag.WithLabelValues(fmt.Sprintf("%d", to)).Set(float64(lag))
    }

    // ... 发送 AppendEntries ...
}
```

#### 4. 心跳间隔跟踪
**位置:** `tick()` 函数

**指标:**
- `tinykv_raft_heartbeat_interval_seconds` - 心跳间隔时间直方图

**新增字段:**
- `Raft.lastHeartbeatTime time.Time` - 跟踪上次心跳时间

**代码示例:**
```go
func (r *Raft) tick() {
    switch r.State {
    case StateLeader:
        r.heartbeatElapsed++
        if r.heartbeatElapsed >= r.heartbeatTimeout {
            // 记录心跳间隔
            if !r.lastHeartbeatTime.IsZero() {
                interval := time.Since(r.lastHeartbeatTime).Seconds()
                metrics.RaftHeartbeatInterval.Observe(interval)
            }
            r.lastHeartbeatTime = time.Now()

            r.heartbeatElapsed = 0
            r.Step(pb.Message{MsgType: pb.MessageType_MsgBeat})
        }
    }
}
```

### 验证结果

**构建状态:** ✅ 成功编译

**指标端点测试:**
```bash
curl http://localhost:9090/metrics | grep tinykv_raft
```

**输出示例:**
```
# HELP tinykv_raft_leader_changes_total Total number of Raft leader changes
# TYPE tinykv_raft_leader_changes_total counter
tinykv_raft_leader_changes_total 0

# HELP tinykv_raft_proposal_duration_seconds Duration of Raft proposal processing
# TYPE tinykv_raft_proposal_duration_seconds histogram
tinykv_raft_proposal_duration_seconds_bucket{le="0.005"} 0
...
```

### 技术亮点

1. **最小侵入性** - 在不改变 Raft 算法逻辑的前提下添加指标
2. **性能优化** - 使用 defer 确保即使发生 panic 也能记录指标
3. **标签化指标** - 使用 `node_id` 和 `peer_id` 标签区分不同节点
4. **状态跟踪** - 通过 Gauge 指标实时反映 Leader 状态

### 下一步

继续 **Phase 3: Transaction Layer Instrumentation** (事务层插桩)

需要插桩的文件:
- `kv/transaction/mvcc/transaction.go` - MVCC 事务处理
- `kv/transaction/latches/latches.go` - 锁管理
- `kv/server/server.go` - 事务 API 处理器
