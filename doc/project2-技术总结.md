# TinyKV Project 2 实现总结：从 Raft 算法到分布式 KV 存储

## 一、项目概述与学习目标

Project 2 是一个循序渐进的分布式系统实践项目，核心目标是实现一个基于 Raft 共识算法的高可用键值存储系统。整个项目分为三个部分：

### 1.1 项目结构

**Part A - Raft 算法核心实现**
- 领导者选举（Leader Election）
- 日志复制（Log Replication）
- RawNode 接口封装

**Part B - 容错 KV 服务构建**
- 持久化存储管理（PeerStorage）
- Raft Ready 处理流程
- 客户端请求处理

**Part C - 快照与日志压缩**
- 日志垃圾回收（Log GC）
- 快照生成与传输
- 快照应用与状态恢复

### 1.2 设计理念

这个项目的设计非常巧妙，它不是让你从零开始实现一个完整的分布式系统，而是提供了一个精心设计的框架，让你专注于核心算法和关键逻辑的实现。这种设计有几个好处：

1. **降低学习曲线**：框架已经处理了网络通信、存储引擎等复杂细节
2. **聚焦核心概念**：可以专注于理解 Raft 算法本身
3. **工程实践**：学习如何在真实系统中应用理论知识

## 二、Part A：Raft 算法核心实现

### 2.1 核心概念理解

#### 逻辑时钟 vs 物理时钟

Raft 模块使用逻辑时钟（tick）而非物理时钟。这意味着：
- 不在 Raft 内部设置定时器
- 由上层应用调用 `RawNode.Tick()` 推进时钟
- 选举超时和心跳超时都用 tick 数量衡量

**为什么这样设计？**

1. **测试更容易**：可以精确控制时间流逝，不依赖真实时间
2. **解耦更彻底**：Raft 模块不依赖系统时钟，更容易移植
3. **确定性更强**：相同的输入序列产生相同的输出，便于调试

#### 异步消息处理

Raft 不会阻塞等待任何请求的响应：
- 发送消息：推入 `raft.Raft.msgs` 队列
- 接收消息：通过 `raft.Raft.Step()` 处理
- 上层应用负责实际的网络收发

这种设计的好处：
- Raft 模块保持纯粹的状态机特性
- 网络层可以灵活实现（TCP、UDP、内存通道等）
- 便于测试和模拟各种网络场景

### 2.2 领导者选举实现

#### 状态机设计

Raft 节点有三种状态：

```go
type StateType uint64
const (
    StateFollower StateType = iota
    StateCandidate
    StateLeader
)
```

每种状态有不同的行为：
- **Follower**: 被动接收消息，超时后发起选举
- **Candidate**: 主动请求投票，获得多数票后成为 Leader
- **Leader**: 发送心跳，处理客户端请求

#### 关键实现点

**1. 选举超时随机化**

```go
r.electionElapsed = 0
r.randomizedElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
```

**为什么要随机化？**
- 避免多个节点同时超时
- 减少选举冲突（split vote）
- 提高选举成功率

在实际实现中，我发现如果不随机化，测试经常会出现多轮选举都无法选出 Leader 的情况。随机化后，通常第一轮就能成功。

**2. 投票规则**

只有满足以下条件才投票：
- 候选人的 term >= 自己的 term
- 本轮还没投过票，或者投给了同一个候选人
- 候选人的日志至少和自己一样新

"日志新旧"的判断：
```go
lastTerm, _ := r.RaftLog.Term(r.RaftLog.LastIndex())
if m.LogTerm > lastTerm || (m.LogTerm == lastTerm && m.Index >= r.RaftLog.LastIndex()) {
    // 候选人日志更新
}
```

这个规则保证了：
- 拥有最新日志的节点更容易当选
- 已提交的日志不会丢失

**3. Leader 选举成功后的操作**

```go
func (r *Raft) becomeLeader() {
    r.State = StateLeader
    r.Lead = r.id

    // 初始化 Progress
    for peer := range r.Prs {
        r.Prs[peer].Next = r.RaftLog.LastIndex() + 1
        r.Prs[peer].Match = 0
    }

    // 追加 noop 日志
    r.appendEntry(pb.Entry{Data: nil})
}
```

**为什么要追加 noop 日志？**

这是 Raft 论文中的一个重要优化。新 Leader 不能直接提交之前 term 的日志，必须先提交当前 term 的日志。noop 日志的作用：
- 快速提交之前 term 的日志
- 让 Leader 知道哪些日志已经被复制
- 建立 Leader 的权威性

在实现中，我一开始忘记了这个 noop 日志，导致测试中出现日志长时间不提交的问题。

### 2.3 日志复制实现

#### 日志结构设计

```go
type RaftLog struct {
    storage   Storage      // 持久化存储
    committed uint64       // 已提交的最高索引
    applied   uint64       // 已应用的最高索引
    stabled   uint64       // 已持久化的最高索引
    entries   []pb.Entry   // 未压缩的日志条目
    pendingSnapshot *pb.Snapshot  // 待应用的快照
}
```

这个设计体现了几个重要概念：
- **committed**: Raft 保证不会丢失
- **applied**: 状态机已经执行
- **stabled**: 已经写入磁盘
- **entries**: 内存中的日志缓存

**为什么要区分这些索引？**

1. **committed vs applied**: 日志提交后不会立即应用，需要上层应用主动拉取
2. **stabled vs committed**: 持久化是异步的，committed 的日志可能还在内存中
3. **entries 缓存**: 避免频繁读取磁盘，提高性能

#### AppendEntries 处理流程

Follower 收到 AppendEntries 时的处理逻辑：

**1. Term 检查**
```go
if m.Term < r.Term {
    // 拒绝来自旧 term 的消息
    return
}
```

**2. 日志一致性检查**
```go
term, err := r.RaftLog.Term(m.Index)
if err != nil || term != m.LogTerm {
    // 日志不匹配，返回冲突信息
    resp.Reject = true
    resp.Index = conflictIndex
    return
}
```

**3. 日志追加**
```go
for i, entry := range m.Entries {
    if entry.Index <= r.RaftLog.LastIndex() {
        // 检查是否冲突
        term, _ := r.RaftLog.Term(entry.Index)
        if term != entry.Term {
            // 删除冲突及之后的所有日志
            r.RaftLog.entries = r.RaftLog.entries[:entry.Index-firstIdx]
        }
    }
    // 追加新日志
    r.RaftLog.entries = append(r.RaftLog.entries, entry)
}
```

**4. 更新 commit index**
```go
if m.Commit > r.RaftLog.committed {
    r.RaftLog.committed = min(m.Commit, m.Index+uint64(len(m.Entries)))
}
```

#### Leader 的日志复制

Leader 需要跟踪每个 Follower 的复制进度：

```go
type Progress struct {
    Match uint64  // 已知已复制的最高索引
    Next  uint64  // 下一个要发送的索引
}
```

**Match 和 Next 的关系：**
- Match: 已经确认复制成功的日志
- Next: 下一次要发送的日志起始位置
- 正常情况下：Next = Match + 1
- 冲突时：Next 会回退，Match 保持不变

发送 AppendEntries 的逻辑：
```go
func (r *Raft) sendAppend(to uint64) {
    pr := r.Prs[to]
    prevIndex := pr.Next - 1
    prevTerm, err := r.RaftLog.Term(prevIndex)

    if err != nil {
        // 日志已被压缩，发送快照
        snapshot, _ := r.RaftLog.storage.Snapshot()
        r.msgs = append(r.msgs, pb.Message{
            MsgType:  pb.MessageType_MsgSnapshot,
            Snapshot: &snapshot,
            ...
        })
        return
    }

    // 获取要发送的日志
    entries := r.RaftLog.getEntries(pr.Next, r.RaftLog.LastIndex()+1)

    // 发送 AppendEntries
    r.msgs = append(r.msgs, pb.Message{
        MsgType: pb.MessageType_MsgAppend,
        Index:   prevIndex,
        LogTerm: prevTerm,
        Entries: entries,
        Commit:  r.RaftLog.committed,
        ...
    })
}
```

#### 提交规则

Leader 更新 commit index 的条件：
1. 日志条目已被多数节点复制
2. 日志条目来自当前 term

```go
func (r *Raft) maybeCommit() {
    // 找到多数节点都有的最大索引
    matches := make([]uint64, 0, len(r.Prs))
    for _, pr := range r.Prs {
        matches = append(matches, pr.Match)
    }
    sort.Slice(matches, func(i, j int) bool {
        return matches[i] > matches[j]
    })

    // 多数节点的中位数
    n := matches[len(matches)/2]

    // 只能提交当前 term 的日志
    term, _ := r.RaftLog.Term(n)
    if n > r.RaftLog.committed && term == r.Term {
        r.RaftLog.committed = n
        r.bcastAppend()  // 广播新的 commit index
    }
}
```

**为什么只能提交当前 term 的日志？**

这是 Raft 论文中的一个关键安全性保证。如果允许提交之前 term 的日志，可能会出现已提交的日志被覆盖的情况。通过 noop 日志，可以间接提交之前 term 的日志。

### 2.4 RawNode 接口实现

#### Ready 机制

Ready 是 Raft 模块与上层应用交互的核心：

```go
type Ready struct {
    SoftState        *SoftState           // Leader 和状态变化
    HardState        pb.HardState         // Term、Vote、Commit
    Entries          []pb.Entry           // 待持久化的日志
    Snapshot         pb.Snapshot          // 待应用的快照
    CommittedEntries []pb.Entry           // 待应用的日志
    Messages         []pb.Message         // 待发送的消息
}
```

**Ready 的设计哲学：**

Ready 将 Raft 内部状态变化打包成一个结构体，上层应用只需要：
1. 调用 `Ready()` 获取变化
2. 持久化需要持久化的内容
3. 发送需要发送的消息
4. 应用需要应用的日志
5. 调用 `Advance()` 通知 Raft 处理完成

这种设计的好处：
- 批量处理，提高效率
- 明确的接口边界
- 便于实现原子性操作

Ready 的生成逻辑：
```go
func (rn *RawNode) Ready() Ready {
    rd := Ready{Messages: r.msgs}

    // SoftState 变化
    if r.Lead != rn.prevSoftState.Lead || r.State != rn.prevSoftState.RaftState {
        rd.SoftState = &SoftState{Lead: r.Lead, RaftState: r.State}
    }

    // HardState 变化
    if r.Term != rn.prevHardState.Term || r.Vote != rn.prevHardState.Vote ||
       r.RaftLog.committed != rn.prevHardState.Commit {
        rd.HardState = pb.HardState{
            Term:   r.Term,
            Vote:   r.Vote,
            Commit: r.RaftLog.committed,
        }
    }

    // 未持久化的日志
    rd.Entries = r.RaftLog.unstableEntries()

    // 待应用的日志
    if r.RaftLog.committed > r.RaftLog.applied {
        rd.CommittedEntries = r.RaftLog.nextEnts()
    }

    // 待应用的快照
    if r.RaftLog.pendingSnapshot != nil {
        rd.Snapshot = *r.RaftLog.pendingSnapshot
    }

    return rd
}
```

#### Advance 机制

处理完 Ready 后，需要调用 Advance 更新内部状态：

```go
func (rn *RawNode) Advance(rd Ready) {
    // 更新 applied index
    if len(rd.CommittedEntries) > 0 {
        lastIdx := rd.CommittedEntries[len(rd.CommittedEntries)-1].Index
        r.RaftLog.applied = lastIdx
    }

    // 更新 stabled index
    if len(rd.Entries) > 0 {
        lastIdx := rd.Entries[len(rd.Entries)-1].Index
        r.RaftLog.stabled = lastIdx
    }

    // 清理快照
    if !IsEmptySnap(&rd.Snapshot) {
        r.RaftLog.pendingSnapshot = nil
        r.RaftLog.maybeCompact()  // 压缩内存中的日志
    }

    // 清理消息
    r.msgs = nil
}
```

**Advance 的重要性：**

如果不调用 Advance，Raft 会认为上层应用还没有处理完 Ready，会一直返回相同的 Ready。这是一种背压机制，防止上层应用处理不过来。


## 三、Part B：容错 KV 服务实现

### 3.1 架构设计理解

#### 三层架构

```
Client Request
     ↓
RaftStorage (接口层)
     ↓
Raftstore (协调层)
     ↓
Raft Module (算法层)
```

**各层职责：**
- **RaftStorage**: 对外提供 KV 接口，处理客户端请求
- **Raftstore**: 协调 Raft 和存储引擎，处理 Ready
- **Raft Module**: 纯粹的 Raft 算法实现

#### 关键概念

- **Store**: TinyKV 服务器实例
- **Peer**: 运行在 Store 上的 Raft 节点
- **Region**: Peer 的集合，即 Raft 组

在 Project 2 中，简化为：
- 每个 Store 只有一个 Peer
- 整个集群只有一个 Region

### 3.2 持久化存储实现

#### 双引擎设计

TinyKV 使用两个 Badger 实例：
- **raftdb**: 存储 Raft 日志和 RaftLocalState
- **kvdb**: 存储状态机数据、RaftApplyState 和 RegionLocalState

**为什么要分开？**

1. **隔离性**：Raft 日志和业务数据分离，互不影响
2. **性能**：可以针对不同数据特点优化（日志是顺序写，KV 是随机读写）
3. **一致性**：与 TiKV 设计保持一致，便于理解

#### 三个关键状态

```go
// RaftLocalState: Raft 的硬状态和日志元信息
type RaftLocalState struct {
    HardState *HardState  // Term, Vote, Commit
    LastIndex uint64      // 最后一条日志的索引
}

// RaftApplyState: 应用状态
type RaftApplyState struct {
    AppliedIndex    uint64           // 已应用的索引
    TruncatedState  *RaftTruncatedState  // 日志截断信息
}

// RegionLocalState: Region 元信息
type RegionLocalState struct {
    State  PeerState  // Normal 或 Tombstone
    Region *Region    // Region 详细信息
}
```

**为什么需要这三个状态？**

- **RaftLocalState**: Raft 算法需要的持久化状态
- **RaftApplyState**: 状态机需要知道应用到哪里了
- **RegionLocalState**: 集群管理需要知道 Region 信息

#### SaveReadyState 实现

这是 Part B 的核心函数：

```go
func (ps *PeerStorage) SaveReadyState(ready *raft.Ready) (*ApplySnapResult, error) {
    raftWB := new(engine_util.WriteBatch)

    // 处理快照（Part C 实现）
    var applySnapResult *ApplySnapResult
    if !raft.IsEmptySnap(&ready.Snapshot) {
        kvWB := new(engine_util.WriteBatch)
        result, err := ps.ApplySnapshot(&ready.Snapshot, kvWB, raftWB)
        if err != nil {
            return nil, err
        }
        applySnapResult = result
        kvWB.WriteToDB(ps.Engines.Kv)
    }

    // 追加日志
    if len(ready.Entries) > 0 {
        ps.Append(ready.Entries, raftWB)
    }

    // 保存 HardState
    if !raft.IsEmptyHardState(ready.HardState) {
        ps.raftState.HardState = &ready.HardState
    }

    // 持久化 RaftLocalState
    raftWB.SetMeta(meta.RaftStateKey(ps.region.Id), ps.raftState)
    raftWB.WriteToDB(ps.Engines.Raft)

    return applySnapResult, nil
}
```

**关键点：**
1. 使用 WriteBatch 保证原子性
2. 先写 kvdb，再写 raftdb
3. 快照和日志不能同时存在

**为什么先写 kvdb？**

因为快照包含了状态机数据，必须先持久化。如果先写 raftdb，崩溃后重启可能会丢失快照数据。

### 3.3 Raft Ready 处理流程

#### HandleRaftReady 实现

```go
func (d *peerMsgHandler) HandleRaftReady() {
    if !d.RaftGroup.HasReady() {
        return
    }

    ready := d.RaftGroup.Ready()

    // 1. 持久化状态
    applySnapResult, err := d.peerStorage.SaveReadyState(&ready)
    if err != nil {
        panic(err)
    }

    // 2. 发送消息
    d.Send(d.ctx.trans, ready.Messages)

    // 3. 处理快照
    if applySnapResult != nil {
        d.peerStorage.SetRegion(applySnapResult.Region)

        // 更新全局 Region 映射
        storeMeta := d.ctx.storeMeta
        storeMeta.Lock()
        storeMeta.regions[applySnapResult.Region.Id] = applySnapResult.Region
        storeMeta.Unlock()

        // 清理所有待处理的 proposal
        for _, prop := range d.proposals {
            NotifyStaleReq(d.Term(), prop.cb)
        }
        d.proposals = nil
    }

    // 4. 应用已提交的日志
    if len(ready.CommittedEntries) > 0 {
        kvWB := new(engine_util.WriteBatch)
        for _, entry := range ready.CommittedEntries {
            // 跳过快照覆盖的日志
            if entry.Index <= snapIndex {
                continue
            }

            kvWB = d.processCommittedEntry(&entry, kvWB)

            // 每条日志后立即刷盘
            if kvWB.Len() > 0 {
                kvWB.MustWriteToDB(d.peerStorage.Engines.Kv)
                kvWB = new(engine_util.WriteBatch)
            }
        }
    }

    // 5. 推进 Raft 状态
    d.RaftGroup.Advance(ready)
}
```

**为什么每条日志后都要刷盘？**

这是为了保证读一致性：
- Get/Snap 请求需要看到之前所有已提交的写入
- 如果批量刷盘，可能读到不一致的状态
- 这是正确性和性能的权衡

在实现中，我一开始想批量刷盘提高性能，但测试失败了。后来理解了读一致性的要求，才明白必须每条日志后刷盘。

#### processCommittedEntry 实现

```go
func (d *peerMsgHandler) processCommittedEntry(entry *eraftpb.Entry, kvWB *engine_util.WriteBatch) *engine_util.WriteBatch {
    // 空日志（noop）
    if len(entry.Data) == 0 {
        d.peerStorage.applyState.AppliedIndex = entry.Index
        kvWB.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState)
        return kvWB
    }

    // 反序列化请求
    var req raft_cmdpb.RaftCmdRequest
    req.Unmarshal(entry.Data)

    // 匹配 proposal
    var prop *proposal
    if len(d.proposals) > 0 && d.proposals[0].index == entry.Index {
        prop = d.proposals[0]
        d.proposals = d.proposals[1:]

        // 检查 term 是否匹配
        if prop.term != entry.Term {
            NotifyStaleReq(entry.Term, prop.cb)
            prop = nil
        }
    }

    // 应用命令
    resp := d.applyCommand(&req, kvWB, prop)

    // 更新 applied index
    d.peerStorage.applyState.AppliedIndex = entry.Index
    kvWB.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState)

    // 返回响应
    if prop != nil {
        prop.cb.Done(resp)
    }

    return kvWB
}
```

#### proposal 机制

Proposal 用于追踪客户端请求：

```go
type proposal struct {
    index uint64           // 日志索引
    term  uint64           // 日志 term
    cb    *message.Callback  // 回调函数
}
```

**流程：**
1. 客户端请求到达，propose 到 Raft
2. 记录 proposal（index, term, callback）
3. 日志提交后，匹配 proposal
4. 执行回调，返回响应

**为什么需要 term 检查？**

因为 Leader 可能会变更，新 Leader 可能会覆盖旧 Leader 的日志。通过 term 检查，可以检测到这种情况，避免返回错误的响应。

#### 错误处理

两个重要错误：

1. **ErrNotLeader**
   - 场景：在 Follower 上 propose
   - 处理：返回错误，客户端重试其他节点

2. **ErrStaleCommand**
   - 场景：Leader 变更，日志被覆盖
   - 处理：返回错误，客户端重试

## 四、Part C：快照与日志压缩

### 4.1 为什么需要快照？

#### 问题

随着系统运行，Raft 日志会无限增长：
- 内存占用增加
- 重启恢复时间变长
- 新节点加入需要重放所有日志

#### 解决方案

快照（Snapshot）：
- 保存某个时间点的完整状态
- 丢弃快照之前的所有日志
- 新节点直接应用快照，无需重放日志

**快照的权衡：**
- 优点：节省空间，加快恢复
- 缺点：生成快照需要时间，可能影响性能

### 4.2 日志垃圾回收实现

#### 触发条件

Raftstore 定期检查日志数量：
```go
func (d *peerMsgHandler) onRaftGcLogTick() {
    if d.RaftGroup.Raft.RaftLog.LastIndex() - d.peerStorage.applyState.TruncatedState.Index
       > RaftLogGcCountLimit {
        // 提议 CompactLog
        compactIndex := d.peerStorage.applyState.AppliedIndex
        compactTerm, _ := d.RaftGroup.Raft.RaftLog.Term(compactIndex)

        req := newCompactLogRequest(d.regionId, d.Meta, compactIndex, compactTerm)
        d.RaftGroup.Propose(req.Marshal())
    }
}
```

**为什么要通过 Raft 提议？**

因为日志压缩会影响所有节点，必须通过 Raft 达成共识。否则不同节点的日志截断点不一致，会导致问题。

#### CompactLog 处理

```go
func (d *peerMsgHandler) applyCompactLog(req *raft_cmdpb.CompactLogRequest, kvWB *engine_util.WriteBatch) {
    compactIndex := req.CompactIndex
    compactTerm := req.CompactTerm

    // 验证：不能压缩未应用的日志
    if compactIndex > d.peerStorage.applyState.AppliedIndex {
        return
    }

    // 验证：不能重复压缩
    if compactIndex <= d.peerStorage.applyState.TruncatedState.Index {
        return
    }

    // 更新截断状态
    d.peerStorage.applyState.TruncatedState = &rspb.RaftTruncatedState{
        Index: compactIndex,
        Term:  compactTerm,
    }

    // 持久化
    kvWB.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState)

    // 调度异步删除
    d.ScheduleCompactLog(compactIndex)
}
```

**为什么要异步删除？**

- 避免阻塞 Raft 主流程
- 批量删除提高效率
- 失败可以重试

### 4.3 快照实现

#### Raft 层面的快照处理

**发送快照**

Leader 在 `sendAppend` 中检测到日志已被压缩时发送快照：

```go
func (r *Raft) sendAppend(to uint64) bool {
    pr := r.Prs[to]
    prevIndex := pr.Next - 1
    prevTerm, err := r.RaftLog.Term(prevIndex)

    if err != nil {
        // 日志不可用，发送快照
        snapshot, err := r.RaftLog.storage.Snapshot()
        if err == ErrSnapshotTemporarilyUnavailable {
            return false  // 快照生成中，稍后重试
        }

        r.msgs = append(r.msgs, pb.Message{
            MsgType:  pb.MessageType_MsgSnapshot,
            Snapshot: &snapshot,
            ...
        })
        return true
    }

    // 正常发送 AppendEntries
    ...
}
```

**接收快照**

Follower 处理快照消息：

```go
func (r *Raft) handleSnapshot(m pb.Message) {
    meta := m.Snapshot.Metadata

    // 拒绝过时的快照
    if meta.Index <= r.RaftLog.committed {
        r.msgs = append(r.msgs, pb.Message{
            MsgType: pb.MessageType_MsgAppendResponse,
            Index:   r.RaftLog.committed,
            ...
        })
        return
    }

    // 更新 term 和 leader
    if m.Term > r.Term {
        r.becomeFollower(m.Term, m.From)
    } else {
        r.Lead = m.From
    }

    // 保存快照到 pendingSnapshot
    r.RaftLog.pendingSnapshot = m.Snapshot

    // 清空所有日志
    r.RaftLog.entries = []pb.Entry{}

    // 更新索引
    r.RaftLog.committed = meta.Index
    r.RaftLog.applied = meta.Index
    r.RaftLog.stabled = meta.Index

    // 恢复配置
    r.Prs = make(map[uint64]*Progress)
    for _, node := range meta.ConfState.Nodes {
        r.Prs[node] = &Progress{
            Match: 0,
            Next:  r.RaftLog.LastIndex() + 1,
        }
    }

    // 响应 Leader
    r.msgs = append(r.msgs, pb.Message{
        MsgType: pb.MessageType_MsgAppendResponse,
        Index:   meta.Index,
        ...
    })
}
```

**关键设计点：**

1. **pendingSnapshot 检查**

   `LastIndex()` 和 `Term()` 需要检查 pendingSnapshot：
   ```go
   func (l *RaftLog) LastIndex() uint64 {
       if len(l.entries) > 0 {
           return l.entries[len(l.entries)-1].Index
       }
       // 检查 pending snapshot
       if l.pendingSnapshot != nil && l.pendingSnapshot.Metadata != nil {
           return l.pendingSnapshot.Metadata.Index
       }
       // 从 storage 获取
       snapshot, _ := l.storage.Snapshot()
       return snapshot.Metadata.Index
   }
   ```

   **为什么要检查 pendingSnapshot？**

   因为快照还没有应用到 storage，但 Raft 模块需要知道最新的索引。这是一个过渡状态。

2. **清空日志**

   接收快照后必须清空所有日志：
   - 快照包含了所有状态
   - 旧日志已经无效
   - 避免日志冲突

#### Raftstore 层面的快照处理

**快照生成**

`PeerStorage.Snapshot()` 触发快照生成：

```go
func (ps *PeerStorage) Snapshot() (eraftpb.Snapshot, error) {
    // 检查是否正在生成
    if ps.snapState.StateType == snap.SnapState_Generating {
        select {
        case s := <-ps.snapState.Receiver:
            if s != nil {
                ps.snapState.StateType = snap.SnapState_Relax
                return *s, nil
            }
        default:
            return eraftpb.Snapshot{}, raft.ErrSnapshotTemporarilyUnavailable
        }
    }

    // 发起新的快照生成任务
    ch := make(chan *eraftpb.Snapshot, 1)
    ps.snapState = snap.SnapState{
        StateType: snap.SnapState_Generating,
        Receiver:  ch,
    }

    ps.regionSched <- &runner.RegionTaskGen{
        RegionId: ps.region.Id,
        Notifier: ch,
    }

    return eraftpb.Snapshot{}, raft.ErrSnapshotTemporarilyUnavailable
}
```

**为什么返回 ErrSnapshotTemporarilyUnavailable？**

因为快照生成是异步的，第一次调用时快照还没有生成完成。Raft 会稍后重试，第二次调用时快照就准备好了。

Region worker 异步生成快照：
- 扫描底层引擎
- 生成 SST 文件
- 返回快照元数据

**快照应用**

```go
func (ps *PeerStorage) ApplySnapshot(snapshot *eraftpb.Snapshot, kvWB, raftWB *engine_util.WriteBatch) (*ApplySnapResult, error) {
    snapData := new(rspb.RaftSnapshotData)
    snapData.Unmarshal(snapshot.Data)

    // 验证快照不是过时的
    if snapshot.Metadata.Index <= ps.applyState.AppliedIndex {
        return nil, fmt.Errorf("stale snapshot")
    }

    // 设置状态为 Applying
    ps.snapState.StateType = snap.SnapState_Applying

    // 发送应用任务到 region worker
    notifier := make(chan bool, 1)
    ps.regionSched <- &runner.RegionTaskApply{
        RegionId: ps.region.Id,
        Notifier: notifier,
        SnapMeta: snapshot.Metadata,
        StartKey: ps.region.StartKey,
        EndKey:   ps.region.EndKey,
    }

    // 等待 region worker 完成
    <-notifier

    // 清理旧元数据
    ps.clearMeta(kvWB, raftWB)

    // 清理额外数据
    ps.clearExtraData(snapData.Region)

    // 更新内存状态
    ps.region = snapData.Region
    ps.raftState.LastIndex = snapshot.Metadata.Index
    ps.raftState.LastTerm = snapshot.Metadata.Term
    ps.applyState.AppliedIndex = snapshot.Metadata.Index
    ps.applyState.TruncatedState = &rspb.RaftTruncatedState{
        Index: snapshot.Metadata.Index,
        Term:  snapshot.Metadata.Term,
    }

    // 持久化所有状态
    kvWB.SetMeta(meta.ApplyStateKey(ps.region.Id), ps.applyState)
    regionLocalState := &rspb.RegionLocalState{Region: ps.region}
    kvWB.SetMeta(meta.RegionStateKey(ps.region.Id), regionLocalState)
    raftWB.SetMeta(meta.RaftStateKey(ps.region.Id), ps.raftState)

    // 重置快照状态
    ps.snapState.StateType = snap.SnapState_Relax

    return &ApplySnapResult{
        PrevRegion: prevRegion,
        Region:     ps.region,
    }, nil
}
```

**快照应用的关键步骤：**
1. 验证快照不是过时的
2. 通知 region worker 应用快照数据
3. 清理旧的元数据和数据
4. 更新所有状态（RaftLocalState、RaftApplyState、RegionLocalState）
5. 持久化状态


## 五、实现过程中遇到的问题与解决方案

### 5.1 Part A 遇到的问题

#### 问题 1：选举冲突导致无法选出 Leader

**现象：**
测试中多个节点同时超时，发起选举，但都无法获得多数票，导致多轮选举失败。

**原因分析：**
所有节点使用相同的选举超时时间，导致同时超时，同时发起选举，票数分散。

**解决方案：**
实现选举超时随机化：
```go
r.randomizedElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
```

**收获：**
理解了 Raft 论文中随机化的重要性。随机化是解决分布式系统中冲突的常用手段。

#### 问题 2：新 Leader 无法提交之前 term 的日志

**现象：**
新 Leader 选举成功后，之前 term 的日志长时间不提交。

**原因分析：**
Raft 规定只能提交当前 term 的日志。新 Leader 没有当前 term 的日志，所以无法提交。

**解决方案：**
在 `becomeLeader()` 中追加 noop 日志：
```go
r.appendEntry(pb.Entry{Data: nil})
```

**收获：**
理解了 Raft 的安全性保证。noop 日志是一个巧妙的设计，既满足了安全性要求，又能快速提交之前的日志。

#### 问题 3：日志冲突处理不正确

**现象：**
Follower 收到 AppendEntries 后，日志出现不一致。

**原因分析：**
没有正确处理日志冲突。当发现冲突时，应该删除冲突及之后的所有日志。

**解决方案：**
```go
if term != entry.Term {
    // 删除冲突及之后的所有日志
    r.RaftLog.entries = r.RaftLog.entries[:entry.Index-firstIdx]
}
```

**收获：**
理解了 Raft 日志复制的一致性保证。冲突处理是保证一致性的关键。

### 5.2 Part B 遇到的问题

#### 问题 4：读一致性问题

**现象：**
Get 请求有时读不到刚刚写入的数据。

**原因分析：**
批量刷盘导致已提交的日志还没有持久化，Get 请求读到了旧数据。

**解决方案：**
每条日志后立即刷盘：
```go
if kvWB.Len() > 0 {
    kvWB.MustWriteToDB(d.peerStorage.Engines.Kv)
    kvWB = new(engine_util.WriteBatch)
}
```

**收获：**
理解了分布式系统中的一致性保证。性能和正确性需要权衡，但正确性永远是第一位的。

#### 问题 5：proposal 匹配错误

**现象：**
客户端收到错误的响应，或者请求超时。

**原因分析：**
Leader 变更后，新 Leader 覆盖了旧 Leader 的日志，但 proposal 还在等待。

**解决方案：**
添加 term 检查：
```go
if prop.term != entry.Term {
    NotifyStaleReq(entry.Term, prop.cb)
    prop = nil
}
```

**收获：**
理解了 Raft 中 term 的重要性。term 不仅用于选举，还用于检测过期的操作。

#### 问题 6：持久化顺序错误

**现象：**
崩溃重启后，数据丢失或不一致。

**原因分析：**
先写 raftdb，再写 kvdb。如果在写 kvdb 前崩溃，重启后会重放已经应用的日志。

**解决方案：**
先写 kvdb，再写 raftdb：
```go
kvWB.WriteToDB(ps.Engines.Kv)  // 先写
raftWB.WriteToDB(ps.Engines.Raft)  // 后写
```

**收获：**
理解了持久化顺序的重要性。在分布式系统中，操作顺序往往决定了正确性。

### 5.3 Part C 遇到的问题

#### 问题 7：快照索引检查错误

**现象：**
`TestRestoreIgnoreSnapshot2C` 测试失败，过时的快照没有被拒绝。

**原因分析：**
使用 `applied` 索引检查快照，但测试中 `applied` 还是 0，而 `committed` 已经是 3。

**解决方案：**
改用 `committed` 索引检查：
```go
if meta.Index <= r.RaftLog.committed {
    // 拒绝过时的快照
    return
}
```

**收获：**
理解了 `committed` 和 `applied` 的区别。`committed` 是 Raft 保证的，`applied` 是状态机的进度。

#### 问题 8：pendingSnapshot 导致 LastIndex 错误

**现象：**
`TestRestoreSnapshot2C` 测试失败，`LastIndex()` 返回 0。

**原因分析：**
快照还在 `pendingSnapshot` 中，还没有应用到 storage，但 `LastIndex()` 没有检查 `pendingSnapshot`。

**解决方案：**
在 `LastIndex()` 中检查 `pendingSnapshot`：
```go
if l.pendingSnapshot != nil && l.pendingSnapshot.Metadata != nil {
    return l.pendingSnapshot.Metadata.Index
}
```

**收获：**
理解了快照应用的过渡状态。在分布式系统中，状态转换往往不是瞬间完成的，需要处理中间状态。

#### 问题 9：快照后 Leader 信息丢失

**现象：**
`TestRestoreFromSnapMsg2C` 测试失败，接收快照后 `Lead` 字段是 0。

**原因分析：**
`handleSnapshot()` 中没有设置 `Lead` 字段。

**解决方案：**
```go
if m.Term > r.Term {
    r.becomeFollower(m.Term, m.From)
} else {
    r.Lead = m.From
}
```

**收获：**
理解了快照不仅包含数据，还包含元信息。接收快照时需要恢复所有相关状态。

#### 问题 10：压力测试超时

**现象：**
`TestSnapshotUnreliableRecoverConcurrentPartition2C` 测试超时。

**原因分析：**
有几个边界情况没有处理：
1. `ErrCompacted` 时没有告诉 Leader 当前索引
2. 快照和日志共存时，没有跳过快照覆盖的日志
3. `HasReady()` 没有检查 `pendingSnapshot`

**解决方案：**

1. 处理 `ErrCompacted`：
```go
if err == ErrCompacted {
    resp.Index = r.RaftLog.LastIndex()
    resp.Reject = true
    return
}
```

2. 跳过快照覆盖的日志：
```go
if entry.Index <= snapIndex {
    continue
}
```

3. 检查 `pendingSnapshot`：
```go
if r.RaftLog.pendingSnapshot != nil && !IsEmptySnap(r.RaftLog.pendingSnapshot) {
    return true
}
```

**收获：**
理解了边界情况的重要性。在分布式系统中，边界情况往往是最容易出错的地方。压力测试能够暴露这些问题。

## 六、关键知识点总结

### 6.1 Raft 算法核心

#### 领导者选举

**核心思想：**
- 使用 term 作为逻辑时钟
- 随机化选举超时避免冲突
- 日志更新的节点优先当选

**关键点：**
- 每个 term 最多一个 Leader
- 投票规则保证安全性
- noop 日志快速提交

#### 日志复制

**核心思想：**
- Leader 负责日志复制
- 多数派确认后提交
- 通过日志匹配保证一致性

**关键点：**
- Match 和 Next 跟踪复制进度
- 冲突时回退 Next
- 只能提交当前 term 的日志

#### 安全性保证

**Leader Completeness**：
已提交的日志不会丢失。通过投票规则和提交规则保证。

**State Machine Safety**：
不同节点在相同索引处应用相同的日志。通过日志匹配保证。

### 6.2 持久化与一致性

#### 持久化内容

**必须持久化：**
- HardState (Term, Vote, Commit)
- 日志条目
- 快照

**不需要持久化：**
- SoftState (Lead, State)
- 内存中的 Progress
- 消息队列

#### 持久化顺序

**原则：**
先持久化数据，再持久化元数据。

**原因：**
如果先持久化元数据，崩溃后可能找不到数据。

#### 原子性保证

**使用 WriteBatch：**
- 批量操作
- 原子提交
- 失败回滚

### 6.3 快照机制

#### 快照时机

**触发条件：**
- 日志数量超过阈值
- 新节点加入
- 手动触发

**权衡：**
- 频繁快照：空间小，但开销大
- 稀疏快照：开销小，但空间大

#### 快照内容

**必须包含：**
- 状态机数据
- 元数据（索引、term、配置）

**不需要包含：**
- 快照之前的日志
- 临时状态

#### 快照应用

**关键步骤：**
1. 验证快照不是过时的
2. 清理旧数据
3. 应用新数据
4. 更新元数据
5. 清理旧日志

### 6.4 性能优化

#### 批量操作

**WriteBatch：**
- 减少系统调用
- 提高吞吐量
- 保证原子性

**批量发送消息：**
- 减少网络往返
- 提高带宽利用率

#### 异步操作

**快照生成：**
- 不阻塞主流程
- 后台生成
- 完成后通知

**日志删除：**
- 异步删除
- 批量删除
- 失败重试

#### 并发控制

**锁的使用：**
- 尽量减少锁的范围
- 避免持锁等待
- 使用读写锁

### 6.5 错误处理

#### 网络错误

**超时：**
- 重试机制
- 指数退避
- 最大重试次数

**丢包：**
- 心跳检测
- 重传机制

#### 节点故障

**Leader 故障：**
- 自动选举新 Leader
- 客户端重试

**Follower 故障：**
- Leader 继续服务
- 故障节点恢复后追赶

#### 数据错误

**日志冲突：**
- 回退 Next
- 重新发送

**快照过时：**
- 拒绝快照
- 继续正常复制

## 七、延伸思考

### 7.1 与其他共识算法的比较

#### Paxos vs Raft

**Paxos：**
- 更通用，但更难理解
- 没有明确的 Leader
- 需要额外的优化才能实用

**Raft：**
- 更易理解和实现
- 强 Leader 模型
- 开箱即用

#### Multi-Paxos vs Raft

**相似点：**
- 都有 Leader
- 都通过日志复制
- 都保证一致性

**不同点：**
- Raft 的 Leader 选举更简单
- Raft 的日志必须连续
- Multi-Paxos 更灵活

### 7.2 生产环境的考虑

#### 性能优化

**Pipeline：**
- Leader 可以并发发送多个 AppendEntries
- 不需要等待响应

**Batch：**
- 批量处理客户端请求
- 减少日志条目数量

**并行应用：**
- 多个 Region 并行应用日志
- 提高吞吐量

#### 可靠性保证

**持久化：**
- 使用 fsync 保证数据落盘
- 考虑使用 SSD

**备份：**
- 定期备份快照
- 异地容灾

**监控：**
- 监控 Leader 变更
- 监控日志延迟
- 监控快照大小

#### 运维友好

**日志：**
- 详细的日志记录
- 结构化日志
- 日志级别控制

**指标：**
- 暴露关键指标
- Prometheus 集成
- 可视化监控

**调试：**
- 提供调试接口
- 支持动态日志级别
- 支持性能分析

### 7.3 TinyKV 的后续项目

#### Project 3: Multi-Raft

**目标：**
- 支持多个 Region
- 实现 Region 分裂和合并
- 实现负载均衡

**挑战：**
- Region 之间的协调
- 分裂和合并的原子性
- 数据迁移

#### Project 4: 事务

**目标：**
- 实现分布式事务
- 支持 ACID
- 实现 MVCC

**挑战：**
- 事务冲突检测
- 死锁检测
- 性能优化

## 八、总结

通过完成 TinyKV Project 2，我深入理解了：

1. **Raft 算法的核心思想**：通过 Leader 选举和日志复制实现一致性
2. **分布式系统的工程实践**：持久化、错误处理、性能优化
3. **调试分布式系统的方法**：日志分析、测试驱动、边界情况

**最大的收获：**

- **理论与实践的结合**：Raft 论文提供了理论基础，但实现中有很多细节需要考虑
- **正确性第一**：分布式系统中，正确性永远比性能重要
- **测试的重要性**：完善的测试能够发现很多边界情况
- **耐心和细心**：分布式系统的 bug 往往很难复现，需要耐心分析

**下一步计划：**

1. 继续完成 Project 3 和 Project 4
2. 阅读 TiKV 源码，学习生产级实现
3. 研究其他共识算法（Paxos、ZAB 等）
4. 学习分布式事务的实现

---

**参考资料：**

1. [Raft 论文](https://raft.github.io/raft.pdf)
2. [TinyKV 文档](https://github.com/talent-plan/tinykv)
3. [TiKV 文档](https://tikv.org/docs/)
4. [Raft 可视化](https://raft.github.io/)

