# TinyKV Project 3 实现总结：从单 Raft 到 Multi-Raft KV

## 一、项目概述

Project 2 实现了一个单 Region 的 Raft KV 存储——整个集群只有一个 Raft 组，所有数据都塞在同一个 Region 里。这在数据量小的时候没问题，但一旦数据增长，单 Region 就成了瓶颈：所有读写都压在一个 Leader 上，没法水平扩展。

Project 3 要解决的就是这个问题。它把系统从"单 Raft"升级到"Multi-Raft"——多个 Raft 组各管一段 key range，通过 Region Split 动态拆分，通过 Scheduler 做负载均衡。这也是 TiKV 的核心架构思路。

### 项目结构

**Part A — Raft 层扩展**
- Leader Transfer：主动转移 Leader 到指定节点
- Conf Change：动态增删 Raft 组成员（AddNode / RemoveNode）

**Part B — Raftstore 层 Admin 命令**
- TransferLeader 命令处理
- ChangePeer 命令处理（成员变更的上层调度）
- Region Split 命令处理（核心难点）

**Part C — Scheduler 调度层**
- processRegionHeartbeat：Region 心跳处理与元数据维护
- balance-region Scheduler：跨 Store 的 Region 负载均衡

## 二、总体架构

```
                    ┌─────────────┐
                    │  Scheduler  │  (PD)
                    │  集群调度中心  │
                    └──────┬──────┘
                           │ Region Heartbeat / 调度指令
              ┌────────────┼────────────┐
              ▼            ▼            ▼
         ┌─────────┐ ┌─────────┐ ┌─────────┐
         │ Store 1 │ │ Store 2 │ │ Store 3 │
         │ ┌─────┐ │ │ ┌─────┐ │ │ ┌─────┐ │
         │ │Peer │ │ │ │Peer │ │ │ │Peer │ │  ← Region A
         │ │ (L) │ │ │ │ (F) │ │ │ │ (F) │ │
         │ └─────┘ │ │ └─────┘ │ │ └─────┘ │
         │ ┌─────┐ │ │ ┌─────┐ │ │ ┌─────┐ │
         │ │Peer │ │ │ │Peer │ │ │ │Peer │ │  ← Region B
         │ │ (F) │ │ │ │ (L) │ │ │ │ (F) │ │
         │ └─────┘ │ │ └─────┘ │ │ └─────┘ │
         └─────────┘ └─────────┘ └─────────┘
```

每个 Store 上可以跑多个 Peer，属于不同的 Region（Raft 组）。每个 Region 负责一段连续的 key range，Region 之间通过 StartKey/EndKey 划分边界。Scheduler（类似 TiKV 中的 PD）负责全局调度：收集心跳、检测负载不均、下发 Split/Transfer/Move 指令。

### RegionEpoch：Multi-Raft 的版本控制

Multi-Raft 环境下，Region 的元数据会频繁变化。RegionEpoch 就是用来追踪这些变化的版本号：

- `conf_ver`：每次成员变更（AddNode/RemoveNode）+1
- `version`：每次 Region Split +1

所有涉及 Region 的操作都要检查 epoch，过期的请求直接拒绝。这是防止 stale 操作的核心机制。

## 三、Part A：Raft 层扩展

### 3.1 Leader Transfer

Leader Transfer 的目的是把 Leader 身份主动转移到指定节点。这在负载均衡和运维场景下很有用——比如某个节点要下线维护，先把它上面的 Leader 转走。

实现上分两步走：

**第一步：Leader 收到 TransferLeader 请求**

```go
func (r *Raft) handleTransferLeader(m pb.Message) {
    transferee := m.From
    pr, exists := r.Prs[transferee]
    if !exists { return }

    // 如果 transferee 日志已经追上，直接发 MsgTimeoutNow
    if pr.Match == r.RaftLog.LastIndex() {
        r.msgs = append(r.msgs, pb.Message{
            MsgType: pb.MessageType_MsgTimeoutNow,
            To:      transferee,
        })
        return
    }

    // 否则先帮它追日志，设置 leadTransferee 阻止新 proposal
    r.leadTransferee = transferee
    r.sendAppend(transferee)
}
```

**第二步：Transferee 收到 MsgTimeoutNow**

Transferee 立即发起选举，不等选举超时。因为它的日志已经是最新的，大概率能赢得选举。

这里有个细节：设置 `leadTransferee` 后，Leader 会拒绝所有新的 Propose。这是为了防止在 transfer 过程中日志继续增长，导致 transferee 永远追不上。

### 3.2 Conf Change（成员变更）

Raft 组的成员不是固定的，需要支持动态增删节点。TinyKV 采用的是单步成员变更（one-at-a-time），每次只加或删一个节点，避免 joint consensus 的复杂性。

**AddNode**

```go
func (r *Raft) addNode(id uint64) {
    r.PendingConfIndex = 0
    if _, exists := r.Prs[id]; exists { return }
    r.Prs[id] = &Progress{Match: 0, Next: r.RaftLog.LastIndex() + 1}
}
```

**RemoveNode**

```go
func (r *Raft) removeNode(id uint64) {
    r.PendingConfIndex = 0
    if _, exists := r.Prs[id]; !exists { return }
    delete(r.Prs, id)

    // 删除自己就退回 Follower
    if id == r.id {
        r.becomeFollower(r.Term, None)
        return
    }
    // 成员减少可能改变多数派，尝试推进 commit
    if r.State == StateLeader {
        r.maybeCommit()
    }
}
```

`PendingConfIndex` 是一个安全阀：在一个 ConfChange 日志被 apply 之前，不允许 propose 新的 ConfChange。这保证了同一时刻最多只有一个 pending 的成员变更。

RemoveNode 后调用 `maybeCommit()` 是个容易忽略的点——成员从 3 变成 2，多数派从 2 变成 2，之前需要 2 票才能 commit 的日志现在可能已经满足条件了。

## 四、Part B：Raftstore 层 Admin 命令

Part B 是整个 Project 3 中最复杂的部分。它要在 raftstore 层实现三个 Admin 命令，每个都涉及 Region 元数据的变更和多组件的协调。

### 4.1 TransferLeader 命令

这个相对简单。raftstore 收到 TransferLeader 的 AdminRequest 后，直接调用 `RawNode.TransferLeader()`，不需要走 Raft 日志（因为 transfer 本身不需要共识）。

### 4.2 ChangePeer 命令

ChangePeer 需要走 Raft 日志，因为所有节点都要知道成员变更了。流程：

1. Leader 收到 ChangePeer 请求，通过 `ProposeConfChange()` 提交到 Raft
2. 日志 commit 后，各节点 apply：更新 Region 元数据、递增 `conf_ver`、持久化
3. 如果是删除自己，调用 `destroyPeer()` 自毁

实现中有个关键设计：apply ConfChange 时先 clone Region，在副本上修改，最后原子替换。不能在原 Region 对象上直接改，否则并发读到的 Region 信息会不一致。

### 4.3 Region Split 命令

Split 是 Part B 的核心难点。当一个 Region 的数据量超过阈值，split checker 会触发 Split 流程：

1. split checker 检测到 Region 过大，找到 split key
2. 向 Scheduler 请求分配新的 Region ID 和 Peer ID
3. Scheduler 返回后，Leader propose Split 日志
4. 各节点 apply Split：创建新 Region、更新 key range、注册新 Peer

Split 后的结果：原 Region（left）负责 `[StartKey, SplitKey)`，新 Region（right）负责 `[SplitKey, EndKey)`。两个 Region 的 `version` 都 +1。

```go
func (d *peerMsgHandler) applySplit(req *raft_cmdpb.SplitRequest, kvWB *engine_util.WriteBatch) {
    // 1. 校验 split key 在当前 region 范围内
    // 2. clone 出 leftRegion，修改 EndKey = SplitKey，version++
    // 3. 构造 newRegion，StartKey = SplitKey，EndKey = 原 EndKey
    // 4. createPeer + register + start 新 Peer
    // 5. 更新 storeMeta 的 regionRanges B-tree
    // 6. 持久化两个 Region 的状态
}
```

这里有几个容易踩坑的地方：

- storeMeta 的 regionRanges 是一个 B-tree，用于根据 key 查找所属 Region。Split 后要先删旧的、再插两个新的，顺序不能乱。
- 新 Region 的 Peer 要通过 `createPeer` + `router.register` + 发送 `MsgTypeStart` 来启动，缺一不可。
- Split 和 ConfChange 都会改 RegionEpoch，所有后续请求都要检查 epoch 是否匹配。

## 五、Part C：Scheduler 调度层

### 5.1 processRegionHeartbeat

每个 Region 的 Leader 会定期向 Scheduler 发送心跳，汇报自己的状态。Scheduler 需要维护全局的 Region 信息，并检测过期的心跳。

核心逻辑是 epoch 比较：

```go
func (c *RaftCluster) processRegionHeartbeat(region *core.RegionInfo) error {
    origin := c.core.GetRegion(region.GetID())
    if origin != nil {
        // 同 ID Region：比较 epoch，旧的拒绝
        if region.GetRegionEpoch().GetVersion() < origin.GetRegionEpoch().GetVersion() ||
            region.GetRegionEpoch().GetConfVer() < origin.GetRegionEpoch().GetConfVer() {
            return ErrRegionIsStale(...)
        }
    } else {
        // 新 Region：检查是否与已有 Region 的 key range 重叠
        for _, overlap := range c.core.GetOverlaps(region) {
            if region epoch < overlap epoch {
                return ErrRegionIsStale(...)
            }
        }
    }
    // 更新 Region 信息和 Store 状态
    c.core.PutRegion(region)
    // 更新所有相关 Store 的状态
    ...
}
```

这里有两种情况要处理：

1. **同 ID Region**：直接比较 epoch，version 或 conf_ver 任一更小就是 stale
2. **不同 ID Region**（Split 产生的新 Region）：通过 `GetOverlaps` 找到 key range 重叠的已有 Region，比较 epoch

一开始我加了一个"跳过无变化更新"的优化——如果 epoch、leader、peers 都没变就跳过。结果两个测试挂了：`TestRegionRemovePeers3C` 和 `TestRegionAddBackPeers3C`。原因是测试会在不改 epoch 的情况下修改 peers 列表。去掉这个优化后，所有测试通过。文档里也说了"冗余更新不影响正确性"，所以这个优化得不偿失。

### 5.2 balance-region Scheduler

负载均衡的目标：把 Region 从数据量大的 Store 搬到数据量小的 Store，让各 Store 的负载尽量均匀。

算法流程：

1. 筛选可用 Store（状态为 Up 且 DownTime 未超限）
2. 按 RegionSize 降序排列
3. 从最大的 Store 开始，依次尝试找一个可搬的 Region（优先 pending > follower > leader）
4. 从最小的 Store 开始，找一个不包含该 Region 副本的目标 Store
5. 检查搬迁价值：源和目标的 RegionSize 差值必须大于 Region 大小的 2 倍
6. 创建 MovePeerOperator 执行搬迁

```go
// 搬迁价值检查：差距太小就不搬，避免来回搬
if sourceStore.GetRegionSize() - targetStore.GetRegionSize() < 2 * region.GetApproximateSize() {
    return nil
}
```

这个 `2 * region size` 的阈值很关键——如果不设这个门槛，可能出现 A 搬到 B、下一轮 B 又搬回 A 的"乒乓"现象。

## 六、Part B 踩坑实录

Part B 是整个 Project 3 中 debug 时间最长的部分。基础功能（TransferLeader、ConfChange、简单 Split）很快就能跑通，但一到不可靠网络 + 并发 + 分区的组合测试，各种诡异问题就冒出来了。

### 6.1 问题发现过程

最初的现象是：单个测试跑能过，但全量回归时 Split 相关的组合测试不稳定。失败形态包括：
- `panic: request timeout`
- 大量 `epoch is stale` 日志刷屏
- scan 结果出现重复片段，值序列被污染

先用 `-race` 跑了一遍，果然报出多处 data race。这说明不是单纯的逻辑错误，而是并发安全问题。

### 6.2 根因分析

经过排查，定位到四个根因：

**根因一：Snap 响应返回可变 Region 指针**

处理 Snap（扫描）请求时，响应里直接返回了 `d.Region()` 的指针。问题是：scan 是一个持续过程，如果 scan 进行到一半 Region 发生了 Split，`Region.EndKey` 会被并发修改，导致 scan 读到错误的范围。

修复：返回 Region 的 clone 副本。

**根因二：Split 异步链路的共享引用**

Split 的调度链路很长：split checker → onPrepareSplitRegion → Scheduler → onAskSplitReply → propose。这条链路上，`Region`、`RegionEpoch`、`SplitKey` 都是以共享引用传递的。如果在传递过程中原对象被修改（比如又发生了一次 Split），后续步骤拿到的就是脏数据，导致 stale 重试风暴。

修复：在每个异步边界做 deep copy。`onPrepareSplitRegion` 中 clone Region，split checker 中复制 `RegionEpoch` 和 `SplitKey`。

**根因三：Callback 空指针**

在复杂场景下（Leader 变更 + 网络分区 + 并发请求），某些 proposal 的 callback 可能为 nil。如果不做 nil 检查，直接调用就会 panic。

修复：所有 callback 调用前统一加 `cb != nil` 判断。

**根因四：ticker 并发竞态**

`tickDriver` 和 `storeWorker` 会并发访问 ticker 的内部状态，没有加锁保护。在高并发场景下会触发 data race。

修复：给 ticker 加读写锁。

### 6.3 修复验证

修复后的验证策略是分层回归：

1. 先跑最容易挂的单测 `TestSplitConfChangeSnapshotUnreliableRecoverConcurrentPartition3B`
2. 再跑高风险子集（6 项 Recover/ConcurrentPartition 测试），连续 2 轮
3. 最后跑 Part B 全量 16 项测试

全部通过。

### 6.4 反思

这次 debug 的核心教训是：**在 Multi-Raft 环境下，任何跨异步边界传递的可变对象都是定时炸弹**。

单 Region 时，Region 元数据变化不频繁，共享引用问题不容易暴露。但 Multi-Raft 下，Split 会频繁修改 Region 的 key range 和 epoch，任何持有旧引用的代码都可能读到不一致的状态。

解决思路很简单：**在异步边界做 deep copy，保证每个异步任务拿到的都是不可变快照**。这和 Raft 本身的设计哲学一致——Raft 的 Ready 机制就是把状态变化打包成不可变的快照交给上层处理。

## 七、三个 Part 的关联

回头看这三个 Part，它们构成了一个完整的 Multi-Raft 系统的三个层次：

- **Part A（Raft 层）**：提供 Leader Transfer 和 Conf Change 的原语。这是最底层的能力，上层的所有调度操作最终都要落到这两个原语上。

- **Part B（Raftstore 层）**：把 Raft 原语包装成 Admin 命令，处理 Region 元数据的变更。Split 是这一层独有的能力——它不是 Raft 协议本身的一部分，而是 Multi-Raft 架构的需求。

- **Part C（Scheduler 层）**：站在全局视角做决策。它不关心 Raft 的细节，只关心"哪个 Store 太重了，该把哪个 Region 搬走"。它下发的指令最终通过 Part B 的 Admin 命令、Part A 的 Raft 原语来执行。

这三层的分工很清晰：Part A 管"怎么做"，Part B 管"做什么"，Part C 管"该不该做"。

## 八、总结

Project 3 的核心收获：

1. **Multi-Raft 的复杂度不在算法，在工程**。Raft 算法本身在 Project 2 就实现完了，Project 3 新增的 Raft 层代码很少。真正的复杂度在于：多个 Raft 组之间的协调、Region 元数据的一致性维护、异步链路的并发安全。

2. **RegionEpoch 是 Multi-Raft 的基石**。没有 epoch 机制，就无法检测 stale 操作，整个系统的正确性就没有保障。

3. **不可变性在并发系统中至关重要**。Part B 的大部分 bug 都源于可变对象的共享。clone 的成本远低于 debug 并发问题的成本。

4. **调度器的设计要保守**。balance-region 的 `2 * region size` 阈值看起来简单，但它避免了搬迁风暴。在分布式系统中，"不做"往往比"做错"好。

---

**参考资料：**

1. [TinyKV Project 3 文档](doc/project3-MultiRaftKV.md)
2. [Part B 会话纪要](doc/project3-partb-session-notes-2026-02-27.md)
3. [Part B 调试聊天记录](doc/chat-record-2026-02-27.md)
4. [Raft 论文 - Section 6: Cluster membership changes](https://raft.github.io/raft.pdf)
