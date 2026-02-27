# TinyKV Project 4 实现总结：从 KV 存储到分布式事务

## 一、项目概述

Project 3 让系统具备了 Multi-Raft 的能力——数据可以拆分到多个 Region，跨 Store 做负载均衡。但到目前为止，系统只支持单 key 的 Get/Put/Delete，没有事务语义。如果一个业务操作需要同时写多个 key（比如转账：A 扣钱 + B 加钱），中间挂了就会出现不一致。

Project 4 要解决的就是这个问题。它引入了 MVCC（多版本并发控制）和 Percolator 两阶段提交协议，让系统支持跨 key 的分布式事务，提供快照隔离（Snapshot Isolation）级别的一致性保证。这也是 TiDB 事务层的核心架构。

### 项目结构

**Part A — MVCC 读写层**
- MvccTxn：基于时间戳的多版本读写抽象
- Key 编码：`EncodeKey(key, ts)` 让同一个 key 的多个版本按时间戳倒序排列
- 三个 Column Family（CfDefault / CfWrite / CfLock）的协作

**Part B — Percolator 2PC 核心**
- KvGet：带锁检测的 MVCC 读
- KvPrewrite：事务的第一阶段，加锁 + 写数据
- KvCommit：事务的第二阶段，提交锁

**Part C — 事务的收尾和容错**
- KvScan：MVCC 感知的范围扫描
- KvCheckTxnStatus：检查事务状态 + TTL 过期回滚
- KvBatchRollback：批量回滚事务
- KvResolveLock：批量提交或回滚某个事务的所有锁

## 二、Percolator 协议：来龙去脉

### 2.1 为什么需要 Percolator？

传统的 2PC（Two-Phase Commit）有个致命问题：协调者是单点。协调者挂了，参与者就卡在 prepared 状态，不知道该提交还是回滚。

Percolator 的巧妙之处在于：它不需要一个独立的协调者。它把事务的提交状态编码在数据本身里——具体来说，编码在 primary key 的 write 记录里。任何参与者（或者任何观察者）都可以通过检查 primary key 的状态来决定事务的结局。

### 2.2 三个 Column Family

Percolator 把每个 key 的数据分散在三个 CF 里：

```
CfDefault:   存实际的值。key 格式: key_startTS → value
CfWrite:     提交记录。key 格式: key_commitTS → Write{StartTS, Kind}
CfLock:      锁信息。key 格式: key → Lock{Primary, Ts, Ttl, Kind}
```

这三者的关系可以这样理解：

```
┌───────────────────────────────────────────────────────────────┐
│                        一次写操作的生命周期                        │
├───────────────────────────────────────────────────────────────┤
│                                                               │
│  Prewrite 阶段:                                                │
│    CfDefault  key_100 → "hello"         ← 数据先落盘           │
│    CfLock     key   → Lock{ts:100, primary:"keyA", ttl:5}     │
│                                           ↑ 加锁，表示"我在写"    │
│                                                               │
│  Commit 阶段:                                                  │
│    CfWrite    key_110 → Write{startTS:100, kind:Put}          │
│                                           ↑ 提交记录，指向数据    │
│    CfLock     key   → (deleted)           ← 删锁               │
│                                                               │
│  读取路径:                                                      │
│    1. 检查 CfLock → 有锁？报冲突                                 │
│    2. 查 CfWrite → 找到 commitTS ≤ 我的 startTS 的最新提交       │
│    3. 从 Write 中拿到 startTS，去 CfDefault 读 key_startTS       │
│                                                               │
└───────────────────────────────────────────────────────────────┘
```

### 2.3 Key 编码的设计

这是整个 MVCC 层最精妙的设计之一。`EncodeKey` 的实现：

```go
func EncodeKey(key []byte, ts uint64) []byte {
    encodedKey := codec.EncodeBytes(key)
    newKey := append(encodedKey, make([]byte, 8)...)
    binary.BigEndian.PutUint64(newKey[len(encodedKey):], ^ts)  // 注意这个 ^ts
    return newKey
}
```

`^ts` 是按位取反。为什么？因为 BadgerDB（底层存储）的 key 是按字典序升序排列的。取反后，**时间戳大的反而排在前面**。这意味着：

- 对某个 key 做 `Seek(EncodeKey(key, readTS))`，第一个命中的就是 `commitTS ≤ readTS` 的最新版本
- 不需要遍历所有版本，一次 Seek 就能定位

这个设计直接决定了后续所有读操作的实现方式。比如 `CurrentWrite` 方法就是利用这一点，从 readTS 开始向前扫描 CfWrite，找到第一个属于指定 key 的 write 记录。

### 2.4 完整的事务流程

以一个转账事务为例：从 A 扣 100，给 B 加 100。

```
时间线:
  startTS = 100（事务开始时从全局时间戳服务获取）
  commitTS = 110（提交时再获取一个更大的时间戳）

Step 1: Prewrite（选 A 为 primary key）
  对 A（primary）:
    检查 write-write 冲突 → CfWrite 里 [100, ∞) 范围内无其他事务的提交
    检查锁冲突 → CfLock 里 A 没有被其他事务锁住
    写入 CfDefault: A_100 → 900   (A 的新值)
    写入 CfLock:    A → Lock{primary: A, ts: 100, ttl: 5, kind: Put}

  对 B（secondary）:
    同样的冲突检查
    写入 CfDefault: B_100 → 1100  (B 的新值)
    写入 CfLock:    B → Lock{primary: A, ts: 100, ttl: 5, kind: Put}

Step 2: Commit
  先提交 primary（A）:
    写入 CfWrite:   A_110 → Write{startTS: 100, kind: Put}
    删除 CfLock:    A

  再提交 secondary（B）:
    写入 CfWrite:   B_110 → Write{startTS: 100, kind: Put}
    删除 CfLock:    B

关键点: primary 提交成功的那一刻，整个事务就算提交成功了。
secondary 的提交可以异步做，如果中间挂了，其他事务读到 B 的锁时，
会去检查 primary（A）的状态，发现已经提交，就帮忙把 B 也提交了。
```

这就是 Percolator 不需要独立协调者的原因——primary key 就是事务状态的"真相来源"（single source of truth）。

## 三、Part A：MVCC 读写层

### 3.1 MvccTxn 结构

```go
type MvccTxn struct {
    StartTS uint64
    Reader  storage.StorageReader
    writes  []storage.Modify  // 缓冲所有写操作，最后批量提交
}
```

`writes` 是一个写缓冲区。所有 Put/Delete 操作先写到这个 buffer 里，最后通过 `Write()` 一次性提交到存储引擎。这保证了原子性——要么全写入，要么全不写。

### 3.2 读操作

**GetValue(key)**：从 CfDefault 读值。注意 key 要编码成 `key_startTS`，因为写入时是以 startTS 作为版本号的。

**CurrentWrite(key)**：从 CfWrite 里找当前事务可见的最新提交。这个方法是整个读路径的核心：

```go
func (txn *MvccTxn) CurrentWrite(key []byte) (*Write, uint64, error) {
    iter := txn.Reader.IterCF(engine_util.CfWrite)
    defer iter.Close()

    // 从 startTS 开始向前扫描
    for iter.Seek(EncodeKey(key, txn.StartTS)); iter.Valid(); iter.Next() {
        item := iter.Item()
        userKey := DecodeUserKey(item.Key())
        if !bytes.Equal(userKey, key) {
            break  // 已经扫到其他 key 了
        }
        value, _ := item.Value()
        write, _ := ParseWrite(value)
        if write.Kind != WriteKindRollback {
            return write, decodeTimestamp(item.Key()), nil
        }
        // Rollback 记录跳过，继续看更早的版本
    }
    return nil, 0, nil
}
```

**MostRecentWrite(key)**：和 `CurrentWrite` 类似，但从 `TsMax`（最大时间戳）开始扫描，找到该 key 的最新 write 记录，不做可见性过滤。这个方法用于冲突检测——Prewrite 时需要检查 `[startTS, +∞)` 范围内有没有其他事务的提交。

### 3.3 写操作

写操作都是往 `writes` buffer 里追加 `Modify`：

- `PutWrite(key, ts, write)` → 写 CfWrite
- `DeleteWrite(key, ts)` → 删 CfWrite
- `PutLock(key, lock)` → 写 CfLock
- `DeleteLock(key)` → 删 CfLock
- `PutValue(key, value)` → 写 CfDefault（key 编码为 `key_startTS`）
- `DeleteValue(key)` → 删 CfDefault

`★ Insight ─────────────────────────────────────`
Write 记录的序列化格式很紧凑：1 字节 Kind + 8 字节 StartTS，总共 9 字节。Kind 用单字节编码（Put=1, Delete=2, Rollback=3），StartTS 用大端序 uint64。这种紧凑编码在 LSM-tree 存储引擎中很重要，因为 CfWrite 的记录量是最大的（每次提交每个 key 都会产生一条），编码越紧凑，compaction 的开销越小。
`─────────────────────────────────────────────────`

## 四、Part B：Percolator 2PC 核心

### 4.1 Server 层的通用模式

4B 和 4C 的所有 handler 都遵循一个共同模式：

```go
func (server *Server) KvXxx(ctx context.Context, req *kvrpcpb.XxxRequest) (*kvrpcpb.XxxResponse, error) {
    // 1. 获取 Reader（处理 RegionError）
    reader, err := server.storage.Reader(req.Context)
    if err != nil {
        // RegionError → 放到 resp.RegionError
        resp.RegionError = ...
        return resp, nil
    }
    defer reader.Close()

    // 2. 创建事务
    txn := mvcc.NewMvccTxn(reader, req.Version)

    // 3. 获取 Latches（写操作需要，读操作不需要）
    server.Latches.WaitForLatches(keysToLatch)
    defer server.Latches.ReleaseLatches(keysToLatch)

    // 4. 业务逻辑
    ...

    // 5. 写入（写操作需要）
    server.storage.Write(req.Context, txn.Writes())
}
```

其中 Latches 是本地的并发控制机制。它的实现很简单：一个 `map[string]*sync.WaitGroup`，每个 key 对应一个 WaitGroup。`WaitForLatches` 会等待所有 key 的锁释放，然后获取锁；`ReleaseLatches` 释放锁。注意这只是本地互斥，跨节点的并发控制靠 Percolator 的锁机制。

### 4.2 KvGet

KvGet 的逻辑很直接，但有一个关键点：**先检查锁，再读值**。

```
1. GetLock(key)
2. 如果有锁且 lock.Ts <= startTS → 报 LockInfo 错误
3. CurrentWrite(key) → 找到可见的最新提交
4. 如果 write.Kind == WriteKindDelete → resp.NotFound = true
5. 否则从 CfDefault 读 key_write.StartTS 的值
```

为什么锁的检查条件是 `lock.Ts <= startTS`？因为如果锁的时间戳比我的 startTS 大，说明那个事务在我之后开始的，对我来说它的写入还不可见，我可以放心读老版本。但如果锁的时间戳 ≤ 我的 startTS，那个事务可能在我的快照时间点之前就已经写了数据但还没提交，我不确定它会提交还是回滚，所以只能报锁冲突让客户端重试。

### 4.3 KvPrewrite

Prewrite 是事务的第一阶段，需要对每个 key 做两件事：冲突检测 + 加锁写值。

```
对每个 key:
  1. MostRecentWrite(key) → 检查 write-write 冲突
     如果找到 write 且 write.commitTS >= startTS → WriteConflict
  2. GetLock(key) → 检查锁冲突
     如果有锁且 lock.Ts != startTS → KeyIsLocked
  3. PutLock(key, Lock{Primary, Ts: startTS, Ttl, Kind})
  4. PutValue(key, value)  或  DeleteValue(key)
```

MostRecentWrite 检查的是 `commitTS >= startTS`，这是快照隔离的核心约束：如果在我的 startTS 之后有其他事务提交了同一个 key，说明我读到的快照已经过时了，继续写入会覆盖别人的数据。

### 4.4 KvCommit

Commit 是第二阶段，把锁转成提交记录：

```
对每个 key:
  1. GetLock(key)
  2. 如果没有锁 → 可能已经被回滚，检查是否有 rollback 记录
  3. 如果锁存在但 lock.Ts != startTS → 锁被其他事务替换了
  4. PutWrite(key, commitTS, Write{StartTS, Kind})
  5. DeleteLock(key)
```

## 五、Part C：事务的收尾和容错

### 5.1 Scanner

Scanner 是一个 MVCC 感知的迭代器，用于实现 KvScan。它的核心挑战是：CfWrite 里同一个 key 可能有很多个版本（多次写入产生多条 write 记录），Scanner 需要跳过不可见的版本，只返回每个 key 的最新可见值。

```go
func (scan *Scanner) Next() ([]byte, []byte, error) {
    for scan.iter.Valid() {
        item := scan.iter.Item()
        userKey := DecodeUserKey(item.Key())
        commitTS := decodeTimestamp(item.Key())

        // 这个版本在我的快照之后提交的，不可见
        if commitTS > scan.txn.StartTS {
            scan.iter.Seek(EncodeKey(userKey, scan.txn.StartTS))
            continue
        }

        write := ParseWrite(item.Value())
        switch write.Kind {
        case WriteKindPut:
            val := scan.txn.Reader.GetCF(CfDefault, EncodeKey(userKey, write.StartTS))
            scan.iter.Seek(EncodeKey(userKey, 0))  // 跳到下一个 key
            return userKey, val, nil

        case WriteKindDelete:
            scan.iter.Seek(EncodeKey(userKey, 0))  // 这个 key 被删了，跳过
            continue

        case WriteKindRollback:
            scan.iter.Next()  // rollback 不算有效版本，看同 key 更早的版本
            continue
        }
    }
    return nil, nil, nil
}
```

这里有个精巧的技巧：**用 `Seek(EncodeKey(userKey, 0))` 来跳到下一个 key**。

为什么这样能跳到下一个 key？因为 `EncodeKey(key, 0)` 编码后 ts 部分是 `^0 = 0xFFFFFFFFFFFFFFFF`，这是该 key 所有版本中字典序最大的。Seek 到这里后，迭代器会跳到比它大的下一个位置——也就是下一个 userKey 的第一个版本。

而对于 Rollback 记录，不能用这个技巧，因为同一个 key 可能在 rollback 之前还有一个有效的 Put 版本，需要用 `iter.Next()` 逐条检查。

### 5.2 KvScan

KvScan 是只读操作，不需要 Latch：

```go
func (server *Server) KvScan(_ context.Context, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
    reader, err := server.storage.Reader(req.Context)
    // ... RegionError 处理
    defer reader.Close()

    txn := mvcc.NewMvccTxn(reader, req.Version)
    scanner := mvcc.NewScanner(req.StartKey, txn)
    defer scanner.Close()

    var pairs []*kvrpcpb.KvPair
    for i := 0; i < int(req.Limit); i++ {
        key, value, err := scanner.Next()
        if key == nil { break }
        pairs = append(pairs, &kvrpcpb.KvPair{Key: key, Value: value})
    }
    return &kvrpcpb.ScanResponse{Pairs: pairs}, nil
}
```

### 5.3 KvCheckTxnStatus

这是 Percolator 容错机制的核心。当一个事务的锁迟迟没有被提交或回滚，其他事务会调用 CheckTxnStatus 来检查它的状态并决定下一步。

检查的是**事务的 primary key**，因为 primary key 的状态就是整个事务的状态：

```
检查 primary key 的锁:

Case 1: 锁存在且属于目标事务
  → 检查 TTL 是否过期
  → 过期：回滚（删锁 + 删值 + 写 rollback 记录），返回 Action_TTLExpireRollback
  → 未过期：返回 LockTtl，调用方等一等再重试

Case 2: 锁不存在（或属于其他事务）
  → 查 CurrentWrite：
  → 找到 Put/Delete 类型的 write → 事务已提交，返回 CommitVersion
  → 找到 Rollback → 事务已回滚，返回 Action_NoAction
  → 什么都没找到 → 事务可能 prewrite 丢失了，写一条"保护性 rollback"，
                     返回 Action_LockNotExistRollback
```

TTL 比较用的是 `PhysicalTime(ts)` 而不是直接比较 ts。这是因为 TinyKV 的时间戳是一个复合值：高位是物理时间（毫秒），低位是逻辑计数器。TTL 是以毫秒为单位的，所以必须提取物理时间部分来比较。

"保护性 rollback"（Case 2 的最后一种情况）是一个很重要的设计：如果一个事务的 prewrite 消息因为网络延迟还没到达，但 CheckTxnStatus 已经判定它超时了，这条 rollback 记录可以防止 prewrite 后来才到达时成功加锁——Prewrite 会检查是否已有 write 记录，发现有 rollback 就会放弃。

### 5.4 KvBatchRollback

批量回滚一个事务涉及的多个 key。核心逻辑：

```
对每个 key:
  1. CurrentWrite(key) → 检查是否已有 write 记录
     - 已有非 Rollback 类型的 write → 事务已经被提交了！返回 Abort 错误
     - 已有 Rollback 类型的 write → 幂等，跳过这个 key
  2. GetLock(key)
     - 锁属于本事务（lock.Ts == startTS）→ 删锁 + 删值
     - 锁属于其他事务 → 不碰它
     - 没有锁 → 正常（prewrite 可能还没到）
  3. 写 Rollback 记录: PutWrite(key, startTS, Write{Kind: Rollback})
```

几个值得注意的细节：

**已提交的事务不能回滚**——这是事务正确性的底线。如果客户端因为超时重试了回滚，但事务其实已经被另一个路径提交了，回滚必须失败。

**Rollback 记录的 commitTS 等于 startTS**——这和正常提交不同（正常提交的 commitTS > startTS）。这样设计是因为 rollback 不需要一个新的时间戳，用 startTS 就够了，而且可以和同一个 key 的正常提交记录区分开。

**即使没有锁也要写 rollback 记录**——这就是前面提到的"保护性 rollback"。防止一条迟到的 prewrite 在 rollback 之后才到达。

### 5.5 KvResolveLock

ResolveLock 用于批量处理一个事务遗留的所有锁。它是 CheckTxnStatus 的后续操作——CheckTxnStatus 告诉你事务的结局（提交 or 回滚），ResolveLock 负责执行这个结局。

```go
// 1. 找到属于该事务的所有锁
locks := mvcc.AllLocksForTxn(txn)

// 2. 根据 CommitVersion 决定是提交还是回滚
for _, kl := range locks {
    if req.CommitVersion > 0 {
        // 提交模式：把锁转成 write 记录
        txn.PutWrite(kl.Key, req.CommitVersion,
            &mvcc.Write{StartTS: req.StartVersion, Kind: kl.Lock.Kind})
        txn.DeleteLock(kl.Key)
    } else {
        // 回滚模式：删锁 + 删值 + 写 rollback
        txn.DeleteLock(kl.Key)
        txn.DeleteValue(kl.Key)
        txn.PutWrite(kl.Key, req.StartVersion,
            &mvcc.Write{StartTS: req.StartVersion, Kind: mvcc.WriteKindRollback})
    }
}
```

`AllLocksForTxn` 会遍历整个 CfLock，只返回 `lock.Ts == txn.StartTS` 的锁。已经被提交或回滚的 key 没有锁，自然不会被返回，所以不会被重复处理。

## 六、实现中的思考

### 6.1 谁需要 Latch，谁不需要？

梳理下来，规律很清晰：

| Handler | 需要 Latch？ | 原因 |
|---------|:---:|------|
| KvGet | 否 | 只读操作，MVCC 保证快照隔离 |
| KvScan | 否 | 只读操作 |
| KvPrewrite | 是 | 写数据 + 加锁 |
| KvCommit | 是 | 删锁 + 写提交记录 |
| KvCheckTxnStatus | 是 | 可能删锁 + 写 rollback |
| KvBatchRollback | 是 | 可能删锁 + 写 rollback |
| KvResolveLock | 是 | 批量删锁 + 写记录 |

Latch 只保护写操作的原子性。读操作不需要 Latch，因为 MVCC 的快照读天然不会和写操作冲突——读的是历史版本，写的是新版本。

### 6.2 Rollback 记录为什么要持久化？

初看有点反直觉——事务都回滚了，为什么还要往存储里写东西？

原因是网络的异步性。考虑这个场景：

```
时间线:
  T1: 客户端发起 Prewrite(key=A, startTS=100)
  T2: 消息在网络中延迟
  T3: 客户端超时，发起 Rollback(key=A, startTS=100)
  T4: Rollback 成功，写入 rollback 记录
  T5: T1 的 Prewrite 消息终于到达！

如果没有 rollback 记录:
  T5 的 Prewrite 会成功加锁，但事务已经被客户端放弃了，
  这把锁永远不会被正常提交，只能等 TTL 过期后被清理。

有 rollback 记录:
  T5 的 Prewrite 检查 MostRecentWrite，发现有 rollback 记录，
  直接拒绝，避免了悬挂锁。
```

这就是分布式系统中常见的"防止迟到消息"模式。Rollback 记录本质上是一个 tombstone，标记"这个时间戳的事务已经死了，别再复活它"。

### 6.3 Scanner 中三种 Write 类型的处理差异

这是实现 Scanner 时需要仔细思考的地方：

- **Put**：找到有效值，读 CfDefault 返回，然后跳到下一个 key
- **Delete**：该 key 在这个版本被删除了，跳到下一个 key（不返回）
- **Rollback**：这只是说明某个事务被回滚了，不代表该 key 没有数据。需要继续看同一个 key 的更早版本

Put 和 Delete 都用 `Seek(EncodeKey(key, 0))` 跳到下一个 key，因为它们都是"最终态"——对于当前读事务来说，这个 key 在这个版本的状态已经确定了（存在 or 已删除），不需要看更早的版本。

Rollback 不一样，它只是说"这个版本被撤销了"，不影响更早版本的有效性。所以用 `iter.Next()` 继续看同一个 key 的下一条 write 记录。

## 七、测试驱动的实现验证

Project 4C 的 22 个测试覆盖了大量边界情况，每一组测试都在验证一个具体的语义约束：

### KvBatchRollback 的边界

| 测试 | 场景 | 验证的约束 |
|------|------|-----------|
| TestRollbackCommitted4C | 回滚一个已提交的事务 | 必须返回 Abort 错误，不能回滚 |
| TestRollbackDuplicate4C | 重复回滚同一个事务 | 幂等性——已有 rollback 记录时直接跳过 |
| TestRollbackOtherTxn4C | key 上有其他事务的锁 | 不碰别人的锁，但仍写 rollback 记录 |
| TestRollbackMissingPrewrite4C | Prewrite 还没到达 | 仍写 rollback 记录（保护性 rollback） |

### KvCheckTxnStatus 的边界

| 测试 | 场景 | 验证的约束 |
|------|------|-----------|
| TestCheckTxnStatusTtlNotExpired4C | 锁未过期 | 返回 LockTtl，不做任何操作 |
| TestCheckTxnStatusTtlExpired4C | 锁已过期 | 回滚 + TTLExpireRollback |
| TestCheckTxnStatusCommitted4C | 事务已提交（无锁） | 返回 CommitVersion |
| TestCheckTxnStatusRolledBack4C | 事务已回滚（无锁） | NoAction |
| TestCheckTxnStatusNoLockNoWrite4C | 无锁无 write | 保护性 rollback + LockNotExistRollback |

这些测试验证了一个核心设计思想：**事务的状态机是确定性的**。给定 primary key 的当前存储状态，任何节点都能独立判断事务的结局，不需要额外的协调。

## 八、知识点总结

### 8.1 快照隔离（Snapshot Isolation）

Project 4 实现的是 SI 级别的隔离。SI 的核心保证：

- **读不阻塞写，写不阻塞读**：读操作看到的是 startTS 时刻的快照，不受并发写的影响
- **Write-Write 冲突检测**：如果两个事务同时写同一个 key，后提交的那个会在 Prewrite 阶段检测到冲突

SI 不能防止 Write Skew（两个事务各读一个值、各写另一个值，结果违反约束），这需要更强的 SSI（Serializable Snapshot Isolation）。TiDB 在 TinyKV 的基础上做了 SSI 的增强。

### 8.2 时间戳的角色

Percolator 的时间戳有两个：

- **startTS**：事务开始时获取，决定了读的快照点
- **commitTS**：提交时获取，决定了写的可见性

这两个时间戳来自一个全局的时间戳服务（TSO），保证严格递增。startTS 和 commitTS 的间隔就是事务的"可见性窗口"——其他在这个窗口内开始的事务，读不到这个事务的写入。

### 8.3 Latch vs Lock

这两个词在代码中都出现了，但含义完全不同：

- **Latch**（`latches.go`）：本地内存中的互斥锁，保护同一个节点上并发请求的原子性。只在请求处理期间持有，请求结束就释放。
- **Lock**（`lock.go`）：持久化在 CfLock 中的分布式锁，是 Percolator 协议的一部分。从 Prewrite 开始持有，到 Commit/Rollback 才释放。可能持续几秒到几分钟。

Latch 是实现层面的并发控制，Lock 是协议层面的事务控制。一个在内存里，一个在磁盘上。一个是毫秒级的，一个是秒级的。

### 8.4 从 Percolator 到 TiDB

TinyKV 的 Project 4 实现的是 Percolator 的核心，但真实的 TiDB/TiKV 还有很多工程上的增强：

- **Async Commit**：secondary key 不需要等 primary 提交成功就可以返回客户端，降低延迟
- **1PC**：如果事务只涉及一个 Region，退化成一阶段提交
- **悲观锁**：Prewrite 之前先加悲观锁，减少冲突时的重试开销
- **Green GC**：异步清理过期的 MVCC 版本，防止数据膨胀
- **Large Transaction**：对大事务的特殊优化，避免 TTL 过期导致误回滚

## 九、总结

Project 4 的核心收获：

1. **MVCC 的本质是"空间换时间"**。通过保留多个版本的数据，让读操作不需要等待写操作完成，大幅提升了并发性能。代价是存储空间的增长和 GC 的复杂度。

2. **Percolator 用"数据即状态"解决了协调者单点问题**。事务的提交状态编码在 primary key 的 write 记录里，任何节点都可以独立判断事务的结局。这个设计思路在分布式系统中很常见——把协调信息分散到数据本身，而不是集中在一个协调者上。

3. **Key 编码决定了上层的算法复杂度**。`^ts` 这个简单的按位取反，让"找到某个时间点的最新版本"从 O(N) 的遍历变成了 O(log N) 的二分查找。好的编码设计能从根本上简化上层逻辑。

4. **Rollback 记录是分布式系统中"防止迟到消息"的经典模式**。在异步网络中，你无法假设消息会按序到达。Tombstone 机制通过占位来阻止过期操作的执行。

5. **容错逻辑的代码量往往超过正常路径**。Part B（正常的 Get/Prewrite/Commit）的 handler 加起来可能 100 行，Part C（CheckTxnStatus/BatchRollback/ResolveLock）的容错逻辑加起来超过 200 行。这是分布式系统的常态——大部分代码都在处理"出了问题怎么办"。

---

**参考资料：**

1. [TinyKV Project 4 文档](doc/project4-Transaction.md)
2. [Percolator 论文 - Large-scale Incremental Processing Using Distributed Transactions and Notifications](https://research.google/pubs/pub36726/)
3. [TiKV 源码解析 - 事务](https://tikv.org/deep-dive/distributed-transaction/introduction/)
