# TinyKV Project1 技术总结

## 项目概述

Project1 实现了一个单机版的 KV 存储引擎，提供基于 gRPC 的键值存储服务，支持 Column Family（列族）特性。这是 TinyKV 分布式存储系统的基础模块。

核心功能包括：
- **Put**: 写入键值对到指定列族
- **Get**: 从指定列族读取键值
- **Delete**: 从指定列族删除键
- **Scan**: 范围扫描指定列族的键值对

## 架构设计

### 整体架构

```
gRPC Server (Server)
    ↓
Storage Interface
    ↓
StandAloneStorage (Badger 封装)
    ↓
engine_util (CF 模拟层)
    ↓
Badger (底层存储引擎)
```

### 核心组件

1. **Server 层** (`kv/server/raw_api.go`)
   - 处理 gRPC 请求
   - 调用 Storage 接口完成操作
   - 负责错误处理和响应构造

2. **Storage 层** (`kv/storage/standalone_storage/standalone_storage.go`)
   - 实现 Storage 接口
   - 封装 Badger 数据库
   - 提供 Reader 和 Write 抽象

3. **engine_util 层** (`kv/util/engine_util/`)
   - 模拟 Column Family 支持
   - 提供批量写入能力
   - 封装迭代器操作

## 核心技术要点

### 1. Column Family 的模拟实现

**问题背景**：Badger 原生不支持 Column Family，但 TinyKV 需要 CF 来支持后续的事务模型。

**解决方案**：通过键前缀模拟 CF

```go
// 将 CF 名称作为前缀拼接到 key 前面
func KeyWithCF(cf string, key []byte) []byte {
    return append([]byte(cf+"_"), key...)
}
```

**实现细节**：
- 预定义三个 CF：`default`、`write`、`lock`
- 存储时：`key` → `${cf}_${key}`
- 读取时：自动添加前缀，返回时去除前缀
- 迭代时：使用 `ValidForPrefix` 限定范围

**优势**：
- 实现简单，无需修改 Badger 源码
- 性能开销小，只是字符串拼接
- 保持了 CF 之间的逻辑隔离

**局限性**：
- 无法像 RocksDB 那样为不同 CF 配置独立参数
- 所有 CF 共享同一个 LSM 树结构

### 2. 事务与快照隔离

**核心设计**：使用 Badger 的只读事务实现快照读

```go
func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
    txn := s.engine.NewTransaction(false)  // false = 只读事务
    return &StandAloneStorageReader{
        txn: txn,
    }, nil
}
```

**关键点**：
- 只读事务（`false` 参数）创建时会获取当前数据库的快照
- 快照保证了读操作的一致性视图
- 多个读操作在同一个 Reader 中共享快照
- 必须调用 `Discard()` 释放事务资源

**为什么需要快照**：
- 防止读取过程中数据被其他写操作修改
- 保证 Scan 操作的一致性（不会读到部分新数据）
- 为后续的 MVCC 实现打基础

### 3. 批量写入与原子性

**WriteBatch 设计**：

```go
type WriteBatch struct {
    entries       []*badger.Entry  // 待写入的条目
    size          int               // 总大小
    safePoint     int               // 安全点位置
    safePointSize int               // 安全点大小
}
```

**原子性保证**：

```go
func (wb *WriteBatch) WriteToDB(db *badger.DB) error {
    return db.Update(func(txn *badger.Txn) error {
        for _, entry := range wb.entries {
            if len(entry.Value) == 0 {
                err = txn.Delete(entry.Key)
            } else {
                err = txn.SetEntry(entry)
            }
        }
        return nil
    })
}
```

**技术细节**：
- 所有修改在一个 Badger 事务中执行
- 要么全部成功，要么全部失败
- Delete 操作通过 `Value` 为空来标识
- SafePoint 机制支持部分回滚

**应用场景**：
- 单个 RawPut/RawDelete 请求
- 后续 Project 中的事务提交
- 批量数据导入

### 4. 迭代器的 CF 适配

**问题**：Badger 的迭代器不知道 CF 的概念，需要适配。

**解决方案**：BadgerIterator 包装器

```go
type BadgerIterator struct {
    iter   *badger.Iterator
    prefix string  // CF 前缀，如 "default_"
}

func (it *BadgerIterator) Seek(key []byte) {
    // 自动添加 CF 前缀
    it.iter.Seek(append([]byte(it.prefix), key...))
}

func (it *BadgerIterator) Valid() bool {
    // 只在当前 CF 范围内有效
    return it.iter.ValidForPrefix([]byte(it.prefix))
}
```

**CFItem 包装器**：

```go
type CFItem struct {
    item      *badger.Item
    prefixLen int  // CF 前缀长度
}

func (i *CFItem) Key() []byte {
    // 返回时去除 CF 前缀
    return i.item.Key()[i.prefixLen:]
}
```

**设计亮点**：
- 对上层透明，使用者无需关心 CF 前缀
- 自动处理边界条件
- 保证迭代器不会越界到其他 CF

### 5. 资源管理

**关键的资源释放**：

```go
// Reader 使用完必须关闭
defer reader.Close()

// 迭代器使用完必须关闭
defer iter.Close()

// Reader.Close() 内部会 Discard 事务
func (r *StandAloneStorageReader) Close() {
    r.txn.Discard()
}
```

**为什么重要**：
- Badger 事务持有内存资源
- 迭代器持有文件句柄
- 不释放会导致内存泄漏和文件描述符耗尽
- 影响后续事务的性能

## 实现过程中的问题与思考

### 问题 1：KeyNotFound 的处理

**问题描述**：Badger 在 key 不存在时返回 `ErrKeyNotFound` 错误，但 gRPC 接口需要返回 `nil` 值而不是错误���

**解决方案**：

```go
func (r *StandAloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
    val, err := engine_util.GetCFFromTxn(r.txn, cf, key)
    if err == badger.ErrKeyNotFound {
        return nil, nil  // 转换为 nil 值
    }
    return val, err
}
```

**思考**：
- 存储层和应用层对"不存在"的语义理解不同
- 存储层：不存在是一种错误状态
- 应用层：不存在是正常的业务场景
- 需要在适配层做语义转换

### 问题 2：Scan 的边界处理

**问题描述**：如何保证 Scan 操作不会读取到其他 CF 的数据？

**解决方案**：

```go
func (it *BadgerIterator) Valid() bool {
    return it.iter.ValidForPrefix([]byte(it.prefix))
}
```

**关键点**：
- `ValidForPrefix` 检查当前 key 是否以 CF 前缀开头
- 一旦迭代到其他 CF 的数据，立即返回 false
- 配合 `Limit` 参数控制返回数量

**边界情况**：
- CF 为空时的处理
- StartKey 不存在时的 Seek 行为（会定位到下一个更大的 key）
- Limit 为 0 的处理

### 问题 3：Value 的拷贝问题

**问题描述**：Badger 的 Value 在事务外部访问会出错。

**原因分析**：
- Badger 的 Value 可能存储在 Value Log 中
- Item 只是一个引用，不是实际数据
- 事务 Discard 后，Value 引用失效

**解决方案**：

```go
value, err := item.ValueCopy(nil)  // 拷贝 Value 到新内存
```

**性能考虑**：
- ValueCopy 会分配新内存并拷贝数据
- 对于大 Value 有性能开销
- 但保证了数据安全性
- 可以传入预分配的 buffer 减少分配

### 问题 4：WriteBatch 的设计权衡

**为什么不直接用 Badger 的 Txn**：

1. **接口统一**：WriteBatch 提供了统一的批量写入接口
2. **延迟执行**：可以先收集修改，最后一次性提交
3. **SafePoint 机制**：支持部分回滚，Badger Txn 不支持
4. **CF 适配**：自动处理 CF 前缀

**设计思考**：
- 牺牲了一点内存（缓存 entries）
- 换取了更好的灵活性和可扩展性
- 为后续的事务实现预留了空间

## 技术延伸

### 1. Badger 的 LSM 树结构

Badger 是基于 LSM（Log-Structured Merge-Tree）的存储引擎：

**写入流程**：
1. 写入 WAL（Write-Ahead Log）保证持久性
2. 写入 MemTable（内存中的跳表）
3. MemTable 满后刷到磁盘成为 SSTable
4. 后台定期 Compaction 合并 SSTable

**读取流程**：
1. 先查 MemTable
2. 再查各层 SSTable（从新到旧）
3. 使用 Bloom Filter 加速查找

**优势**：
- 写入性能极高（顺序写）
- 适合写多读少的场景
- 支持高效的范围扫描

### 2. MVCC 的基础

虽然 Project1 还没有实现 MVCC，但已经打下了基础：

**快照隔离**：
- 只读事务提供了时间点快照
- 这是 MVCC 的核心机制

**Column Family**：
- `write` CF 存储版本信息
- `lock` CF 存储锁信息
- `default` CF 存储实际数据

**后续扩展**：
- 为每个 key 添加时间戳
- 实现多版本并发控制
- 支持事务的 ACID 特性

### 3. gRPC 的流式处理

当前实现是简单的 Unary RPC，但 gRPC 支持流式处理：

**潜在优化**：
- Scan 可以用 Server Streaming 返回大量数据
- 批量写入可以用 Client Streaming 接收数据
- 双向流可以实现更复杂的交互

**权衡**：
- 流式处理更复杂
- 需要处理背压和流控
- 当前简单实现已满足需求

### 4. 性能优化方向

**读优化**：
- 添加 LRU 缓存减少磁盘访问
- 使用 Bloom Filter 加速不存在的 key 查询
- 批量读取减少 RPC 开销

**写优化**：
- 批量写入减少事务开销
- 异步写入提高吞吐量
- 调整 Badger 的 Compaction 参数

**空间优化**：
- 定期 GC 清理旧版本数据
- 压缩 Value 减少存储空间
- 调整 SSTable 大小和层数

## 测试验证

所有测试用例均通过：

```
TestRawGet1                  ✓
TestRawGetNotFound1          ✓
TestRawPut1                  ✓
TestRawGetAfterRawPut1       ✓
TestRawGetAfterRawDelete1    ✓
TestRawDelete1               ✓
TestRawScan1                 ✓
TestRawScanAfterRawPut1      ✓
TestRawScanAfterRawDelete1   ✓
TestIterWithRawDelete1       ✓
```

**测试覆盖**：
- 基本的 CRUD 操作
- 不存在 key 的处理
- 删除后的读取行为
- 范围扫描的正确性
- 迭代器与删除的交互

## 总结

Project1 虽然是单机版本，但设计上已经考虑了后续分布式系统的需求：

1. **Column Family** 为事务模型做准备
2. **快照隔离** 为 MVCC 打基础
3. **批量写入** 为分布式事务做准备
4. **清晰的分层** 便于后续扩展

核心收获：
- 理解了 LSM 树的基本原理
- 掌握了 Badger 的使用方法
- 学会了如何适配第三方库
- 体会了接口设计的重要性

这个项目看似简单，实则包含了存储系统的核心概念，为后续的 Raft、事务、调度等复杂功能奠定了坚实基础。
