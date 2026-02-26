// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"

// RaftLog manage the log entries, its struct look like:
//
//  snapshot/first.....applied....committed....stabled.....last
//  --------|------------------------------------------------|
//                            log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64

	// all entries that have not yet compact.
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	firstIndex, err := storage.FirstIndex()
	if err != nil {
		panic(err)
	}
	lastIndex, err := storage.LastIndex()
	if err != nil {
		panic(err)
	}

	entries, err := storage.Entries(firstIndex, lastIndex+1)
	if err != nil {
		panic(err)
	}

	return &RaftLog{
		storage:   storage,
		committed: firstIndex - 1,
		applied:   firstIndex - 1,
		stabled:   lastIndex,
		entries:   entries,
	}
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
	// Get the first index from storage (reflects truncated state after compaction)
	firstIndex, err := l.storage.FirstIndex()
	if err != nil {
		return
	}

	// Remove entries from l.entries that are before firstIndex
	// These entries are already included in the snapshot
	if len(l.entries) > 0 && l.entries[0].Index < firstIndex {
		// Find the position where entries start from firstIndex
		offset := firstIndex - l.entries[0].Index
		if offset >= uint64(len(l.entries)) {
			// All entries are compacted
			l.entries = []pb.Entry{}
		} else {
			// Keep only entries from firstIndex onwards
			l.entries = l.entries[offset:]
		}
	}
}

// allEntries return all the entries not compacted.
// note, exclude any dummy entries from the return value.
// note, this is one of the test stub functions you need to implement.
func (l *RaftLog) allEntries() []pb.Entry {
	return l.entries
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	if len(l.entries) == 0 {
		return []pb.Entry{}
	}
	firstIdx := l.entries[0].Index
	if l.stabled >= l.LastIndex() {
		return []pb.Entry{}
	}
	return l.entries[l.stabled-firstIdx+1:]
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	if len(l.entries) == 0 {
		return nil
	}
	firstIdx := l.entries[0].Index
	if l.applied >= l.committed {
		return nil
	}
	return l.entries[l.applied-firstIdx+1 : l.committed-firstIdx+1]
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	if len(l.entries) > 0 {
		return l.entries[len(l.entries)-1].Index
	}
	// Check pending snapshot first (2C)
	if l.pendingSnapshot != nil && l.pendingSnapshot.Metadata != nil {
		return l.pendingSnapshot.Metadata.Index
	}
	// If no entries, get from snapshot
	snapshot, err := l.storage.Snapshot()
	if err != nil {
		// If snapshot is temporarily unavailable, return stabled index
		// This can happen during initialization
		return l.stabled
	}
	return snapshot.Metadata.Index
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Check if index is in entries
	if len(l.entries) > 0 {
		firstIdx := l.entries[0].Index
		if i >= firstIdx && i <= l.LastIndex() {
			return l.entries[i-firstIdx].Term, nil
		}
	}

	// Check pending snapshot (2C)
	if l.pendingSnapshot != nil && l.pendingSnapshot.Metadata != nil && i == l.pendingSnapshot.Metadata.Index {
		return l.pendingSnapshot.Metadata.Term, nil
	}

	// Check snapshot
	snapshot, err := l.storage.Snapshot()
	if err == nil && i == snapshot.Metadata.Index {
		return snapshot.Metadata.Term, nil
	}

	// Try storage
	return l.storage.Term(i)
}
