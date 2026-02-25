package raft

import pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"

// append appends entries to the log
// It handles conflict resolution by truncating conflicting entries
func (l *RaftLog) append(ents ...pb.Entry) {
	if len(ents) == 0 {
		return
	}

	if len(l.entries) == 0 {
		l.entries = ents
		return
	}

	firstIdx := l.entries[0].Index

	// For each entry to append, check if it conflicts with existing entries
	for i, ent := range ents {
		if ent.Index < firstIdx {
			// Entry is before our log, skip it
			continue
		}

		if ent.Index <= l.LastIndex() {
			// Entry index exists in our log
			existingTerm, err := l.Term(ent.Index)
			if err == nil && existingTerm == ent.Term {
				// Same term, entry matches, skip it
				continue
			}
			// Conflict detected: delete this entry and all that follow
			if ent.Index-firstIdx < uint64(len(l.entries)) {
				l.entries = l.entries[:ent.Index-firstIdx]
			}
			// Update stabled since we're truncating
			if l.stabled >= ent.Index {
				l.stabled = ent.Index - 1
			}
		}

		// Append this entry and all remaining entries
		l.entries = append(l.entries, ents[i:]...)
		return
	}
}

// getEntries returns entries in range [lo, hi)
func (l *RaftLog) getEntries(lo, hi uint64) []pb.Entry {
	if len(l.entries) == 0 {
		return nil
	}

	firstIdx := l.entries[0].Index
	if lo < firstIdx {
		return nil
	}

	if hi > l.LastIndex()+1 {
		hi = l.LastIndex() + 1
	}

	if lo >= hi {
		return nil
	}

	return l.entries[lo-firstIdx : hi-firstIdx]
}
