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

import (
	"errors"
	"math/rand"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// randomized election timeout for this peer
	randomizedElectionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}

	// Initialize RaftLog
	raftLog := newLog(c.Storage)

	// Get initial state from storage
	hs, cs, err := c.Storage.InitialState()
	if err != nil {
		panic(err)
	}

	// Restore committed index from HardState
	if hs.Commit > 0 {
		raftLog.committed = hs.Commit
	}

	// Initialize peers map
	peers := c.peers
	if len(peers) == 0 {
		// Get from ConfState if restarting
		peers = cs.Nodes
	}

	r := &Raft{
		id:                        c.ID,
		Term:                      hs.Term,
		Vote:                      hs.Vote,
		RaftLog:                   raftLog,
		Prs:                       make(map[uint64]*Progress),
		State:                     StateFollower,
		votes:                     make(map[uint64]bool),
		msgs:                      nil,
		Lead:                      None,
		heartbeatTimeout:          c.HeartbeatTick,
		electionTimeout:           c.ElectionTick,
		randomizedElectionTimeout: c.ElectionTick + rand.Intn(c.ElectionTick),
		heartbeatElapsed:          0,
		electionElapsed:           0,
		leadTransferee:            None,
		PendingConfIndex:          0,
	}

	// Initialize Progress for all peers
	for _, peer := range peers {
		r.Prs[peer] = &Progress{
			Match: 0,
			Next:  1,
		}
	}

	// Set applied index if provided
	if c.Applied > 0 {
		raftLog.applied = c.Applied
	}

	return r
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	pr := r.Prs[to]
	prevIndex := pr.Next - 1
	prevTerm, err := r.RaftLog.Term(prevIndex)
	if err != nil {
		// Entry not available, need snapshot (will implement in 2C)
		return false
	}

	// Get entries to send
	entries := r.RaftLog.getEntries(pr.Next, r.RaftLog.LastIndex()+1)

	// Convert to pointers
	var entryPtrs []*pb.Entry
	for i := range entries {
		entryPtrs = append(entryPtrs, &entries[i])
	}

	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		LogTerm: prevTerm,
		Index:   prevIndex,
		Entries: entryPtrs,
		Commit:  r.RaftLog.committed,
	}
	r.msgs = append(r.msgs, msg)
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	commit := min(r.Prs[to].Match, r.RaftLog.committed)
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		Commit:  commit,
	}
	r.msgs = append(r.msgs, msg)
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	switch r.State {
	case StateFollower, StateCandidate:
		r.electionElapsed++
		if r.electionElapsed >= r.randomizedElectionTimeout {
			r.electionElapsed = 0
			r.Step(pb.Message{MsgType: pb.MessageType_MsgHup})
		}
	case StateLeader:
		r.heartbeatElapsed++
		if r.heartbeatElapsed >= r.heartbeatTimeout {
			r.heartbeatElapsed = 0
			r.Step(pb.Message{MsgType: pb.MessageType_MsgBeat})
		}
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	r.State = StateFollower
	r.Term = term
	r.Vote = None
	r.Lead = lead
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	r.State = StateCandidate
	r.Term++
	r.Vote = r.id
	r.Lead = None
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.randomizedElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
	r.votes = make(map[uint64]bool)
	r.votes[r.id] = true
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// NOTE: Leader should propose a noop entry on its term
	r.State = StateLeader
	r.Lead = r.id
	r.heartbeatElapsed = 0
	r.electionElapsed = 0

	// Initialize Progress for all peers
	lastIndex := r.RaftLog.LastIndex()
	for peer := range r.Prs {
		if peer == r.id {
			r.Prs[peer].Match = lastIndex + 1
			r.Prs[peer].Next = lastIndex + 2
		} else {
			r.Prs[peer].Match = 0
			r.Prs[peer].Next = lastIndex + 1
		}
	}

	// Append noop entry
	r.RaftLog.entries = append(r.RaftLog.entries, pb.Entry{
		Term:  r.Term,
		Index: r.RaftLog.LastIndex() + 1,
	})
	r.Prs[r.id].Match = r.RaftLog.LastIndex()
	r.Prs[r.id].Next = r.RaftLog.LastIndex() + 1

	// If single node, commit immediately
	if len(r.Prs) == 1 {
		r.RaftLog.committed = r.RaftLog.LastIndex()
	} else {
		// Broadcast to replicate
		r.bcastAppend()
	}
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Handle messages with higher term
	if m.Term > r.Term {
		leadHint := None
		if m.MsgType == pb.MessageType_MsgAppend || m.MsgType == pb.MessageType_MsgHeartbeat {
			leadHint = m.From
		}
		r.becomeFollower(m.Term, leadHint)
	}

	switch m.MsgType {
	case pb.MessageType_MsgHup:
		// Start election
		if r.State != StateLeader {
			r.campaign()
		}
	case pb.MessageType_MsgBeat:
		// Leader sends heartbeats
		if r.State == StateLeader {
			r.bcastHeartbeat()
		}
	case pb.MessageType_MsgPropose:
		// Only leader can handle proposals
		if r.State == StateLeader {
			// Append entries to log
			lastIndex := r.RaftLog.LastIndex()
			for i, ent := range m.Entries {
				ent.Term = r.Term
				ent.Index = lastIndex + uint64(i) + 1
				r.RaftLog.entries = append(r.RaftLog.entries, *ent)
			}
			// Update leader's progress
			r.Prs[r.id].Match = r.RaftLog.LastIndex()
			r.Prs[r.id].Next = r.RaftLog.LastIndex() + 1
			// Broadcast to followers
			if len(r.Prs) == 1 {
				// Single node, commit immediately
				r.RaftLog.committed = r.RaftLog.LastIndex()
			} else {
				r.bcastAppend()
			}
		}
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse:
		r.handleRequestVoteResponse(m)
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgHeartbeatResponse:
		// Leader receives heartbeat response
		if r.State == StateLeader {
			r.handleHeartbeatResponse(m)
		}
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgAppendResponse:
		if r.State == StateLeader {
			r.handleAppendResponse(m)
		}
	}

	return nil
}

// campaign starts a new election
func (r *Raft) campaign() {
	r.becomeCandidate()

	// If single node, become leader immediately
	if len(r.Prs) == 1 {
		r.becomeLeader()
		return
	}

	// Send RequestVote to all peers
	lastIndex := r.RaftLog.LastIndex()
	lastTerm, _ := r.RaftLog.Term(lastIndex)

	for peer := range r.Prs {
		if peer == r.id {
			continue
		}
		msg := pb.Message{
			MsgType: pb.MessageType_MsgRequestVote,
			To:      peer,
			From:    r.id,
			Term:    r.Term,
			LogTerm: lastTerm,
			Index:   lastIndex,
		}
		r.msgs = append(r.msgs, msg)
	}
}

// handleRequestVote handles RequestVote RPC
func (r *Raft) handleRequestVote(m pb.Message) {
	reject := true

	// Check if we can vote
	if m.Term < r.Term {
		reject = true
	} else if r.Vote != None && r.Vote != m.From {
		reject = true
	} else {
		// Check if candidate's log is up-to-date
		lastIndex := r.RaftLog.LastIndex()
		lastTerm, _ := r.RaftLog.Term(lastIndex)

		if m.LogTerm > lastTerm || (m.LogTerm == lastTerm && m.Index >= lastIndex) {
			reject = false
			r.Vote = m.From
			r.electionElapsed = 0
		}
	}

	resp := pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		To:      m.From,
		From:    r.id,
		Term:    r.Term,
		Reject:  reject,
	}
	r.msgs = append(r.msgs, resp)
}

// handleRequestVoteResponse handles RequestVote response
func (r *Raft) handleRequestVoteResponse(m pb.Message) {
	if r.State != StateCandidate {
		return
	}

	r.votes[m.From] = !m.Reject

	// Count votes
	granted := 0
	for _, vote := range r.votes {
		if vote {
			granted++
		}
	}

	// Check if won election
	if granted > len(r.Prs)/2 {
		r.becomeLeader()
		r.bcastHeartbeat()
	} else if len(r.votes)-granted > len(r.Prs)/2 {
		// Lost election
		r.becomeFollower(r.Term, None)
	}
}

// bcastHeartbeat sends heartbeat to all peers
func (r *Raft) bcastHeartbeat() {
	for peer := range r.Prs {
		if peer == r.id {
			continue
		}
		r.sendHeartbeat(peer)
	}
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// If candidate receives AppendEntries with current or higher term, become follower
	if r.State == StateCandidate && m.Term >= r.Term {
		r.becomeFollower(m.Term, m.From)
	}

	r.electionElapsed = 0
	r.Lead = m.From

	// Reject if term is less than current term
	if m.Term < r.Term {
		resp := pb.Message{
			MsgType: pb.MessageType_MsgAppendResponse,
			To:      m.From,
			From:    r.id,
			Term:    r.Term,
			Reject:  true,
		}
		r.msgs = append(r.msgs, resp)
		return
	}

	// Check log matching
	lastIndex := r.RaftLog.LastIndex()
	if m.Index > lastIndex {
		// Missing entries
		resp := pb.Message{
			MsgType: pb.MessageType_MsgAppendResponse,
			To:      m.From,
			From:    r.id,
			Term:    r.Term,
			Index:   lastIndex,
			Reject:  true,
		}
		r.msgs = append(r.msgs, resp)
		return
	}

	// Check if term matches at prevLogIndex
	if m.Index > 0 {
		term, err := r.RaftLog.Term(m.Index)
		if err != nil || term != m.LogTerm {
			// Conflict - find the index to retry
			conflictIndex := m.Index
			if err == nil {
				// Find first index of conflicting term
				for i := m.Index; i > r.RaftLog.committed; i-- {
					t, e := r.RaftLog.Term(i)
					if e != nil || t != term {
						break
					}
					conflictIndex = i
				}
			}

			resp := pb.Message{
				MsgType: pb.MessageType_MsgAppendResponse,
				To:      m.From,
				From:    r.id,
				Term:    r.Term,
				Index:   conflictIndex - 1,
				Reject:  true,
			}
			r.msgs = append(r.msgs, resp)
			return
		}
	}

	// Append entries
	if len(m.Entries) > 0 {
		// Convert pointers to values
		entries := make([]pb.Entry, len(m.Entries))
		for i, e := range m.Entries {
			entries[i] = *e
		}
		r.RaftLog.append(entries...)
	}

	// Update commit index
	if m.Commit > r.RaftLog.committed {
		r.RaftLog.committed = min(m.Commit, m.Index+uint64(len(m.Entries)))
	}

	// Send success response
	resp := pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		To:      m.From,
		From:    r.id,
		Term:    r.Term,
		Index:   r.RaftLog.LastIndex(),
		Reject:  false,
	}
	r.msgs = append(r.msgs, resp)
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Reset election timeout
	r.electionElapsed = 0
	r.Lead = m.From

	// Update commit index
	if m.Commit > r.RaftLog.committed {
		r.RaftLog.committed = min(m.Commit, r.RaftLog.LastIndex())
	}

	// Send response with current log index
	resp := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		To:      m.From,
		From:    r.id,
		Term:    r.Term,
		Index:   r.RaftLog.LastIndex(),
	}
	r.msgs = append(r.msgs, resp)
}

// handleHeartbeatResponse handles heartbeat response from followers
func (r *Raft) handleHeartbeatResponse(m pb.Message) {
	if m.Term < r.Term {
		return
	}

	// Check if follower is behind
	pr := r.Prs[m.From]
	if pr == nil {
		return
	}

	// If follower's log is behind, send append entries
	if m.Index < r.RaftLog.LastIndex() {
		r.sendAppend(m.From)
	}
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// handleAppendResponse handles AppendEntries response from followers
func (r *Raft) handleAppendResponse(m pb.Message) {
	if m.Term < r.Term {
		return
	}

	pr := r.Prs[m.From]
	if m.Reject {
		// Follower rejected, decrease Next and retry
		pr.Next = m.Index + 1
		r.sendAppend(m.From)
	} else {
		// Success - update Match and Next
		pr.Match = m.Index
		pr.Next = m.Index + 1

		// Try to advance commit index
		r.maybeCommit()
	}
}

// maybeCommit attempts to advance the commit index
func (r *Raft) maybeCommit() {
	// Find the highest index replicated on majority
	matches := make([]uint64, 0, len(r.Prs))
	for _, pr := range r.Prs {
		matches = append(matches, pr.Match)
	}

	// Sort in descending order
	for i := 0; i < len(matches); i++ {
		for j := i + 1; j < len(matches); j++ {
			if matches[j] > matches[i] {
				matches[i], matches[j] = matches[j], matches[i]
			}
		}
	}

	// Get the median (majority threshold)
	majorityIndex := matches[len(matches)/2]

	// Only commit entries from current term
	if majorityIndex > r.RaftLog.committed {
		term, err := r.RaftLog.Term(majorityIndex)
		if err == nil && term == r.Term {
			r.RaftLog.committed = majorityIndex
			// Broadcast commit index
			r.bcastAppend()
		}
	}
}

// bcastAppend sends append messages to all peers
func (r *Raft) bcastAppend() {
	for peer := range r.Prs {
		if peer == r.id {
			continue
		}
		r.sendAppend(peer)
	}
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}
