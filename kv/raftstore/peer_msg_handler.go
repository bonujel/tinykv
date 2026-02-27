package raftstore

import (
	"fmt"
	"time"

	"github.com/Connor1996/badger/y"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/message"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/meta"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/runner"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/snap"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
	rspb "github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
	"github.com/pingcap-incubator/tinykv/raft"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/btree"
	"github.com/pingcap/errors"
)

type PeerTick int

const (
	PeerTickRaft               PeerTick = 0
	PeerTickRaftLogGC          PeerTick = 1
	PeerTickSplitRegionCheck   PeerTick = 2
	PeerTickSchedulerHeartbeat PeerTick = 3
)

type peerMsgHandler struct {
	*peer
	ctx *GlobalContext
}

func newPeerMsgHandler(peer *peer, ctx *GlobalContext) *peerMsgHandler {
	return &peerMsgHandler{
		peer: peer,
		ctx:  ctx,
	}
}

func (d *peerMsgHandler) HandleRaftReady() {
	if d.stopped {
		return
	}
	// Your Code Here (2B).

	// Check if there is a Ready
	if !d.RaftGroup.HasReady() {
		return
	}

	// Get Ready
	ready := d.RaftGroup.Ready()

	// Persist state (logs + HardState + snapshot)
	applySnapResult, err := d.peerStorage.SaveReadyState(&ready)
	if err != nil {
		log.Panicf("%s failed to save ready state: %v", d.Tag, err)
	}

	// Send Raft messages to other peers
	d.Send(d.ctx.trans, ready.Messages)

	// Handle snapshot application (2C)
	var snapIndex uint64
	if !raft.IsEmptySnap(&ready.Snapshot) && ready.Snapshot.Metadata != nil {
		snapIndex = ready.Snapshot.Metadata.Index
	}
	if applySnapResult != nil {
		// Update peer's region
		d.peerStorage.SetRegion(applySnapResult.Region)

		// Update store metadata (global region map)
		storeMeta := d.ctx.storeMeta
		storeMeta.Lock()
		storeMeta.regions[applySnapResult.Region.Id] = applySnapResult.Region
		// If this peer was created by snapshot, regionRanges may not contain it yet.
		if applySnapResult.PrevRegion != nil && len(applySnapResult.PrevRegion.Peers) > 0 {
			storeMeta.regionRanges.Delete(&regionItem{region: applySnapResult.PrevRegion})
		}
		storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: applySnapResult.Region})
		storeMeta.Unlock()

		// Clear all proposals - they're stale after snapshot
		for _, prop := range d.proposals {
			if prop.cb != nil {
				NotifyStaleReq(d.Term(), prop.cb)
			}
		}
		d.proposals = nil

	}

	// Apply committed entries (skip ones covered by snapshot)
	// IMPORTANT: Must flush after each entry to ensure read consistency
	// Get/Snap requests need to see all previously committed writes
	if len(ready.CommittedEntries) > 0 {
		kvWB := new(engine_util.WriteBatch)
		for _, entry := range ready.CommittedEntries {
			if entry.Index <= snapIndex {
				continue
			}
			kvWB = d.processCommittedEntry(&entry, kvWB)
			// Flush after each entry to ensure read consistency
			if kvWB != nil && kvWB.Len() > 0 {
				kvWB.MustWriteToDB(d.peerStorage.Engines.Kv)
				kvWB = new(engine_util.WriteBatch)
			}
			// The peer may destroy itself after applying a conf change.
			if d.stopped {
				break
			}
		}
	}
	if d.stopped {
		return
	}

	// Notify Raft module that Ready has been processed
	d.RaftGroup.Advance(ready)
}

func (d *peerMsgHandler) HandleMsg(msg message.Msg) {
	switch msg.Type {
	case message.MsgTypeRaftMessage:
		raftMsg := msg.Data.(*rspb.RaftMessage)
		if err := d.onRaftMsg(raftMsg); err != nil {
			log.Errorf("%s handle raft message error %v", d.Tag, err)
		}
	case message.MsgTypeRaftCmd:
		raftCMD := msg.Data.(*message.MsgRaftCmd)
		d.proposeRaftCommand(raftCMD.Request, raftCMD.Callback)
	case message.MsgTypeTick:
		d.onTick()
	case message.MsgTypeSplitRegion:
		split := msg.Data.(*message.MsgSplitRegion)
		log.Infof("%s on split with %v", d.Tag, split.SplitKey)
		d.onPrepareSplitRegion(split.RegionEpoch, split.SplitKey, split.Callback)
	case message.MsgTypeRegionApproximateSize:
		d.onApproximateRegionSize(msg.Data.(uint64))
	case message.MsgTypeGcSnap:
		gcSnap := msg.Data.(*message.MsgGCSnap)
		d.onGCSnap(gcSnap.Snaps)
	case message.MsgTypeStart:
		d.startTicker()
	}
}

func (d *peerMsgHandler) preProposeRaftCommand(req *raft_cmdpb.RaftCmdRequest) error {
	// Check store_id, make sure that the msg is dispatched to the right place.
	if err := util.CheckStoreID(req, d.storeID()); err != nil {
		return err
	}

	// Check whether the store has the right peer to handle the request.
	regionID := d.regionId
	leaderID := d.LeaderId()
	if !d.IsLeader() {
		leader := d.getPeerFromCache(leaderID)
		return &util.ErrNotLeader{RegionId: regionID, Leader: leader}
	}
	// peer_id must be the same as peer's.
	if err := util.CheckPeerID(req, d.PeerId()); err != nil {
		return err
	}
	// Check whether the term is stale.
	if err := util.CheckTerm(req, d.Term()); err != nil {
		return err
	}
	err := util.CheckRegionEpoch(req, d.Region(), true)
	if errEpochNotMatching, ok := err.(*util.ErrEpochNotMatch); ok {
		// Attach the region which might be split from the current region. But it doesn't
		// matter if the region is not split from the current region. If the region meta
		// received by the TiKV driver is newer than the meta cached in the driver, the meta is
		// updated.
		siblingRegion := d.findSiblingRegion()
		if siblingRegion != nil {
			errEpochNotMatching.Regions = append(errEpochNotMatching.Regions, siblingRegion)
		}
		return errEpochNotMatching
	}
	if err != nil {
		return err
	}

	// Verify key range before proposing so stale route requests return a
	// proper region error instead of being committed on wrong region.
	region := d.Region()
	if req.AdminRequest == nil {
		for _, request := range req.Requests {
			switch request.CmdType {
			case raft_cmdpb.CmdType_Get:
				if err := util.CheckKeyInRegion(request.Get.Key, region); err != nil {
					return err
				}
			case raft_cmdpb.CmdType_Put:
				if err := util.CheckKeyInRegion(request.Put.Key, region); err != nil {
					return err
				}
			case raft_cmdpb.CmdType_Delete:
				if err := util.CheckKeyInRegion(request.Delete.Key, region); err != nil {
					return err
				}
			}
		}
	} else if req.AdminRequest.CmdType == raft_cmdpb.AdminCmdType_Split && req.AdminRequest.Split != nil {
		if err := util.CheckKeyInRegionExclusive(req.AdminRequest.Split.SplitKey, region); err != nil {
			return err
		}
	}
	return nil
}

func (d *peerMsgHandler) proposeRaftCommand(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	err := d.preProposeRaftCommand(msg)
	if err != nil {
		if cb != nil {
			cb.Done(ErrResp(err))
		}
		return
	}
	// Your Code Here (2B).

	// Handle admin commands
	if msg.AdminRequest != nil {
		switch msg.AdminRequest.CmdType {
		case raft_cmdpb.AdminCmdType_TransferLeader:
			// Transfer leader doesn't go through Raft log
			transferReq := msg.AdminRequest.TransferLeader
			d.RaftGroup.TransferLeader(transferReq.Peer.Id)
			if cb != nil {
				cb.Done(&raft_cmdpb.RaftCmdResponse{
					Header: &raft_cmdpb.RaftResponseHeader{},
					AdminResponse: &raft_cmdpb.AdminResponse{
						CmdType:        raft_cmdpb.AdminCmdType_TransferLeader,
						TransferLeader: &raft_cmdpb.TransferLeaderResponse{},
					},
				})
			}
			return

		case raft_cmdpb.AdminCmdType_ChangePeer:
			// ChangePeer goes through Raft log as conf change
			changePeerReq := msg.AdminRequest.ChangePeer

			// Create ConfChange
			cc := &eraftpb.ConfChange{
				ChangeType: changePeerReq.ChangeType,
				NodeId:     changePeerReq.Peer.Id,
			}

			// Marshal peer info to Context
			cc.Context, err = changePeerReq.Peer.Marshal()
			if err != nil {
				if cb != nil {
					cb.Done(ErrResp(err))
				}
				return
			}

			// Propose conf change
			err = d.RaftGroup.ProposeConfChange(*cc)
			if err != nil {
				if cb != nil {
					cb.Done(ErrResp(err))
				}
				return
			}

			// Record proposal
			proposalIndex := d.RaftGroup.Raft.RaftLog.LastIndex()
			d.proposals = append(d.proposals, &proposal{
				index: proposalIndex,
				term:  d.Term(),
				cb:    cb,
			})
			return
		}
	}

	// Serialize request
	data, err := msg.Marshal()
	if err != nil {
		if cb != nil {
			cb.Done(ErrResp(err))
		}
		return
	}

	// Propose to Raft module
	err = d.RaftGroup.Propose(data)
	if err != nil {
		if cb != nil {
			cb.Done(ErrResp(err))
		}
		return
	}

	// After Propose, the entry has been appended to RaftLog.entries
	// Get the index from RaftLog.LastIndex()
	proposalIndex := d.RaftGroup.Raft.RaftLog.LastIndex()

	// Record proposal for later callback matching
	d.proposals = append(d.proposals, &proposal{
		index: proposalIndex,
		term:  d.Term(),
		cb:    cb,
	})
}

func (d *peerMsgHandler) onTick() {
	if d.stopped {
		return
	}
	d.ticker.tickClock()
	if d.ticker.isOnTick(PeerTickRaft) {
		d.onRaftBaseTick()
	}
	if d.ticker.isOnTick(PeerTickRaftLogGC) {
		d.onRaftGCLogTick()
	}
	if d.ticker.isOnTick(PeerTickSchedulerHeartbeat) {
		d.onSchedulerHeartbeatTick()
	}
	if d.ticker.isOnTick(PeerTickSplitRegionCheck) {
		d.onSplitRegionCheckTick()
	}
	d.ctx.tickDriverSender <- d.regionId
}

func (d *peerMsgHandler) startTicker() {
	d.ticker = newTicker(d.regionId, d.ctx.cfg)
	d.ctx.tickDriverSender <- d.regionId
	d.ticker.schedule(PeerTickRaft)
	d.ticker.schedule(PeerTickRaftLogGC)
	d.ticker.schedule(PeerTickSplitRegionCheck)
	d.ticker.schedule(PeerTickSchedulerHeartbeat)
}

func (d *peerMsgHandler) onRaftBaseTick() {
	d.RaftGroup.Tick()
	d.ticker.schedule(PeerTickRaft)
}

func (d *peerMsgHandler) ScheduleCompactLog(truncatedIndex uint64) {
	raftLogGCTask := &runner.RaftLogGCTask{
		RaftEngine: d.ctx.engine.Raft,
		RegionID:   d.regionId,
		StartIdx:   d.LastCompactedIdx,
		EndIdx:     truncatedIndex + 1,
	}
	d.LastCompactedIdx = raftLogGCTask.EndIdx
	d.ctx.raftLogGCTaskSender <- raftLogGCTask
}

func (d *peerMsgHandler) onRaftMsg(msg *rspb.RaftMessage) error {
	log.Debugf("%s handle raft message %s from %d to %d",
		d.Tag, msg.GetMessage().GetMsgType(), msg.GetFromPeer().GetId(), msg.GetToPeer().GetId())
	if !d.validateRaftMessage(msg) {
		return nil
	}
	if d.stopped {
		return nil
	}
	if msg.GetIsTombstone() {
		// we receive a message tells us to remove self.
		d.handleGCPeerMsg(msg)
		return nil
	}
	if d.checkMessage(msg) {
		return nil
	}
	key, err := d.checkSnapshot(msg)
	if err != nil {
		return err
	}
	if key != nil {
		// If the snapshot file is not used again, then it's OK to
		// delete them here. If the snapshot file will be reused when
		// receiving, then it will fail to pass the check again, so
		// missing snapshot files should not be noticed.
		s, err1 := d.ctx.snapMgr.GetSnapshotForApplying(*key)
		if err1 != nil {
			return err1
		}
		d.ctx.snapMgr.DeleteSnapshot(*key, s, false)
		return nil
	}
	d.insertPeerCache(msg.GetFromPeer())
	err = d.RaftGroup.Step(*msg.GetMessage())
	if err != nil {
		return err
	}
	if d.AnyNewPeerCatchUp(msg.FromPeer.Id) {
		d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
	}
	return nil
}

// return false means the message is invalid, and can be ignored.
func (d *peerMsgHandler) validateRaftMessage(msg *rspb.RaftMessage) bool {
	regionID := msg.GetRegionId()
	from := msg.GetFromPeer()
	to := msg.GetToPeer()
	log.Debugf("[region %d] handle raft message %s from %d to %d", regionID, msg, from.GetId(), to.GetId())
	if to.GetStoreId() != d.storeID() {
		log.Warnf("[region %d] store not match, to store id %d, mine %d, ignore it",
			regionID, to.GetStoreId(), d.storeID())
		return false
	}
	if msg.RegionEpoch == nil {
		log.Errorf("[region %d] missing epoch in raft message, ignore it", regionID)
		return false
	}
	return true
}

// / Checks if the message is sent to the correct peer.
// /
// / Returns true means that the message can be dropped silently.
func (d *peerMsgHandler) checkMessage(msg *rspb.RaftMessage) bool {
	fromEpoch := msg.GetRegionEpoch()
	isVoteMsg := util.IsVoteMessage(msg.Message)
	fromStoreID := msg.FromPeer.GetStoreId()

	// Let's consider following cases with three nodes [1, 2, 3] and 1 is leader:
	// a. 1 removes 2, 2 may still send MsgAppendResponse to 1.
	//  We should ignore this stale message and let 2 remove itself after
	//  applying the ConfChange log.
	// b. 2 is isolated, 1 removes 2. When 2 rejoins the cluster, 2 will
	//  send stale MsgRequestVote to 1 and 3, at this time, we should tell 2 to gc itself.
	// c. 2 is isolated but can communicate with 3. 1 removes 3.
	//  2 will send stale MsgRequestVote to 3, 3 should ignore this message.
	// d. 2 is isolated but can communicate with 3. 1 removes 2, then adds 4, remove 3.
	//  2 will send stale MsgRequestVote to 3, 3 should tell 2 to gc itself.
	// e. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader.
	//  After 2 rejoins the cluster, 2 may send stale MsgRequestVote to 1 and 3,
	//  1 and 3 will ignore this message. Later 4 will send messages to 2 and 2 will
	//  rejoin the raft group again.
	// f. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader, and 4 removes 2.
	//  unlike case e, 2 will be stale forever.
	// TODO: for case f, if 2 is stale for a long time, 2 will communicate with scheduler and scheduler will
	// tell 2 is stale, so 2 can remove itself.
	region := d.Region()
	if util.IsEpochStale(fromEpoch, region.RegionEpoch) && util.FindPeer(region, fromStoreID) == nil {
		// The message is stale and not in current region.
		handleStaleMsg(d.ctx.trans, msg, region.RegionEpoch, isVoteMsg)
		return true
	}
	target := msg.GetToPeer()
	if target.Id < d.PeerId() {
		log.Infof("%s target peer ID %d is less than %d, msg maybe stale", d.Tag, target.Id, d.PeerId())
		return true
	} else if target.Id > d.PeerId() {
		if d.MaybeDestroy() {
			log.Infof("%s is stale as received a larger peer %s, destroying", d.Tag, target)
			d.destroyPeer()
			d.ctx.router.sendStore(message.NewMsg(message.MsgTypeStoreRaftMessage, msg))
		}
		return true
	}
	return false
}

func handleStaleMsg(trans Transport, msg *rspb.RaftMessage, curEpoch *metapb.RegionEpoch,
	needGC bool) {
	regionID := msg.RegionId
	fromPeer := msg.FromPeer
	toPeer := msg.ToPeer
	msgType := msg.Message.GetMsgType()

	if !needGC {
		log.Infof("[region %d] raft message %s is stale, current %v ignore it",
			regionID, msgType, curEpoch)
		return
	}
	gcMsg := &rspb.RaftMessage{
		RegionId:    regionID,
		FromPeer:    toPeer,
		ToPeer:      fromPeer,
		RegionEpoch: curEpoch,
		IsTombstone: true,
	}
	if err := trans.Send(gcMsg); err != nil {
		log.Errorf("[region %d] send message failed %v", regionID, err)
	}
}

func (d *peerMsgHandler) handleGCPeerMsg(msg *rspb.RaftMessage) {
	fromEpoch := msg.RegionEpoch
	if !util.IsEpochStale(d.Region().RegionEpoch, fromEpoch) {
		return
	}
	if !util.PeerEqual(d.Meta, msg.ToPeer) {
		log.Infof("%s receive stale gc msg, ignore", d.Tag)
		return
	}
	log.Infof("%s peer %s receives gc message, trying to remove", d.Tag, msg.ToPeer)
	if d.MaybeDestroy() {
		d.destroyPeer()
	}
}

// Returns `None` if the `msg` doesn't contain a snapshot or it contains a snapshot which
// doesn't conflict with any other snapshots or regions. Otherwise a `snap.SnapKey` is returned.
func (d *peerMsgHandler) checkSnapshot(msg *rspb.RaftMessage) (*snap.SnapKey, error) {
	if msg.Message.Snapshot == nil {
		return nil, nil
	}
	regionID := msg.RegionId
	snapshot := msg.Message.Snapshot
	key := snap.SnapKeyFromRegionSnap(regionID, snapshot)
	snapData := new(rspb.RaftSnapshotData)
	err := snapData.Unmarshal(snapshot.Data)
	if err != nil {
		return nil, err
	}
	snapRegion := snapData.Region
	peerID := msg.ToPeer.Id
	var contains bool
	for _, peer := range snapRegion.Peers {
		if peer.Id == peerID {
			contains = true
			break
		}
	}
	if !contains {
		log.Infof("%s %s doesn't contains peer %d, skip", d.Tag, snapRegion, peerID)
		return &key, nil
	}
	meta := d.ctx.storeMeta
	meta.Lock()
	defer meta.Unlock()
	if !util.RegionEqual(meta.regions[d.regionId], d.Region()) {
		if !d.isInitialized() {
			log.Infof("%s stale delegate detected, skip", d.Tag)
			return &key, nil
		} else {
			panic(fmt.Sprintf("%s meta corrupted %s != %s", d.Tag, meta.regions[d.regionId], d.Region()))
		}
	}

	existRegions := meta.getOverlapRegions(snapRegion)
	for _, existRegion := range existRegions {
		if existRegion.GetId() == snapRegion.GetId() {
			continue
		}
		log.Infof("%s region overlapped %s %s", d.Tag, existRegion, snapRegion)
		return &key, nil
	}

	// check if snapshot file exists.
	_, err = d.ctx.snapMgr.GetSnapshotForApplying(key)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

func (d *peerMsgHandler) destroyPeer() {
	log.Infof("%s starts destroy", d.Tag)
	regionID := d.regionId
	// We can't destroy a peer which is applying snapshot.
	meta := d.ctx.storeMeta
	meta.Lock()
	defer meta.Unlock()
	isInitialized := d.isInitialized()
	if err := d.Destroy(d.ctx.engine, false); err != nil {
		// If not panic here, the peer will be recreated in the next restart,
		// then it will be gc again. But if some overlap region is created
		// before restarting, the gc action will delete the overlap region's
		// data too.
		panic(fmt.Sprintf("%s destroy peer %v", d.Tag, err))
	}
	d.ctx.router.close(regionID)
	d.stopped = true
	if isInitialized && meta.regionRanges.Delete(&regionItem{region: d.Region()}) == nil {
		panic(d.Tag + " meta corruption detected")
	}
	if _, ok := meta.regions[regionID]; !ok {
		panic(d.Tag + " meta corruption detected")
	}
	delete(meta.regions, regionID)
}

func (d *peerMsgHandler) findSiblingRegion() (result *metapb.Region) {
	meta := d.ctx.storeMeta
	meta.RLock()
	defer meta.RUnlock()
	item := &regionItem{region: d.Region()}
	meta.regionRanges.AscendGreaterOrEqual(item, func(i btree.Item) bool {
		result = i.(*regionItem).region
		return true
	})
	return
}

func (d *peerMsgHandler) onRaftGCLogTick() {
	d.ticker.schedule(PeerTickRaftLogGC)
	if !d.IsLeader() {
		return
	}

	appliedIdx := d.peerStorage.AppliedIndex()
	firstIdx, _ := d.peerStorage.FirstIndex()
	var compactIdx uint64
	if appliedIdx > firstIdx && appliedIdx-firstIdx >= d.ctx.cfg.RaftLogGcCountLimit {
		compactIdx = appliedIdx
	} else {
		return
	}

	y.Assert(compactIdx > 0)
	compactIdx -= 1
	if compactIdx < firstIdx {
		// In case compact_idx == first_idx before subtraction.
		return
	}

	term, err := d.RaftGroup.Raft.RaftLog.Term(compactIdx)
	if err != nil {
		log.Fatalf("appliedIdx: %d, firstIdx: %d, compactIdx: %d", appliedIdx, firstIdx, compactIdx)
		panic(err)
	}

	// Create a compact log request and notify directly.
	regionID := d.regionId
	request := newCompactLogRequest(regionID, d.Meta, compactIdx, term)
	d.proposeRaftCommand(request, nil)
}

func (d *peerMsgHandler) onSplitRegionCheckTick() {
	d.ticker.schedule(PeerTickSplitRegionCheck)
	// To avoid frequent scan, we only add new scan tasks if all previous tasks
	// have finished.
	if len(d.ctx.splitCheckTaskSender) > 0 {
		return
	}

	if !d.IsLeader() {
		return
	}
	if d.ApproximateSize != nil && d.SizeDiffHint < d.ctx.cfg.RegionSplitSize/8 {
		return
	}
	d.ctx.splitCheckTaskSender <- &runner.SplitCheckTask{
		Region: d.Region(),
	}
	d.SizeDiffHint = 0
}

func (d *peerMsgHandler) onPrepareSplitRegion(regionEpoch *metapb.RegionEpoch, splitKey []byte, cb *message.Callback) {
	if err := d.validateSplitRegion(regionEpoch, splitKey); err != nil {
		if cb != nil {
			cb.Done(ErrResp(err))
		}
		return
	}

	// Use an immutable region snapshot for async scheduler task.
	region := new(metapb.Region)
	if err := util.CloneMsg(d.Region(), region); err != nil {
		if cb != nil {
			cb.Done(ErrResp(err))
		}
		return
	}
	d.ctx.schedulerTaskSender <- &runner.SchedulerAskSplitTask{
		Region:   region,
		SplitKey: util.SafeCopy(splitKey),
		Peer:     d.Meta,
		Callback: cb,
	}
}

func (d *peerMsgHandler) validateSplitRegion(epoch *metapb.RegionEpoch, splitKey []byte) error {
	if len(splitKey) == 0 {
		err := errors.Errorf("%s split key should not be empty", d.Tag)
		log.Error(err)
		return err
	}

	if !d.IsLeader() {
		// region on this store is no longer leader, skipped.
		log.Infof("%s not leader, skip", d.Tag)
		return &util.ErrNotLeader{
			RegionId: d.regionId,
			Leader:   d.getPeerFromCache(d.LeaderId()),
		}
	}

	region := d.Region()
	latestEpoch := region.GetRegionEpoch()

	// This is a little difference for `check_region_epoch` in region split case.
	// Here we just need to check `version` because `conf_ver` will be update
	// to the latest value of the peer, and then send to Scheduler.
	if latestEpoch.Version != epoch.Version {
		log.Infof("%s epoch changed, retry later, prev_epoch: %s, epoch %s",
			d.Tag, latestEpoch, epoch)
		return &util.ErrEpochNotMatch{
			Message: fmt.Sprintf("%s epoch changed %s != %s, retry later", d.Tag, latestEpoch, epoch),
			Regions: []*metapb.Region{region},
		}
	}
	if err := util.CheckKeyInRegionExclusive(splitKey, region); err != nil {
		return err
	}
	return nil
}

func (d *peerMsgHandler) onApproximateRegionSize(size uint64) {
	d.ApproximateSize = &size
}

func (d *peerMsgHandler) onSchedulerHeartbeatTick() {
	d.ticker.schedule(PeerTickSchedulerHeartbeat)

	if !d.IsLeader() {
		return
	}
	d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
}

func (d *peerMsgHandler) onGCSnap(snaps []snap.SnapKeyWithSending) {
	compactedIdx := d.peerStorage.truncatedIndex()
	compactedTerm := d.peerStorage.truncatedTerm()
	for _, snapKeyWithSending := range snaps {
		key := snapKeyWithSending.SnapKey
		if snapKeyWithSending.IsSending {
			snap, err := d.ctx.snapMgr.GetSnapshotForSending(key)
			if err != nil {
				log.Errorf("%s failed to load snapshot for %s %v", d.Tag, key, err)
				continue
			}
			if key.Term < compactedTerm || key.Index < compactedIdx {
				log.Infof("%s snap file %s has been compacted, delete", d.Tag, key)
				d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
			} else if fi, err1 := snap.Meta(); err1 == nil {
				modTime := fi.ModTime()
				if time.Since(modTime) > 4*time.Hour {
					log.Infof("%s snap file %s has been expired, delete", d.Tag, key)
					d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
				}
			}
		} else if key.Term <= compactedTerm &&
			(key.Index < compactedIdx || key.Index == compactedIdx) {
			log.Infof("%s snap file %s has been applied, delete", d.Tag, key)
			a, err := d.ctx.snapMgr.GetSnapshotForApplying(key)
			if err != nil {
				log.Errorf("%s failed to load snapshot for %s %v", d.Tag, key, err)
				continue
			}
			d.ctx.snapMgr.DeleteSnapshot(key, a, false)
		}
	}
}

func newAdminRequest(regionID uint64, peer *metapb.Peer) *raft_cmdpb.RaftCmdRequest {
	return &raft_cmdpb.RaftCmdRequest{
		Header: &raft_cmdpb.RaftRequestHeader{
			RegionId: regionID,
			Peer:     peer,
		},
	}
}

func newCompactLogRequest(regionID uint64, peer *metapb.Peer, compactIndex, compactTerm uint64) *raft_cmdpb.RaftCmdRequest {
	req := newAdminRequest(regionID, peer)
	req.AdminRequest = &raft_cmdpb.AdminRequest{
		CmdType: raft_cmdpb.AdminCmdType_CompactLog,
		CompactLog: &raft_cmdpb.CompactLogRequest{
			CompactIndex: compactIndex,
			CompactTerm:  compactTerm,
		},
	}
	return req
}

func cloneRegion(region *metapb.Region) *metapb.Region {
	if region == nil {
		return nil
	}
	cloned := new(metapb.Region)
	if err := util.CloneMsg(region, cloned); err != nil {
		panic(err)
	}
	return cloned
}

// popProposal finds the proposal for a committed entry and drains stale
// proposals that can never be committed on this leader.
func (d *peerMsgHandler) popProposal(entry *eraftpb.Entry) *proposal {
	for len(d.proposals) > 0 {
		p := d.proposals[0]
		if p.index < entry.Index {
			if p.cb != nil {
				NotifyStaleReq(entry.Term, p.cb)
			}
			d.proposals = d.proposals[1:]
			continue
		}
		if p.index > entry.Index {
			return nil
		}
		d.proposals = d.proposals[1:]
		if p.term != entry.Term {
			if p.cb != nil {
				NotifyStaleReq(entry.Term, p.cb)
			}
			return nil
		}
		return p
	}
	return nil
}

// processCommittedEntry applies a single committed log entry
func (d *peerMsgHandler) processCommittedEntry(entry *eraftpb.Entry, kvWB *engine_util.WriteBatch) *engine_util.WriteBatch {
	if d.stopped {
		return kvWB
	}
	// Handle empty log (noop entry from leader election)
	if len(entry.Data) == 0 {
		d.peerStorage.applyState.AppliedIndex = entry.Index
		kvWB.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState)
		return kvWB
	}

	// Handle conf change entries
	if entry.EntryType == eraftpb.EntryType_EntryConfChange {
		var cc eraftpb.ConfChange
		err := cc.Unmarshal(entry.Data)
		if err != nil {
			panic(err)
		}

		// Find corresponding proposal
		prop := d.popProposal(entry)

		// Apply conf change
		resp := d.applyConfChange(&cc, kvWB)

		// If this peer has been destroyed (removed self), don't update apply state.
		// Still respond to the proposal if needed.
		if d.stopped {
			if prop != nil && prop.cb != nil && resp != nil {
				prop.cb.Done(resp)
			}
			return kvWB
		}

		// If resp is nil, it means the peer has been destroyed (removed self)
		// Don't update apply state or return response
		if resp == nil {
			return kvWB
		}

		// Update applied index
		d.peerStorage.applyState.AppliedIndex = entry.Index
		kvWB.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState)

		// Return response to client
		if prop != nil && prop.cb != nil {
			prop.cb.Done(resp)
		}

		return kvWB
	}

	// Deserialize request
	var req raft_cmdpb.RaftCmdRequest
	err := req.Unmarshal(entry.Data)
	if err != nil {
		panic(err)
	}

	// Find corresponding proposal (match callback)
	prop := d.popProposal(entry)

	// Apply command to state machine
	resp := d.applyCommand(&req, kvWB, prop)

	// Update applied index
	d.peerStorage.applyState.AppliedIndex = entry.Index
	kvWB.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState)

	// Return response to client
	if prop != nil && prop.cb != nil {
		prop.cb.Done(resp)
	}

	return kvWB
}

// applyCommand applies a command to the state machine (kvdb)
func (d *peerMsgHandler) applyCommand(req *raft_cmdpb.RaftCmdRequest, kvWB *engine_util.WriteBatch, prop *proposal) *raft_cmdpb.RaftCmdResponse {
	resp := newCmdResp()

	// Handle admin requests (2C)
	if req.AdminRequest != nil {
		switch req.AdminRequest.CmdType {
		case raft_cmdpb.AdminCmdType_CompactLog:
			d.applyCompactLog(req.AdminRequest.CompactLog, kvWB)
			resp.AdminResponse = &raft_cmdpb.AdminResponse{
				CmdType:    raft_cmdpb.AdminCmdType_CompactLog,
				CompactLog: &raft_cmdpb.CompactLogResponse{},
			}
		case raft_cmdpb.AdminCmdType_Split:
			// Split can be proposed before conf changes/snapshot are applied on this
			// peer. Re-check epoch at apply time to avoid applying stale split on an
			// uninitialized or outdated region.
			if err := util.CheckRegionEpoch(req, d.Region(), true); err != nil {
				return ErrResp(err)
			}
			resp = d.applySplit(req.AdminRequest.Split, kvWB)
		}
		return resp
	}

	// Requests can be proposed before a split/conf change and applied after the
	// epoch changes. Re-check epoch at apply time to avoid returning stale Snap
	// region metadata.
	if err := util.CheckRegionEpoch(req, d.Region(), true); err != nil {
		return ErrResp(err)
	}

	// Process Get/Put/Delete/Snap requests
	for _, r := range req.Requests {
		var err error
		switch r.CmdType {
		case raft_cmdpb.CmdType_Get:
			err = util.CheckKeyInRegion(r.Get.Key, d.Region())
		case raft_cmdpb.CmdType_Put:
			err = util.CheckKeyInRegion(r.Put.Key, d.Region())
		case raft_cmdpb.CmdType_Delete:
			err = util.CheckKeyInRegion(r.Delete.Key, d.Region())
		}
		if err != nil {
			return ErrResp(err)
		}

		switch r.CmdType {
		case raft_cmdpb.CmdType_Get:
			val, _ := engine_util.GetCF(d.peerStorage.Engines.Kv, r.Get.Cf, r.Get.Key)
			resp.Responses = append(resp.Responses, &raft_cmdpb.Response{
				CmdType: raft_cmdpb.CmdType_Get,
				Get:     &raft_cmdpb.GetResponse{Value: val},
			})
		case raft_cmdpb.CmdType_Put:
			kvWB.SetCF(r.Put.Cf, r.Put.Key, r.Put.Value)
			resp.Responses = append(resp.Responses, &raft_cmdpb.Response{
				CmdType: raft_cmdpb.CmdType_Put,
				Put:     &raft_cmdpb.PutResponse{},
			})
		case raft_cmdpb.CmdType_Delete:
			kvWB.DeleteCF(r.Delete.Cf, r.Delete.Key)
			resp.Responses = append(resp.Responses, &raft_cmdpb.Response{
				CmdType: raft_cmdpb.CmdType_Delete,
				Delete:  &raft_cmdpb.DeleteResponse{},
			})
		case raft_cmdpb.CmdType_Snap:
			// For Snap request, create a Txn and set it to callback
			if prop != nil && prop.cb != nil {
				prop.cb.SetTxn(d.peerStorage.Engines.Kv.NewTransaction(false))
			}
			region := cloneRegion(d.Region())
			resp.Responses = append(resp.Responses, &raft_cmdpb.Response{
				CmdType: raft_cmdpb.CmdType_Snap,
				Snap:    &raft_cmdpb.SnapResponse{Region: region},
			})
		}
	}

	return resp
}

// applyConfChange applies a configuration change (3B)
func (d *peerMsgHandler) applyConfChange(cc *eraftpb.ConfChange, kvWB *engine_util.WriteBatch) *raft_cmdpb.RaftCmdResponse {
	// Unmarshal peer info from Context
	var peer metapb.Peer
	if err := peer.Unmarshal(cc.Context); err != nil {
		panic(err)
	}

	// Clone region metadata before applying conf change.
	region := d.Region()
	newRegion := new(metapb.Region)
	if err := util.CloneMsg(region, newRegion); err != nil {
		panic(err)
	}
	changed := false

	// Apply the change
	switch cc.ChangeType {
	case eraftpb.ConfChangeType_AddNode:
		// Ignore duplicated AddNode.
		if util.FindPeer(newRegion, peer.StoreId) == nil {
			newRegion.Peers = append(newRegion.Peers, &peer)
			d.insertPeerCache(&peer)
			changed = true
		}

	case eraftpb.ConfChangeType_RemoveNode:
		// Ignore duplicated or stale RemoveNode.
		existing := util.FindPeer(newRegion, peer.StoreId)
		if existing != nil && existing.Id == peer.Id {
			newPeers := make([]*metapb.Peer, 0, len(newRegion.Peers)-1)
			for _, p := range newRegion.Peers {
				if p.Id != peer.Id {
					newPeers = append(newPeers, p)
				}
			}
			newRegion.Peers = newPeers
			d.removePeerCache(peer.Id)
			changed = true
		}
	}

	// Apply conf change to Raft
	d.RaftGroup.ApplyConfChange(*cc)
	if changed {
		newRegion.RegionEpoch.ConfVer++
	}

	// If removing self, destroy peer and skip writing normal region state.
	// Otherwise the pending kvWB would overwrite the tombstone state.
	// Return nil to signal that the peer has been destroyed.
	if cc.ChangeType == eraftpb.ConfChangeType_RemoveNode && peer.Id == d.PeerId() && changed {
		d.destroyPeer()
		return nil
	}

	respRegion := region
	if changed {
		// Update store metadata
		storeMeta := d.ctx.storeMeta
		storeMeta.Lock()
		storeMeta.regions[newRegion.Id] = newRegion
		storeMeta.Unlock()

		// Persist region state
		meta.WriteRegionState(kvWB, newRegion, rspb.PeerState_Normal)

		// Update peer's region
		d.SetRegion(newRegion)
		respRegion = newRegion
	}

	// Return response
	return &raft_cmdpb.RaftCmdResponse{
		Header: &raft_cmdpb.RaftResponseHeader{},
		AdminResponse: &raft_cmdpb.AdminResponse{
			CmdType: raft_cmdpb.AdminCmdType_ChangePeer,
			ChangePeer: &raft_cmdpb.ChangePeerResponse{
				Region: respRegion,
			},
		},
	}
}

// applySplit applies a Split admin command (3B)
func (d *peerMsgHandler) applySplit(req *raft_cmdpb.SplitRequest, kvWB *engine_util.WriteBatch) *raft_cmdpb.RaftCmdResponse {
	// Validate split key is within current region range
	region := d.Region()
	if err := util.CheckKeyInRegionExclusive(req.SplitKey, region); err != nil {
		return ErrResp(err)
	}
	if len(region.Peers) == 0 || len(region.Peers) != len(req.NewPeerIds) {
		return ErrResp(fmt.Errorf("split peer count mismatch, region %d peers %d, new peer ids %d",
			region.Id, len(region.Peers), len(req.NewPeerIds)))
	}

	oldRegion := cloneRegion(region)
	leftRegion := cloneRegion(region)

	// Create new region
	newRegion := &metapb.Region{
		Id:       req.NewRegionId,
		StartKey: util.SafeCopy(req.SplitKey),
		EndKey:   util.SafeCopy(leftRegion.EndKey),
		RegionEpoch: &metapb.RegionEpoch{
			ConfVer: leftRegion.RegionEpoch.ConfVer,
			Version: leftRegion.RegionEpoch.Version + 1,
		},
		Peers: make([]*metapb.Peer, 0, len(req.NewPeerIds)),
	}

	// Create peers for new region
	for i, peerID := range req.NewPeerIds {
		newRegion.Peers = append(newRegion.Peers, &metapb.Peer{
			Id:      peerID,
			StoreId: leftRegion.Peers[i].StoreId,
		})
	}

	// Build left region as a new immutable region object.
	leftRegion.EndKey = util.SafeCopy(req.SplitKey)
	leftRegion.RegionEpoch.Version++

	// Create new peer for the new region
	newPeer, err := createPeer(d.storeID(), d.ctx.cfg, d.ctx.regionTaskSender, d.ctx.engine, newRegion)
	if err != nil {
		panic(err)
	}

	// Register new peer in router
	d.ctx.router.register(newPeer)

	// Start new peer
	_ = d.ctx.router.send(newRegion.Id, message.Msg{Type: message.MsgTypeStart})

	// Update store metadata for both regions
	storeMeta := d.ctx.storeMeta
	storeMeta.Lock()
	storeMeta.regions[leftRegion.Id] = leftRegion
	storeMeta.regions[newRegion.Id] = newRegion
	// Update region ranges - delete old, insert both new
	storeMeta.regionRanges.Delete(&regionItem{region: oldRegion})
	storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: leftRegion})
	storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: newRegion})
	storeMeta.Unlock()

	// Persist both regions
	meta.WriteRegionState(kvWB, leftRegion, rspb.PeerState_Normal)
	meta.WriteRegionState(kvWB, newRegion, rspb.PeerState_Normal)

	// Update peer's region
	d.SetRegion(leftRegion)

	// Return response
	return &raft_cmdpb.RaftCmdResponse{
		Header: &raft_cmdpb.RaftResponseHeader{},
		AdminResponse: &raft_cmdpb.AdminResponse{
			CmdType: raft_cmdpb.AdminCmdType_Split,
			Split: &raft_cmdpb.SplitResponse{
				Regions: []*metapb.Region{leftRegion, newRegion},
			},
		},
	}
}

// applyCompactLog applies a CompactLog admin command (2C)
func (d *peerMsgHandler) applyCompactLog(req *raft_cmdpb.CompactLogRequest, kvWB *engine_util.WriteBatch) {
	compactIndex := req.CompactIndex
	compactTerm := req.CompactTerm

	// Validate: can't compact beyond applied index
	if compactIndex > d.peerStorage.applyState.AppliedIndex {
		// Invalid compact request, ignore
		return
	}

	// Validate: can't compact already compacted entries
	if compactIndex <= d.peerStorage.applyState.TruncatedState.Index {
		// Already compacted, ignore
		return
	}

	// Update truncated state
	d.peerStorage.applyState.TruncatedState = &rspb.RaftTruncatedState{
		Index: compactIndex,
		Term:  compactTerm,
	}

	// Persist updated apply state
	kvWB.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState)

	// Schedule async deletion of compacted log entries
	d.ScheduleCompactLog(compactIndex)
}
