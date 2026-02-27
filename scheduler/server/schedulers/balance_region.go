// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package schedulers

import (
	"sort"

	"github.com/pingcap-incubator/tinykv/scheduler/server/core"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/operator"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/opt"
)

func init() {
	schedule.RegisterSliceDecoderBuilder("balance-region", func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			return nil
		}
	})
	schedule.RegisterScheduler("balance-region", func(opController *schedule.OperatorController, storage *core.Storage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		return newBalanceRegionScheduler(opController), nil
	})
}

const (
	// balanceRegionRetryLimit is the limit to retry schedule for selected store.
	balanceRegionRetryLimit = 10
	balanceRegionName       = "balance-region-scheduler"
)

type balanceRegionScheduler struct {
	*baseScheduler
	name         string
	opController *schedule.OperatorController
}

// newBalanceRegionScheduler creates a scheduler that tends to keep regions on
// each store balanced.
func newBalanceRegionScheduler(opController *schedule.OperatorController, opts ...BalanceRegionCreateOption) schedule.Scheduler {
	base := newBaseScheduler(opController)
	s := &balanceRegionScheduler{
		baseScheduler: base,
		opController:  opController,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// BalanceRegionCreateOption is used to create a scheduler with an option.
type BalanceRegionCreateOption func(s *balanceRegionScheduler)

func (s *balanceRegionScheduler) GetName() string {
	if s.name != "" {
		return s.name
	}
	return balanceRegionName
}

func (s *balanceRegionScheduler) GetType() string {
	return "balance-region"
}

func (s *balanceRegionScheduler) IsScheduleAllowed(cluster opt.Cluster) bool {
	return s.opController.OperatorCount(operator.OpRegion) < cluster.GetRegionScheduleLimit()
}

func (s *balanceRegionScheduler) Schedule(cluster opt.Cluster) *operator.Operator {
	// Your Code Here (3C).

	// 1. Select all suitable stores (Up and not down too long).
	stores := make([]*core.StoreInfo, 0)
	for _, store := range cluster.GetStores() {
		if store.IsUp() && store.DownTime() < cluster.GetMaxStoreDownTime() {
			stores = append(stores, store)
		}
	}
	if len(stores) == 0 {
		return nil
	}

	// 2. Sort by region size descending.
	sort.Slice(stores, func(i, j int) bool {
		return stores[i].GetRegionSize() > stores[j].GetRegionSize()
	})

	// 3. Try to find a region to move from the store with the biggest region size.
	var region *core.RegionInfo
	var sourceStore *core.StoreInfo
	for _, store := range stores {
		storeID := store.GetID()
		// Try pending regions first.
		cluster.GetPendingRegionsWithLock(storeID, func(rc core.RegionsContainer) {
			region = rc.RandomRegion(nil, nil)
		})
		if region != nil {
			sourceStore = store
			break
		}
		// Try follower regions.
		cluster.GetFollowersWithLock(storeID, func(rc core.RegionsContainer) {
			region = rc.RandomRegion(nil, nil)
		})
		if region != nil {
			sourceStore = store
			break
		}
		// Try leader regions.
		cluster.GetLeadersWithLock(storeID, func(rc core.RegionsContainer) {
			region = rc.RandomRegion(nil, nil)
		})
		if region != nil {
			sourceStore = store
			break
		}
	}
	if region == nil || sourceStore == nil {
		return nil
	}

	// 4. Select the store with the smallest region size as target.
	// The target must not already have a peer of this region.
	storeIDs := region.GetStoreIds()
	if len(storeIDs) >= cluster.GetMaxReplicas() {
		// Find target: smallest region size store that doesn't have this region's peer.
		var targetStore *core.StoreInfo
		for i := len(stores) - 1; i >= 0; i-- {
			if _, ok := storeIDs[stores[i].GetID()]; !ok {
				targetStore = stores[i]
				break
			}
		}
		if targetStore == nil {
			return nil
		}

		// 5. Judge whether this movement is valuable.
		// The difference must be bigger than 2 * approximate size of the region.
		if sourceStore.GetRegionSize()-targetStore.GetRegionSize() < 2*region.GetApproximateSize() {
			return nil
		}

		// 6. Create move peer operator.
		newPeer, err := cluster.AllocPeer(targetStore.GetID())
		if err != nil {
			return nil
		}
		op, err := operator.CreateMovePeerOperator("balance-region", cluster, region, operator.OpBalance, sourceStore.GetID(), targetStore.GetID(), newPeer.GetId())
		if err != nil {
			return nil
		}
		return op
	}

	return nil
}
