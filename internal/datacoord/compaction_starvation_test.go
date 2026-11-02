// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

package datacoord

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/task"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// TestL0CompactionNotStarved verifies that L0 compaction tasks are admitted
// even under heavy Mix/Sort load, preventing the channel-exclusion starvation
// described in issue #53740.
func TestL0CompactionNotStarved(t *testing.T) {
	mockMeta := NewMockCompactionMeta(t)
	mockMeta.EXPECT().SaveCompactionTask(mock.Anything, mock.Anything).Return(nil).Maybe()
	mockAlloc := allocator.NewMockAllocator(t)
	mockAlloc.EXPECT().AllocTimestamp(mock.Anything).Return(uint64(1000), nil).Maybe()
	mockScheduler := task.NewMockGlobalScheduler(t)
	mockScheduler.EXPECT().Enqueue(mock.Anything).Return().Maybe()

	handler := newCompactionInspector(mockMeta, mockAlloc, nil, mockScheduler, mockScheduler, newMockVersionManager())

	// Helper: create Mix compaction task on a given channel
	newMix := func(planID int64, channel string) CompactionTask {
		task := newMixCompactionTask(&datapb.CompactionTask{
			PlanID:  planID,
			Type:    datapb.CompactionType_MixCompaction,
			State:   datapb.CompactionTaskState_pipelining,
			Channel: channel,
			NodeID:  100,
		}, nil, mockMeta, newMockVersionManager())
		return task
	}

	// Helper: create L0 compaction task on a given channel
	newL0 := func(planID int64, channel string) CompactionTask {
		task := newL0CompactionTask(&datapb.CompactionTask{
			PlanID:  planID,
			Type:    datapb.CompactionType_Level0DeleteCompaction,
			State:   datapb.CompactionTaskState_pipelining,
			Channel: channel,
			NodeID:  100,
		}, nil, mockMeta)
		return task
	}

	t.Run("L0 admitted after maxMixSortPerL0 Mix tasks on same channel", func(t *testing.T) {
		// Enqueue 5 Mix tasks all on channel "ch-1", then 1 L0 task on "ch-1"
		// Without fairness, L0 would be excluded indefinitely because Mix keeps
		// "ch-1" in mixChannelExcludes.
		for i := int64(1); i <= 5; i++ {
			handler.submitTask(newMix(i, "ch-1"))
		}
		handler.submitTask(newL0(100, "ch-1"))

		gotTasks := handler.schedule()

		// Verify L0 task (planID 100) was admitted
		l0Admitted := false
		for _, gt := range gotTasks {
			if gt.GetTaskProto().GetPlanID() == 100 {
				l0Admitted = true
				break
			}
		}
		assert.True(t, l0Admitted, "L0 task should be admitted after Mix/Sort threshold")
	})

	t.Run("Multiple L0 tasks all get admitted across cycles", func(t *testing.T) {
		// Enqueue 20 Mix tasks + 5 L0 tasks on same channel
		// Over multiple schedule() calls, all L0 should eventually be admitted
		for i := int64(1); i <= 20; i++ {
			handler.submitTask(newMix(i, "ch-1"))
		}
		for i := int64(101; i <= 105; i++) {
			handler.submitTask(newL0(i, "ch-1"))
		}

		admittedL0 := make(map[int64]bool)
		// Run schedule multiple times to give L0 a chance
		for cycle := 0; cycle < 10; cycle++ {
			gotTasks := handler.schedule()
			for _, gt := range gotTasks {
				if gt.GetTaskProto().GetType() == datapb.CompactionType_Level0DeleteCompaction {
					admittedL0[gt.GetTaskProto().GetPlanID()] = true
				}
			}
		}

		assert.Len(t, admittedL0, 5, "All 5 L0 tasks should eventually be admitted")
	})

	t.Run("L0 on different channel is never starved", func(t *testing.T) {
		// Mix tasks on ch-1, L0 on ch-2 - no channel conflict, L0 should be admitted immediately
		handler.submitTask(newMix(1, "ch-1"))
		handler.submitTask(newMix(2, "ch-1"))
		handler.submitTask(newMix(3, "ch-1"))
		handler.submitTask(newMix(4, "ch-1"))
		handler.submitTask(newL0(200, "ch-2"))

		gotTasks := handler.schedule()

		l0Admitted := false
		for _, gt := range gotTasks {
			if gt.GetTaskProto().GetPlanID() == 200 {
				l0Admitted = true
				break
			}
		}
		assert.True(t, l0Admitted, "L0 on non-conflicting channel should be admitted immediately")
	})
}
