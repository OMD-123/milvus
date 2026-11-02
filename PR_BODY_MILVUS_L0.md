## Fix: L0 Compaction Starvation under Heavy BM25 Upsert Load

### Summary
Fixes L0 compaction starvation in `DataCoord.schedule()` where Mix/Sort tasks continuously exclude L0 tasks on the same channels, preventing L0 compaction from ever being admitted during heavy write workloads (e.g., 30 MiB/s BM25 upserts with 1-8 DataNodes).

### Root Cause
In `internal/datacoord/compaction_inspector.go`, the `schedule()` method builds channel-level exclusion sets from currently executing tasks. When a Mix/Sort task is executing on a channel, any pending L0 task on that same channel is excluded and re-queued. Under continuous write load, new Mix/Sort tasks keep arriving and re-excluding the channel, so L0 tasks are never admitted — `L0 pending` grows while `L0 executing` stays at 0.

### Fix
Added a fairness counter in `schedule()` that tracks Mix/Sort vs L0 admissions per scheduling cycle. After `maxMixSortPerL0` (4) Mix/Sort admissions without any L0 admission, the next pending L0 task is force-admitted even if its channel is currently excluded. The counter resets after each L0 admission.

### Files Changed
- `internal/datacoord/compaction_inspector.go` — fairness logic in `schedule()`
- `internal/datacoord/compaction_starvation_test.go` — regression tests

### Testing
- `TestL0CompactionNotStarved` — 3 subtests:
  1. L0 admitted after `maxMixSortPerL0` Mix tasks on same channel
  2. Multiple L0 tasks all eventually admitted across cycles
  3. L0 on non-conflicting channel admitted immediately

### Verification Steps (for reviewer)
1. Run `go test ./internal/datacoord/ -run TestL0CompactionNotStarved -v`
2. Reproduce issue #53740 workload: 30 MiB/s BM25 upsert, 1-8 DNs, verify `L0 pending` stays bounded
3. Confirm no regression in existing compaction tests: `go test ./internal/datacoord/`

### Related Issue
Fixes #53740

---

Signed-off-by: Om Dandagvhal <omd4485@gmail.com>
