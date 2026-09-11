package raft

// ═══════════════════════════════════════════════════════════════════════
// MILESTONE 4 — Snapshots & log compaction (§7)
//
// Goal: the log stops growing forever. The state machine periodically
// hands Raft a snapshot of itself; Raft discards the log prefix the
// snapshot covers. A follower that has fallen behind the compaction
// point can no longer be caught up entry-by-entry — the leader ships it
// the whole snapshot via InstallSnapshot. `make test-m4`.
//
// The Log type already handles offset indexing (log.go — NOW is the time
// to read CompactTo). The kv.Server side (deciding when to snapshot,
// serializing its map) is milestone 4's other half — see kv/server.go.
// ═══════════════════════════════════════════════════════════════════════

// Snapshot is called BY the state machine (kv.Server): "here is my state
// through log index `index`; you may forget everything up to there."
//
// TODO(you) — milestone 4:
//  1. Lock. Ignore stale calls: index <= r.log.SnapshotIndex() or
//     index > r.commitIndex → return.
//  2. term := r.log.Term(index); r.log.CompactTo(index, term).
//  3. Persist state and snapshot ATOMICALLY:
//     r.storage.SaveSnapshot(<same bytes persist() builds>, snapshot).
//     (Factor persist()'s encoding into a raftStateBytes() helper so
//     both call sites share it.)
func (r *Raft) Snapshot(index int, snapshot []byte) {
	// TODO(you): implement (milestone 4).
}

// InstallSnapshot is the RPC handler for receiving a leader's snapshot
// (§7, Figure 13 — simplified: send the whole snapshot in one RPC, no
// chunking; real systems chunk, yours doesn't need to).
//
// TODO(you) — milestone 4:
//  1. Lock. Usual term dance: stale term → reply and return; newer term
//     → becomeFollowerLocked. resetElectionTimerLocked() — this is
//     leader contact.
//  2. Stale snapshot (args.LastIncludedIndex <= r.commitIndex or
//     <= r.log.SnapshotIndex()) → ignore.
//  3. If our log has an entry at LastIncludedIndex with the matching
//     term, just CompactTo it (we only lacked the prefix). Otherwise the
//     whole log is garbage: Restore(nil, LastIncludedIndex,
//     LastIncludedTerm).
//  4. Advance commitIndex and lastApplied to LastIncludedIndex; persist
//     with SaveSnapshot.
//  5. Hand the snapshot to the state machine: unlock, then send an
//     ApplyMsg{SnapshotValid: true, ...} on r.applyCh. (Never send on
//     applyCh with the lock held. One acceptable simplification: send it
//     from this handler directly; ordering with the applier is safe
//     because lastApplied was advanced under the same lock.)
//
// Leader side: in broadcastAppendEntries, when a peer's nextIndex has
// fallen below r.log.FirstIndex(), send InstallSnapshot (data from
// r.storage.ReadSnapshot()) instead of AppendEntries; on success set
// nextIndex = LastIncludedIndex + 1, matchIndex = LastIncludedIndex.
func (r *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	// TODO(you): implement (milestone 4).
}
