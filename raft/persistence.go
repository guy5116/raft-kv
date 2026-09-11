package raft

// ═══════════════════════════════════════════════════════════════════════
// MILESTONE 3 — Persistence & crash recovery (§5.1, Figure 2 left box)
//
// Goal: a node can be killed at ANY moment and restart with its promises
// intact. Only three things need to survive: currentTerm, votedFor, and
// the log. Everything else is safely reconstructible. Why these three:
//   - votedFor: forgetting a vote lets you vote twice in one term → two
//     leaders in the same term → committed data diverges.
//   - currentTerm: reverting to an old term lets you accept a stale
//     leader.
//   - log: it IS the data.
// `make test-m3` kills and restarts nodes mid-replication to check this.
//
// The plumbing below (storage interface, gob, atomic file writes) is
// done; you write the encode/decode and — the actual work of this
// milestone — audit every place persistent state changes and make sure
// persist() is called BEFORE the change is visible to anyone else (i.e.
// before replying to an RPC or sending one that reveals the new state).
// becomeFollowerLocked already calls it; your election/replication code
// must too (the TODOs there mark the spots).
// ═══════════════════════════════════════════════════════════════════════

// persist saves Raft's persistent state to stable storage. Call with
// r.mu held. Until you implement M3 it is an intentional no-op so
// milestones 1–2 run purely in memory.
//
// TODO(you) — milestone 3:
//  1. Build the byte slice with encoding/gob: encode, in order,
//     r.currentTerm, r.votedFor, r.log.Entries(), r.log.SnapshotIndex(),
//     r.log.SnapshotTerm() into a bytes.Buffer.
//  2. r.storage.SaveState(buf.Bytes()) — the storage layer handles
//     atomicity and fsync.
//
// (Performance note for your README benchmarks: this runs on every vote,
// term bump, and append. Real systems batch writes; yours doesn't need
// to, but measuring the difference between MemoryStorage and FileStorage
// makes a great graph.)
func (r *Raft) persist() {
	// TODO(you): implement (milestone 3). Deliberately a no-op until then.
}

// readPersist restores state saved by persist. Called from NewNode before
// any goroutine starts, so no locking is needed here.
//
// TODO(you) — milestone 3:
//  1. data, _ := r.storage.ReadState(); if len(data) == 0 → fresh node,
//     return.
//  2. gob-decode the same fields in the same order into locals; on any
//     error, panic — silently proceeding with half-restored consensus
//     state is how you lose data.
//  3. Restore: r.currentTerm, r.votedFor, and
//     r.log.Restore(entries, snapIndex, snapTerm).
//  4. M4 addition: r.commitIndex and r.lastApplied start at
//     r.log.SnapshotIndex() (everything up to there is already inside
//     the snapshot the KV server will reload).
func (r *Raft) readPersist() {
	// TODO(you): implement (milestone 3).
}
