package raft

// ═══════════════════════════════════════════════════════════════════════
// MILESTONE 2 — Log replication (§5.3, §5.4.2)
//
// Goal: Submit() on the leader gets commands committed on a majority and
// applied everywhere, in the same order, through partitions, leader
// changes, and lossy networks. `make test-m2` is the scoreboard. This is
// the hardest milestone — budget real time for it, and turn on
// RAFT_DEBUG=1 early.
//
// Suggested order:
//   1. heartbeatLoop + a basic AppendEntries handler (empty entries) —
//      this finishes milestone 1's heartbeats.
//   2. Real entries: consistency check, truncation, appending.
//   3. Leader side: nextIndex backoff on rejection, matchIndex advance.
//   4. Commit rules — including the §5.4.2 "current term only" rule.
// ═══════════════════════════════════════════════════════════════════════

// AppendEntries is the RPC handler for heartbeats and replication (§5.3).
//
// TODO(you) — the AppendEntries box of Figure 2, in order:
//  1. Lock (defer unlock); reply.Term = r.currentTerm at the end always.
//  2. args.Term < r.currentTerm → reply false, done (stale leader).
//  3. args.Term >= r.currentTerm → this leader is legitimate:
//     becomeFollowerLocked if the term is newer OR we're a Candidate;
//     either way resetElectionTimerLocked() — hearing from the current
//     leader is exactly what postpones elections.
//  4. Consistency check: our log must contain an entry at
//     args.PrevLogIndex with term args.PrevLogTerm
//     (r.log.Term(args.PrevLogIndex) — remember it returns -1 for
//     "don't have it"). If not → reply false. (Optionally fill the
//     Conflict* fields for fast backup — skip on the first pass.)
//  5. Walk args.Entries: at the first position where our log has a
//     DIFFERENT term, TruncateFrom there, then Append the rest. Do NOT
//     blindly truncate when logs agree — an old, delayed AppendEntries
//     arriving after a newer one must not chop off entries the newer one
//     added (this is the subtlest bug in this handler; the chaos tests
//     hunt for it).
//  6. persist() if anything changed.
//  7. If args.LeaderCommit > r.commitIndex: advance commitIndex to
//     min(args.LeaderCommit, index of last NEW entry), then
//     r.applyCond.Signal().
//  8. reply.Success = true.
func (r *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// TODO(you): implement (milestone 2; step 1-3 already give you
	// working heartbeats for milestone 1).
}

// heartbeatLoop runs on the leader, broadcasting AppendEntries every
// HeartbeatInterval until this node stops being the leader of `term`.
//
// TODO(you):
//
//	for !r.killed() {
//	    lock; if r.state != Leader || r.currentTerm != term { unlock; return }; unlock
//	    r.broadcastAppendEntries()
//	    time.Sleep(HeartbeatInterval)
//	}
func (r *Raft) heartbeatLoop(term int) {
	// TODO(you): implement (milestone 1).
}

// broadcastAppendEntries sends one round of AppendEntries to every peer —
// used for heartbeats and called from Submit for low-latency replication.
//
// TODO(you) — leader side of §5.3:
//  1. Lock; bail unless still Leader. For each peer build args from that
//     peer's r.nextIndex:
//     PrevLogIndex = nextIndex-1, PrevLogTerm = r.log.Term(that),
//     Entries = r.log.Slice(nextIndex) (empty slice ⇒ heartbeat —
//     heartbeats and replication are the SAME code path),
//     LeaderCommit = r.commitIndex.
//     M4 note: if nextIndex-1 has been compacted away
//     (r.log.Term returns -1 below FirstIndex), this peer needs
//     InstallSnapshot instead — see snapshot.go.
//  2. Unlock; send each RPC in its own goroutine.
//  3. Handling each reply (re-lock first, re-check still Leader in the
//     same term):
//     - reply.Term > r.currentTerm → becomeFollowerLocked, stop.
//     - Success → matchIndex[peer] = PrevLogIndex + len(Entries) you
//     SENT (from your local copy of args — nextIndex may have moved),
//     nextIndex[peer] = matchIndex[peer] + 1, then
//     maybeAdvanceCommitLocked().
//     - Failure → decrement nextIndex[peer] (never below
//     r.log.FirstIndex()) and let the next round retry; with the
//     Conflict* fields you can jump a whole term at a time.
func (r *Raft) broadcastAppendEntries() {
	// TODO(you): implement (milestone 2; heartbeat-only version in milestone 1).
}

// maybeAdvanceCommitLocked recomputes commitIndex after matchIndex moved.
// Call with r.mu held.
//
// TODO(you) — §5.3 + the §5.4.2 safety rule:
//  1. Find the largest N > r.commitIndex such that a majority of
//     matchIndex[i] >= N (count yourself — you always match your own log).
//     (Walk N down from r.log.LastIndex(), or sort matchIndex and take
//     the median — both fine at this scale.)
//  2. Commit N ONLY IF r.log.Term(N) == r.currentTerm. A leader must
//     never count replicas to commit an entry from an OLDER term —
//     Figure 8 of the paper is a 5-node counterexample where doing so
//     loses committed data. Entries from old terms commit indirectly
//     when the first current-term entry commits.
//  3. If commitIndex advanced: r.applyCond.Signal().
func (r *Raft) maybeAdvanceCommitLocked() {
	// TODO(you): implement (milestone 2).
}
