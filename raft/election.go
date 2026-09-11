package raft

// ═══════════════════════════════════════════════════════════════════════
// MILESTONE 1 — Leader election (§5.2, §5.4.1)
//
// Goal: with no failures, the cluster elects exactly one leader; when the
// leader dies or is partitioned away, a new one takes over; terms only
// move forward. `make test-m1` is the scoreboard.
//
// Suggested order:
//   1. becomeFollowerLocked is given — read it, it sets the style.
//   2. RequestVote handler (the receiver side is easier to reason about).
//   3. startElection.
//   4. becomeLeaderLocked + heartbeats (heartbeat sending lives in
//      replication.go because heartbeats ARE AppendEntries).
// ═══════════════════════════════════════════════════════════════════════

// becomeFollowerLocked (PROVIDED as a worked example) transitions to
// Follower for a newer term. This implements the rule that appears in
// every Figure 2 box: "if RPC request or response contains term
// T > currentTerm: set currentTerm = T, convert to follower".
//
// Call it, with r.mu held, from EVERY place you observe a higher term —
// both RPC handlers and every RPC reply you process. Missing one of those
// call sites is a classic source of stuck clusters.
func (r *Raft) becomeFollowerLocked(term int) {
	r.debugLocked("stepping down to follower in term %d", term)
	r.state = Follower
	r.currentTerm = term
	r.votedFor = -1
	r.persist() // term & vote are persistent state (no-op until M3)
	r.resetElectionTimerLocked()
}

// RequestVote is the RPC handler candidates hit us with (§5.2).
//
// TODO(you) — translate the RequestVote box of Figure 2:
//  1. Lock (defer unlock). Fill reply.Term = r.currentTerm at the end no
//     matter what — callers use it to detect their own staleness.
//  2. If args.Term < r.currentTerm: reply false (stale candidate).
//  3. If args.Term > r.currentTerm: becomeFollowerLocked(args.Term).
//     (You still might not vote for them — keep going.)
//  4. Grant the vote iff BOTH:
//     a. r.votedFor is -1 or already args.CandidateID (one vote per term
//     — this is why votedFor must be persistent), AND
//     b. the candidate's log is at least as up-to-date as ours (§5.4.1):
//     args.LastLogTerm > our last term, OR equal terms and
//     args.LastLogIndex >= our last index. This check is what stops a
//     stale node from winning and erasing committed entries.
//  5. If granting: set r.votedFor, persist(), resetElectionTimerLocked()
//     — granting a vote counts as "heard from someone legitimate".
func (r *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// TODO(you): implement (milestone 1).
}

// startElection converts to candidate and canvasses the cluster for
// votes. The ticker calls it when the election timer fires.
//
// TODO(you) — the candidate box of Figure 2, plus Go concurrency:
//  1. Lock. Become Candidate; increment r.currentTerm; vote for yourself
//     (r.votedFor = r.me); persist(); resetElectionTimerLocked().
//  2. Snapshot everything the RPCs need into locals (term, last log
//     index/term) BEFORE unlocking — you must not touch r.* fields from
//     the goroutines below without re-locking.
//  3. For each peer (skip yourself) launch a goroutine:
//     - callRequestVote (blocking, no lock held).
//     - On reply: re-lock. If reply.Term > r.currentTerm →
//     becomeFollowerLocked and stop. If we're still a Candidate IN
//     THE SAME TERM we started (check! — a lot can happen during an
//     RPC) and the vote was granted, count it.
//     - On reaching a majority (len(r.peers)/2 + 1, counting our own
//     vote): becomeLeaderLocked(). Guard so it runs only once.
//     Share the tally between goroutines with a small mutex-protected
//     counter or a channel — your choice; the mutex+counter version is
//     shorter.
//  4. Do NOT wait for all replies before deciding — majority is enough,
//     and slow peers may never answer. (This is why each peer gets its
//     own goroutine rather than a sequential loop.)
//
// If the election times out with no winner, the ticker simply fires
// again and a fresh election starts in a higher term — you get retry for
// free, no extra code.
func (r *Raft) startElection() {
	// TODO(you): implement (milestone 1).
}

// becomeLeaderLocked initializes leader state and starts heartbeats.
// Call with r.mu held, only from a Candidate that just won.
//
// TODO(you):
//
//  1. r.state = Leader.
//
//  2. Reinitialize r.nextIndex[peer] = r.log.LastIndex() + 1 and
//     r.matchIndex[peer] = 0 for every peer (Figure 2, "volatile state on
//     leaders" — the optimistic guess that everyone matches us, corrected
//     by AppendEntries rejections).
//
//  3. Kick off the heartbeat loop:
//
//     go r.heartbeatLoop(r.currentTerm)
//
//     heartbeatLoop (write it in replication.go) broadcasts
//     AppendEntries every HeartbeatInterval and exits as soon as the
//     node is no longer Leader in that term — passing the term in makes
//     that exit check trivial and kills a whole class of "zombie old
//     leader keeps heartbeating" bugs.
func (r *Raft) becomeLeaderLocked() {
	// TODO(you): implement (milestone 1).
}
