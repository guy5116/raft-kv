// Package raft implements the Raft consensus algorithm from:
//
//	"In Search of an Understandable Consensus Algorithm" (Ongaro &
//	Ousterhout, 2014) — https://raft.github.io/raft.pdf
//
// The plumbing (struct, goroutines, RPC dispatch, apply loop) is provided.
// The algorithm itself — elections, replication, persistence, snapshots —
// is yours: grep for "TODO(you)" and work milestone by milestone (see the
// README). Keep Figure 2 of the paper open at all times; nearly every
// TODO is a direct translation of one of its boxes.
package raft

import (
	"fmt"
	"math/rand"
	"os"
	"sync"
	"time"

	"github.com/guy5116/raft-kv/storage"
	"github.com/guy5116/raft-kv/transport"
)

// Timing constants. Rule of thumb (§5.6): broadcastTime ≪ electionTimeout,
// and timeouts are randomized so two nodes rarely time out together.
const (
	// HeartbeatInterval is how often a leader sends AppendEntries even
	// with nothing new to replicate, to suppress elections.
	HeartbeatInterval = 50 * time.Millisecond
	// ElectionTimeoutMin/Max bound the randomized election timeout. Each
	// time a node resets its timer it should pick a fresh random value in
	// this range — that randomness is what breaks split-vote livelock.
	ElectionTimeoutMin = 250 * time.Millisecond
	ElectionTimeoutMax = 500 * time.Millisecond

	tickInterval = 10 * time.Millisecond
)

// Raft is one consensus node. All fields below mu are protected by it.
//
// LOCKING RULES (read this twice — most bugs you'll hit are here):
//  1. Take r.mu before touching any protected field; release it with defer.
//  2. NEVER hold r.mu across a blocking operation: an RPC (r.transport.Call)
//     or a channel send. Copy what you need, unlock, then block. Holding
//     the lock across an RPC deadlocks the moment two nodes call each
//     other at once.
//  3. After re-acquiring the lock post-RPC, re-check your assumptions:
//     the world may have moved on (term changed, you lost leadership).
type Raft struct {
	mu        sync.Mutex
	me        int   // this node's ID
	peers     []int // IDs of all nodes, including me
	transport transport.Transport
	storage   storage.Storage

	// --- Persistent state (Figure 2: update on stable storage BEFORE
	// responding to RPCs — that's what persist() is for, milestone 3).
	currentTerm int  // latest term this node has seen (starts at 0)
	votedFor    int  // candidate that received our vote in currentTerm, or -1
	log         *Log // the replicated log

	// --- Volatile state on all nodes.
	state         State
	commitIndex   int       // highest log index known to be committed
	lastApplied   int       // highest log index applied to the state machine
	electionReset time.Time // last moment we heard from a valid leader / granted a vote
	timeoutSpan   time.Duration

	// --- Volatile state on leaders (reinitialized after election).
	nextIndex  map[int]int // per peer: next log index to send
	matchIndex map[int]int // per peer: highest log index known replicated

	// --- Runtime plumbing.
	applyCh   chan ApplyMsg
	applyCond *sync.Cond // signaled whenever commitIndex advances
	rng       *rand.Rand
	dead      bool
}

// NewNode creates a Raft node. It does not start any goroutines — call
// Start once the node is registered with its network, and send committed
// entries' ApplyMsgs will appear on applyCh.
func NewNode(me int, peers []int, t transport.Transport, st storage.Storage, applyCh chan ApplyMsg) *Raft {
	r := &Raft{
		me:        me,
		peers:     peers,
		transport: t,
		storage:   st,
		votedFor:  -1,
		log:       NewLog(),
		state:     Follower,
		applyCh:   applyCh,
		rng:       rand.New(rand.NewSource(int64(me)*7919 + time.Now().UnixNano())),
	}
	r.applyCond = sync.NewCond(&r.mu)
	r.resetElectionTimerLocked()

	// Recover from a previous life, if the disk has one (milestone 3).
	r.readPersist()

	return r
}

// Start launches the node's background goroutines.
func (r *Raft) Start() {
	go r.ticker()
	go r.applier()
}

// Kill stops the node. The harness calls this to simulate a crash; a real
// process would just die, which is why the code checks killed() rather
// than doing any graceful shutdown.
func (r *Raft) Kill() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.dead = true
	r.applyCond.Broadcast()
}

func (r *Raft) killed() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.dead
}

// GetState reports the node's current term and whether it believes it is
// the leader.
func (r *Raft) GetState() (term int, isLeader bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.currentTerm, r.state == Leader
}

// Submit proposes a command for the replicated log. If this node is not
// the leader it returns isLeader=false and does nothing — the client must
// find the leader. If it is, Submit returns immediately with the index
// the command WILL occupy if it commits; the caller learns the outcome by
// watching applyCh.
//
// TODO(you) — milestone 2 (§5.3, one small step at a time):
//  1. Lock. If killed or not Leader, return false.
//  2. Append LogEntry{Term: r.currentTerm, Command: command} to r.log.
//  3. persist() — the entry must survive a crash before we promise it to
//     anyone (this becomes real in milestone 3; the call is a no-op today).
//  4. Record index := r.log.LastIndex(), term := r.currentTerm; update
//     r.matchIndex[r.me] = index.
//  5. Unlock, then trigger replication to all peers right away (don't
//     wait for the next heartbeat — latency matters): r.broadcastAppendEntries().
//  6. Return index, term, true.
func (r *Raft) Submit(command []byte) (index int, term int, isLeader bool) {
	// TODO(you): implement (milestone 2).
	return -1, -1, false
}

// ---------------------------------------------------------------------------
// RPC dispatch — the transport delivers every incoming RPC here.

func (r *Raft) HandleRPC(method string, args any, reply any) bool {
	switch method {
	case "Raft.RequestVote":
		r.RequestVote(args.(*RequestVoteArgs), reply.(*RequestVoteReply))
	case "Raft.AppendEntries":
		r.AppendEntries(args.(*AppendEntriesArgs), reply.(*AppendEntriesReply))
	case "Raft.InstallSnapshot":
		r.InstallSnapshot(args.(*InstallSnapshotArgs), reply.(*InstallSnapshotReply))
	default:
		return false
	}
	return true
}

// callRequestVote / callAppendEntries / callInstallSnapshot are typed
// wrappers around the transport. They BLOCK until the RPC completes or
// the network gives up — never call them while holding r.mu.
func (r *Raft) callRequestVote(peer int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	return r.transport.Call(peer, "Raft.RequestVote", args, reply)
}

func (r *Raft) callAppendEntries(peer int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	return r.transport.Call(peer, "Raft.AppendEntries", args, reply)
}

func (r *Raft) callInstallSnapshot(peer int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	return r.transport.Call(peer, "Raft.InstallSnapshot", args, reply)
}

// ---------------------------------------------------------------------------
// Background goroutines (provided — study them, they're idiomatic Go).

// ticker wakes up every tickInterval and starts an election if we haven't
// heard from a leader within the randomized election timeout (§5.2).
func (r *Raft) ticker() {
	for !r.killed() {
		time.Sleep(tickInterval)

		r.mu.Lock()
		timedOut := r.state != Leader && time.Since(r.electionReset) >= r.timeoutSpan
		r.mu.Unlock()

		if timedOut {
			r.startElection()
		}
	}
}

// applier is the ONLY goroutine allowed to send on applyCh, which
// guarantees the state machine sees commands strictly in log order. It
// sleeps on a condition variable; your code must call
// r.applyCond.Signal() whenever it advances r.commitIndex.
//
// Note the shape: gather work under the lock, RELEASE the lock, then do
// the blocking channel send. Sending on applyCh with r.mu held is the
// single most common deadlock in Raft labs (the KV server on the other
// end may be calling back into Raft).
func (r *Raft) applier() {
	// On restart, re-deliver the snapshot (if any) before anything else so
	// the state machine can rebuild itself, then continue from there (M4).
	r.mu.Lock()
	if si := r.log.SnapshotIndex(); si > 0 && r.lastApplied <= si {
		snap, _ := r.storage.ReadSnapshot()
		term := r.log.SnapshotTerm()
		r.lastApplied = si
		if r.commitIndex < si {
			r.commitIndex = si
		}
		r.mu.Unlock()
		if len(snap) > 0 {
			r.applyCh <- ApplyMsg{SnapshotValid: true, Snapshot: snap, SnapshotIndex: si, SnapshotTerm: term}
		}
	} else {
		r.mu.Unlock()
	}

	for {
		r.mu.Lock()
		for !r.dead && r.lastApplied >= r.commitIndex {
			r.applyCond.Wait()
		}
		if r.dead {
			r.mu.Unlock()
			return
		}
		var msgs []ApplyMsg
		for r.lastApplied < r.commitIndex {
			r.lastApplied++
			if r.lastApplied < r.log.FirstIndex() {
				continue // compacted away; a snapshot ApplyMsg covered it (M4)
			}
			e := r.log.Entry(r.lastApplied)
			msgs = append(msgs, ApplyMsg{
				CommandValid: true,
				Command:      e.Command,
				CommandIndex: r.lastApplied,
				CommandTerm:  e.Term,
			})
		}
		r.mu.Unlock()

		for _, m := range msgs {
			r.applyCh <- m
		}
	}
}

// ---------------------------------------------------------------------------
// Small helpers.

// resetElectionTimerLocked restarts the election clock with a fresh random
// timeout. Call it (with r.mu held) exactly when Figure 2 says to: on
// granting a vote, and on receiving AppendEntries from a legitimate leader.
func (r *Raft) resetElectionTimerLocked() {
	r.electionReset = time.Now()
	span := int64(ElectionTimeoutMax - ElectionTimeoutMin)
	r.timeoutSpan = ElectionTimeoutMin + time.Duration(r.rng.Int63n(span))
}

// debugLocked prints when RAFT_DEBUG=1 — sprinkle these liberally while
// debugging; grepping a full-cluster trace is how Raft bugs get found.
// Must be called with r.mu HELD (it reads term/state), hence the name.
func (r *Raft) debugLocked(format string, args ...any) {
	if os.Getenv("RAFT_DEBUG") == "" {
		return
	}
	prefix := fmt.Sprintf("[%v n%d t%d %s] ", time.Now().Format("15:04:05.000"), r.me, r.currentTerm, r.state)
	fmt.Fprintf(os.Stderr, prefix+format+"\n", args...)
}
