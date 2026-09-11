// Package kv is the replicated key/value store built on top of raft: the
// "state machine" in Raft's replicated-state-machine model. Each server
// owns one Raft node; all servers apply the same committed commands in
// the same order, so their maps stay identical.
//
// This layer is milestone 2's second half (`make test-m2`), and it's
// where linearizability gets real: a client Put must survive duplicate
// delivery (client retries after a leader crash) without applying twice.
package kv

import (
	"bytes"
	"encoding/gob"
	"sync"
	"time"

	"github.com/guy5116/raft-kv/raft"
)

// Op is the command gob-encoded into Raft log entries.
//
// ClientID+Seq exist for exactly-once semantics: a client stamps every
// request, and servers remember the highest Seq applied per client. A
// retried Put that already committed is then recognized and NOT applied
// again — without this, "retry on timeout" (which every client must do)
// silently double-appends. Interviewers love this detail.
type Op struct {
	Type     string // "Get", "Put" or "Append"
	Key      string
	Value    string
	ClientID int64
	Seq      int64
}

// applyResult is what the apply loop reports back to a waiting RPC.
type applyResult struct {
	op    Op
	value string // for Get
}

const opTimeout = 2 * time.Second

// Server is one replica of the KV service.
type Server struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	data    map[string]string        // the actual store
	lastSeq map[int64]int64          // per-client dedup: highest Seq applied
	waiters map[int]chan applyResult // log index -> waiting Submit call

	// maxRaftState: when Raft's persisted state exceeds this many bytes,
	// snapshot (milestone 4). -1 disables snapshotting.
	maxRaftState int
}

// NewServer wires a KV server to its Raft node. The applyCh handed to
// raft.NewNode must be the same one passed here.
func NewServer(me int, rf *raft.Raft, applyCh chan raft.ApplyMsg, maxRaftState int) *Server {
	s := &Server{
		me:           me,
		rf:           rf,
		applyCh:      applyCh,
		data:         make(map[string]string),
		lastSeq:      make(map[int64]int64),
		waiters:      make(map[int]chan applyResult),
		maxRaftState: maxRaftState,
	}
	go s.applyLoop()
	return s
}

// Raft exposes the underlying node (the harness uses it).
func (s *Server) Raft() *raft.Raft { return s.rf }

// ---------------------------------------------------------------------------
// Client-facing operations. In this project clients call these directly
// (in-process); putting them behind HTTP is a stretch goal in cmd/raftkv.

// Get returns the value for key ("" if absent) and ok=false if this
// server is not the leader or the operation timed out.
//
// Note that even Get goes through the log. A leader that answers reads
// from memory without committing them can serve STALE data when it has
// been deposed but doesn't know it yet — logging the read is the simple
// correct fix (real systems use ReadIndex/leases; mention both in your
// README).
func (s *Server) Get(key string, clientID, seq int64) (value string, ok bool) {
	res, ok := s.submit(Op{Type: "Get", Key: key, ClientID: clientID, Seq: seq})
	return res.value, ok
}

// Put sets key=value. Append appends to the existing value.
func (s *Server) Put(key, value string, clientID, seq int64) bool {
	_, ok := s.submit(Op{Type: "Put", Key: key, Value: value, ClientID: clientID, Seq: seq})
	return ok
}

func (s *Server) Append(key, value string, clientID, seq int64) bool {
	_, ok := s.submit(Op{Type: "Append", Key: key, Value: value, ClientID: clientID, Seq: seq})
	return ok
}

// submit pushes an Op through Raft and waits for it to be applied.
//
// TODO(you) — milestone 2:
//  1. index, _, isLeader := s.rf.Submit(encodeOp(op)); if !isLeader →
//     return ok=false immediately (the client will try another server).
//  2. Register a buffered channel in s.waiters[index] (under s.mu).
//  3. Wait on it with a timeout (select on the channel and
//     time.After(opTimeout) — a leader that got partitioned after
//     Submit will never commit that index, and the client must not hang
//     forever).
//  4. When the apply loop delivers a result for that index, check it is
//     OUR op (same ClientID+Seq). A DIFFERENT op at our index means we
//     lost leadership and someone else's entry took the slot → ok=false.
//  5. Clean up the waiter entry in all paths.
func (s *Server) submit(op Op) (applyResult, bool) {
	// TODO(you): implement (milestone 2).
	return applyResult{}, false
}

// applyLoop consumes committed entries from Raft — the ONLY place s.data
// is ever mutated, which is what keeps replicas identical.
//
// TODO(you) — milestone 2 (and a bit of 4):
//  1. Range over s.applyCh. For each msg with CommandValid:
//     a. decodeOp(msg.Command).
//     b. Under s.mu: if op.Seq > s.lastSeq[op.ClientID], APPLY it
//     (mutate s.data for Put/Append) and record lastSeq. Otherwise
//     it's a duplicate — skip the mutation. Gets always read
//     s.data[op.Key] (reads are idempotent; no dedup needed, but
//     fill the result's value AFTER the dedup block so a duplicated
//     Get still returns the current value).
//     c. If a waiter is registered at msg.CommandIndex, send it the
//     result (non-blocking or buffered — never let the apply loop
//     stall on a vanished waiter).
//  2. Milestone 4: after applying, if s.maxRaftState != -1 and
//     s.rf (storage state size) has outgrown it, build a snapshot —
//     gob-encode s.data AND s.lastSeq (forget lastSeq and a restarted
//     server re-applies duplicates!) — and call
//     s.rf.Snapshot(msg.CommandIndex, bytes).
//  3. Milestone 4: for msgs with SnapshotValid, decode and REPLACE
//     s.data and s.lastSeq wholesale.
func (s *Server) applyLoop() {
	// TODO(you): implement (milestone 2 + 4).
	// Until then, drain the channel so Raft's applier never blocks:
	for range s.applyCh {
	}
}

// ---------------------------------------------------------------------------
// Encoding helpers (provided).

func encodeOp(op Op) []byte {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(op); err != nil {
		panic(err) // an Op that can't encode is a programming error
	}
	return buf.Bytes()
}

func decodeOp(b []byte) Op {
	var op Op
	if err := gob.NewDecoder(bytes.NewReader(b)).Decode(&op); err != nil {
		panic(err)
	}
	return op
}
