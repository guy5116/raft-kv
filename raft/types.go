package raft

import "fmt"

// State is the role a node currently plays. Every node starts as a
// Follower (paper §5.1).
type State int

const (
	Follower State = iota
	Candidate
	Leader
)

func (s State) String() string {
	switch s {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return fmt.Sprintf("State(%d)", int(s))
	}
}

// LogEntry is one entry in the replicated log (paper Figure 2).
// Command is opaque to Raft — the KV layer gob-encodes its ops into it.
type LogEntry struct {
	Term    int
	Command []byte
}

// ApplyMsg is what Raft delivers on the apply channel once an entry is
// committed. The state machine (kv.Server) consumes these strictly in
// order — that ordering is the entire point of Raft.
type ApplyMsg struct {
	// A committed log entry:
	CommandValid bool
	Command      []byte
	CommandIndex int
	CommandTerm  int

	// Or (M4) a snapshot the state machine must reset itself to:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotIndex int
	SnapshotTerm  int
}

// ---------------------------------------------------------------------------
// RPC messages. Field-by-field these mirror the paper's Figure 2 — keep it
// open next to you while implementing the handlers.

// RequestVoteArgs — sent by candidates during elections (§5.2, §5.4.1).
type RequestVoteArgs struct {
	Term         int // candidate's term
	CandidateID  int // candidate requesting the vote
	LastLogIndex int // index of candidate's last log entry (for the §5.4.1 up-to-date check)
	LastLogTerm  int // term of candidate's last log entry
}

type RequestVoteReply struct {
	Term        int  // currentTerm of the responder, so the candidate can update itself
	VoteGranted bool // true means the candidate received this node's vote
}

// AppendEntriesArgs — sent by the leader, both as heartbeat (empty
// Entries) and to replicate log entries (§5.3).
type AppendEntriesArgs struct {
	Term         int        // leader's term
	LeaderID     int        // so followers can redirect clients
	PrevLogIndex int        // index of the log entry immediately before the new ones
	PrevLogTerm  int        // term of that entry (the consistency check)
	Entries      []LogEntry // entries to store (empty for heartbeat)
	LeaderCommit int        // leader's commitIndex
}

type AppendEntriesReply struct {
	Term    int  // currentTerm of the responder
	Success bool // true if follower had the entry matching PrevLogIndex/PrevLogTerm

	// Fast-backup fields (optional optimization, end of §5.3): let the
	// leader skip back a whole term per rejection instead of one index.
	// Leave them unused for your first working version.
	ConflictTerm  int // term of the conflicting entry, -1 if log too short
	ConflictIndex int // first index that holds ConflictTerm, or len(log) if too short
}

// InstallSnapshotArgs — sent by the leader when a follower is so far
// behind that the entries it needs have been compacted away (M4, §7).
type InstallSnapshotArgs struct {
	Term              int
	LeaderID          int
	LastIncludedIndex int    // the snapshot replaces the log through this index
	LastIncludedTerm  int    // term of that entry
	Data              []byte // the state-machine snapshot itself
}

type InstallSnapshotReply struct {
	Term int
}
