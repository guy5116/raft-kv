package raft

// Log wraps the entry slice to hide Raft's two classic indexing headaches:
//
//  1. Raft indexes are 1-based (the paper's first entry is index 1).
//  2. After snapshotting (M4), the in-memory slice no longer starts at
//     index 1 — everything through snapshotIndex has been thrown away, so
//     global index i lives at slice position i - snapshotIndex - 1.
//
// This type is provided as plumbing so off-by-one bugs don't eat your
// weekends; the interesting decisions (WHAT to append, truncate, or
// compact, and WHEN) stay in your Raft code. Read it — it's short, and
// tests in the M4 milestone will exercise the offset math hard.
//
// Invariant: entry at global index snapshotIndex has term snapshotTerm and
// is not stored; entries[0] holds global index snapshotIndex+1.
type Log struct {
	entries       []LogEntry
	snapshotIndex int // last index compacted into the snapshot (0 = none)
	snapshotTerm  int // term of that entry
}

// NewLog returns an empty log: last index 0, term 0, no snapshot.
func NewLog() *Log { return &Log{} }

// LastIndex is the global index of the last entry (0 when empty).
func (l *Log) LastIndex() int {
	return l.snapshotIndex + len(l.entries)
}

// FirstIndex is the lowest global index still stored in memory.
// Entries below it exist only inside the snapshot.
func (l *Log) FirstIndex() int {
	return l.snapshotIndex + 1
}

// Term returns the term of the entry at global index i. It handles the
// snapshot boundary (i == snapshotIndex) and returns -1 for indexes that
// have been compacted away or don't exist yet — callers must check.
func (l *Log) Term(i int) int {
	switch {
	case i == l.snapshotIndex:
		return l.snapshotTerm
	case i < l.snapshotIndex || i > l.LastIndex():
		return -1
	default:
		return l.entries[i-l.snapshotIndex-1].Term
	}
}

// Entry returns the entry at global index i. Panics if i has been
// compacted away or doesn't exist — that's always a caller bug worth
// crashing loudly on in a lab project.
func (l *Log) Entry(i int) LogEntry {
	return l.entries[i-l.snapshotIndex-1]
}

// Slice returns a copy of entries from global index i through the end —
// what a leader packs into AppendEntries. Copying matters: the caller
// hands this slice to other goroutines, and the underlying array must not
// be shared with a log that later truncates.
func (l *Log) Slice(i int) []LogEntry {
	src := l.entries[i-l.snapshotIndex-1:]
	out := make([]LogEntry, len(src))
	copy(out, src)
	return out
}

// Append adds entries at the end of the log.
func (l *Log) Append(entries ...LogEntry) {
	l.entries = append(l.entries, entries...)
}

// TruncateFrom deletes the entry at global index i and everything after
// it — what a follower does when it finds a conflicting entry (§5.3).
func (l *Log) TruncateFrom(i int) {
	l.entries = l.entries[:i-l.snapshotIndex-1]
}

// CompactTo discards entries through global index i, which becomes the
// new snapshot boundary (M4). No-op if already compacted past i.
func (l *Log) CompactTo(i, term int) {
	if i <= l.snapshotIndex {
		return
	}
	if i >= l.LastIndex() {
		l.entries = nil
	} else {
		// Re-allocate instead of re-slicing so the discarded prefix can
		// actually be garbage collected.
		rest := l.entries[i-l.snapshotIndex:]
		l.entries = make([]LogEntry, len(rest))
		copy(l.entries, rest)
	}
	l.snapshotIndex = i
	l.snapshotTerm = term
}

// SnapshotIndex and SnapshotTerm expose the compaction boundary.
func (l *Log) SnapshotIndex() int { return l.snapshotIndex }
func (l *Log) SnapshotTerm() int  { return l.snapshotTerm }

// Entries exposes the raw slice for persistence (gob-encode it together
// with snapshotIndex/Term in persist()). Don't mutate it elsewhere.
func (l *Log) Entries() []LogEntry { return l.entries }

// Restore rebuilds the log from persisted parts (readPersist / M3).
func (l *Log) Restore(entries []LogEntry, snapshotIndex, snapshotTerm int) {
	l.entries = entries
	l.snapshotIndex = snapshotIndex
	l.snapshotTerm = snapshotTerm
}
