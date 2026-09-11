// Package storage provides the stable-storage layer Raft persists its
// state to. Raft's safety guarantees depend on certain state (currentTerm,
// votedFor, the log) surviving crashes — see paper §5.1 (Figure 2,
// "Persistent state").
//
// Two implementations:
//   - MemoryStorage: for tests. "Crashing" a node in the harness keeps its
//     MemoryStorage around and hands it to the restarted node, simulating
//     a disk that survived the crash.
//   - FileStorage: real files with atomic writes, for running actual
//     clusters (cmd/raftkv).
package storage

import (
	"os"
	"path/filepath"
	"sync"
)

// Storage is stable storage for one Raft node.
//
// SaveState must be atomic and durable: after it returns, a crash at any
// moment must recover exactly the old bytes or the new bytes, never a
// mix. Raft calls it on every term change, vote, and log append, so it is
// also the main performance cost of a real deployment (fsync!).
type Storage interface {
	// SaveState durably stores the Raft persistent state (term, vote, log).
	SaveState(state []byte) error
	// ReadState returns the last saved state, or nil if none exists.
	ReadState() ([]byte, error)
	// SaveSnapshot durably stores a state-machine snapshot together with
	// the Raft state that must change atomically with it (M4).
	SaveSnapshot(state []byte, snapshot []byte) error
	// ReadSnapshot returns the last snapshot, or nil if none exists.
	ReadSnapshot() ([]byte, error)
	// StateSize reports the size in bytes of the saved Raft state — used
	// to decide when the log has grown enough to snapshot (M4).
	StateSize() int
}

// ---------------------------------------------------------------------------
// MemoryStorage

// MemoryStorage keeps everything in RAM. It survives a *simulated* crash
// because the test harness owns it and passes it to the restarted node.
type MemoryStorage struct {
	mu       sync.Mutex
	state    []byte
	snapshot []byte
}

func NewMemoryStorage() *MemoryStorage { return &MemoryStorage{} }

func (m *MemoryStorage) SaveState(state []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.state = clone(state)
	return nil
}

func (m *MemoryStorage) ReadState() ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return clone(m.state), nil
}

func (m *MemoryStorage) SaveSnapshot(state, snapshot []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.state = clone(state)
	m.snapshot = clone(snapshot)
	return nil
}

func (m *MemoryStorage) ReadSnapshot() ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return clone(m.snapshot), nil
}

func (m *MemoryStorage) StateSize() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.state)
}

func clone(b []byte) []byte {
	if b == nil {
		return nil
	}
	out := make([]byte, len(b))
	copy(out, b)
	return out
}

// ---------------------------------------------------------------------------
// FileStorage

// FileStorage persists to <dir>/raft.state and <dir>/raft.snapshot using
// the classic write-temp-then-rename trick so each save is atomic: rename(2)
// on the same filesystem either fully happens or doesn't.
type FileStorage struct {
	mu  sync.Mutex
	dir string
}

func NewFileStorage(dir string) (*FileStorage, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	return &FileStorage{dir: dir}, nil
}

func (f *FileStorage) statePath() string    { return filepath.Join(f.dir, "raft.state") }
func (f *FileStorage) snapshotPath() string { return filepath.Join(f.dir, "raft.snapshot") }

func (f *FileStorage) SaveState(state []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return atomicWrite(f.statePath(), state)
}

func (f *FileStorage) ReadState() ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return readIfExists(f.statePath())
}

func (f *FileStorage) SaveSnapshot(state, snapshot []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	// Snapshot first, then state: if we crash between the two writes we
	// have a newer snapshot with an older state, which is safe — the
	// reverse order could leave state referencing a snapshot that was
	// never written.
	if err := atomicWrite(f.snapshotPath(), snapshot); err != nil {
		return err
	}
	return atomicWrite(f.statePath(), state)
}

func (f *FileStorage) ReadSnapshot() ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return readIfExists(f.snapshotPath())
}

func (f *FileStorage) StateSize() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	info, err := os.Stat(f.statePath())
	if err != nil {
		return 0
	}
	return int(info.Size())
}

func atomicWrite(path string, data []byte) error {
	tmp, err := os.CreateTemp(filepath.Dir(path), ".tmp-*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name()) // no-op after successful rename
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil { // the fsync that makes it durable
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmp.Name(), path)
}

func readIfExists(path string) ([]byte, error) {
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil, nil
	}
	return data, err
}
