package storage

// Plumbing self-tests (`make test-harness`) — pass from day one.

import (
	"bytes"
	"testing"
)

func testStorage(t *testing.T, st Storage) {
	t.Helper()

	if s, err := st.ReadState(); err != nil || s != nil {
		t.Fatalf("fresh storage: state=%v err=%v, want nil,nil", s, err)
	}
	if err := st.SaveState([]byte("state-v1")); err != nil {
		t.Fatal(err)
	}
	if s, _ := st.ReadState(); !bytes.Equal(s, []byte("state-v1")) {
		t.Fatalf("read back %q", s)
	}
	if err := st.SaveState([]byte("state-v2")); err != nil {
		t.Fatal(err)
	}
	if s, _ := st.ReadState(); !bytes.Equal(s, []byte("state-v2")) {
		t.Fatalf("read back %q after overwrite", s)
	}
	if st.StateSize() != len("state-v2") {
		t.Fatalf("StateSize = %d", st.StateSize())
	}

	if err := st.SaveSnapshot([]byte("state-v3"), []byte("snap-1")); err != nil {
		t.Fatal(err)
	}
	if s, _ := st.ReadState(); !bytes.Equal(s, []byte("state-v3")) {
		t.Fatalf("state after SaveSnapshot = %q", s)
	}
	if s, _ := st.ReadSnapshot(); !bytes.Equal(s, []byte("snap-1")) {
		t.Fatalf("snapshot = %q", s)
	}
}

func TestMemoryStorage(t *testing.T) {
	testStorage(t, NewMemoryStorage())
}

func TestFileStorage(t *testing.T) {
	st, err := NewFileStorage(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	testStorage(t, st)
}

func TestFileStorageSurvivesReopen(t *testing.T) {
	dir := t.TempDir()
	st, _ := NewFileStorage(dir)
	if err := st.SaveSnapshot([]byte("the-state"), []byte("the-snap")); err != nil {
		t.Fatal(err)
	}

	// "Restart": a brand-new FileStorage on the same dir sees everything.
	st2, _ := NewFileStorage(dir)
	if s, _ := st2.ReadState(); !bytes.Equal(s, []byte("the-state")) {
		t.Fatalf("state after reopen = %q", s)
	}
	if s, _ := st2.ReadSnapshot(); !bytes.Equal(s, []byte("the-snap")) {
		t.Fatalf("snapshot after reopen = %q", s)
	}
}
