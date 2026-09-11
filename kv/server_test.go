package kv_test

// KV-layer milestone tests: the second half of M2 (a working store with
// exactly-once semantics) and M4 (snapshots bound the log).

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/guy5116/raft-kv/harness"
)

const seed = 42

func TestM2_KVBasic(t *testing.T) {
	c := harness.NewKVCluster(3, seed, -1)
	defer c.Shutdown()
	ck := c.Clerk()

	if v := ck.Get("missing"); v != "" {
		t.Fatalf("Get on missing key = %q, want \"\"", v)
	}
	ck.Put("color", "orange")
	if v := ck.Get("color"); v != "orange" {
		t.Fatalf("Get(color) = %q, want orange", v)
	}
	ck.Put("color", "green")
	ck.Append("color", "+blue")
	if v := ck.Get("color"); v != "green+blue" {
		t.Fatalf("Get(color) = %q, want green+blue", v)
	}
}

func TestM2_KVSurvivesLeaderCrash(t *testing.T) {
	c := harness.NewKVCluster(3, seed, -1)
	defer c.Shutdown()
	ck := c.Clerk()

	ck.Put("k", "v1")
	leader, _, err := c.CheckSingleLeader()
	if err != nil {
		t.Fatal(err)
	}
	c.Crash(leader)

	// The clerk must find the new leader on its own and the data must
	// still be there — it was committed on a majority.
	if v := ck.Get("k"); v != "v1" {
		t.Fatalf("after leader crash Get(k) = %q, want v1", v)
	}
	ck.Put("k", "v2")
	if v := ck.Get("k"); v != "v2" {
		t.Fatalf("Get(k) = %q, want v2", v)
	}
}

// TestM2_KVExactlyOnce is the dedup test: concurrent clerks hammer
// Append over a lossy network, which forces client retries; every append
// must land EXACTLY once despite those retries. Duplicated appends mean
// the ClientID/Seq table isn't working.
func TestM2_KVExactlyOnce(t *testing.T) {
	c := harness.NewKVCluster(3, seed, -1)
	defer c.Shutdown()

	c.Net().SetDropRate(0.15)
	defer c.Net().SetDropRate(0)

	const clients, appends = 3, 10
	var wg sync.WaitGroup
	for ci := 0; ci < clients; ci++ {
		wg.Add(1)
		go func(ci int) {
			defer wg.Done()
			ck := c.Clerk() // each client is its own Clerk (own ClientID)
			for a := 0; a < appends; a++ {
				ck.Append("board", fmt.Sprintf("(%d-%d)", ci, a))
			}
		}(ci)
	}
	wg.Wait()

	c.Net().SetDropRate(0)
	final := c.Clerk().Get("board")
	for ci := 0; ci < clients; ci++ {
		for a := 0; a < appends; a++ {
			token := fmt.Sprintf("(%d-%d)", ci, a)
			switch strings.Count(final, token) {
			case 1: // perfect
			case 0:
				t.Fatalf("append %s lost entirely; value: %q", token, final)
			default:
				t.Fatalf("append %s applied more than once (dedup broken); value: %q", token, final)
			}
		}
	}
}

// ═══════════════════════════════════════════════════════════════════════
// Milestone 4 — snapshots

func TestM4_SnapshotBoundsState(t *testing.T) {
	const maxRaftState = 2048
	c := harness.NewKVCluster(3, seed, maxRaftState)
	defer c.Shutdown()
	ck := c.Clerk()

	big := strings.Repeat("x", 200)
	for i := 0; i < 60; i++ {
		ck.Put(fmt.Sprintf("key%d", i), big)
	}

	// Give snapshotting a beat to run, then check every node's persisted
	// Raft state is bounded — without compaction it would be ~60*200 bytes
	// of log and climbing.
	time.Sleep(1 * time.Second)
	for i := 0; i < c.N(); i++ {
		if size := c.RaftStateSize(i); size > 8*maxRaftState {
			t.Fatalf("node %d raft state is %d bytes — log compaction isn't happening", i, size)
		}
	}

	// And of course the data must all still be there.
	for i := 0; i < 60; i += 7 {
		if v := ck.Get(fmt.Sprintf("key%d", i)); v != big {
			t.Fatalf("key%d lost after snapshotting", i)
		}
	}
}

func TestM4_InstallSnapshotCatchesUpFollower(t *testing.T) {
	const maxRaftState = 1024
	c := harness.NewKVCluster(3, seed, maxRaftState)
	defer c.Shutdown()
	ck := c.Clerk()

	ck.Put("k0", "v0")
	leader, _, err := c.CheckSingleLeader()
	if err != nil {
		t.Fatal(err)
	}
	follower := (leader + 1) % 3
	c.Disconnect(follower)

	// Write enough that the leader compacts past everything the
	// disconnected follower has — catching it up entry-by-entry is now
	// impossible; only InstallSnapshot can save it.
	big := strings.Repeat("y", 200)
	for i := 0; i < 40; i++ {
		ck.Put(fmt.Sprintf("key%d", i), big)
	}

	c.Reconnect(follower)
	time.Sleep(2 * time.Second) // time to receive the snapshot

	// Kill the OTHER two nodes: the once-lagging follower plus a
	// restarted node must now hold a majority of the data by themselves.
	other := 3 - leader - follower
	c.Crash(leader)
	c.Crash(other)
	c.Restart(other)

	ck2 := c.Clerk()
	if v := ck2.Get("key3"); v != big {
		t.Fatalf("catch-up follower serves key3=%q — snapshot install failed", v)
	}
	if v := ck2.Get("k0"); v != "v0" {
		t.Fatalf("pre-snapshot key lost: k0=%q", v)
	}
}
