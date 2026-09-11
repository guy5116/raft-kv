package raft_test

// The milestone scoreboard. Run with the Makefile:
//
//	make test-m1   — leader election
//	make test-m2   — log replication (see also kv/server_test.go)
//	make test-m3   — persistence & crash recovery
//	make test-m4   — snapshots (see kv/server_test.go)
//
// Every test runs under -race. When one fails, rerun it alone with
// RAFT_DEBUG=1 and read the trace:
//
//	RAFT_DEBUG=1 go test -race ./raft/ -run TestM1_LeaderDisconnect -v 2>debug.log
//
// The harness seeds its RNG from the constant below so failures
// reproduce; change the seed to shake out timing-dependent bugs, and try
// `go test -count 10` before calling a milestone done — Raft bugs love
// hiding in the run you didn't do.

import (
	"fmt"
	"testing"
	"time"

	"github.com/guy5116/raft-kv/harness"
)

const seed = 42

func cmd(i int) []byte { return []byte(fmt.Sprintf("cmd-%d", i)) }

// ═══════════════════════════════════════════════════════════════════════
// Milestone 1 — elections

func TestM1_InitialElection(t *testing.T) {
	c := harness.NewCluster(3, seed)
	defer c.Shutdown()

	leader, term, err := c.CheckSingleLeader()
	if err != nil {
		t.Fatal(err)
	}

	// With no failures the leader and term must be stable.
	time.Sleep(1 * time.Second)
	leader2, term2, err := c.CheckSingleLeader()
	if err != nil {
		t.Fatal(err)
	}
	if leader2 != leader || term2 != term {
		t.Fatalf("leadership churned with no failures: node %d term %d -> node %d term %d",
			leader, term, leader2, term2)
	}
}

func TestM1_LeaderDisconnect(t *testing.T) {
	c := harness.NewCluster(3, seed)
	defer c.Shutdown()

	leader, term, err := c.CheckSingleLeader()
	if err != nil {
		t.Fatal(err)
	}

	c.Disconnect(leader)
	newLeader, newTerm, err := c.CheckSingleLeader()
	if err != nil {
		t.Fatal(err)
	}
	if newLeader == leader {
		t.Fatalf("disconnected node %d still counted as leader", leader)
	}
	if newTerm <= term {
		t.Fatalf("new leader's term %d not greater than old term %d", newTerm, term)
	}

	// The old leader rejoins as a has-been: it must step down, not split
	// the cluster.
	c.Reconnect(leader)
	time.Sleep(1 * time.Second)
	if _, _, err := c.CheckSingleLeader(); err != nil {
		t.Fatal(err)
	}
}

func TestM1_NoQuorumNoLeader(t *testing.T) {
	c := harness.NewCluster(3, seed)
	defer c.Shutdown()

	leader, _, err := c.CheckSingleLeader()
	if err != nil {
		t.Fatal(err)
	}

	// Disconnect the leader; a new one takes over. Disconnect that one
	// too: the single remaining follower has no majority, so it must keep
	// holding elections and keep losing. A node that elects itself here
	// would happily diverge from the rest of the cluster later.
	c.Disconnect(leader)
	leader2, _, err := c.CheckSingleLeader()
	if err != nil {
		t.Fatal(err)
	}
	c.Disconnect(leader2)

	remaining := 3 - leader - leader2 // the one node still connected
	time.Sleep(2 * time.Second)
	if err := c.CheckNoLeader(remaining); err != nil {
		t.Fatal(err)
	}

	// Restore a majority and leadership must return.
	c.Reconnect(leader)
	c.Reconnect(leader2)
	if _, _, err := c.CheckSingleLeader(); err != nil {
		t.Fatal(err)
	}
}

func TestM1_PartitionMajorityElects(t *testing.T) {
	c := harness.NewCluster(5, seed)
	defer c.Shutdown()

	leader, term, err := c.CheckSingleLeader()
	if err != nil {
		t.Fatal(err)
	}

	// Classic split: the leader plus one follower on the minority side.
	minority := []int{leader, (leader + 1) % 5}
	var majority []int
	for i := 0; i < 5; i++ {
		if i != minority[0] && i != minority[1] {
			majority = append(majority, i)
		}
	}
	c.Partition(minority, majority)

	_, newTerm, err := c.CheckSingleLeader(majority...)
	if err != nil {
		t.Fatal(err)
	}
	if newTerm <= term {
		t.Fatalf("majority-side term %d did not advance past %d", newTerm, term)
	}

	// Heal: the cluster must converge back to exactly one leader, in a
	// term at least as new as the majority side's (the deposed leader
	// steps down the moment it hears the higher term).
	c.Heal()
	time.Sleep(1 * time.Second)
	_, finalTerm, err := c.CheckSingleLeader()
	if err != nil {
		t.Fatal(err)
	}
	if finalTerm < newTerm {
		t.Fatalf("post-heal term %d went backwards from %d", finalTerm, newTerm)
	}
}

// ═══════════════════════════════════════════════════════════════════════
// Milestone 2 — replication

func TestM2_BasicAgreement(t *testing.T) {
	c := harness.NewCluster(3, seed)
	defer c.Shutdown()

	for i := 0; i < 5; i++ {
		before, _ := c.NCommitted(i + 1)
		if before > 0 {
			t.Fatalf("index %d committed before anything was submitted", i+1)
		}
		if _, err := c.One(cmd(i), 3); err != nil {
			t.Fatal(err)
		}
	}
}

func TestM2_FollowerCatchesUp(t *testing.T) {
	c := harness.NewCluster(3, seed)
	defer c.Shutdown()

	if _, err := c.One(cmd(0), 3); err != nil {
		t.Fatal(err)
	}

	leader, _, err := c.CheckSingleLeader()
	if err != nil {
		t.Fatal(err)
	}
	follower := (leader + 1) % 3
	c.Disconnect(follower)

	// The cluster keeps working with 2 of 3.
	for i := 1; i <= 3; i++ {
		if _, err := c.One(cmd(i), 2); err != nil {
			t.Fatal(err)
		}
	}

	// The rejoining follower must be brought fully up to date.
	c.Reconnect(follower)
	if _, err := c.One(cmd(4), 3); err != nil {
		t.Fatal(err)
	}
}

func TestM2_LeaderCrashClusterContinues(t *testing.T) {
	c := harness.NewCluster(3, seed)
	defer c.Shutdown()

	if _, err := c.One(cmd(0), 3); err != nil {
		t.Fatal(err)
	}
	leader, _, err := c.CheckSingleLeader()
	if err != nil {
		t.Fatal(err)
	}
	c.Crash(leader)

	if _, err := c.One(cmd(1), 2); err != nil {
		t.Fatal(err)
	}
}

func TestM2_LossyNetwork(t *testing.T) {
	c := harness.NewCluster(5, seed)
	defer c.Shutdown()

	// 20% of all messages vanish. Correct Raft grinds through on
	// retries; Raft that confuses "no reply" with "rejected" does not.
	c.Net().SetDropRate(0.20)
	for i := 0; i < 10; i++ {
		if _, err := c.One(cmd(i), 5); err != nil {
			t.Fatal(err)
		}
	}
	c.Net().SetDropRate(0)
}

func TestM2_StaleLeaderCannotCommit(t *testing.T) {
	c := harness.NewCluster(3, seed)
	defer c.Shutdown()

	leader, _, err := c.CheckSingleLeader()
	if err != nil {
		t.Fatal(err)
	}

	// Isolate the leader. It may accept a command, but with no majority
	// that command must NEVER commit.
	c.Disconnect(leader)
	time.Sleep(200 * time.Millisecond)
	staleIndex, _, wasLeader := c.Node(leader).Submit([]byte("doomed"))

	// Meanwhile the healthy majority commits real entries.
	if _, err := c.One(cmd(0), 2); err != nil {
		t.Fatal(err)
	}
	if _, err := c.One(cmd(1), 2); err != nil {
		t.Fatal(err)
	}

	if wasLeader {
		time.Sleep(1 * time.Second)
		if n, got := c.NCommitted(staleIndex); n > 0 && string(got) == "doomed" {
			t.Fatalf("SAFETY VIOLATION: entry submitted to an isolated leader was committed")
		}
	}

	// After rejoining, the stale entry must be overwritten, and the
	// cluster must still agree on everything.
	c.Reconnect(leader)
	if _, err := c.One(cmd(2), 3); err != nil {
		t.Fatal(err)
	}
}

// ═══════════════════════════════════════════════════════════════════════
// Milestone 3 — persistence

func TestM3_RestartAllRemembers(t *testing.T) {
	c := harness.NewCluster(3, seed)
	defer c.Shutdown()

	for i := 0; i < 3; i++ {
		if _, err := c.One(cmd(i), 3); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 3; i++ {
		c.Crash(i)
	}
	for i := 0; i < 3; i++ {
		c.Restart(i)
	}

	// A cluster that forgot its log (or its terms/votes) will fail here —
	// either no leader, or fresh commits colliding with forgotten ones.
	if _, err := c.One(cmd(99), 3); err != nil {
		t.Fatal(err)
	}
}

func TestM3_CrashedLeaderRejoins(t *testing.T) {
	c := harness.NewCluster(3, seed)
	defer c.Shutdown()

	if _, err := c.One(cmd(0), 3); err != nil {
		t.Fatal(err)
	}
	leader, _, err := c.CheckSingleLeader()
	if err != nil {
		t.Fatal(err)
	}

	c.Crash(leader)
	for i := 1; i <= 3; i++ {
		if _, err := c.One(cmd(i), 2); err != nil {
			t.Fatal(err)
		}
	}

	c.Restart(leader)
	if _, err := c.One(cmd(4), 3); err != nil {
		t.Fatal(err)
	}
}

func TestM3_RepeatedCrashCycles(t *testing.T) {
	if testing.Short() {
		t.Skip("long test")
	}
	c := harness.NewCluster(5, seed)
	defer c.Shutdown()

	// Rolling crashes: every round, commit with one node down, restart
	// it, crash the next. Persistent state must survive every cycle.
	for round := 0; round < 5; round++ {
		victim := round % 5
		c.Crash(victim)
		if _, err := c.One(cmd(round), 4); err != nil {
			t.Fatalf("round %d: %v", round, err)
		}
		c.Restart(victim)
		if _, err := c.One(cmd(100+round), 5); err != nil {
			t.Fatalf("round %d after restart: %v", round, err)
		}
	}
}
