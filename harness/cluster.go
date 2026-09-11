// Package harness spins up multi-node clusters in one process and breaks
// them on purpose: crashes, restarts, partitions, lossy links. The
// milestone tests are built on it. It is fully implemented — read it to
// see exactly how your Raft gets exercised, and reuse it for your own
// experiments.
package harness

import (
	"bytes"
	"fmt"
	"sync"
	"time"

	"github.com/guy5116/raft-kv/kv"
	"github.com/guy5116/raft-kv/raft"
	"github.com/guy5116/raft-kv/storage"
	"github.com/guy5116/raft-kv/transport"
)

// Cluster is an in-process Raft (optionally Raft+KV) cluster.
type Cluster struct {
	mu           sync.Mutex
	n            int
	net          *transport.InMemNetwork
	kvOn         bool
	maxRaftState int

	nodes     []*raft.Raft
	storages  []*storage.MemoryStorage // survives Crash/Restart, like a disk
	applyChs  []chan raft.ApplyMsg
	servers   []*kv.Server // nil entries when kvOn == false
	alive     []bool
	connected []bool

	// committed[index][node] records what each raft-only node applied at
	// each log index, so the harness can detect the cardinal sin: two
	// nodes committing different commands at the same index.
	committed map[int]map[int][]byte
}

// NewCluster starts n Raft nodes (no KV layer) on a chaos-capable
// in-memory network. Use the seed to reproduce failing runs.
func NewCluster(n int, seed int64) *Cluster {
	return newCluster(n, seed, false, -1)
}

// NewKVCluster starts n full KV servers. maxRaftState is the snapshot
// threshold in bytes; -1 disables snapshotting (use -1 until M4).
func NewKVCluster(n int, seed int64, maxRaftState int) *Cluster {
	return newCluster(n, seed, true, maxRaftState)
}

func newCluster(n int, seed int64, kvOn bool, maxRaftState int) *Cluster {
	c := &Cluster{
		n:            n,
		net:          transport.NewInMemNetwork(seed),
		kvOn:         kvOn,
		maxRaftState: maxRaftState,
		nodes:        make([]*raft.Raft, n),
		storages:     make([]*storage.MemoryStorage, n),
		applyChs:     make([]chan raft.ApplyMsg, n),
		servers:      make([]*kv.Server, n),
		alive:        make([]bool, n),
		connected:    make([]bool, n),
		committed:    make(map[int]map[int][]byte),
	}
	for i := 0; i < n; i++ {
		c.storages[i] = storage.NewMemoryStorage()
		c.startNode(i)
	}
	return c
}

func peerIDs(n int) []int {
	ids := make([]int, n)
	for i := range ids {
		ids[i] = i
	}
	return ids
}

// startNode boots node i against whatever its storage already holds.
func (c *Cluster) startNode(i int) {
	c.mu.Lock()
	ch := make(chan raft.ApplyMsg, 64)
	c.applyChs[i] = ch
	node := raft.NewNode(i, peerIDs(c.n), c.net.NodeTransport(i), c.storages[i], ch)
	c.nodes[i] = node
	if c.kvOn {
		c.servers[i] = kv.NewServer(i, node, ch, c.maxRaftState)
	} else {
		go c.collect(i, node, ch)
	}
	c.alive[i] = true
	c.connected[i] = true
	c.mu.Unlock()

	c.net.Register(i, node)
	node.Start()
}

// collect drains a raft-only node's applyCh, recording commits and
// panicking loudly if two nodes ever disagree about an index — that is a
// consensus-safety violation no test should tolerate.
func (c *Cluster) collect(id int, node *raft.Raft, ch chan raft.ApplyMsg) {
	for msg := range ch {
		if !msg.CommandValid {
			continue
		}
		c.mu.Lock()
		byNode := c.committed[msg.CommandIndex]
		if byNode == nil {
			byNode = make(map[int][]byte)
			c.committed[msg.CommandIndex] = byNode
		}
		for other, cmd := range byNode {
			if !bytes.Equal(cmd, msg.Command) {
				c.mu.Unlock()
				panic(fmt.Sprintf(
					"CONSISTENCY VIOLATION at index %d: node %d applied %q, node %d applied %q",
					msg.CommandIndex, other, cmd, id, msg.Command))
			}
		}
		byNode[id] = msg.Command
		stale := c.nodes[id] != node
		c.mu.Unlock()
		if stale {
			return // a restarted incarnation owns this slot now
		}
	}
}

// ---------------------------------------------------------------------------
// Chaos controls.

// Crash kills node i (process death: its goroutines stop, its network
// registration disappears, but its "disk" — MemoryStorage — survives).
func (c *Cluster) Crash(i int) {
	c.mu.Lock()
	node := c.nodes[i]
	c.alive[i] = false
	c.connected[i] = false
	c.mu.Unlock()
	c.net.Deregister(i)
	if node != nil {
		node.Kill()
	}
}

// Restart brings a crashed node back up from its persisted state.
func (c *Cluster) Restart(i int) {
	c.mu.Lock()
	alive := c.alive[i]
	c.mu.Unlock()
	if alive {
		c.Crash(i)
	}
	c.startNode(i)
}

// Disconnect unplugs node i's network cable (it keeps running).
func (c *Cluster) Disconnect(i int) {
	c.mu.Lock()
	c.connected[i] = false
	c.mu.Unlock()
	c.net.Disconnect(i)
}

// Reconnect plugs it back in.
func (c *Cluster) Reconnect(i int) {
	c.mu.Lock()
	c.connected[i] = true
	c.mu.Unlock()
	c.net.Reconnect(i)
}

// Partition splits the network into groups that cannot reach each other.
func (c *Cluster) Partition(groups ...[]int) { c.net.SetPartition(groups...) }

// Heal removes all partitions.
func (c *Cluster) Heal() { c.net.HealPartition() }

// Net exposes the network for drop-rate / delay tweaking.
func (c *Cluster) Net() *transport.InMemNetwork { return c.net }

// ---------------------------------------------------------------------------
// Inspection helpers.

func (c *Cluster) N() int                { return c.n }
func (c *Cluster) Node(i int) *raft.Raft { c.mu.Lock(); defer c.mu.Unlock(); return c.nodes[i] }
func (c *Cluster) Server(i int) *kv.Server {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.servers[i]
}

func (c *Cluster) upIDs() []int {
	c.mu.Lock()
	defer c.mu.Unlock()
	var ids []int
	for i := 0; i < c.n; i++ {
		if c.alive[i] && c.connected[i] {
			ids = append(ids, i)
		}
	}
	return ids
}

// CheckSingleLeader waits until exactly one of the given nodes (default:
// all alive, connected nodes) claims leadership, and returns its ID and
// term. Two leaders in the SAME term is an immediate, fatal error; a
// deposed leader in an old term is normal life in Raft and is ignored.
func (c *Cluster) CheckSingleLeader(among ...int) (leader int, term int, err error) {
	if len(among) == 0 {
		among = c.upIDs()
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		leadersByTerm := make(map[int][]int)
		for _, id := range among {
			node := c.Node(id)
			if node == nil {
				continue
			}
			if t, isLeader := node.GetState(); isLeader {
				leadersByTerm[t] = append(leadersByTerm[t], id)
			}
		}
		maxTerm := -1
		for t, ids := range leadersByTerm {
			if len(ids) > 1 {
				return -1, -1, fmt.Errorf("SAFETY VIOLATION: term %d has %d leaders: %v", t, len(ids), ids)
			}
			if t > maxTerm {
				maxTerm = t
			}
		}
		if maxTerm >= 0 {
			return leadersByTerm[maxTerm][0], maxTerm, nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return -1, -1, fmt.Errorf("no leader elected among %v within 5s", among)
}

// CheckNoLeader verifies that none of the given nodes currently claims to
// be leader (e.g. a minority partition must not elect anyone).
func (c *Cluster) CheckNoLeader(among ...int) error {
	if len(among) == 0 {
		among = c.upIDs()
	}
	for _, id := range among {
		node := c.Node(id)
		if node == nil {
			continue
		}
		if t, isLeader := node.GetState(); isLeader {
			return fmt.Errorf("node %d unexpectedly claims leadership in term %d", id, t)
		}
	}
	return nil
}

// NCommitted reports how many raft-only nodes have applied an entry at
// the given index, and the command there.
func (c *Cluster) NCommitted(index int) (int, []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	byNode := c.committed[index]
	var cmd []byte
	for _, v := range byNode {
		cmd = v
		break
	}
	return len(byNode), cmd
}

// One submits cmd through whichever node turns out to be leader and waits
// until at least expectedServers nodes have applied it at the same index.
// It retries around leader changes for up to 10 seconds (modeled on the
// MIT 6.5840 harness's cfg.one). Raft-only clusters.
func (c *Cluster) One(cmd []byte, expectedServers int) (int, error) {
	if c.kvOn {
		panic("harness.One is for raft-only clusters; use a Clerk with KV clusters")
	}
	overall := time.Now()
	for time.Since(overall) < 10*time.Second {
		// Hunt for the leader by just asking everyone.
		index := -1
		for _, id := range c.upIDs() {
			node := c.Node(id)
			if node == nil {
				continue
			}
			if idx, _, isLeader := node.Submit(cmd); isLeader {
				index = idx
				break
			}
		}
		if index == -1 {
			time.Sleep(50 * time.Millisecond)
			continue
		}
		// A leader accepted it — but it only counts once a majority
		// applies it. Leadership may be lost before that; then we retry.
		attempt := time.Now()
		for time.Since(attempt) < 2*time.Second {
			n, got := c.NCommitted(index)
			if n >= expectedServers && bytes.Equal(got, cmd) {
				return index, nil
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
	return -1, fmt.Errorf("One(%q): no agreement after 10s", cmd)
}

// RaftStateSize reports how many bytes of persisted Raft state node i is
// holding — the M4 tests use it to prove log compaction actually bounds
// state growth.
func (c *Cluster) RaftStateSize(i int) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.storages[i].StateSize()
}

// Clerk returns a KV client talking to every server in the cluster.
func (c *Cluster) Clerk() *kv.Clerk {
	c.mu.Lock()
	defer c.mu.Unlock()
	servers := make([]*kv.Server, len(c.servers))
	copy(servers, c.servers)
	return kv.NewClerk(servers)
}

// Shutdown stops every node.
func (c *Cluster) Shutdown() {
	for i := 0; i < c.n; i++ {
		c.mu.Lock()
		alive := c.alive[i]
		c.mu.Unlock()
		if alive {
			c.Crash(i)
		}
	}
}
