package transport

import (
	"bytes"
	"encoding/gob"
	"math/rand"
	"reflect"
	"sync"
	"time"
)

// InMemNetwork is a simulated network connecting all nodes in a test
// cluster. It is deliberately hostile: it can drop messages, delay them,
// disconnect individual nodes, and partition the cluster into groups that
// cannot reach each other.
//
// Messages are gob-encoded and decoded on delivery, exactly like a real
// network would serialize them. This is not busywork — it guarantees the
// caller and callee share no memory, so a handler mutating its args can
// never silently corrupt the caller's state. (Real-network bugs, simulated
// faithfully.)
//
// All methods are safe for concurrent use.
type InMemNetwork struct {
	mu        sync.Mutex
	handlers  map[int]Handler
	connected map[int]bool
	group     map[int]int // partition group per node; nodes talk iff same group
	dropRate  float64     // probability [0,1) that any message is lost
	minDelay  time.Duration
	maxDelay  time.Duration
	rng       *rand.Rand
	rpcCount  int64
}

// NewInMemNetwork creates an empty network with mild default delays
// (0–10ms) and no message loss. Seed the RNG explicitly so failures can be
// reproduced.
func NewInMemNetwork(seed int64) *InMemNetwork {
	return &InMemNetwork{
		handlers:  make(map[int]Handler),
		connected: make(map[int]bool),
		group:     make(map[int]int),
		minDelay:  0,
		maxDelay:  10 * time.Millisecond,
		rng:       rand.New(rand.NewSource(seed)),
	}
}

// Register attaches a node's RPC handler to the network and connects it.
func (n *InMemNetwork) Register(id int, h Handler) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.handlers[id] = h
	n.connected[id] = true
	n.group[id] = 0
}

// Deregister removes a node entirely (simulates a crashed process whose
// handler must never run again). The harness uses this when killing nodes.
func (n *InMemNetwork) Deregister(id int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	delete(n.handlers, id)
	n.connected[id] = false
}

// Disconnect unplugs a node's network cable: in-flight and future messages
// to and from it are lost, but the node keeps running. Reconnect plugs it
// back in.
func (n *InMemNetwork) Disconnect(id int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.connected[id] = false
}

// Reconnect restores a previously disconnected node.
func (n *InMemNetwork) Reconnect(id int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if _, ok := n.handlers[id]; ok {
		n.connected[id] = true
	}
}

// SetPartition splits the cluster into groups; nodes in different groups
// cannot exchange messages. Nodes not mentioned keep their current group.
// Example: SetPartition([]int{0,1}, []int{2,3,4}) isolates {0,1} from
// {2,3,4} — the classic minority/majority split.
func (n *InMemNetwork) SetPartition(groups ...[]int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	for g, ids := range groups {
		for _, id := range ids {
			n.group[id] = g + 1 // group 0 is reserved for "healed"
		}
	}
}

// HealPartition puts every node back in one group.
func (n *InMemNetwork) HealPartition() {
	n.mu.Lock()
	defer n.mu.Unlock()
	for id := range n.group {
		n.group[id] = 0
	}
}

// SetDropRate makes the network lossy: each message is independently lost
// with probability p (applied separately to request and reply, like real
// packet loss).
func (n *InMemNetwork) SetDropRate(p float64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.dropRate = p
}

// SetDelay sets the per-message artificial latency range.
func (n *InMemNetwork) SetDelay(min, max time.Duration) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.minDelay, n.maxDelay = min, max
}

// RPCCount returns how many RPC attempts have been made in total. Tests
// can use it to catch grossly chatty implementations.
func (n *InMemNetwork) RPCCount() int64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.rpcCount
}

// NodeTransport returns the Transport a specific node should use, so each
// outgoing call knows its source (needed for partition checks).
func (n *InMemNetwork) NodeTransport(id int) Transport {
	return &nodeTransport{net: n, src: id}
}

type nodeTransport struct {
	net *InMemNetwork
	src int
}

func (t *nodeTransport) Call(peer int, method string, args any, reply any) bool {
	return t.net.call(t.src, peer, method, args, reply)
}

func (n *InMemNetwork) call(src, dst int, method string, args any, reply any) bool {
	n.mu.Lock()
	n.rpcCount++
	handler, ok := n.handlers[dst]
	deliverable := ok && n.linkUpLocked(src, dst) && n.rng.Float64() >= n.dropRate
	delay := n.randDelayLocked()
	n.mu.Unlock()

	// Simulate network latency outside the lock so calls overlap.
	if delay > 0 {
		time.Sleep(delay)
	}
	if !deliverable {
		return false
	}

	// Serialize args across the "wire" so the handler gets its own copy.
	argsCopy, err := gobRoundTrip(args)
	if err != nil {
		return false
	}
	replyCopy := reflect.New(reflect.TypeOf(reply).Elem()).Interface()
	if !handler.HandleRPC(method, argsCopy, replyCopy) {
		return false
	}

	// The reply also crosses the network: it can be dropped or delayed,
	// and the link must still be up when it arrives.
	n.mu.Lock()
	replyDeliverable := n.linkUpLocked(src, dst) && n.rng.Float64() >= n.dropRate
	delay = n.randDelayLocked()
	n.mu.Unlock()
	if delay > 0 {
		time.Sleep(delay)
	}
	if !replyDeliverable {
		return false
	}
	return copyInto(replyCopy, reply)
}

func (n *InMemNetwork) linkUpLocked(a, b int) bool {
	return n.connected[a] && n.connected[b] && n.group[a] == n.group[b]
}

func (n *InMemNetwork) randDelayLocked() time.Duration {
	if n.maxDelay <= n.minDelay {
		return n.minDelay
	}
	return n.minDelay + time.Duration(n.rng.Int63n(int64(n.maxDelay-n.minDelay)))
}

// gobRoundTrip returns a deep copy of v (which must be a pointer to a
// struct) by encoding and decoding it, like sending it over a network.
func gobRoundTrip(v any) (any, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(v); err != nil {
		return nil, err
	}
	out := reflect.New(reflect.TypeOf(v).Elem()).Interface()
	if err := gob.NewDecoder(&buf).Decode(out); err != nil {
		return nil, err
	}
	return out, nil
}

// copyInto gob-copies src (pointer) into dst (pointer of the same type).
func copyInto(src, dst any) bool {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(src); err != nil {
		return false
	}
	return gob.NewDecoder(&buf).Decode(dst) == nil
}
