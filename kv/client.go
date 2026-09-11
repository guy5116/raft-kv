package kv

import (
	"crypto/rand"
	"math/big"
	"time"
)

// Clerk is the client library: it hides leader discovery and retries so
// callers get a plain Get/Put/Append API with exactly-once semantics.
//
// In this project the Clerk holds direct references to the servers
// (in-process cluster). Swapping this for network calls is part of the
// cmd/raftkv stretch goal; the retry logic stays identical.
type Clerk struct {
	servers  []*Server
	clientID int64
	seq      int64
	leader   int // index of the last server that answered as leader
}

func NewClerk(servers []*Server) *Clerk {
	return &Clerk{servers: servers, clientID: nrand()}
}

// Get / Put / Append retry until the cluster answers. They NEVER give up:
// during an election there is simply no leader for a moment, and the only
// correct client behavior is to keep trying (this is the availability "A"
// CAP trades away under partition — good README material).
//
// TODO(you) — milestone 2, one small loop:
//  1. Bump c.seq (each operation gets a fresh Seq; reuse it across
//     retries of the SAME operation — that's what makes retries safe).
//  2. Loop forever: try c.servers[c.leader]; on ok return; otherwise
//     advance c.leader round-robin, and after a full lap sleep ~50ms so a
//     leaderless cluster isn't hammered.
func (c *Clerk) Get(key string) string {
	// TODO(you): implement (milestone 2).
	return ""
}

func (c *Clerk) Put(key, value string) {
	// TODO(you): implement (milestone 2).
}

func (c *Clerk) Append(key, value string) {
	// TODO(you): implement (milestone 2).
}

// retryPause is the sleep between full laps of the cluster.
const retryPause = 50 * time.Millisecond

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	x, _ := rand.Int(rand.Reader, max)
	return x.Int64()
}
