// Package transport abstracts how Raft nodes talk to each other, so the
// same Raft code can run over an in-memory network in tests (with failure
// injection) or over a real network later (a TCP/gRPC transport is a
// stretch-goal milestone).
package transport

// Transport is what a single node uses to reach its peers.
//
// Call performs a synchronous RPC to peer `id`. It returns false if the
// call failed to complete for any reason (peer down, network partition,
// message dropped, encoding error). Raft must treat a false return exactly
// like a lost packet: no reply, no error detail — because that is all a
// real network gives you.
type Transport interface {
	// Call invokes `method` on the given peer. args is encoded, sent,
	// and decoded into reply. Safe for concurrent use.
	Call(peer int, method string, args any, reply any) bool
}

// Handler is implemented by anything that can serve RPCs (a Raft node).
// The transport delivers incoming calls here.
//
// HandleRPC must decode nothing — args/reply are already concrete structs.
// It returns false if the method is unknown.
type Handler interface {
	HandleRPC(method string, args any, reply any) bool
}
