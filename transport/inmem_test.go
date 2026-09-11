package transport

// Plumbing self-tests — these pass before you write any Raft code
// (`make test-harness`). If they ever fail, the bug is in the harness,
// not in your Raft.

import (
	"testing"
	"time"
)

type EchoArgs struct{ Msg string }
type EchoReply struct{ Msg string }

type echoServer struct{ id int }

func (e *echoServer) HandleRPC(method string, args any, reply any) bool {
	if method != "Echo.Echo" {
		return false
	}
	reply.(*EchoReply).Msg = args.(*EchoArgs).Msg
	return true
}

func newPair(t *testing.T) (*InMemNetwork, Transport) {
	t.Helper()
	n := NewInMemNetwork(1)
	n.SetDelay(0, time.Millisecond)
	n.Register(0, &echoServer{0})
	n.Register(1, &echoServer{1})
	return n, n.NodeTransport(0)
}

func TestBasicCall(t *testing.T) {
	_, tr := newPair(t)
	var reply EchoReply
	if !tr.Call(1, "Echo.Echo", &EchoArgs{Msg: "hi"}, &reply) {
		t.Fatal("call failed on healthy network")
	}
	if reply.Msg != "hi" {
		t.Fatalf("reply = %q, want hi", reply.Msg)
	}
}

func TestUnknownMethodFails(t *testing.T) {
	_, tr := newPair(t)
	var reply EchoReply
	if tr.Call(1, "Echo.Nope", &EchoArgs{}, &reply) {
		t.Fatal("unknown method should fail")
	}
}

func TestDisconnect(t *testing.T) {
	n, tr := newPair(t)
	n.Disconnect(1)
	var reply EchoReply
	if tr.Call(1, "Echo.Echo", &EchoArgs{Msg: "hi"}, &reply) {
		t.Fatal("call to disconnected node should fail")
	}
	n.Reconnect(1)
	if !tr.Call(1, "Echo.Echo", &EchoArgs{Msg: "hi"}, &reply) {
		t.Fatal("call after reconnect should succeed")
	}
}

func TestPartition(t *testing.T) {
	n, tr := newPair(t)
	n.SetPartition([]int{0}, []int{1})
	var reply EchoReply
	if tr.Call(1, "Echo.Echo", &EchoArgs{Msg: "hi"}, &reply) {
		t.Fatal("call across partition should fail")
	}
	n.HealPartition()
	if !tr.Call(1, "Echo.Echo", &EchoArgs{Msg: "hi"}, &reply) {
		t.Fatal("call after heal should succeed")
	}
}

func TestDropRate(t *testing.T) {
	n, tr := newPair(t)
	n.SetDelay(0, 0)
	n.SetDropRate(1.0)
	var reply EchoReply
	if tr.Call(1, "Echo.Echo", &EchoArgs{Msg: "hi"}, &reply) {
		t.Fatal("call should fail with 100% drop rate")
	}
	n.SetDropRate(0)
	ok := 0
	for i := 0; i < 20; i++ {
		if tr.Call(1, "Echo.Echo", &EchoArgs{Msg: "hi"}, &reply) {
			ok++
		}
	}
	if ok != 20 {
		t.Fatalf("only %d/20 calls succeeded with 0%% drop rate", ok)
	}
}

func TestArgsAreCopied(t *testing.T) {
	n := NewInMemNetwork(1)
	n.SetDelay(0, 0)
	mut := &mutatingServer{}
	n.Register(0, &echoServer{0})
	n.Register(1, mut)
	tr := n.NodeTransport(0)

	args := EchoArgs{Msg: "original"}
	var reply EchoReply
	if !tr.Call(1, "Echo.Echo", &args, &reply) {
		t.Fatal("call failed")
	}
	if args.Msg != "original" {
		t.Fatalf("handler mutation leaked back into caller's args: %q", args.Msg)
	}
}

type mutatingServer struct{}

func (m *mutatingServer) HandleRPC(method string, args any, reply any) bool {
	a := args.(*EchoArgs)
	reply.(*EchoReply).Msg = a.Msg
	a.Msg = "MUTATED" // must not be visible to the caller
	return true
}
