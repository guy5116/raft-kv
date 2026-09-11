# raft-kv

A distributed, fault-tolerant key-value store built on the [Raft consensus
algorithm](https://raft.github.io/raft.pdf), implemented from scratch in Go.

The same architecture that sits at the core of etcd (and therefore
Kubernetes), Consul, and CockroachDB: a replicated log driven by Raft, with
a key-value state machine on top. Survives node crashes, network
partitions, and lossy links while guaranteeing linearizable operations —
no committed write is ever lost, and no two nodes ever disagree about the
log.

```
   clients ──► Clerk (leader discovery, retries, exactly-once)
                  │
        ┌─────────┼─────────┐
        ▼         ▼         ▼
   ┌────────┐ ┌────────┐ ┌────────┐
   │ kv.Server │ kv.Server │ kv.Server │   state machines (map + dedup)
   ├────────┤ ├────────┤ ├────────┤
   │  Raft   │◄►  Raft   ◄►│  Raft   │   consensus: elections, log
   ├────────┤ ├────────┤ ├────────┤     replication, snapshots
   │ Storage │ │ Storage │ │ Storage │   persistence (atomic, fsync'd)
   └────────┘ └────────┘ └────────┘
        ▲          ▲          ▲
        └── chaos-capable transport: partitions, drops, delays ──┘
```

## Status

- [ ] **Milestone 1 — Leader election** (`make test-m1`): randomized
      election timeouts, RequestVote, terms, step-down on higher term
- [ ] **Milestone 2 — Log replication** (`make test-m2`): AppendEntries
      consistency checking, commit rules (§5.4.2!), the KV state machine,
      and exactly-once client semantics over a lossy network
- [ ] **Milestone 3 — Persistence** (`make test-m3`): crash-and-restart
      recovery of term/vote/log via atomic, fsync'd writes
- [ ] **Milestone 4 — Snapshots** (`make test-m4`): log compaction and
      InstallSnapshot for lagging followers
- [ ] Stretch: TCP/gRPC transport (real multi-process cluster), fast
      log backup (conflict-term hints), read leases / ReadIndex,
      Jepsen-style linearizability checker, membership changes (§6)

## Layout

| Path         | What                                                        | Provided?              |
| ------------ | ----------------------------------------------------------- | ---------------------- |
| `raft/`      | The consensus algorithm                                     | **The project** — skeleton + guided TODOs |
| `kv/`        | Replicated KV state machine, client library                 | Skeleton + guided TODOs |
| `transport/` | Pluggable RPC; in-memory net with failure injection          | Provided               |
| `storage/`   | Stable storage: in-memory (tests) & atomic files (real runs) | Provided               |
| `harness/`   | In-process chaos cluster: crash, restart, partition, drop    | Provided               |
| `cmd/raftkv` | Interactive demo cluster REPL                                | Provided               |

Work milestone by milestone; every TODO in `raft/` and `kv/` is written as
a numbered translation of the paper's Figure 2. Start in
`raft/election.go`.

## Running

```sh
make test-harness   # plumbing self-tests — green from day one
make test-m1        # your scoreboard for milestone 1
RAFT_DEBUG=1 go test -race ./raft/ -run TestM1_LeaderDisconnect -v 2>debug.log
go run ./cmd/raftkv # interactive cluster (after milestone 2)
```

All tests run under the race detector. Before calling a milestone done,
run it repeatedly — Raft bugs are timing-dependent and love the run you
didn't do:

```sh
go test -race -count 10 ./raft/ -run 'TestM1'
```

## Reading list

- [The Raft paper](https://raft.github.io/raft.pdf) — keep Figure 2 open
  at all times; this codebase is a direct translation of it
- [The Secret Lives of Data](https://thesecretlivesofdata.com/raft/) —
  animated Raft, great for intuition
- [Raft visualization](https://raft.github.io/) — interactive elections
- [MIT 6.5840 lectures](https://pdos.csail.mit.edu/6.824/) — the course
  whose labs inspired this project's shape
- [Students' Guide to Raft](https://thesquareplanet.com/blog/students-guide-to-raft/)
  — a catalog of the exact bugs you are about to write

## Design notes worth knowing (and mentioning in interviews)

**Why reads go through the log.** A deposed leader that hasn't noticed
yet would serve stale reads from memory. Committing reads makes them
linearizable at the cost of a round of consensus per read; production
systems use ReadIndex or leader leases to get both. (Stretch goal.)

**Why clients stamp requests with (ClientID, Seq).** Clients must retry
on timeout, but "timeout" doesn't mean "didn't happen" — the op may have
committed just before the leader died. Servers keep a per-client
highest-applied Seq and skip duplicates, turning at-least-once delivery
into exactly-once application.

**Why a leader only commits entries from its own term** (§5.4.2). Counting
replicas of an old-term entry can commit data that a future leader is
allowed to erase — Figure 8 of the paper is the counterexample. Old
entries commit indirectly when the first current-term entry commits.

**Why `votedFor` must hit disk before replying.** Forget a vote, restart,
vote again in the same term → two leaders in one term → divergence. The
fsync'd atomic write in `storage/` is what makes the promise real.
