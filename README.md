# raftkv — Raft Consensus & a Fault-Tolerant Key-Value Store in Go

![Language](https://img.shields.io/badge/language-Go-00ADD8?style=flat-square)
![Status](https://img.shields.io/badge/status-work%20in%20progress%20(week%203%20of%208)-yellow?style=flat-square)
![Dependencies](https://img.shields.io/badge/dependencies-standard%20library%20only-brightgreen?style=flat-square)

raftkv is my implementation of the Raft consensus algorithm in Go, built from the original paper, together with a simulated unreliable network to test it on. The end goal is a linearizable key-value service replicated across a cluster that keeps working while nodes crash, messages are lost, and the network partitions.

It's the follow-up to [clox](https://github.com/Spiky2123/CLox). clox taught me how a language executes; raftkv is about what happens when the machines running a system can fail independently. I'm building it as an 8-week project, test first, and committing in small steps. Each milestone is tagged (`week1-network`, `week2-election`, ...), and [`NOTES.md`](NOTES.md) is my running journal of the design decisions and bugs along the way.

> **Status:** the simulated network and leader election are done and stress-tested. Log replication is in progress. Everything after that is planned and listed in the [roadmap](#roadmap), and nothing there is claimed as built until it's tagged.

## Highlights

- **Leader election that holds up under failure**: randomized election timeouts, term-based step-down, one vote per term, and heartbeats. A randomized test with 5 nodes, 20% message loss, delays, and repeated leader isolation checks that no term ever has two leaders
- **A simulated network built for fault injection**: message drops, delays, partitions, and per-node isolation, driven by a seeded random number generator
- **Raft knows nothing about the network**: the consensus code depends only on a small `Transport` interface, so a real TCP/gRPC transport could replace the simulation without touching Raft
- **No shared memory between nodes**: every message is gob-encoded and deep-copied in transit, so a node can't accidentally read another node's state through a shared pointer
- **A strict locking discipline**: one mutex per node, never held while sending an RPC or sleeping, checked with Go's race detector
- **Tested against the hard interleavings**: randomized chaos tests plus deterministic tests that force rare orderings with a fake transport, instead of hoping a timing-based test stumbles into them

## Roadmap

| Week | Milestone | Status | Tag |
|------|-----------|--------|-----|
| 1 | Simulated network: drops, delays, partitions | Done | `week1-network` |
| 2 | Leader election | Done | `week2-election` |
| 3 | Log replication: `AppendEntries` consistency check, conflict resolution, commit index, apply channel, "candidate's log must be up to date" voting rule | In progress | |
| 4 | Persistence and crash recovery, fast log-conflict backup, Figure 8 tests | Planned | |
| 5 | Key-value service: Get/Put, client that finds the leader and retries, request deduplication, reads through the log for linearizability | Planned | |
| 6 | Snapshots, log truncation, `InstallSnapshot` | Planned | |
| 7 | Chaos testing with seeded failure schedules, recorded client histories checked with [Porcupine](https://github.com/anishathalye/porcupine) | Planned | |
| 8 | Benchmarks (throughput and p99 latency at 3 vs 5 nodes, leader-failover window), write-up, demo | Planned | |

Target completion: late November 2026.

## What works today

**Simulated network (`labnet`)**
- Nodes register a handler and call each other through `Call(from, to, method, args, reply)`
- The network can drop messages, add delay, partition groups of nodes, isolate a single node, and heal everything again
- A call returns `false` if the target has no handler, is unreachable, or the message was dropped. The caller can't tell which of those happened, just like on a real network
- Reachability is re-checked after the request delay, so a partition created while a message is in flight still takes effect
- Handlers run on a deep copy of the arguments, and the result is copied back into the caller's reply

**Leader election (`raft`)**
- Follower, candidate, and leader states with terms acting as a logical clock
- A node that sees a higher term in any message or reply immediately adopts it and steps down to follower
- One vote per term: a node grants its vote only if it hasn't voted in that term or is voting again for the same candidate
- Randomized election timeouts to break ties between candidates
- Leaders send empty `AppendEntries` heartbeats every 50 ms; a candidate that hears from a leader of its own term steps back to follower
- Stale replies are ignored: a candidate counts a vote only if it is still a candidate in the term it started the election in
- `Kill` shuts a node down cleanly: all of its goroutines exit, and it ignores any later RPC

## Testing

The `raft` package has 20+ tests, all run under the race detector:

- **Rule-level unit tests**: the vote and heartbeat handlers are called directly with hand-built arguments, covering rejecting a lower term, adopting a higher term, one vote per term, and when the election timer is reset
- **Cluster tests**: 3-node and 5-node clusters elect exactly one leader, still elect one with a node down, and re-elect after the leader is disconnected
- **Deterministic interleaving tests**: a fake `Transport` that always replies with a higher term forces a leader to step down at an exact moment, which a timing-based stress test would hit only occasionally
- **Chaos test**: 5 nodes with 20% message loss, random delays, and the leader repeatedly isolated; the check is Raft's *Election Safety* property, that at most one leader exists per term
- **Lifecycle tests**: a killed node stops all its goroutines and stays dead

## Example

The cluster test helper builds a cluster on the simulated network. This is a simplified version of what the election tests do:

```go
c := makeCluster(t, 5)          // 5 nodes on a simulated network, one seeded RNG each
c.start()

leader := c.checkOneLeader(t)   // wait until exactly one leader exists for a term
c.disconnect(leader)            // cut the leader off from everyone else
c.checkOneLeader(t)             // the remaining nodes elect a new leader
c.reconnect(leader)             // the old leader rejoins, sees the higher term, steps down
```

## Getting started

```bash
git clone https://github.com/Spiky2123/raftkv.git
cd raftkv

make test                                           # run all tests
make race                                           # run all tests with the race detector
make stress ARGS="-run TestReElection ./raft/"      # repeat one test 100 times under -race
```

Requires Go (version in `go.mod`). No other dependencies.

## Design & architecture

```text
   ┌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┐
   ╎ Client  (planned)                        ╎
   ╎ finds the leader, retries, dedups        ╎
   └╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┘
                         ╎
   ┌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┐
   ╎ KV service  (planned)                    ╎
   ╎ Get / Put on top of Raft                 ╎
   └╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┘
                         ╎
   ┌──────────────────────────────────────────┐
   │ raft.Raft                                │
   │   leader election: done                  │
   │   log replication: in progress           │
   └─────────────────────┬────────────────────┘
                         │  Transport interface
   ┌─────────────────────┴────────────────────┐
   │ labnet.Endpoint                          │
   │   per-node handle on the network         │
   └─────────────────────┬────────────────────┘
                         │
   ┌─────────────────────┴────────────────────┐
   │ labnet.Network                           │
   │   drops, delays, partitions, seeded RNG  │
   └──────────────────────────────────────────┘
```

Dashed boxes are planned layers.

**Layering.** `raft.Raft` talks to the outside world only through the `Transport` interface (`Call(to, method, args, reply) bool`). `labnet.Endpoint` implements it on top of the simulated network. Raft replicates opaque commands and will know nothing about key-value types, so the KV service in week 5 sits on top of it without Raft changing.

**Messages.** RPC arguments and replies are plain structs with exported fields, registered with `gob` in one place (`raft/types.go`). The network gob-encodes each message and decodes it into a fresh value, which is a deliberate way of preventing shared memory between nodes.

**Concurrency.** Each node protects all of its state with a single mutex that is never held while sending an RPC or sleeping. Helpers that need the lock are named `...Locked` and say so in a comment. The long-lived goroutines are a ticker (every 10 ms it checks the election deadline), one `leaderLoop` per leadership term, and one short-lived goroutine per outgoing RPC. When a reply comes back, the node first checks for a higher term, then checks that the term and role are still the ones the request was sent in, before acting on it.

**Elections.** A candidate increments its term, votes for itself, and asks every peer for a vote in parallel. Votes are counted in a variable local to that election, only while holding the lock, and a majority (`>=`) makes it leader. The `leaderLoop` for a term exits as soon as the node is no longer the leader of that term, so goroutines from an old leadership can't act in a new one.

**Determinism.** The network and every node use their own seeded `*rand.Rand`; the global `rand` is never used, and each node's generator is touched only while holding its lock. Seeds make a failing run much easier to reproduce, but Go's goroutine scheduling isn't deterministic, so they don't make a run perfectly replayable. Fully replayable failure schedules are part of week 7.

**Planned: the log.** Log entries will live behind their own wrapper type (`LastIndex`, `TermAt`, `Slice`, `TruncateFrom`) so nothing else indexes the raw slice. That keeps the week-6 snapshot work, which shifts indices by an offset, from touching the rest of the code.

Code comments cite the paper's rules (for example, `// Figure 2: AppendEntries receiver, step 3`), so each branch can be checked against the specification.

## Repository layout

```
raftkv/
├── raft/        # the Raft implementation and its tests
├── labnet/      # simulated network: drops, delays, partitions
├── Makefile     # test, race, stress, bench
├── NOTES.md     # design journal, week by week
└── go.mod
```

## Tech stack

- Go
- Standard library only so far: `sync`, `time`, `math/rand`, `encoding/gob`, `testing`
- Go's race detector (`-race`) on every test run

## What I learned

**Consensus**
- Terms as a logical clock: why a higher term must be obeyed before anything else, including before checking whether a reply is stale
- Why a node gets one vote per term, and how randomized timeouts keep elections from repeatedly splitting
- Why election safety ("at most one leader per term") is the property to test, and how to test it under failure

**Concurrency**
- Never holding a lock while making an RPC: a slow or dropped message must not freeze a node
- Guarding against stale replies with the election term and the node's role
- One mutex plus clear ownership of each goroutine, instead of fine-grained locking
- Why a deterministic test with a fake transport beats timing-based stress for rare interleavings

**Distributed-systems thinking**
- A failed call looks the same whether the request was dropped, the reply was lost, or the node is partitioned, so the protocol has to be correct without knowing which one happened
- Designing a network simulation so failures are controlled, repeatable enough to debug, and visible in the tests

**Go**
- Goroutines, `sync.Mutex`, and the race detector
- `encoding/gob`: only exported fields are copied, and concrete types sent through `any` must be registered
- `go test` details: result caching, `-count` for stress runs, and why one panicking test kills the whole test binary

## References

- Diego Ongaro and John Ousterhout, [*In Search of an Understandable Consensus Algorithm*](https://raft.github.io/raft.pdf) (the Raft paper)
- [raft.github.io](https://raft.github.io) for the interactive visualization