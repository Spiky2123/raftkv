# raftkv — Raft Consensus & a Fault-Tolerant Key-Value Store in Go

![Language](https://img.shields.io/badge/language-Go-00ADD8?style=flat-square)
![Status](https://img.shields.io/badge/status-work%20in%20progress%20(week%204%20of%208)-yellow?style=flat-square)
![Dependencies](https://img.shields.io/badge/dependencies-standard%20library%20only-brightgreen?style=flat-square)

raftkv is my implementation of the Raft consensus algorithm in Go, built from the original paper, together with a simulated unreliable network to test it on. The end goal is a linearizable key-value service replicated across a cluster that keeps working while nodes crash, messages are lost, and the network partitions.

It's the follow-up to [clox](https://github.com/Spiky2123/CLox). clox taught me how a language executes; raftkv is about what happens when the machines running a system can fail independently. I'm building it as an 8-week project, test first, and committing in small steps. Each milestone is tagged (`week1-network`, `week2-election`, `week3-replication`, ...), and [`NOTES.md`](NOTES.md) is my running journal of the design decisions and bugs along the way.

> **Status:** the simulated network, leader election, and log replication are done and stress-tested. Persistence and crash recovery are next. Everything after that is planned and listed in the [roadmap](#roadmap), and nothing there is claimed as built until it's tagged.

## Highlights

- **Leader election that holds up under failure**: randomized election timeouts, term-based step-down, one vote per term, and heartbeats. A randomized test with 5 nodes, 20% message loss, delays, and repeated leader isolation checks that no term ever has two leaders
- **Log replication that converges under churn**: the `AppendEntries` consistency check, conflict resolution, majority commit, and the "candidate's log must be up to date" voting rule. A 5-node test keeps isolating up to 2 nodes under 10% message loss while submitting commands, then checks that every node applied exactly the same log
- **A simulated network built for fault injection**: message drops, delays, partitions, and per-node isolation, driven by a seeded random number generator
- **Raft knows nothing about the network**: the consensus code depends only on a small `Transport` interface, so a real TCP/gRPC transport could replace the simulation without touching Raft
- **No shared memory between nodes**: every message is gob-encoded and deep-copied in transit, so a node can't accidentally read another node's state through a shared pointer
- **A strict locking discipline**: one mutex per node, never held while sending an RPC, sleeping, or delivering to the apply channel, checked with Go's race detector
- **Tested against the hard interleavings**: randomized chaos tests plus deterministic tests that call handlers directly or force rare orderings with a fake transport, instead of hoping a timing-based test stumbles into them

## Roadmap

| Week | Milestone | Status | Tag |
|------|-----------|--------|-----|
| 1 | Simulated network: drops, delays, partitions | Done | `week1-network` |
| 2 | Leader election | Done | `week2-election` |
| 3 | Log replication: `AppendEntries` consistency check, conflict resolution, commit index, apply channel, "candidate's log must be up to date" voting rule | Done | `week3-replication` |
| 4 | Persistence and crash recovery, fast log-conflict backup, Figure 8 tests | Next | |
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
- Leaders send `AppendEntries` to every follower every 50 ms, which doubles as the heartbeat; a candidate that hears from a leader of its own term steps back to follower
- Stale replies are ignored: a candidate counts a vote only if it is still a candidate in the term it started the election in
- `Kill` shuts a node down cleanly: all of its goroutines exit, including an applier blocked on a send, and it ignores any later RPC

**Log replication (`raft`)**
- `Submit(cmd)` appends a command to the leader's log and returns its index and term; non-leaders refuse it
- The leader tracks `nextIndex` and `matchIndex` per follower and sends each one the entries it's missing, along with the index and term of the entry just before them
- Followers reject an `AppendEntries` whose previous entry doesn't match their log, and the leader backs up and retries
- On a real conflict (same index, different term) a follower deletes the conflicting entry and everything after it; entries that already match are kept, so a delayed or duplicated message can't erase anything
- The leader commits an entry once a majority holds it, but only entries from its own term (Raft's Figure 8 rule); earlier entries become committed along with them
- Followers learn the commit index from the leader, capped at the part of their log the leader just confirmed
- Voting restriction: a node only votes for a candidate whose log is at least as up to date as its own (last term first, then length), so every new leader holds every committed entry
- One applier goroutine per node delivers committed entries on an apply channel, in order and exactly once

## Testing

The project has 80+ tests, all run under the race detector:

- **Rule-level unit tests**: the vote and `AppendEntries` handlers are called directly with hand-built arguments, covering term handling, one vote per term, the up-to-date voting rule, the consistency check, conflict truncation, and how the commit index moves (never past what the leader confirmed, never backwards)
- **Leader-side unit tests**: building per-follower requests, handling success and failure replies (including duplicate and out-of-order ones), and advancing the commit index only on a majority of current-term entries
- **Log unit tests**: the log wrapper's bounds checks, truncation, and that slices are copies rather than views into the log
- **Applier tests**: entries are delivered in order, the lock isn't held while sending, and a killed node's applier exits even when nobody reads the channel
- **Cluster tests**: 3-node and 5-node clusters elect exactly one leader, re-elect after the leader is disconnected, agree on submitted commands, don't commit without a majority, let a disconnected follower catch up, and overwrite a deposed leader's uncommitted entries
- **Deterministic interleaving tests**: a fake `Transport` that always replies with a higher term forces a leader to step down at an exact moment, which a timing-based stress test would hit only occasionally
- **Chaos tests**:
  - 5 nodes with 20% message loss, random delays, and the leader repeatedly isolated, checking Raft's *Election Safety* property (at most one leader per term)
  - Two 5-node churn tests, one on a reliable network and one with 10% message loss and random delays. Every 200–400 ms they isolate a random 0–2 nodes while submitting commands to every node, then heal the network and require all five nodes to have applied an identical log
- **Lifecycle tests**: a killed node stops all its goroutines and stays dead

## Example

The cluster test helper builds a cluster on the simulated network. This is a simplified version of what the election and replication tests do:

```go
c := makeCluster(t, 5)          // 5 nodes on a simulated network, one seeded RNG each
c.start()

leader := c.checkOneLeader(t)   // wait until exactly one leader exists for a term
c.one(t, "x", 5)                // submit "x" and wait until all 5 nodes have applied it

c.disconnect(leader)            // cut the leader off from everyone else
c.one(t, "y", 4)                // the other 4 elect a new leader and commit "y"
c.reconnect(leader)             // the old leader rejoins, steps down, and catches up
c.one(t, "z", 5)                // all 5 agree again
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
                         ╎  Submit / apply channel
   ┌─────────────────────┴────────────────────┐
   │ raft.Raft                                │
   │   leader election: done                  │
   │   log replication: done                  │
   │   persistence: next                      │
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

**Layering.** `raft.Raft` talks to the outside world only through the `Transport` interface (`Call(to, method, args, reply) bool`). `labnet.Endpoint` implements it on top of the simulated network. Raft replicates opaque commands and knows nothing about key-value types: commands go in through `Submit` and come out, once committed, on the apply channel. The KV service in week 5 will sit on top of that without Raft changing.

**Messages.** RPC arguments and replies are plain structs with exported fields, registered with `gob` in one place (`raft/types.go`). The network gob-encodes each message and decodes it into a fresh value, which is a deliberate way of preventing shared memory between nodes.

**Concurrency.** Each node protects all of its state with a single mutex that is never held while sending an RPC, sleeping, or sending on the apply channel. Helpers that need the lock are named `...Locked` and say so in a comment. The long-lived goroutines are a ticker (every 10 ms it checks the election deadline), one `leaderLoop` per leadership term, one applier, and one short-lived goroutine per outgoing RPC. When a reply comes back, the node first checks for a higher term, then checks that the term and role are still the ones the request was sent in, before acting on it. `Kill` closes a `done` channel, so a goroutine blocked on a channel send can still exit.

**Elections.** A candidate increments its term, votes for itself, and asks every peer for a vote in parallel. Votes are counted in a variable local to that election, only while holding the lock, and a majority (`>=`) makes it leader. The `leaderLoop` for a term exits as soon as the node is no longer the leader of that term, so goroutines from an old leadership can't act in a new one.

**Replication.** Each round, the leader builds one `AppendEntries` per follower from that follower's `nextIndex`. A success reply only proves the follower matches up to the last entry *in that request*, so `matchIndex` is set from the request, not from the leader's current log, which may have grown while the RPC was in flight. The commit index advances to the highest current-term entry held by a majority. On the follower, the commit index is capped at the same confirmed prefix, so a follower never applies an entry it hasn't checked against this leader. A rejected request currently backs `nextIndex` up by one entry; jumping back a whole term at a time is part of week 4.

**The log.** Log entries live behind their own wrapper type (`LastIndex`, `TermAt`, `Append`, `Slice`, `TruncateFrom`), with a placeholder entry at index 0 so indices match the paper's 1-based numbering. Nothing else indexes the raw slice, which keeps the week-6 snapshot work, where indices are shifted by an offset, contained in one file.

**Determinism.** The network and every node use their own seeded `*rand.Rand`; the global `rand` is never used, and each node's generator is touched only while holding its lock. Seeds make a failing run much easier to reproduce, but Go's goroutine scheduling isn't deterministic, so they don't make a run perfectly replayable. Fully replayable failure schedules are part of week 7.

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
- Why the voting restriction protects committed entries: the majority that stores an entry and the majority that elects a leader always overlap
- Why a leader only commits entries from its own term by counting replicas, and how older entries get committed along with them
- Why a follower must only truncate on a real conflict: a delayed or duplicated message knows less than the follower does

**Concurrency**
- Never holding a lock while making an RPC or delivering to a channel: a slow peer or consumer must not freeze a node
- Guarding against stale replies with the election term and the node's role
- One mutex plus clear ownership of each goroutine, instead of fine-grained locking
- Using a `done` channel with `select` so a blocked goroutine can still be shut down
- Why a deterministic test with a fake transport beats timing-based stress for rare interleavings

**Distributed-systems thinking**
- A failed call looks the same whether the request was dropped, the reply was lost, or the node is partitioned, so the protocol has to be correct without knowing which one happened
- A reply only proves what was in the request it answers, not what the sender has now
- Designing a network simulation so failures are controlled, repeatable enough to debug, and visible in the tests

**Go**
- Goroutines, channels, `sync.Mutex`, and the race detector
- `encoding/gob`: only exported fields are copied, and concrete types sent through `any` must be registered
- `go test` details: result caching, `-count` for stress runs, `-timeout`, and why one panicking test kills the whole test binary

## References

- Diego Ongaro and John Ousterhout, [*In Search of an Understandable Consensus Algorithm*](https://raft.github.io/raft.pdf) (the Raft paper)
- [raft.github.io](https://raft.github.io) for the interactive visualization
