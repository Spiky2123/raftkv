## Week 1 Notes

The main workflow for this project: write a test that only passes once the intended functionality exists, then implement it and run the test until it passes.

**RPC (Remote Procedure Call)** - A function that runs on another node. It can fail in ways a normal function call can't, and we emulate those failure modes in our simulated network: random delays, a chance for messages to get dropped, and partitioning nodes so they can't reach each other.

**Endpoint** - Besides the normal Network object, we've implemented Endpoint, a wrapper around the network that remembers the ID of the node it belongs to, so that ID doesn't have to be passed into the call function every time.

**Seeded rng** - It's hard to re-simulate a network failure to see what went wrong, so seeding the rng for message drops and delays lets us reproduce a failure for sequential calls. Once parallelism is involved, CPU scheduling isn't guaranteed to be the same each time, so we lose that guarantee there.

**Gob deep copy** - We simulate a network on one machine using goroutines as nodes, all inside the same process, so their data is directly accessible to each other. This is fine for immutable values like ints and strings, but for anything that's a pointer under the hood (slices, maps, pointers themselves), passing it only copies the reference, so multiple nodes could end up modifying the same underlying data. That couldn't happen on a real network, since data is sent over the wire and each side only ever has its own copy. So we deep-copy args and replies (encode with gob, decode into a fresh value) to make sure each RPC gets its own independent copy, and no node can reach into another's data.

## Week 2 Notes

**Term** - Each node keeps a number, its current term, which acts as a logical clock. A node adopts the highest term it sees in any RPC, and it increments its own term when it starts an election. The purpose of terms is to filter out old data and to make it easy to tell when a node is behind.

**Randomized election timeouts** - Election timeouts are random so that, once a leader dies, the nodes don't all become candidates at the same moment. If they did, each would vote for itself, no one would reach a majority, and the election would repeat indefinitely. With random timeouts, one node usually times out first and collects votes before the others become candidates.

**Heartbeats** - The leader regularly sends empty AppendEntries RPCs to its followers at a fixed interval. They announce that the leader is still alive and still running its term. If a node doesn't receive one within its election timeout, it assumes the leader has failed and switches from follower to candidate.

**Never hold the lock during an RPC** - If a node holds its lock while waiting for an RPC reply, it can't do anything else until the reply arrives. It can't answer RPCs from other nodes, serve `GetState`, or check its election deadline in the ticker. That makes the node look dead to its peers, which can trigger needless elections, and it can lead to deadlock.

**Stale replies** - A reply can arrive late because of network delays, possibly after the election it belongs to has already ended. Such a reply is obsolete, so we drop it instead of counting it.

## Week 3 Notes

**Voting restriction** - A node only votes for a candidate whose log is at least as up to date as its own. This protects committed entries. An entry is committed once a majority holds it, and a candidate needs a majority of votes to win, so the two groups always share at least one node. That node refuses any candidate missing the entry, so every winner holds every committed entry. Without the rule, a candidate missing a committed entry could win, and its followers would delete that entry while resolving conflicts, even though the client was already told it succeeded.

**Up to date** - Compare the last entries' terms first, and compare log lengths only if the terms are equal. A longer log with an older last term loses.

**Leader appends before replicating** - The leader's log is the only source for what it replicates, so `Submit` appends locally first. This is safe because `Submit` promises nothing until the entry is committed. If the leader crashes first, the entry may vanish, and that's allowed because no client was told it succeeded.

**Confirmed prefix** - A successful AppendEntries confirms only that the follower's log matches the leader's up to `PrevLogIndex + len(Entries)`. The leader sets `matchIndex` from that value, not from `LastIndex()`, because `Submit` can add entries while the RPC is in flight. The follower sets `commitIndex = min(LeaderCommit, PrevLogIndex + len(Entries))`, because it hasn't verified anything beyond that against this leader.

**Truncate only on a real conflict** - A follower deletes entries only when an existing entry has a different term than the leader's entry at that index. A delayed, duplicated or heartbeat RPC knows less than the follower's log, and truncating on it could delete entries the leader already counted toward a majority. Entries that match are skipped, not treated as the end of the message.

**Commit counts only current-term entries** - A leader never commits an entry from an older term just by counting replicas (Figure 8). I'll explain it fully in week 4.

**Applier** - Committed entries are collected under the lock, then sent on the apply channel after unlocking, so a slow consumer can't freeze the node. Exactly one applier goroutine runs per node, which keeps order and delivers each entry once. Known gap: a blocked send can't be cancelled after `Kill()`, so week 4 adds a `done` channel with `select`.

**Bugs I hit** - Truncating on every AppendEntries wiped the log on a heartbeat. Using `break` instead of `continue` in the entries loop skipped entries after a matching one. An `if / else if` treated heartbeat successes as failures. `nextIndex` started at zeros instead of `lastIndex + 1`. `Submit` ran without the lock. The election timer reset even when a vote was denied. The up-to-date check compared `currentTerm` instead of the last log terms. Each now has a test that guards it.

**Churn tests** - `TestChurn5` (seed 1) converged with 152 entries applied out of 970 submitted. The ratio is low because most submissions hit non-leaders or isolated nodes and are dropped.