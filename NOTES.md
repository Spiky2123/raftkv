## Week 1 Notes

The main workflow for this project: write a test that only passes once the intended functionality exists, then implement it and run the test until it passes.

**RPC (Remote Procedure Call)** - A function that runs on another node. It can fail in ways a normal function call can't, and we emulate those failure modes in our simulated network: random delays, a chance for messages to get dropped, and partitioning nodes so they can't reach each other.

**Endpoint** - Besides the normal Network object, we've implemented Endpoint, a wrapper around the network that remembers the ID of the node it belongs to, so that ID doesn't have to be passed into the call function every time.

**Seeded rng** - It's hard to re-simulate a network failure to see what went wrong, so seeding the rng for message drops and delays lets us reproduce a failure for sequential calls. Once parallelism is involved, CPU scheduling isn't guaranteed to be the same each time, so we lose that guarantee there.

**Gob deep copy** - We simulate a network on one machine using goroutines as nodes, all inside the same process, so their data is directly accessible to each other. This is fine for immutable values like ints and strings, but for anything that's a pointer under the hood (slices, maps, pointers themselves), passing it only copies the reference, so multiple nodes could end up modifying the same underlying data. That couldn't happen on a real network, since data is sent over the wire and each side only ever has its own copy. So we deep-copy args and replies (encode with gob, decode into a fresh value) to make sure each RPC gets its own independent copy, and no node can reach into another's data.