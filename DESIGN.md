# Design notes

Why these tests are shaped the way they are, and the limits you will hit when
running them. Read this before trusting — or dismissing — a red result.

## Kommander is a library, so the suite ships a server

`Kommander.Server` maps only the inter-node Raft transport
(`/v1/raft/append-logs`, `/request-vote`, `/get-leader/{p}`, …) and runs a demo
`ReplicationService` that replicates `"Hello, World!"` in a loop. There is no
client-facing state machine, so there is nothing for a linearizability checker
to be linearizable *about*.

`harness/` fills that gap: an ASP.NET host that constructs a `RaftManager`,
applies committed entries to a replicated key/value map via
`OnReplicationReceived` / `OnLogRestored`, and exposes read / write / CAS and a
raw per-partition append.

**This means the harness is part of the trusted computing base of every
verdict.** A bug in it produces a red run that is not Kommander's fault. Three
decisions carry that weight, and each is deliberate:

### Reads go through the read-index round

`AmILeader` and `AmILeaderQuick` report *local belief*. A leader partitioned
into a minority keeps believing it leads until it **receives** a higher-term
message, so a read gated on them serves stale state as an authoritative
success — a linearizability violation manufactured by the harness.

Every read therefore gates on `ConfirmLeadershipAsync`, which performs a
same-term quorum ack round and waits for the local applied frontier to cover the
commit index observed at confirmation time (Raft dissertation §6.4). An
unconfirmed read answers `leadership-unconfirmed`, which the client records as
`:fail` — a read that asserts nothing.

### CAS is decided at apply time, not at propose time

The tempting implementation evaluates the comparison on the leader and
replicates the *resolved* absolute write. It is unsound. A leader can read the
current value, be deposed, watch another leader commit a write, be re-elected in
a later term, and only then have its proposal accepted — deciding the comparison
against a value that is no longer current. The resulting history is
non-linearizable because of the harness.

So a CAS is replicated as *condition plus intent* and evaluated identically on
every replica when the entry reaches its log position. The proposer learns the
outcome by waiting for its own apply and reading the recorded result for that
index. This also removes any need for leader-side key locking.

### The harness routes; the client does not

Each Jepsen client is bound to one node for the whole run. A node that does not
lead the key's partition forwards the request to the leader over HTTP rather
than answering "ask someone else" — otherwise most operations in a 5-node
cluster would be `:fail` and the history would be about routing, not consensus.

Forwarding introduces its own indeterminacy: a lost response may still have
committed. That is why a failed forward answers `forward-failed`, mapped to
`:info`, never `:fail`.

**A consequence worth knowing:** forwarding assumes every node serves the client
API on the same port, which is true in the Docker topology (distinct hostnames,
shared ports) and false if you run several harnesses on one host with different
`--http-port` values. In that setup only the leader answers and everything else
reports `not-leader`.

## Error classification is the whole ballgame

`kommander.client/response-class` decides whether a write outcome is definite or
unknown. Getting it wrong in either direction invalidates the analysis: `:fail`
on an operation that later commits produces phantom violations, and `:ok` on one
that never commits hides real ones.

The rule is asymmetric on purpose. A status is `:fail` only when the rejection
provably happened *before* anything could be appended to any log — routing
misses, admission control, membership-layer rejections, and a `cas-mismatch`
(which is a statement about applied state, not an inference). **Everything else
is `:info`.** A wrong `:fail` manufactures violations; a wrong `:info` only
costs search time.

The indeterminate cases that actually show up under a nemesis are
`proposal-timeout`, `replication-failed`, `forward-failed`, and `apply-timeout`
— the last being a CAS that committed but whose *effect* this node had not yet
applied, so whether the comparison held is genuinely unknown.

`NodeIsNotLeader` is classified `:fail` because Kommander checks leadership
before creating the entry. If that ever stops being true, this is the first line
to revisit.

## Transport is plaintext, and TLS is not under test

Kommander defaults to `RequireTls = true` on the inter-node transport. The
harness sets `RequireTls = false` and `GrpcScheme = "http://"` so Raft traffic
is h2c.

This is not laziness. A TLS handshake failure and a network partition are
indistinguishable in a history, and only one of them is the fault under test. A
suite that ran over TLS would attribute certificate problems to consensus. Node
authentication (`SharedSecret`, `MutualTls`) is a separate axis and is left at
its default; testing it would be a different suite.

The client API is on a **different port** (8081) from Raft (8082) for the same
reason: a partition of 8082 breaks replication while client connections survive,
so an operation's outcome is decided by consensus rather than by the test's own
transport dying.

## The harness must not block Kestrel from binding

Under minimal hosting the web host's own hosted service is registered during
`builder.Build()` — **after** anything registered with `AddHostedService`. A
hosted service that blocks in `StartAsync` therefore delays Kestrel from
binding. Since joining a cluster means waiting for peers who are themselves
waiting to bind, every node blocks forever with no port open and no error.

`ClusterService` is a `BackgroundService` for exactly this reason:
`BackgroundService.StartAsync` returns as soon as `ExecuteAsync` first yields.

The consequence for tests is that a node answers `/health` long before it is
useful, so `kommander.db/await-cluster!` waits for a committed **Voter** role,
not for the port.

## Partitions are 1..N inclusive; 0 is the system partition

`InitialPartitions = 4` yields application partitions 1, 2, 3 **and** 4, plus
the reserved system partition 0. An exclusive upper bound silently drops the
last partition from the workload's key space, and a proposal to partition 0 is
rejected by Kommander — a run where every append failed for a reason unrelated
to the property under test.

## Log-append checks less than it could, on purpose

**Gaps are legal.** The index space is shared with entries this workload never
wrote: checkpoints, system-partition traffic, promotion barriers, other clients.
A node's applied sequence is expected to skip indices, so a "no gaps" check
would fail every run while testing nothing Kommander claimed.

**Order across partitions is not a property.** Each partition is an independent
Raft group; there is no global order to violate.

What remains are four properties that hold under any interleaving: replica
agreement at an index (Log Matching), presence of every acknowledged entry on
every node that answers, no value applied twice, and monotonic application.

**A node that did not answer the final read cannot lose anything.** It is
excluded rather than treated as empty — otherwise a slow restart becomes a
fabricated durability violation. This is also why `--recovery-time` defaults to
90 s: a re-joined Learner needs to be backfilled and promoted before the
comparison is meaningful. The number is measured, not guessed — on a 4-CPU
Docker Desktop VM, partition-fault runs left a node behind by 53 entries at 30 s
and by 5 at 60 s, while 90 s was never short.

A fixed sleep is a blunt instrument. The principled version polls every node's
applied frontier until they agree or a deadline passes, which would be faster
*and* safer than any constant; until that exists, prefer waiting too long. A
tail loss that survives a generous window is worth investigating — but raise the
window first, because that is the cheaper experiment.

**A hole is not a truncated tail, and the verdict says which it found.** An
acknowledged entry missing at an index *below* what the node has already applied
is a hole: the node moved past that position without ever applying it, and no
amount of further waiting fills it in. An entry missing *beyond* the node's
frontier is a tail loss, which is exactly what a replica that had not finished
catching up looks like. Both fail the run, but `:holes` is evidence on its own
while `:tail-losses` is first a prompt to raise `--recovery-time` and rerun —
30 s has been observed to be too short for a node to close a 50-entry gap on a
laptop-sized VM. Reporting them as one number invites both mistakes: filing a
tuning artifact as a data-loss bug, and dismissing a real one as slowness.

**The harness proves it did not eat the entry itself.** `StateMachine.Apply`
declines payloads it cannot decode, and that path deliberately does *not* advance
the applied frontier — which produces a hole with the same fingerprint as one
Kommander caused. Declined indices are therefore recorded and served as
`undecodable`, and every lost entry is tagged `:harness-dropped`. A non-empty
`:undecodable` fails the run on its own: a harness that silently drops
deliveries makes every other property here vacuous.

**A run with too few appends is `:unknown`, not clean.** Every property above is
trivially satisfied by a history in which nothing was acknowledged, so without a
floor a botched setup reads as a pass.

## Knossos memory is the practical limit

Search cost is driven by per-key concurrency and by the number of
*indeterminate* (`:info`) operations — not by wall-clock time. Turn
`--concurrency-per-key` down before anything else, keep `-Xmx` below the VM's
actual RAM (a heap larger than the machine gets the JVM OOM-killed by the kernel
with no Java stack trace at all, which looks like a mysterious hang), and prefer
a longer run at lower density over a dense one that cannot be checked.

`--concurrency` must be an exact multiple of `--concurrency-per-key`;
`jepsen.independent` asserts at start-up otherwise.

**An unanalyzable history proves nothing.** `:valid? :unknown` is not a pass.

## Durability

Runs fsync the WAL by default. `--disable-wal-sync-writes` turns that off, at
which point a node that is SIGKILLed may legitimately lose acknowledged writes —
a finding about the flag rather than about Kommander. Run it as a separate,
weaker-expectation test rather than mixing the two.

## Membership churn is weaker than it looks when combined

The membership nemesis refuses to act unless the cluster is fully formed, which
a cluster missing a partitioned or killed node is not. Combined with `partition`
or `kill`, most membership operations therefore decline. That is the
conservative choice — churn is only exercised from a healthy state — but it
means you must read the `:leave` values in the history before concluding a
combined run tested membership at all.

The `:removed` field records whether the roster actually shrank, verified
against a *surviving* node. An unverified leave is exactly how a no-op nemesis
passes for a working one.

## Clock faults are off by default

`settimeofday` inside a container moves the shared kernel clock — on Docker
Desktop that means the whole VM. Kommander stamps proposal tickets with a hybrid
logical clock, so clock skew is likely the richest untested source of bugs here;
run `--faults partition,clock` on a disposable Linux host using
`docker/compose.clock.yml`, not on your laptop.

## Why CI is not a per-PR gate

Jepsen results are nondeterministic, and a slow, contended runner manufactures
indeterminate operations a fast machine would never produce. As a required check
it would go red for reasons unrelated to the change under review, and a check
people learn to ignore is worse than no check.

A red nightly means "download the artifact and look at the history", not "this
PR is broken".
