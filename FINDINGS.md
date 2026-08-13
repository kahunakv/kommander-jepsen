# Findings

Anomalies found by this suite, with the evidence behind each and an explicit
account of what that evidence does *not* establish. Each entry states whether it
has been reproduced on demand; a single occurrence is recorded as a single
occurrence, not promoted to a confirmed defect by being written down.

## 1. A leader does not apply some of its own committed entries

**Status:** **fixed** in `RaftPartitionStateMachine.cs`; verified below.
**Found on:** `d293da0` ("Fence old leaders on higher-term votes"), `origin/main`.
**Workload:** `log-append`.

### Resolution

Pipelined proposals complete in *network* order, not log order: a later proposal
can reach quorum while an earlier one is still in flight. Delivering the later
batch immediately advanced the leader's applied frontier over the in-flight
entry, and the exactly-once guard then suppressed that entry's own delivery
forever — the permanent hole.

The fix parks such a batch in a `deferredLeaderApplies` map keyed by its lowest
log id and flushes in id order as the blocking proposals resolve, with the
buffer invalidated on a term change (after a step-down the WAL-based drains own
in-order delivery, and a rolled-back id from the stale tenure could be
re-proposed with a different payload). `DrainInheritedAppliesAsync` gained a
`BlockedByInFlight` status so it stops without advancing the cursor over a
current-term unresolved entry — including under `skipGaps`, since a sole voter's
proposals still resolve via self-quorum.

### Verification

~14 runs carrying real load at the reproducing settings, **all `:holes 0`**,
with `:undecodable []` and no `:harness-dropped true` anywhere.

This is strong evidence, not proof. The pre-fix rate was 1 hole-run in 8 loaded
runs (~12.5%), so 14 clean runs leaves roughly `0.875^14 ≈ 0.15` — about a 1-in-7
chance of having missed a still-live bug by luck. Re-run the loop below if you
want to drive that lower; each additional clean loaded run multiplies it by
0.875.

Note the fix was verified against the **working tree**, which at the time had
uncommitted changes to `Kommander/RaftPartitionStateMachine.cs`. The harness
project-references Kommander, so that is what was under test.

### The original report follows.

### What happens

An append is proposed, committed, and acknowledged to the client with an index.
Every follower applies it at that index. The **leader of that partition never
applies it at all**, and goes on applying later entries — leaving a hole in the
middle of its applied sequence.

Observed twice, on three different nodes, under two different fault profiles.

Run B (`--faults partition,kill`), four holes in one run:

| partition | index | value | absent on | role of that node | present on |
|---|---|---|---|---|---|
| 1 | 22 | `v1841` | n4 | **Leader of p1** | n1, n2, n3, n5 |
| 2 | 15 | `v1833` | n2 | **Leader of p2** | n1, n3, n4, n5 |
| 2 | 17 | `v1845` | n2 | **Leader of p2** | n1, n3, n4, n5 |
| 2 | 18 | `v1991` | n2 | **Leader of p2** | n1, n3, n4, n5 |

n4's partition 1 applied sequence runs 20, 21, **·**, 23, 24 … 29. n2's
partition 2 runs 13, 14, **·**, 16, **·**, **·**, 19 … 23. The other four
replicas agree at every index, including the missing ones.

Run A (`--faults partition`, no kills) had the same shape on n3, in exactly the
two partitions n3 led — index 7 of partition 3 and index 11 of partition 2, each
with a complete applied sequence on both sides of the hole. In that run both
lost entries were the two slowest proposals on the node (525 ms and 740 ms),
while proposals of 153 ms and 306 ms that interleaved between them applied fine.

### Why this is not the test's fault

Every alternative explanation was checked and excluded:

* **Not replica lag.** The entries are absent at indices *below* the node's
  applied frontier, with up to 180 later entries applied. No amount of waiting
  fills a hole — and `--recovery-time` was 90 s. This is why the checker reports
  `:holes` separately from `:tail-losses`.
* **Not a restart or WAL replay.** In run B only n1 was killed; n2 and n4 were
  never killed or restarted. Run A had no kill fault at all.
* **Not the harness dropping the entry.** `StateMachine.Apply` declines payloads
  it cannot decode without advancing the applied frontier, which would produce
  an identical hole. Those indices are now recorded and served as
  `undecodable`, and the run reports `:undecodable []` with every lost entry
  tagged `:harness-dropped false`.
* **Not duplicate-delivery suppression.** The harness's below-frontier guard
  counts every suppression; `:redeliveries` is 0 on all nodes.
* **Not divergence.** `:diverged []` — no node holds a *different* value at
  those indices. The entry is simply absent on one.
* **Not an unanswered final read.** All five nodes answered.

### Reproducing

Roughly 1 run in 8 at these settings. Shorter runs reproduce faster: the
anomaly clusters around leadership churn and partition initialization, and a
short run packs more of both per unit time.

```bash
scripts/build-tarball.sh ~/kommander linux-arm64
docker/up.sh

# then, on the control node — loop it, this is intermittent
for i in $(seq 1 10); do
  lein run test --workload log-append --nodes n1,n2,n3,n4,n5 \
    --time-limit 30 --concurrency 10 --rate 25 \
    --faults partition,kill --recovery-time 90 \
    --tarball target/kommander-harness.tar.gz \
  | grep -E ':holes|:acked-count'
done
```

A run with `:holes 0` proves nothing; a run with `:holes` > 0 and
`:undecodable []` is the finding. Ignore runs with `:acked-count 0` — the kill
nemesis sometimes rolls `:kill :all` and destroys a short run, which the checker
correctly reports as `:insufficient-data` rather than as a pass.

### What is not established

The mechanism. The evidence says a committed entry never reached the leader's
`OnReplicationReceived`, but not why. Run A's correlation with unusually slow
proposals suggests the leader's apply path can skip an entry whose commit
completes late relative to the entries around it — consistent with an apply
frontier advancing past a still-in-flight position — but that is a hypothesis,
not a diagnosis. Confirming it needs instrumentation inside
`RaftPartitionStateMachine.ApplyLogToConsumerAsync`, not more Jepsen runs.

### Why it matters

Every replica is supposed to be a copy. Here the node most likely to serve a
read — the leader — is the one missing state that a quorum committed and
acknowledged. Reads gated on `ConfirmLeadershipAsync` wait for the applied
frontier to cover the commit index, and that check passes: the frontier *did*
advance past the missing index. So this is not visible as a stall; it surfaces
as a leader silently serving state that is missing a committed write.

---

## 2. An acknowledged append is absent from every replica, at an index another entry occupies

**Status:** **open. Reproduced — three occurrences across two runs.**
**Found on:** Kommander `f1658ba` ("Fix leader apply ordering for pipelined
commits") and `985f017`; suite `698c48b` and `7b4a8e4`.
**Workload:** `log-append`, under `partition`, `partition,kill` and `kill`.
**Runs:** GitHub Actions [31709747704] (artifact `jepsen-log-append-5-1`) and
[31731331616] (artifacts `jepsen-log-append-6-1`, `-8-1`). Artifacts expire 14
days after their run.

Note the Kommander commit: `f1658ba` is the build that *fixed* finding 1, and
neither run reports the finding-1 shape. This is a different anomaly, not a
regression of that one.

### The shape, three times

Every occurrence is the same: a *later* append is definitively acknowledged at
an index, and an *earlier* append that had returned indeterminate is what
actually lands there. The acknowledged value is then in no replica's log at all.

| run | partition/index | acknowledged (`:ok` at that index) | actually applied there | on |
|---|---|---|---|---|
| [31709747704] | p1/199 | `v3013` | `v2686`, earlier `:info` `:timeout` | n1–n5 |
| [31731331616] | p1/51 | `v2116` | `v1069`, earlier `:info` `forward-failed` | n1–n5 |
| [31731331616] | p1/114 | `v1570` | `v1880`, earlier | n1, n2 |

In each case the acknowledged value appears **exactly twice in the entire
history** — its `:invoke` and its `:ok` — and nowhere in any node's final read.

The displaced-by value being indeterminate is the constant worth noting: an
operation that timed out or lost its forwarding response is entitled to commit
later, so its arrival at that index is legal. The violation is the *other* half
— that a second proposal was told, definitely and with a position, that it held
the index the first one got.

A fourth record in [31731331616] has a different shape and is not this finding:
`partition,kill` p1/72, `v2112` acknowledged and simply absent on n2 with
nothing at that index, the other nodes fine.

### What happens (worked example: the first occurrence)

An append is acknowledged `{"status": "ok", "index": 199}`. The value is then
absent from all five replicas, and partition 1's index 199 holds a *different*
value on every one of them.

| | value | partition | index | outcome |
|---|---|---|---|---|
| acknowledged | `v3013` | 1 | 199 | `:ok`, invoked 14:25:52.572, acked 14:25:57.511 (4.94 s) |
| actually applied at p1/199 | `v2686` | 1 | 199 | earlier `:info` / `:timeout`, on n1–n5 |

`v2686` timing out and committing later is legal — an indeterminate operation
may apply at any time. What is not legal is `v3013` receiving a definite
acknowledgement naming a position it never occupied.

`v3013` appears **exactly twice in the entire history**: its `:invoke` and its
`:ok`. It is in no node's final read-log, at index 199 or anywhere else.

The verdict for the run: `:holes 5` — one logical entry, missing on five nodes
— with `:tail-losses 333`, `:acked-count 877`, `:diverged []`, `:duplicated []`,
`:redelivered []`, `:undecodable []`.

Partition 1 held a Term=3 election at 14:25:40, roughly twelve seconds before
the append was acknowledged; n2 was leader by 14:25:46.

### Why this is not the test's fault

* **Not a hole in one replica.** Unlike finding 1, the entry is missing from
  *all five* nodes. There is no replica holding it.
* **Not an entry that moved index.** The value appears nowhere in any node's
  applied sequence, so this is not the "acknowledged at 5, applied at 6" shape
  the checker also detects.
* **Not divergence.** All five nodes agree that p1/199 is `v2686`, which is why
  `:diverged []`. The replicas are consistent with each other and inconsistent
  with the acknowledgement.
* **Not the harness inventing an index.** `Api.Propose` sets
  `response.Index = result.LogIndex` and returns it verbatim; the harness
  reports what `RaftReplicationResult` gave it.
* **Not a harness decode failure.** `:undecodable []`, and the lost entry is
  tagged `:harness-dropped false`.
* **Not an un-caught-up replica.** All five answered the final read, and the
  entry is missing *below* every one of their frontiers — `:hole? true` on all
  five records.

### The acknowledgement was Kommander's, not the harness's

This was open when the entry was first written — the harness returns `"ok"` for
an append on `result.Success` alone, ignoring `WaitApplied`, so it mattered
whether `Success` meant *committed* or merely *replicated to a quorum in the
proposing node's term*. Reading Kommander settles it:

* `RaftManager.WaitForQuorum` (`RaftManager.cs:2582`) sets `Success = true` only
  where the proposal ticket reaches `RaftProposalTicketState.Committed`. Every
  other terminal state returns `ReplicationFailed` or `ProposalTimeout`.
* The index it reports comes from `proposal.LastLogIndex` /
  `completion.MaxLogIndex` (`RaftPartitionStateMachine.cs:3850, 4128, 4467,
  4528`) — the proposal's **own** log index, not the partition's commit
  frontier. So this is not a case of a client being handed someone else's
  index by a naming conflation.

Kommander therefore reported `v3013` committed, at 199, of its own accord. The
harness repeated that faithfully, and tightening it to demand local apply would
have hidden the finding rather than fixed it.

What remains is a **log index assigned to two proposals**: `v2686` holds p1/199
on every replica, `v3013` was told it had it, and `v3013`'s ticket still reached
`Committed`. The fix belongs at the commit-completion sites above — completing a
waiter as `Committed` needs to establish that the entry at that index is still
this proposal's after a term change, rather than trusting the index recorded
when the proposal was made.

### What is not established

**The mechanism.** Three occurrences now, all under fault injection, all
correlated with leadership churn. What the histories establish is the *shape* —
a definite acknowledgement handed to a proposal whose index an earlier
indeterminate proposal ends up holding — not the code path that produces it.

Two candidates fit the evidence and are worth separating before either is
touched:

* **Index re-use across a term change.** The proposal is assigned an index by a
  leader that is then superseded; the new leader places the earlier, still
  in-flight proposal there instead, and the first proposal's ticket completes as
  `Committed` anyway.
* **Ticket/entry mis-binding.** The ticket completes against a commit that is
  not its own entry — in which case the term change is incidental and the bug is
  in how completion is matched to a proposal.

The `:hole? true` records cannot distinguish these; the leader's own view of the
proposal at the moment it completed can. That is a Kommander-side instrumentation
question, not another Jepsen run.

**Whether the two anomalies in [31731331616] are one event or two.** Both are in
partition 1 of the same run, indices 51 and 114.

### Reproducing

Reproduced twice in the second run, so this is not rare at these settings — but
it is not on-demand either, and both runs were heavily faulted. The invocation
that produced it:

```bash
lein run test --workload log-append --faults partition \
  --nodes n1,n2,n3,n4,n5 --time-limit 180 --rate 10 \
  --concurrency 10 --concurrency-per-key 3 --ops-per-key 60 \
  --nemesis-interval 15 --recovery-time 180 \
  --tarball target/kommander-harness.tar.gz
```

Look for `:holes` > 0 with `:undecodable []`, then check whether the lost value
appears anywhere in the final read-logs. If it appears at another index the
anomaly is a moved entry; if it appears nowhere, it is this one.

### Why it matters

A client was told, definitely and with a position, that a write was committed.
No replica has it, and its position belongs to something else. Every use of
that acknowledgement — an application recording the index, a downstream reader,
a retry decision made on the strength of "ok" — is built on a fact the cluster
does not hold. This is the failure mode consensus exists to make impossible.

---

## 3. WAL backpressure rejects replication, and the follower never catches up

**Status:** **partially fixed. The exception escape and the log amplification
are gone; the livelock is not.**
**Found on:** Kommander `f1658ba`, suite `698c48b`.
**Workload:** `log-append`; observed under `partition`, `kill` and
`partition,kill`.
**Runs:** GitHub Actions [31709747704] (reported), [31731331616] (after the
partial fix).

### Partial resolution, and what it did not do

A follower's WAL enqueue could throw `BackpressureExceededException` out of
`AppendLogsCoreAsync` with nothing catching it anywhere in the replication path.
An escaped exception produces no reply, and no reply is indistinguishable from a
lost message: the leader re-sent having learned nothing, and the follower
rejected the re-send just as fast.

Kommander `985f017` adds `RaftOperationStatus.FollowerWalSaturated` and answers
the leader with it, in the same shape as the Log Matching rejections
immediately above the enqueue, and throttles `RaftPartitionExecutor`'s
per-failure stack-trace logging by exception type.

Measured in [31731331616], both of those worked:

| | before | after |
|---|---|---|
| escaped `BackpressureExceededException` | 237,976 / 13,054 / 2,226 (n1/n5/n3) | **0 on every node** |
| largest node log | 251 MB | 43 MB |

**But the follower still never catches up.** All three `log-append` jobs again
exhausted the deadline with `:converged? false`, and every lagging entry
reported `:advanced 0` — no replica moved a single index in ~180 s of healed,
idle cluster across 46–90 polls.

The reason is a prediction in the fix that turned out to be wrong. It assumed
that because `CompleteAppendLogsAsync` returns on a non-`Success` status before
touching `matchIndex`/`nextIndex`, the re-send would fall back to the ordinary
heartbeat/backfill cadence. It does not. Over the run n2 logged 15,484
`FollowerWalSaturated` acks and n3 9,478, peaking at **2,365 in a single second**
(18:40:56) against a 500 ms `HeartbeatInterval` that should cap the leader at
roughly 32/s across four partitions and four peers. The exception storm became a
status storm.

The damning part is *when*. The recovery phase — network healed, nemesis
stopped, no client load — ran 18:38:41 → 18:41:42, and n2's ack rate **rose**
through it:

| minute | phase | acks |
|---|---|---|
| 18:36 | under load | 1,235 |
| 18:39 | quiescent recovery | 1,018 |
| 18:40 | quiescent recovery | 5,378 |
| 18:41 | quiescent recovery | 7,853 |

An idle cluster accelerating its own retries is a spin, not a workload.

The remaining fix is leader-side: a per-peer cooldown suppressing re-sends to a
saturated peer for an interval. A typed status is only backpressure if the
sender acts on it, and nothing on the leader currently does.

Not addressed either: the load skew. n1 took 237,976 rejections on partition 4
while n2 and n4 took none, which a fair scheduler should not produce.

### The original report follows.

### What happens

`FairWalScheduler` rejects WAL writes once a partition's queue is full:

```
BackpressureExceededException: FairWalScheduler: partition 1 queue depth 4096 exceeded limit.
   at Kommander.WAL.IO.FairWalScheduler.Enqueue(WALWriteOperation) FairWalScheduler.cs:276
   at Kommander.RaftWriteAhead.EnqueueProposeOrCommit(...)          RaftWriteAhead.cs:1131
   at Kommander.RaftPartitionStateMachine.AppendLogsCoreAsync(...)
   at Kommander.Scheduling.RaftPartitionExecutor.ExecuteOneAsync(PendingOperation) RaftPartitionExecutor.cs:963
```

It **rejects rather than blocks**, so the replication those calls carried is
dropped, not delayed. A follower that falls far enough behind stops being able
to accept the entries that would let it catch up.

Counts for the `partition` job, whole run:

| node | exceptions | partition | last occurrence |
|---|---|---|---|
| n1 | 237,976 | 4 | 14:25:59.138 |
| n5 | 13,054 | 1 | 14:29:58.809 |
| n3 | 2,226 | 1 | 14:29:39.671 |
| n2 | 0 | — | — |
| n4 | 0 | — | — |

### Why this is not lag

The recovery phase ran **14:26:41.994 → 14:29:44.909** — 182.966 s, 63 polls,
with the network healed, the nemesis stopped and no client load. n3 and n5 were
still throwing this *inside that window*, on partition 1, and the wait ended
with 12 node/partitions short of the acknowledged high-water mark
(`:converged? false`, `:missing []` — every node answering, none advancing).

This is what the convergence wait was built to distinguish, and it is the
distinction that matters: the replicas were not slow, they were stuck. No value
of `--recovery-time` fixes a queue that rejects writes. The three red
`log-append` jobs in this run all carry `:converged? false`; the one that
passed, `membership`, is the one whose earlier failure genuinely *was* lag.

### What is not established

Whether the leader retries a rejected `AppendLogs` indefinitely, and therefore
whether a follower in this state recovers once load stops. The evidence says it
had not recovered after three minutes of quiescence; it does not say it never
would.

Whether the queue bound is simply too low for a 5-node cluster on a contended
CI runner, or whether reject-on-full is the wrong policy for a replication
path at any depth. The limit was not read from the source — only from the
message, which reports the depth reached — so the configured value is worth
confirming before drawing conclusions from it. Note also that n1 took 237,976
rejections on partition 4 while n2 and n4 took none at all: the load is
extremely unevenly distributed, which is itself worth explaining.

### Why it matters

A follower that cannot accept replication is a replica in name only. It will
answer reads with an applied state that is arbitrarily far behind, and the
cluster has no way to tell an operator that one of its copies has stopped being
a copy. It also degrades every durability guarantee that counts replicas: the
quorum still forms, but the surviving-replica arithmetic after a failure is
quietly wrong.

---

## 4. Two different entries committed at the same log index (Log Matching violation)

**Status:** **open.** One occurrence.
**Found on:** Kommander `985f017`, suite `7b4a8e4`.
**Workload:** `log-append`, `--faults partition,kill`.
**Run:** GitHub Actions [31731331616], job `log-append / partition,kill`,
artifact `jepsen-log-append-6-1`.

### What happens

Partition 2, index 104, at the final read:

```clojure
:diverged [{:index 104,
            :values {"n1" "v5174", "n2" "v5174", "n5" "v5174", "n3" "v6067"},
            :partition 2}]
```

Four replicas hold `v5174` at p2/104. n3 holds `v6067` there. This is the Log
Matching Property stated directly — *if two logs contain an entry with the same
index and term, the logs are identical in all entries up through that index* —
and it is the property backfill and the snapshot handoff exist to preserve.

Run verdict: `:diverged` as above, `:holes 6`, `:tail-losses 152`,
`:acked-count 412`, `:duplicated []`, `:redelivered []`, `:undecodable []`.

### Why this is the most serious entry in this file

Findings 1–3 are all *absence*: an entry that should be somewhere and is not.
This one is *contradiction*. A replica does not merely lag the quorum, it
disagrees with it, and there is no amount of catch-up that resolves a
disagreement — a follower whose log conflicts at an index cannot accept anything
anchored above it, so the divergence is self-sustaining. Any read served by n3
for that partition returns a different history than a read served by anyone
else, and both look healthy.

### The same node is the wedged one

n3 is also the replica reporting `:advanced 0` in this job, and its log shows
two other symptoms at scale:

* **180,787** `Ignoring stale CompleteAppendLogs … responseTerm/currentTerm`
  warnings — by a wide margin the dominant content of its 43 MB log.
* **1,364** pre-vote rounds and 5,456 vote requests: a continuous election loop.
* **10,069** `RpcException: Status(StatusCode="Unavailable")` from the transport
  dispatcher.

Whether the divergence causes the wedge, the wedge causes the divergence, or
both follow from the election loop is **not established**. The divergence is in
partition 2 while n3's lagging entries in this job are partitions 1 and 4, which
argues against the simplest story (that the conflicting index blocks that
partition's replication) and is the first thing to check.

### What is not established

**Which entry is the legitimate one.** `v5174` has the majority, but this suite
records what each replica applied, not which one Raft would consider committed.
Establishing that needs the term at index 104 on each node, which the harness
does not currently expose — it serves `{:index :value}` only. Adding the term to
`/log/entries` would make this decidable and is a cheap change to this repo.

**Whether it reproduces.** One occurrence, one run.

[31709747704]: https://github.com/kahunakv/kommander-jepsen/actions/runs/31709747704
[31731331616]: https://github.com/kahunakv/kommander-jepsen/actions/runs/31731331616
