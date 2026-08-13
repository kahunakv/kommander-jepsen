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

**Status:** **open.**
**Found on:** Kommander `f1658ba` ("Fix leader apply ordering for pipelined
commits"), suite `698c48b`.
**Workload:** `log-append`, `--faults partition`.
**Run:** GitHub Actions [31709747704], job `log-append / partition`, artifact
`jepsen-log-append-5-1` (retention 14 days from 2026-08-13).

Note the Kommander commit: this is the build that *fixed* finding 1, and the
verdict below reports the finding-1 shape nowhere. This is a different
anomaly, not a regression of that one.

### What happens

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

**The mechanism.** One occurrence, in a heavily faulted run, correlated with a
leader election and with the WAL backpressure storm in finding 3. Correlation
only — and the correlation is with two things at once, which is a reason to
reproduce it before fixing it. A speculative change to commit completion, in a
consensus implementation, with no test that would catch a mistake, is a good way
to turn a rare bug into a common one.

### Reproducing

Not yet reproduced on demand — this is one occurrence in one run. The run that
produced it:

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

**Status:** **fix written, not yet verified under load.** Unit-tested and the
full Kommander suite is green, but no Jepsen run has exercised it — the
condition needs a loaded, faulted cluster to appear, so the next nightly is the
first real test.
**Found on:** Kommander `f1658ba`, suite `698c48b`.
**Workload:** `log-append`; observed under `partition`, `kill` and
`partition,kill`.
**Run:** GitHub Actions [31709747704].

### Resolution

A follower's WAL enqueue could throw `BackpressureExceededException` out of
`AppendLogsCoreAsync` with nothing catching it anywhere in the replication path.
An escaped exception produces no reply, and no reply is indistinguishable from a
lost message: the leader re-sent on its next tick having learned nothing, and
the follower rejected the re-send just as fast.

The fix adds `RaftOperationStatus.FollowerWalSaturated` and answers the leader
with it, in the same shape as the Log Matching rejections immediately above the
enqueue. `CompleteAppendLogsAsync` already returns on any non-`Success` status
before touching `matchIndex`/`nextIndex`, so the batch is not counted as
delivered and the retry rides the normal heartbeat/backfill cadence instead of
spinning against a queue that never gets a chance to drain.

Separately, the per-failure logging that amplified this — `RaftPartitionExecutor`
wrote a full stack trace per failed operation — is now throttled per exception
type, so a repeated fault costs one line per second with a suppressed count
instead of competing for the disk it is complaining about.

Not fixed: the load skew. n1 took 237,976 rejections on partition 4 while n2 and
n4 took none, which a fair scheduler should not produce and which nothing above
explains.

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

[31709747704]: https://github.com/kahunakv/kommander-jepsen/actions/runs/31709747704
