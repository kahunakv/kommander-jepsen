# Findings

Confirmed anomalies found by this suite, with enough detail to reproduce.

## 1. A leader does not apply some of its own committed entries

**Status:** reproduced, 2 of 2 independent occurrences show the same shape.
**Kommander:** `d293da0` ("Fence old leaders on higher-term votes"), `origin/main`.
**Workload:** `log-append`.

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
