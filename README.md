# kommander-jepsen

Jepsen tests for [Kommander](https://github.com/kahunakv/kommander) — a
partitioned Raft consensus library for C#/.NET.

Kommander is a **library**, not a server. `Kommander.Server` exposes only the
inter-node Raft transport and a demo replication loop, so there is no
application state machine to check. This suite therefore ships its own: a small
ASP.NET host (`harness/`) that embeds `IRaft`, applies committed entries to a
replicated key/value map, and exposes the operations the workloads drive. The
harness references Kommander **by project**, so what gets tested is the code in
your working tree — not the last published NuGet package.

Two workloads run against a 5-node cluster while a nemesis partitions, kills,
pauses, and removes/rejoins nodes:

| Workload | What it checks |
|---|---|
| `register` | linearizability of a CAS register over the replicated KV map (Knossos) |
| `log-append` | Raft's Log Matching Property directly: replica agreement, no lost acknowledged entries, no duplicates, exactly-once apply |

- **[DESIGN.md](DESIGN.md)** — why the tests are shaped this way, what the
  harness decides on Kommander's behalf, and the limits you will hit. Read it
  before trusting or dismissing a red result.
- **[FINDINGS.md](FINDINGS.md)** — confirmed anomalies, with reproducers and the
  alternative explanations that were ruled out.

## Requirements

- Docker with **≥2 CPUs** allocated to its VM (`docker info` → `NCPU`)
- .NET 10 SDK, to build the harness tarball
- A checkout of Kommander (default `~/kommander`)
- Leiningen and a JDK

## Running

```bash
# 1. Build the harness against your Kommander checkout. Second arg is the RID
#    matching your node containers' architecture (linux-arm64 on Apple Silicon
#    by default).
scripts/build-tarball.sh ~/kommander linux-arm64

# 2. Bring up the cluster and land on the control node.
docker/up.sh

# 3. From the control node shell:
lein run test --workload register \
              --nodes n1,n2,n3,n4,n5 \
              --time-limit 60 \
              --concurrency 9 \
              --rate 15 \
              --ops-per-key 60 \
              --faults partition

# 4. Browse results
lein run serve   # http://localhost:8080
```

`lein run test --help` lists every option. The ones you are most likely to reach
for:

| Flag | Meaning |
|---|---|
| `--workload` | `register` or `log-append` |
| `--faults` | comma-separated `partition,kill,pause,membership,clock`, or `all` |
| `--partitions` | Raft partitions (independent Raft groups) per cluster |
| `--transport` | `grpc` (default, the production path) or `rest` |
| `--concurrency` | total client threads; **must** be an exact multiple of `--concurrency-per-key` |
| `--rate` | requests/sec per client |
| `--time-limit` | seconds of load |
| `--recovery-time` | deadline (seconds) on the wait for replicas to catch up after healing; the wait ends as soon as they do |
| `--poll-interval` | seconds between frontier polls during that wait |
| `--disable-wal-sync-writes` | run without WAL fsync (expect data loss on kill) |

Run `lein test` for the unit tests — negative controls proving the log-append
checker actually rejects divergence, lost writes, duplicates and redeliveries.

`docker/up.sh` generates an SSH key pair into `docker/secret/` (gitignored) and
bakes the public half into the node images. Key auth is mandatory rather than
cosmetic: Jepsen uploads the harness tarball by shelling out to `scp`, which
cannot use a password.

## Reading a result

A run ends with a verdict map and either `Everything looks good!` or
`Analysis invalid!`. Two verdicts mean less than they appear to:

- `:valid? :unknown` — the checker could not finish, usually out of memory. It
  is **not** a pass. See
  [DESIGN.md](DESIGN.md#knossos-memory-is-the-practical-limit).
- `:insufficient-data` (log-append) — fewer than `--min-appends` appends were
  acknowledged, so "no divergence" is vacuous. The checker returns
  `:valid? :unknown` rather than success; a run where every operation failed
  must not read as a clean run.
- `:no-successful-ops` (stats) — some operation never once succeeded, so the run
  did not exercise it. Usually availability: under a kill nemesis a register run
  can land zero successful CAS because the cluster was down for most of it. Also
  `:valid? :unknown`, for the same reason as above — and note this still fails
  the run (jepsen exits 1 on `false` and 2 on `:unknown`). It says *which* kind
  of red, not that the red is harmless.

A red `log-append` run reports `:holes` and `:tail-losses` separately, and they
mean different things:

- `:holes` — an acknowledged entry is missing at an index the node has already
  applied past. Waiting longer cannot fix it. This is a finding.
- `:tail-losses` — the loss is beyond the node's applied frontier, which is also
  what an un-caught-up replica looks like. Read it against `:convergence` in the
  same verdict: `:converged? true` means every node had caught up to the
  acknowledged high-water mark before the read, so the entries really are
  missing; `:converged? false` means the wait hit `--recovery-time` with the
  nodes in `:lagging`/`:missing` still behind, and the count proves nothing.
  Raise `--recovery-time` and rerun.
- `:undecodable` / `:harness-dropped` — the harness was handed the entry and
  refused it. That is a bug in **this repo**, not in Kommander.

Read `:undelivered` **before** either of the above, because it changes which
subsystem the run is about. It counts losses where Kommander physically holds the
entry on that node — its log reaches the index — and never delivered it to the
state machine. Replication succeeded; the apply path did not. `:delivery` lists
the node/partitions whose log runs ahead of what they applied, with `:behind`.

A run whose losses are mostly `:undelivered` is not a replication or backfill
problem, and investigating it as one wastes the run: four consecutive
investigations chased a replication fault that was not there, because applied
entries alone cannot distinguish "never arrived" from "arrived and was never
handed over".

Everything from a run lands in `store/<test>/<timestamp>/` — history, verdict,
timeline HTML, latency plots and per-node harness logs.

## What's here

| Path | Purpose |
|---|---|
| `harness/` | the .NET test server: `IRaft` + a replicated KV state machine + the client API |
| `harness/StateMachine.cs` | apply-time CAS, the applied log, and the redelivery counter |
| `harness/Api.cs` | routing, leader forwarding, read-index reads, status mapping |
| `src/kommander/client.clj` | HTTP client + the status → ok/fail/info mapping |
| `src/kommander/db.clj` | install / start / stop / kill / pause / leave / join a node |
| `src/kommander/workload/register.clj` | linearizable CAS register (Knossos) |
| `src/kommander/workload/log_append.clj` | Log Matching, durability, uniqueness, exactly-once |
| `src/kommander/nemesis/membership.clj` | removes a node from the roster and rejoins it |
| `src/kommander/core.clj` | test map, nemesis wiring, CLI |
| `test/` | negative controls proving the log-append checker can actually fail |
| `docker/` | 5 Jepsen nodes + a control node |
| `scripts/build-tarball.sh` | self-contained harness publish → `target/kommander-harness.tar.gz` |

## Continuous integration

`.github/workflows/jepsen.yml` runs nightly (04:00 UTC) and on manual dispatch,
as a matrix over workloads and fault sets. It is deliberately not a per-PR gate
([why](DESIGN.md#why-ci-is-not-a-per-pr-gate)).

Differences from a local run:

* **Topology.** On a Linux runner the host reaches container IPs directly, so
  CI starts only `n1..n5` and runs Jepsen on the runner itself — no control
  container, no bind mount.
* **Heap.** `lein with-profile +ci` raises `-Xmx` to 11g, which a 16 GB runner
  supports and a 4 GB Docker Desktop VM does not.
* **Architecture.** The tarball is built for `linux-x64`, not `linux-arm64`.
* **Rate.** Defaults to 10 req/s: four vCPUs hosting five .NET servers plus the
  JVM cannot sustain laptop throughput, and pushing harder just converts into
  timeouts and an unanalyzable history.

`jepsen.cli` exits non-zero when the checker returns `:valid? false`, so the job
fails on its own. The whole `store/` directory uploads as an artifact.
