using System.Collections.Concurrent;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using Kommander.Data;

namespace KommanderJepsen.Harness;

/// <summary>
/// One replicated log entry's payload.
/// </summary>
/// <remarks>
/// <para>
/// <b>Conditional operations are decided at apply time, not at propose time.</b>
/// A compare-and-set is replicated as the *condition plus the intent*
/// (<c>Op=cas, Expected, Value</c>) and evaluated identically on every replica
/// when the entry reaches its log position — the textbook replicated state
/// machine. The tempting alternative is to evaluate the comparison on the
/// leader, then replicate the resolved absolute write; that is subtly wrong.
/// A leader can read the current value, be deposed, watch another leader commit
/// a write, be re-elected in a later term, and *then* have its resolved
/// proposal accepted. The CAS would have been decided against a value that was
/// no longer current, and the resulting history would be non-linearizable
/// because of the harness rather than because of Kommander. Deciding at apply
/// time removes the window entirely: the condition is evaluated against the
/// state at exactly the log position the entry occupies.
/// </para>
/// </remarks>
public sealed class LogEntry
{
    [JsonPropertyName("op")]
    public string Op { get; set; } = "";

    [JsonPropertyName("key")]
    public string? Key { get; set; }

    [JsonPropertyName("value")]
    public string? Value { get; set; }

    [JsonPropertyName("expected")]
    public string? Expected { get; set; }
}

/// <summary>What an entry did once it was applied.</summary>
public enum ApplyOutcome
{
    /// <summary>The entry has not been applied on this node (yet).</summary>
    Unknown = 0,

    /// <summary>The write took effect.</summary>
    Applied = 1,

    /// <summary>A compare-and-set whose comparison did not hold.</summary>
    Mismatch = 2,

    /// <summary>The payload was not one of ours (or was unparseable).</summary>
    Ignored = 3
}

/// <summary>
/// The replicated key/value state machine the Jepsen workloads drive, plus the
/// per-partition applied log the log-append workload reads back.
/// </summary>
/// <remarks>
/// Kommander delivers committed entries through <c>OnReplicationReceived</c> and
/// replays them on restart through <c>OnLogRestored</c>. Delivery is
/// exactly-once and in log order per partition
/// (<c>RaftPartitionStateMachine.ApplyLogToConsumerAsync</c> withholds any entry
/// at or below the applied frontier), but this class re-checks the frontier
/// anyway: the whole point of the suite is to not take that guarantee on trust,
/// and a duplicate delivery must show up as a *finding*, not as corrupted state
/// that produces an unrelated one.
/// </remarks>
public sealed class StateMachine
{
    /// <summary>The <c>logType</c> stamped on every entry this harness proposes.</summary>
    public const string JepsenLogType = "jepsen";

    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);

    private sealed class Partition
    {
        public readonly Lock Gate = new();
        public long AppliedIndex;

        /// <summary>Entries this node has applied, in apply order.</summary>
        public readonly List<AppliedEntry> Applied = [];

        /// <summary>
        /// index -> outcome, so a proposer can learn whether its own CAS held
        /// once the entry lands. Bounded: a Jepsen run is finite but long, and
        /// an unbounded map here would be the harness's own memory leak.
        /// </summary>
        public readonly Dictionary<long, ApplyOutcome> Outcomes = [];

        public readonly Queue<long> OutcomeOrder = new();

        /// <summary>Duplicate or out-of-order deliveries seen. Reported, never hidden.</summary>
        public long Redeliveries;

        /// <summary>
        /// Deliveries this harness declined to apply: a payload it could not
        /// parse, or one carrying an <c>Op</c> it does not know. These do not
        /// advance <see cref="AppliedIndex"/> and do not land in
        /// <see cref="Applied"/>, so without a counter they are indistinguishable
        /// from an entry Kommander never delivered at all — and the two have
        /// completely different culprits. Recorded with the offending index so a
        /// missing entry can be attributed on the spot rather than by rerunning.
        /// </summary>
        public readonly List<long> Undecodable = [];
    }

    /// <summary>
    /// Entries delivered with a <c>LogType</c> other than <see cref="JepsenLogType"/>,
    /// counted globally because the skip happens before a partition is resolved.
    /// Expected to be non-zero — Kommander replicates its own system entries
    /// through the same callback — so this is context for a missing entry, not a
    /// finding on its own.
    /// </summary>
    private long foreign;

    public readonly record struct AppliedEntry(long Index, string? Value);

    private const int MaxRememberedOutcomes = 200_000;

    private readonly ConcurrentDictionary<int, Partition> partitions = new();
    private readonly ConcurrentDictionary<string, string> values = new();

    private Partition Get(int partitionId) =>
        partitions.GetOrAdd(partitionId, static _ => new Partition());

    /// <summary>
    /// Applies one committed entry. Called from Kommander's per-partition apply
    /// loop; returns false only if the harness itself failed, which Kommander
    /// surfaces as a replication error.
    /// </summary>
    public bool Apply(int partitionId, RaftLog log)
    {
        if (log.LogType != JepsenLogType || log.LogData is null || log.LogData.Length == 0)
        {
            Interlocked.Increment(ref foreign);
            return true;
        }

        Partition p = Get(partitionId);

        LogEntry? entry;
        try
        {
            entry = JsonSerializer.Deserialize<LogEntry>(log.LogData, JsonOptions);
        }
        catch (JsonException)
        {
            // A payload we cannot read is a harness bug, not a Kommander one.
            // Record it and keep the apply loop moving: stalling here would
            // convert a cosmetic problem into a cluster-wide liveness failure
            // and destroy the run's evidence.
            NoteUndecodable(p, log.Id);
            return true;
        }

        if (entry is null || string.IsNullOrEmpty(entry.Op))
        {
            NoteUndecodable(p, log.Id);
            return true;
        }

        lock (p.Gate)
        {
            if (log.Id <= p.AppliedIndex)
            {
                p.Redeliveries++;
                return true;
            }

            ApplyOutcome outcome = ApplyOutcome.Ignored;

            switch (entry.Op)
            {
                case "set":
                    if (entry.Key is not null)
                    {
                        if (entry.Value is null)
                            values.TryRemove(entry.Key, out _);
                        else
                            values[entry.Key] = entry.Value;
                        outcome = ApplyOutcome.Applied;
                    }
                    break;

                case "cas":
                    if (entry.Key is not null)
                    {
                        values.TryGetValue(entry.Key, out string? current);
                        if (string.Equals(current, entry.Expected, StringComparison.Ordinal))
                        {
                            if (entry.Value is null)
                                values.TryRemove(entry.Key, out _);
                            else
                                values[entry.Key] = entry.Value;
                            outcome = ApplyOutcome.Applied;
                        }
                        else
                        {
                            outcome = ApplyOutcome.Mismatch;
                        }
                    }
                    break;

                case "append":
                    // Opaque payload for the log-append workload: the state is
                    // the sequence itself.
                    outcome = ApplyOutcome.Applied;
                    break;
            }

            p.Applied.Add(new AppliedEntry(log.Id, entry.Value));
            p.AppliedIndex = log.Id;
            Record(p, log.Id, outcome);
        }

        return true;
    }

    /// <summary>
    /// Notes an entry the harness could not decode. Takes the same lock as the
    /// apply path: <see cref="Record"/> mutates a plain dictionary and queue, and
    /// the apply loop is not the only caller.
    /// </summary>
    private static void NoteUndecodable(Partition p, long index)
    {
        lock (p.Gate)
        {
            p.Undecodable.Add(index);
            Record(p, index, ApplyOutcome.Ignored);
        }
    }

    private static void Record(Partition p, long index, ApplyOutcome outcome)
    {
        p.Outcomes[index] = outcome;
        p.OutcomeOrder.Enqueue(index);

        while (p.OutcomeOrder.Count > MaxRememberedOutcomes)
            p.Outcomes.Remove(p.OutcomeOrder.Dequeue());
    }

    public long AppliedIndex(int partitionId)
    {
        Partition p = Get(partitionId);
        lock (p.Gate)
            return p.AppliedIndex;
    }

    public long Redeliveries(int partitionId)
    {
        Partition p = Get(partitionId);
        lock (p.Gate)
            return p.Redeliveries;
    }

    /// <summary>Indices delivered to this partition that the harness could not decode.</summary>
    public long[] Undecodable(int partitionId)
    {
        Partition p = Get(partitionId);
        lock (p.Gate)
            return p.Undecodable.ToArray();
    }

    /// <summary>Entries delivered with someone else's <c>LogType</c>.</summary>
    public long Foreign => Interlocked.Read(ref foreign);

    public ApplyOutcome Outcome(int partitionId, long index)
    {
        Partition p = Get(partitionId);
        lock (p.Gate)
            return p.Outcomes.TryGetValue(index, out ApplyOutcome o) ? o : ApplyOutcome.Unknown;
    }

    public string? Read(string key) => values.GetValueOrDefault(key);

    public IReadOnlyList<AppliedEntry> Entries(int partitionId)
    {
        Partition p = Get(partitionId);
        lock (p.Gate)
            return p.Applied.ToArray();
    }

    /// <summary>
    /// Blocks until this node has applied everything up to <paramref name="index"/>.
    /// </summary>
    /// <remarks>
    /// Polling rather than signalling is deliberate. The apply loop runs on
    /// Kommander's partition executor, and a waiter list touched from that
    /// thread turns a harness bug into a stalled partition — which would look
    /// exactly like a Kommander liveness failure and waste an investigation. A
    /// 1 ms poll costs nothing at the request rates these tests run at.
    /// </remarks>
    public async Task<bool> WaitApplied(int partitionId, long index, TimeSpan timeout, CancellationToken ct)
    {
        long deadline = Environment.TickCount64 + (long)timeout.TotalMilliseconds;

        while (AppliedIndex(partitionId) < index)
        {
            if (ct.IsCancellationRequested || Environment.TickCount64 > deadline)
                return false;

            await Task.Delay(1, CancellationToken.None).ConfigureAwait(false);
        }

        return true;
    }

    public static byte[] Encode(LogEntry entry) =>
        JsonSerializer.SerializeToUtf8Bytes(entry, JsonOptions);

    public static string Describe(byte[] data) => Encoding.UTF8.GetString(data);
}
