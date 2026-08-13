using System.Net.Http.Json;
using System.Text.Json.Serialization;
using Kommander;
using Kommander.Data;
using Kommander.System;

namespace KommanderJepsen.Harness;

// --------------------------------------------------------------------------
// Wire types. Serialized with ASP.NET's web defaults, so every property below
// is camelCase on the wire.
// --------------------------------------------------------------------------

public sealed class ReadRequest
{
    public string Key { get; set; } = "";
}

public sealed class WriteRequest
{
    public string Key { get; set; } = "";
    public string? Value { get; set; }
}

public sealed class CasRequest
{
    public string Key { get; set; } = "";
    public string? Expected { get; set; }
    public string? Value { get; set; }
}

public sealed class AppendRequest
{
    public int Partition { get; set; }
    public string Value { get; set; } = "";
}

public sealed class OpResponse
{
    /// <summary>
    /// The single field every verdict in this suite hangs off. See
    /// <c>kommander.client/response-class</c> for the ok / fail / info mapping,
    /// and DESIGN.md for why getting it wrong invalidates a run in both
    /// directions.
    /// </summary>
    [JsonPropertyName("status")]
    public string Status { get; set; } = "";

    [JsonPropertyName("value")]
    public string? Value { get; set; }

    [JsonPropertyName("index")]
    public long Index { get; set; }

    [JsonPropertyName("partition")]
    public int Partition { get; set; }

    [JsonPropertyName("node")]
    public string? Node { get; set; }

    /// <summary>Set when the request was answered by a peer this node forwarded to.</summary>
    [JsonPropertyName("forwardedTo")]
    public string? ForwardedTo { get; set; }
}

public sealed class EntriesResponse
{
    public string Status { get; set; } = "";
    public int Partition { get; set; }
    public long AppliedIndex { get; set; }

    /// <summary>
    /// Duplicate or below-frontier deliveries this node saw. Exposed rather
    /// than asserted internally: a non-zero count is a finding about
    /// Kommander's exactly-once apply contract, and the checker should be the
    /// thing that says so.
    /// </summary>
    public long Redeliveries { get; set; }

    /// <summary>
    /// Indices this node was handed but could not decode. An entry missing from
    /// <see cref="Entries"/> means "Kommander never delivered it" only if its
    /// index is absent here too — otherwise the harness dropped it, and the
    /// finding is a different one.
    /// </summary>
    public List<long> Undecodable { get; set; } = [];

    /// <summary>
    /// Deliveries carrying another component's log type. Non-zero is normal.
    /// </summary>
    public long Foreign { get; set; }

    public List<EntryDto> Entries { get; set; } = [];
}

public sealed class EntryDto
{
    public long Index { get; set; }
    public string? Value { get; set; }
}

public sealed class MembershipResponse
{
    public string Status { get; set; } = "";
    public long MembershipVersion { get; set; }
    public string LocalRole { get; set; } = "";
    public string LocalEndpoint { get; set; } = "";
    public bool Joined { get; set; }
    public List<MemberDto> Members { get; set; } = [];
}

public sealed class MemberDto
{
    public string Endpoint { get; set; } = "";
    public int NodeId { get; set; }
    public string Role { get; set; } = "";
}

// --------------------------------------------------------------------------
// Handlers
// --------------------------------------------------------------------------

/// <summary>
/// The client API the Jepsen workloads drive: a linearizable register over the
/// replicated key/value state machine, and a raw per-partition log append.
/// </summary>
public sealed class Api(IRaft raft, StateMachine sm, HarnessOptions options, IHttpClientFactory clients, ILogger<Api> logger)
{
    /// <summary>Marks a request that a peer already forwarded, so forwarding never loops.</summary>
    public const string ForwardedHeader = "X-Jepsen-Forwarded";

    private static readonly TimeSpan ApplyWait = TimeSpan.FromSeconds(5);
    private static readonly TimeSpan LeaderWait = TimeSpan.FromSeconds(3);

    /// <summary>
    /// Kommander's <see cref="RaftOperationStatus"/> as a kebab-case string. The
    /// enum is reproduced on the wire verbatim rather than pre-classified here
    /// so that the ok/fail/info decision lives in exactly one reviewed place —
    /// <c>kommander.client/response-class</c> — instead of being split across
    /// two languages.
    /// </summary>
    private static string Kebab(RaftOperationStatus status) => status switch
    {
        RaftOperationStatus.Success => "success",
        RaftOperationStatus.Errored => "errored",
        RaftOperationStatus.NodeIsNotLeader => "node-is-not-leader",
        RaftOperationStatus.LeaderInOldTerm => "leader-in-old-term",
        RaftOperationStatus.LeaderAlreadyElected => "leader-already-elected",
        RaftOperationStatus.LogsFromAnotherLeader => "logs-from-another-leader",
        RaftOperationStatus.ActiveProposal => "active-proposal",
        RaftOperationStatus.ProposalNotFound => "proposal-not-found",
        RaftOperationStatus.ProposalTimeout => "proposal-timeout",
        RaftOperationStatus.ReplicationFailed => "replication-failed",
        RaftOperationStatus.Pending => "pending",
        RaftOperationStatus.ProposalQueueFull => "proposal-queue-full",
        RaftOperationStatus.RestoreInProgress => "restore-in-progress",
        RaftOperationStatus.PartitionMoved => "partition-moved",
        RaftOperationStatus.StaleMembership => "stale-membership",
        RaftOperationStatus.ConcurrentMembershipChange => "concurrent-membership-change",
        RaftOperationStatus.InsufficientVoters => "insufficient-voters",
        RaftOperationStatus.LogMismatch => "log-mismatch",
        RaftOperationStatus.SnapshotRequired => "snapshot-required",
        RaftOperationStatus.OperationCancelled => "operation-cancelled",
        _ => "unknown-" + (int)status
    };

    private OpResponse Local(string status) =>
        new() { Status = status, Node = raft.GetLocalNodeName() };

    // ----------------------------------------------------------------------
    // Routing
    // ----------------------------------------------------------------------

    /// <summary>
    /// Where should this partition's request be handled?
    /// </summary>
    /// <remarks>
    /// The Jepsen client talks to whichever node its process is bound to and
    /// stays there for the whole run, so the harness has to route. Returning
    /// "ask someone else" instead would turn every non-leader op into a
    /// <c>:fail</c> and the history would be mostly noise about routing rather
    /// than about consensus.
    /// </remarks>
    private async Task<(bool local, string? leader)> Route(int partitionId, CancellationToken ct)
    {
        if (await raft.AmILeaderQuick(partitionId).ConfigureAwait(false))
            return (true, raft.GetLocalEndpoint());

        try
        {
            using CancellationTokenSource cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            cts.CancelAfter(LeaderWait);

            string leader = await raft.WaitForLeader(partitionId, cts.Token).ConfigureAwait(false);

            if (string.IsNullOrEmpty(leader))
                return (false, null);

            return (string.Equals(leader, raft.GetLocalEndpoint(), StringComparison.Ordinal), leader);
        }
        catch (OperationCanceledException)
        {
            return (false, null);
        }
    }

    /// <summary>
    /// Proxies the request body to the partition leader's harness API.
    /// </summary>
    /// <remarks>
    /// A forward introduces its own indeterminacy: if the response is lost the
    /// proposal may still have committed. That is why a failed forward answers
    /// <c>forward-failed</c> (mapped to <c>:info</c>) and never <c>:fail</c>.
    /// </remarks>
    private async Task<OpResponse> Forward<T>(string leaderEndpoint, string path, T body, CancellationToken ct)
    {
        string host = leaderEndpoint.Split(':')[0];
        string url = $"http://{host}:{options.HttpPort}{path}";

        try
        {
            HttpClient http = clients.CreateClient("forward");

            using HttpRequestMessage request = new(HttpMethod.Post, url)
            {
                Content = JsonContent.Create(body)
            };
            request.Headers.Add(ForwardedHeader, "1");

            using HttpResponseMessage response = await http.SendAsync(request, ct).ConfigureAwait(false);

            if (!response.IsSuccessStatusCode)
                return Local("forward-failed");

            OpResponse? forwarded = await response.Content
                .ReadFromJsonAsync<OpResponse>(ct)
                .ConfigureAwait(false);

            if (forwarded is null)
                return Local("forward-failed");

            forwarded.ForwardedTo = leaderEndpoint;
            return forwarded;
        }
        catch (Exception ex)
        {
            logger.LogWarning("forward to {Leader} failed: {Message}", leaderEndpoint, ex.Message);
            return Local("forward-failed");
        }
    }

    // ----------------------------------------------------------------------
    // Key/value
    // ----------------------------------------------------------------------

    /// <summary>
    /// Which partition owns <paramref name="key"/>, or null if this node cannot
    /// say yet.
    /// </summary>
    /// <remarks>
    /// <c>GetPartitionKey</c> throws until the node has a partition map to
    /// resolve the key's hash range against, which is the normal state for the
    /// first seconds after boot and again while a split or merge is in flight.
    /// Letting that escape would surface as a 500 and be recorded as an
    /// unhandled exception rather than as the routing miss it is.
    /// </remarks>
    private int? PartitionFor(string key)
    {
        try
        {
            return raft.GetPartitionKey(key);
        }
        catch (RaftException)
        {
            return null;
        }
    }

    public async Task<OpResponse> Read(ReadRequest req, bool forwarded, CancellationToken ct)
    {
        if (PartitionFor(req.Key) is not int partitionId)
            return Local("no-partition-map");

        (bool local, string? leader) = await Route(partitionId, ct).ConfigureAwait(false);

        if (!local)
        {
            if (forwarded || leader is null)
                return Local(leader is null ? "no-leader" : "not-leader");

            return await Forward(leader, "/kv/read", req, ct).ConfigureAwait(false);
        }

        // A leader that has been partitioned into a minority keeps believing it
        // leads until it *receives* a higher-term message, so AmILeader alone
        // would happily serve stale state as an authoritative read. This is the
        // Raft read-index round; without it the register workload would report
        // stale reads that are the harness's fault, not Kommander's.
        if (!await raft.ConfirmLeadershipAsync(partitionId, ct).ConfigureAwait(false))
            return Local("leadership-unconfirmed");

        OpResponse response = Local("ok");
        response.Partition = partitionId;
        response.Value = sm.Read(req.Key);
        response.Index = sm.AppliedIndex(partitionId);
        return response;
    }

    public Task<OpResponse> Write(WriteRequest req, bool forwarded, CancellationToken ct) =>
        PartitionFor(req.Key) is int partitionId
            ? Propose(
                partitionId,
                new LogEntry { Op = "set", Key = req.Key, Value = req.Value },
                req, "/kv/write", forwarded, expectOutcome: false, ct)
            : Task.FromResult(Local("no-partition-map"));

    public Task<OpResponse> Cas(CasRequest req, bool forwarded, CancellationToken ct) =>
        PartitionFor(req.Key) is int partitionId
            ? Propose(
                partitionId,
                new LogEntry { Op = "cas", Key = req.Key, Expected = req.Expected, Value = req.Value },
                req, "/kv/cas", forwarded, expectOutcome: true, ct)
            : Task.FromResult(Local("no-partition-map"));

    public Task<OpResponse> Append(AppendRequest req, bool forwarded, CancellationToken ct)
    {
        // Partition 0 is reserved for replicated system configuration; a client
        // proposal there is rejected by Kommander, and silently accepting the
        // request would produce a run where every append failed for a reason
        // that has nothing to do with the property under test.
        //
        // Application partitions are 1..InitialPartitions *inclusive* — the
        // system partition is extra, not one of the N. An exclusive bound here
        // silently drops the last partition from the workload's key space.
        if (req.Partition < 1 || req.Partition > options.InitialClusterPartitions)
            return Task.FromResult(Local("invalid-partition"));

        return Propose(
            req.Partition,
            new LogEntry { Op = "append", Value = req.Value },
            req, "/log/append", forwarded, expectOutcome: false, ct);
    }

    /// <summary>
    /// The single write path: route, replicate, wait for local apply, report.
    /// </summary>
    private async Task<OpResponse> Propose<TRequest>(
        int partitionId,
        LogEntry entry,
        TRequest request,
        string path,
        bool forwarded,
        bool expectOutcome,
        CancellationToken ct)
    {
        (bool local, string? leader) = await Route(partitionId, ct).ConfigureAwait(false);

        if (!local)
        {
            if (forwarded || leader is null)
                return Local(leader is null ? "no-leader" : "not-leader");

            return await Forward(leader, path, request, ct).ConfigureAwait(false);
        }

        RaftReplicationResult result;
        try
        {
            result = await raft
                .ReplicateLogs(partitionId, StateMachine.JepsenLogType, StateMachine.Encode(entry), autoCommit: true, cancellationToken: ct)
                .ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            // The proposal may have been appended and may still commit. There is
            // exactly one safe answer here and it is not ":fail".
            logger.LogWarning("replicate threw on partition {Partition}: {Message}", partitionId, ex.Message);
            return Local("errored");
        }

        OpResponse response = Local(Kebab(result.Status));
        response.Partition = partitionId;
        response.Index = result.LogIndex;

        if (!result.Success)
            return response;

        bool applied = await sm.WaitApplied(partitionId, result.LogIndex, ApplyWait, ct).ConfigureAwait(false);

        if (!expectOutcome)
        {
            // Quorum committed the entry, so it *is* the cluster's state whether
            // or not this node's apply loop has caught up. Reporting :info here
            // would be conservative to the point of dishonesty and would bloat
            // Knossos's search for nothing.
            response.Status = "ok";
            return response;
        }

        if (!applied)
        {
            // The entry committed, but a conditional entry's *effect* is only
            // known once it is applied — this node cannot yet say whether the
            // comparison held.
            response.Status = "apply-timeout";
            return response;
        }

        response.Status = sm.Outcome(partitionId, result.LogIndex) switch
        {
            ApplyOutcome.Applied => "ok",
            ApplyOutcome.Mismatch => "cas-mismatch",
            _ => "apply-timeout"
        };

        return response;
    }

    // ----------------------------------------------------------------------
    // Introspection
    // ----------------------------------------------------------------------

    public EntriesResponse Entries(int partitionId)
    {
        EntriesResponse response = new()
        {
            Status = "ok",
            Partition = partitionId,
            AppliedIndex = sm.AppliedIndex(partitionId),
            Redeliveries = sm.Redeliveries(partitionId),
            Undecodable = [.. sm.Undecodable(partitionId)],
            Foreign = sm.Foreign
        };

        foreach (StateMachine.AppliedEntry e in sm.Entries(partitionId))
            response.Entries.Add(new EntryDto { Index = e.Index, Value = e.Value });

        return response;
    }

    public MembershipResponse Membership()
    {
        ClusterMembership membership = raft.GetMembership();

        MembershipResponse response = new()
        {
            Status = "ok",
            MembershipVersion = membership.MembershipVersion,
            LocalRole = raft.LocalRole.ToString(),
            LocalEndpoint = raft.GetLocalEndpoint(),
            Joined = raft.Joined
        };

        foreach (ClusterMember m in membership.Members)
            response.Members.Add(new MemberDto
            {
                Endpoint = m.Endpoint,
                NodeId = m.NodeId,
                Role = m.Role.ToString()
            });

        return response;
    }
}
