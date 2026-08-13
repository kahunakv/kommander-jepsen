using System.Net;
using CommandLine;
using Kommander;
using Kommander.Communication;
using Kommander.Communication.Grpc;
using Kommander.Communication.Rest;
using Kommander.Data;
using Kommander.Discovery;
using Kommander.Time;
using Kommander.WAL;
using KommanderJepsen.Harness;
using Microsoft.AspNetCore.Server.Kestrel.Core;

ParserResult<HarnessOptions> parsed = Parser.Default.ParseArguments<HarnessOptions>(args);

HarnessOptions? opts = parsed.Value;
if (opts is null)
    return 1;

List<RaftNode> peers = (opts.InitialCluster ?? []).Select(e => new RaftNode(e)).ToList();

if (peers.Count < 1)
{
    Console.Error.WriteLine("--initial-cluster must list at least one peer (this node excluded)");
    return 1;
}

// Plaintext HTTP/2 for the inter-node gRPC transport. TLS is deliberately out
// of the picture: a handshake failure and a partition are indistinguishable in
// a history, and the point of a network fault here is to break replication, not
// certificate validation.
AppContext.SetSwitch("System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true);

// RocksDB creates its own revision subdirectory but not the parents above it,
// and the failure is a bare "No such file or directory" from native code at
// host-start time. Create the tree here so a fresh node never dies at boot for
// a reason that has nothing to do with the test.
Directory.CreateDirectory(opts.WalPath);

RaftConfiguration configuration = new()
{
    NodeName = string.IsNullOrEmpty(opts.RaftNodeName) ? Environment.MachineName : opts.RaftNodeName,
    NodeId = opts.RaftNodeId,
    Host = opts.RaftHost,
    Port = opts.RaftPort,
    InitialPartitions = opts.InitialClusterPartitions,
    GrpcScheme = "http://",
    HttpScheme = "http://",
    TransportSecurity = new RaftTransportSecurityOptions
    {
        // Kommander requires TLS on the inter-node transport by default and
        // refuses to construct with a plain-HTTP scheme. Turned off here for
        // the reason above: TLS failures and network partitions look identical
        // in a history, and only one of them is the fault under test. Node
        // authentication is a separate axis and is left at its default.
        RequireTls = false
    }
};

WebApplicationBuilder builder = WebApplication.CreateBuilder(args);

builder.Logging.AddSimpleConsole(o =>
{
    o.TimestampFormat = "yyyy-MM-ddTHH:mm:ss.fffZ ";
    o.UseUtcTimestamp = true;
    o.SingleLine = true;
});

StateMachine stateMachine = new();

builder.Services.AddSingleton(opts);
builder.Services.AddSingleton(stateMachine);
builder.Services.AddSingleton<Api>();

builder.Services.AddHttpClient("forward", client =>
{
    // Shorter than the Jepsen client's own socket timeout so a stuck leader
    // surfaces as a `forward-failed` status rather than as a client-side
    // timeout; both classify as :info, but the former says where it broke.
    client.Timeout = TimeSpan.FromSeconds(4);
});

builder.Services.AddSingleton<IRaft>(services =>
{
    ILogger<IRaft> logger = services.GetRequiredService<ILogger<IRaft>>();

    ICommunication communication = opts.Transport.Equals("rest", StringComparison.OrdinalIgnoreCase)
        ? new RestCommunication()
        : new GrpcCommunication();

    RaftManager manager = new(
        configuration,
        new StaticDiscovery(peers),
        new RocksDbWAL(
            path: opts.WalPath,
            revision: opts.WalRevision,
            logger,
            syncWrites: !opts.DisableWalSyncWrites),
        communication,
        new HybridLogicalClock(),
        logger
    );

    // Committed entries reach the state machine here, exactly once and in log
    // order per partition, on every node including the leader.
    manager.OnReplicationReceived += (partitionId, log) =>
        Task.FromResult(stateMachine.Apply(partitionId, log));

    // Restart replay. Same code path on purpose: a divergence between "applied
    // live" and "applied on restore" is precisely the kind of bug that would
    // otherwise be attributed to Kommander.
    manager.OnLogRestored += (partitionId, log) =>
        Task.FromResult(stateMachine.Apply(partitionId, log));

    manager.OnReplicationError += (partitionId, log) =>
        logger.LogWarning("replication error on partition {Partition}: {LogType} #{Id}", partitionId, log.LogType, log.Id);

    return manager;
});

builder.Services.AddHostedService<ClusterService>();
builder.Services.AddKommanderGrpc();

builder.WebHost.ConfigureKestrel(kestrel =>
{
    // Raft traffic: h2c only. Separating it from the client port is what lets a
    // partition of the Raft port break replication while the client connection
    // stays up, so an operation's outcome is decided by consensus rather than
    // by the test's own transport dying.
    kestrel.Listen(IPAddress.Any, opts.RaftPort, listen =>
    {
        listen.Protocols = HttpProtocols.Http2;
    });

    // Client API: plain HTTP/1.1.
    kestrel.Listen(IPAddress.Any, opts.HttpPort, listen =>
    {
        listen.Protocols = HttpProtocols.Http1;
    });
});

ThreadPool.SetMinThreads(128, 128);

WebApplication app = builder.Build();

app.MapRestRaftRoutes();
app.MapGrpcRaftRoutes();

static bool Forwarded(HttpRequest request) => request.Headers.ContainsKey(Api.ForwardedHeader);

app.MapGet("/health", (IRaft raft) => Results.Ok(new
{
    status = "ok",
    node = raft.GetLocalNodeName(),
    endpoint = raft.GetLocalEndpoint(),
    joined = raft.Joined
}));

app.MapGet("/cluster/membership", (Api api) => Results.Ok(api.Membership()));

app.MapPost("/kv/read", async (ReadRequest request, Api api, HttpContext ctx) =>
    Results.Ok(await api.Read(request, Forwarded(ctx.Request), ctx.RequestAborted)));

app.MapPost("/kv/write", async (WriteRequest request, Api api, HttpContext ctx) =>
    Results.Ok(await api.Write(request, Forwarded(ctx.Request), ctx.RequestAborted)));

app.MapPost("/kv/cas", async (CasRequest request, Api api, HttpContext ctx) =>
    Results.Ok(await api.Cas(request, Forwarded(ctx.Request), ctx.RequestAborted)));

app.MapPost("/log/append", async (AppendRequest request, Api api, HttpContext ctx) =>
    Results.Ok(await api.Append(request, Forwarded(ctx.Request), ctx.RequestAborted)));

app.MapGet("/log/entries/{partition:int}", (int partition, Api api) =>
    Results.Ok(api.Entries(partition)));

Console.WriteLine("Kommander Jepsen harness: raft={0}:{1} http={2} partitions={3} transport={4}",
    configuration.Host, configuration.Port, opts.HttpPort, configuration.InitialPartitions, opts.Transport);

await app.RunAsync();

return 0;
