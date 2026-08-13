using Kommander;

namespace KommanderJepsen.Harness;

/// <summary>
/// Drives the node's cluster lifecycle: join on start, optionally leave on stop.
/// </summary>
/// <remarks>
/// <para>
/// Kommander changes membership through <see cref="IRaft.JoinCluster()"/> and
/// <see cref="IRaft.LeaveCluster"/>, not through an HTTP endpoint, so the
/// membership nemesis drives both through the process lifecycle: a leave is
/// SIGTERM-and-wait and a join is a restart with <c>--join-existing</c>. See
/// <c>src/kommander/nemesis/membership.clj</c>.
/// </para>
/// <para>
/// The leave runs from <see cref="StopAsync"/>, which SIGKILL never reaches, so
/// the <c>:kill</c> fault still models a crash rather than a polite departure.
/// </para>
/// </remarks>
/// <para>
/// It is a <see cref="BackgroundService"/> rather than a plain
/// <see cref="IHostedService"/> for a reason worth remembering: under minimal
/// hosting the web host's own hosted service is registered during
/// <c>builder.Build()</c>, i.e. <i>after</i> anything registered with
/// <c>AddHostedService</c>. A hosted service that blocks in <c>StartAsync</c>
/// therefore delays Kestrel from binding — and since joining a cluster means
/// waiting for peers who are themselves waiting to bind, every node blocks
/// forever with no port open and no error. <c>BackgroundService.StartAsync</c>
/// returns as soon as <c>ExecuteAsync</c> first yields, so the join proceeds
/// concurrently with the listener coming up.
/// </para>
public sealed class ClusterService(IRaft raft, HarnessOptions options, ILogger<ClusterService> logger) : BackgroundService
{
    /// <summary>
    /// How long a graceful leave may take before the host gives up on it.
    /// </summary>
    /// <remarks>
    /// This has to fit inside the host's own shutdown budget *and* inside the
    /// SIGTERM grace period <c>kommander.db/graceful-stop-timeout-s</c> allows,
    /// or the node is SIGKILLed mid-leave and the roster never shrinks — which
    /// looks exactly like a server-side membership bug and is not one.
    /// </remarks>
    private static readonly TimeSpan LeaveDeadline = TimeSpan.FromSeconds(20);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // Yield before touching Raft so the host can get on with binding
        // Kestrel; see the note on the class.
        await Task.Yield();

        List<string> seeds = options.InitialCluster?.ToList() ?? [];

        try
        {
            if (options.JoinExisting)
            {
                logger.LogInformation("joining existing cluster via seeds {Seeds}", string.Join(",", seeds));
                await raft.JoinCluster(seeds, stoppingToken).ConfigureAwait(false);
            }
            else
            {
                logger.LogInformation("joining cluster via static discovery");
                await raft.JoinCluster(stoppingToken).ConfigureAwait(false);
            }

            logger.LogInformation("node {Node} joined as {Role}", raft.GetLocalNodeName(), raft.LocalRole);
        }
        catch (OperationCanceledException)
        {
            // Shutting down mid-join.
        }
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        await base.StopAsync(cancellationToken).ConfigureAwait(false);

        if (!options.GracefulLeaveOnShutdown)
            return;

        logger.LogInformation("committing RemoveMember(self) before shutdown");

        try
        {
            using CancellationTokenSource cts = new(LeaveDeadline);
            await raft.LeaveCluster(dispose: false, cts.Token).ConfigureAwait(false);
            logger.LogInformation("left the cluster");
        }
        catch (Exception ex)
        {
            // A leave that does not commit must not stop the process from
            // exiting: the nemesis verifies the roster against a *survivor*
            // afterwards and records the outcome in the history.
            logger.LogWarning("graceful leave failed: {Message}", ex.Message);
        }
    }
}
