using CommandLine;

namespace KommanderJepsen.Harness;

/// <summary>
/// Command line for the Jepsen harness node. Mirrors Kommander.Server's options
/// where they overlap so a harness invocation reads like a real deployment, and
/// adds the few knobs the tests need (join-existing, graceful leave, transport).
/// </summary>
public sealed class HarnessOptions
{
    [Option("raft-nodename", Required = false, HelpText = "Unique Raft node name")]
    public string RaftNodeName { get; set; } = "";

    [Option("raft-nodeid", Required = false, HelpText = "Unique Raft node id", Default = 1)]
    public int RaftNodeId { get; set; } = 1;

    [Option("raft-host", Required = false, HelpText = "Host advertised to peers for Raft traffic", Default = "localhost")]
    public string RaftHost { get; set; } = "localhost";

    [Option("raft-port", Required = false, HelpText = "Port serving Raft consensus/replication traffic", Default = 8082)]
    public int RaftPort { get; set; } = 8082;

    [Option("http-port", Required = false, HelpText = "Port serving the plain-HTTP client API the tests drive", Default = 8081)]
    public int HttpPort { get; set; } = 8081;

    [Option("initial-cluster", Required = false, HelpText = "Peer endpoints (host:port) for static discovery, excluding self")]
    public IEnumerable<string>? InitialCluster { get; set; }

    [Option("initial-cluster-partitions", Required = false, HelpText = "Number of Raft partitions", Default = 4)]
    public int InitialClusterPartitions { get; set; } = 4;

    [Option("wal-path", Required = false, HelpText = "RocksDB WAL directory", Default = "/opt/kommander/wal")]
    public string WalPath { get; set; } = "/opt/kommander/wal";

    [Option("wal-revision", Required = false, HelpText = "WAL revision", Default = "v1")]
    public string WalRevision { get; set; } = "v1";

    /// <summary>
    /// Whether the WAL fsyncs before acknowledging. Left on by default: a node
    /// SIGKILLed without an fsynced WAL may legitimately lose acknowledged
    /// writes, and a run that finds that has found a fact about this flag, not
    /// about Kommander.
    /// </summary>
    [Option("disable-wal-sync-writes", Required = false, HelpText = "Run without WAL fsync (expect data loss on kill)", Default = false)]
    public bool DisableWalSyncWrites { get; set; }

    /// <summary>
    /// grpc (default, the production transport) or rest.
    /// </summary>
    [Option("transport", Required = false, HelpText = "Inter-node transport: grpc or rest", Default = "grpc")]
    public string Transport { get; set; } = "grpc";

    /// <summary>
    /// Start by asking the seeds in --initial-cluster for admission instead of
    /// booting as a static-discovery member. Only meaningful for a node that
    /// previously left the roster; see src/kommander/nemesis/membership.clj.
    /// </summary>
    [Option("join-existing", Required = false, HelpText = "Join an existing cluster as a Learner via the seed endpoints", Default = false)]
    public bool JoinExisting { get; set; }

    /// <summary>
    /// Commit RemoveMember(self) from the shutdown hook. This is a *start* flag:
    /// a node booted without it will never shrink the roster no matter how
    /// politely it is later asked to stop. Only reachable via SIGTERM, so the
    /// :kill fault still models a crash rather than a polite departure.
    /// </summary>
    [Option("graceful-leave-on-shutdown", Required = false, HelpText = "Commit RemoveMember(self) on SIGTERM", Default = false)]
    public bool GracefulLeaveOnShutdown { get; set; }
}
