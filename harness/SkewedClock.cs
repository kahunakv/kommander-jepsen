namespace KommanderJepsen.Harness;

/// <summary>
/// The physical-time source behind this node's hybrid logical clock, with an offset the Jepsen
/// <c>:skew</c> nemesis can move at run time.
/// </summary>
/// <remarks>
/// <para>
/// <b>Why this exists instead of Jepsen's clock nemesis.</b> <c>jepsen.nemesis.time</c> calls
/// <c>settimeofday</c>, and every container on a host shares one kernel clock. Bumping n1 bumps
/// n2..n5 too, so in Docker it produces a cluster-wide jump and never skew <em>between</em> nodes —
/// a run with it enabled passes while testing nothing it claims to. Skewing the HLC's physical
/// input per process gives real per-node skew with no privileges, on Docker Desktop and CI alike.
/// </para>
/// <para>
/// <b>Only the HLC is skewed.</b> Every elapsed-time gate in Kommander (election timeouts,
/// heartbeat freshness, the leadership lease) runs on <c>Stopwatch</c> ticks, which
/// <c>settimeofday</c> does not move either, and the HLC is the one consensus component that
/// reads wall time. So this covers what the kernel-clock fault would cover, per node.
/// </para>
/// <para>
/// <b>A restart clears the skew.</b> The offset lives in this process, so a node that is killed
/// while skewed comes back on true time. That is a wall-clock correction across a restart — the
/// exact case Kommander's persisted HLC floor exists for — which is why <c>skew,kill</c> is worth
/// running together.
/// </para>
/// </remarks>
public sealed class SkewedClock
{
    /// <summary>
    /// Largest offset accepted, either way. The HLC throws <c>Corrupted HLC clock</c> on every event
    /// once its physical component leaves (0, 2^42) ms, which would turn a typo into a node that
    /// crashes on every message. Ten days is far past any skew NTP would leave standing.
    /// </summary>
    public const long MaxOffsetMs = 10L * 24 * 60 * 60 * 1000;

    /// <summary>
    /// One immutable snapshot, swapped whole, so a reader never sees an offset from one setting
    /// paired with a strobe from another.
    /// </summary>
    /// <param name="OffsetMs">Constant offset added to the wall clock.</param>
    /// <param name="StrobeDeltaMs">Extra offset applied on every other strobe period.</param>
    /// <param name="StrobePeriodMs">Length of one strobe half-cycle; 0 means no strobe.</param>
    /// <param name="StrobeUntilTicks">
    /// <see cref="Environment.TickCount64"/> at which the strobe stops. Monotonic on purpose: a
    /// deadline on the skewed clock would move with the skew it is timing.
    /// </param>
    private sealed record Skew(long OffsetMs, long StrobeDeltaMs, long StrobePeriodMs, long StrobeUntilTicks);

    private static readonly Skew None = new(0, 0, 0, 0);

    private volatile Skew current = None;

    /// <summary>Unix milliseconds as this node's HLC should see them.</summary>
    public long NowMs() => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + CurrentOffsetMs();

    /// <summary>The offset in force right now, strobe included.</summary>
    public long CurrentOffsetMs()
    {
        Skew s = current;

        // Odd half-cycles carry the extra delta, even ones do not: the clock jumps by
        // StrobeDeltaMs and back again every StrobePeriodMs until the strobe ends.
        bool shifted = Strobing(s, out long ticks) && ((ticks / s.StrobePeriodMs) & 1) == 1;
        return s.OffsetMs + (shifted ? s.StrobeDeltaMs : 0);
    }

    /// <summary>
    /// Replaces the skew. A zero <paramref name="strobePeriodMs"/> or duration sets a constant
    /// offset only. Returns an error string, or null when the setting was accepted.
    /// </summary>
    public string? Set(long offsetMs, long strobeDeltaMs, long strobePeriodMs, long strobeDurationMs)
    {
        if (Math.Abs(offsetMs) > MaxOffsetMs || Math.Abs(offsetMs + strobeDeltaMs) > MaxOffsetMs)
            return "offset-out-of-range";

        if (strobePeriodMs < 0 || strobeDurationMs < 0)
            return "invalid-strobe";

        current = strobePeriodMs > 0 && strobeDurationMs > 0
            ? new Skew(offsetMs, strobeDeltaMs, strobePeriodMs, Environment.TickCount64 + strobeDurationMs)
            : new Skew(offsetMs, 0, 0, 0);

        return null;
    }

    public ClockState Describe()
    {
        Skew s = current;
        return new ClockState
        {
            OffsetMs = s.OffsetMs,
            Strobing = Strobing(s, out _),
            StrobeDeltaMs = s.StrobeDeltaMs,
            StrobePeriodMs = s.StrobePeriodMs
        };
    }

    private static bool Strobing(Skew s, out long ticks)
    {
        ticks = Environment.TickCount64;
        return s.StrobePeriodMs > 0 && ticks < s.StrobeUntilTicks;
    }
}

public sealed class ClockRequest
{
    public long OffsetMs { get; set; }
    public long StrobeDeltaMs { get; set; }
    public long StrobePeriodMs { get; set; }
    public long StrobeDurationMs { get; set; }
}

public sealed class ClockState
{
    public string Status { get; set; } = "ok";
    public long OffsetMs { get; set; }
    public bool Strobing { get; set; }
    public long StrobeDeltaMs { get; set; }
    public long StrobePeriodMs { get; set; }

    /// <summary>True wall-clock time on this node when the state was read.</summary>
    public long RealMs { get; set; }

    /// <summary>
    /// A timestamp minted from this node's HLC as the state was read. Minting is a local event and
    /// installs the stamp like any other, so reading it cannot produce a duplicate.
    /// </summary>
    public long HlcL { get; set; }
    public uint HlcC { get; set; }

    /// <summary>
    /// How far this node's HLC runs ahead of true time. The evidence that skew actually
    /// propagated: a node with no offset of its own and a large lead here was pulled forward by a
    /// peer, which is the HLC doing its job — and what makes a forward-skewed node matter to the
    /// rest of the cluster.
    /// </summary>
    public long HlcLeadMs { get; set; }
}
