using System.Diagnostics.Metrics;

namespace CloudEventDotNet.Diagnostics.Aggregators;

internal class HistogramBucketAggregator
{
    private long _value = 0;
    private readonly KeyValuePair<string, object?>[] _tags;
    public double Bound { get; }

    public HistogramBucketAggregator(
        KeyValuePair<string, object?>[] tags,
        double bound,
        KeyValuePair<string, object?> label)
    {
        _tags = tags.Concat(new[] { label }).ToArray();
        Bound = bound;
    }

    public ReadOnlySpan<KeyValuePair<string, object?>> Tags => _tags;

    public long Value => _value;

    public void Add(long measurement) => Interlocked.Add(ref _value, measurement);
}
