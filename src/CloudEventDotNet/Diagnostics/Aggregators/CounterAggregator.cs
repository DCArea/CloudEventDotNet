using System.Diagnostics.Metrics;

namespace CloudEventDotNet.Diagnostics.Aggregators;

public sealed class CounterAggregator
{
    private readonly KeyValuePair<string, object?>[] _tags;
    private readonly ObservableCounter<long>? _instrument;
    private long _value = 0;

    public CounterAggregator(
        in TagList tagList,
        Meter meter,
        string name,
        string? unit = null,
        string? description = null) : this(tagList)
    {
        _instrument = meter.CreateObservableCounter(name, Collect, unit, description);
    }

    public CounterAggregator(Meter meter, string name, string? unit = null, string? description = null)
        : this(new TagList(), meter, name, unit, description) { }

    public CounterAggregator() : this(new TagList()) { }

    public CounterAggregator(in TagList tagList)
    {
        _tags = tagList.ToArray();
    }

    public bool Enabled => _instrument?.Enabled ?? false;

    public long Value => _value;

    public void Add(long measurement) => Interlocked.Add(ref _value, measurement);

    public Measurement<long> Collect() => new(_value, _tags);
}
