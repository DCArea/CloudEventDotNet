using System.Collections.Concurrent;
using System.Diagnostics.Metrics;

namespace DCA.DotNet.Extensions.CloudEvents.Diagnostics.Aggregators;


internal class HistogramAggregatorGroup
{
    private readonly HistogramAggregatorOptions _options;

    internal ConcurrentDictionary<TagList, HistogramAggregator> Aggregators { get; } = new();
    public HistogramAggregatorGroup(
        HistogramAggregatorOptions options,
        Meter meter,
        string name,
        string? unit = null,
        string? description = null) : this(options)
    {
        meter.CreateObservableCounter(name + "-bucket", CollectBuckets, description: description);
        meter.CreateObservableCounter(name + "-count", CollectCount, description: description);
        meter.CreateObservableCounter(name + "-sum", CollectSum, unit, description);
    }

    public HistogramAggregatorGroup(HistogramAggregatorOptions options)
    {
        _options = options;
    }

    public HistogramAggregator FindOrCreate(in TagList tagList)
    {
        if (Aggregators.TryGetValue(tagList, out var stat))
        {
            return stat;
        }
        return Aggregators.GetOrAdd(tagList, new HistogramAggregator(tagList, _options));
    }

    public IEnumerable<Measurement<long>> CollectBuckets()
    {
        return Aggregators.Values.SelectMany(x => x.CollectBuckets());
    }

    public IEnumerable<Measurement<long>> CollectCount()
    {
        return Aggregators.Values.Select(x => x.CollectCount());
    }

    public IEnumerable<Measurement<long>> CollectSum()
    {
        return Aggregators.Values.Select(x => x.CollectSum());
    }
}
