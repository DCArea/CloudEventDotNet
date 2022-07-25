using System.Diagnostics.Metrics;

namespace CloudEventDotNet.Diagnostics.Aggregators;

public class HistogramAggregator
{
    private readonly KeyValuePair<string, object?>[] _tags;
    private readonly HistogramBucketAggregator[] _buckets;
    private readonly HistogramAggregatorOptions _options;
    private long _count;
    private long _sum;

    public HistogramAggregator(
        in TagList tagList,
        HistogramAggregatorOptions options,
        Meter meter,
        string name,
        string? unit = null,
        string? description = null) : this(tagList, options)
    {
        meter.CreateObservableCounter(name + "-bucket", CollectBuckets, description: description);
        meter.CreateObservableCounter(name + "-count", CollectCount, description: description);
        meter.CreateObservableCounter(name + "-sum", CollectSum, unit, description);
    }

    public HistogramAggregator(
        in TagList tagList,
        HistogramAggregatorOptions options)
    {
        _tags = tagList.ToArray();

        long[] buckets = options.Buckets;
        if (buckets[^1] != long.MaxValue)
        {
            buckets = buckets.Concat(new[] { long.MaxValue }).ToArray();
        }

        Func<long, KeyValuePair<string, object?>> getLabel;
        if (options.GetLabel is not null)
        {
            getLabel = options.GetLabel;
        }
        else
        {
            if (options.AggregationType == HistogramAggregationType.Cumulative)
            {
                getLabel = b => b == long.MaxValue ? new("le", "+Inf") : new("le", b);
            }
            else
            {
                getLabel = b => new("bucket", b);
            }
        }
        _buckets = buckets.Select(b => new HistogramBucketAggregator(_tags, b, getLabel(b))).ToArray();
        _options = options;
    }

    public void Record(long number)
    {
        int i;
        for (i = 0; i < _buckets.Length; i++)
        {
            if (number <= _buckets[i].Bound)
            {
                break;
            }
        }
        _buckets[i].Add(1);
        Interlocked.Increment(ref _count);
        Interlocked.Add(ref _sum, number);
    }

    public IEnumerable<Measurement<long>> CollectBuckets()
    {
        return _options.AggregationType switch
        {
            HistogramAggregationType.Delta => CollectDelta(),
            _ => CollectCumulative(),
        };

        IEnumerable<Measurement<long>> CollectDelta()
        {
            foreach (var bucket in _buckets)
            {
                yield return new(bucket.Value, bucket.Tags);
            }
        }

        IEnumerable<Measurement<long>> CollectCumulative()
        {
            long sum = 0;
            foreach (var bucket in _buckets)
            {
                sum += bucket.Value;
                yield return new(sum, bucket.Tags);
            }
        }
    }

    public Measurement<long> CollectCount() => new(_count, _tags);

    public Measurement<long> CollectSum() => new(_sum, _tags);
}
