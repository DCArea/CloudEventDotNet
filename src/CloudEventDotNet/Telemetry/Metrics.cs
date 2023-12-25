using System.Diagnostics.Metrics;
using CloudEventDotNet.Diagnostics.Aggregators;

namespace CloudEventDotNet.Telemetry;

internal static class Metrics
{
    internal static Meter Meter { get; } = new("DCA.CloudEvents", "0.0.1");

    private static readonly CounterAggregatorGroup s_cloudEventsPublishedCounter = new(Meter, "dca_cloudevents_published");

    internal static readonly long[] buckets = [5, 20, 50, 100, 200, 500, 1_000, 2_000, 5_000, 10_000, 20_000, 60_000, 2 * 60_000, 5 * 60_000, 10 * 60_000];

    internal static readonly HistogramAggregatorGroup s_ProcessLatency =
        new(
            new HistogramAggregatorOptions(buckets),
            Meter, "dca_cloudevents_process_latency", "ms"
        );

    internal static readonly HistogramAggregatorGroup s_DeliveryTardiness =
        new(
            new HistogramAggregatorOptions(buckets),
            Meter, "dca_cloudevents_delivery_tardiness", "ms"
        );

    public static void CloudEventPublished(CloudEventMetadata metadata)
    {
        var counter = s_cloudEventsPublishedCounter.FindOrCreate(new("pubsub", metadata.PubSubName, "topic", metadata.Topic, "type", metadata.Type));
        counter.Add(1);
    }
}

internal class CloudEventMetricsContext
{
    public HistogramAggregator ProcessLatency { get; }
    public HistogramAggregator DeliveryTardiness { get; }

    public CloudEventMetricsContext(string pubsub, string topic, string type)
    {
        var tagList = new TagList("pubsub", pubsub, "topic", topic);

        DeliveryTardiness = Metrics.s_DeliveryTardiness.FindOrCreate(new("pubsub", pubsub, "topic", topic));
        ProcessLatency = Metrics.s_ProcessLatency.FindOrCreate(new("pubsub", pubsub, "topic", topic, "type", type));
    }
}
