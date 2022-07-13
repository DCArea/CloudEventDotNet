
using System.Diagnostics.Metrics;
using DCA.DotNet.Extensions.CloudEvents.Diagnostics.Aggregators;

namespace DCA.DotNet.Extensions.CloudEvents;

internal static class Metrics
{
    public static Meter Meter = new("DCA.CloudEvents", "0.0.1");

    private static readonly CounterAggregatorGroup s_cloudEventsPublished = new(Meter, "dca_cloudevents_published");
    public static void OnCloudEventPublished(CloudEventMetadata metadata)
    {
        var aggregator = s_cloudEventsPublished.FindOrCreate(new("pubsub", metadata.PubSubName, "topic", metadata.Topic, "type", metadata.Type));
        aggregator.Add(1);
    }

    private static readonly HistogramAggregatorGroup s_cloudEventsDeliveryLatency =
        new(
            new HistogramAggregatorOptions(new long[] { 5, 20, 50, 100, 200, 500, 1_000, 2_000, 5_000, 10_000, 20_000, 60_000, 2 * 60_000, 5 * 60_000, 10 * 60_000 }),
            Meter, "dca_cloudevents_delivery_latency", "ms"
        );

    public static void OnCloudEventProcessed(CloudEventMetadata metadata, TimeSpan latency)
    {
        var aggregator = s_cloudEventsDeliveryLatency.FindOrCreate(new("pubsub", metadata.PubSubName, "topic", metadata.Topic, "type", metadata.Type));
        aggregator.Record((long)latency.TotalMilliseconds);
    }
}
