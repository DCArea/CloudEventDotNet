
using System.Diagnostics;
using System.Diagnostics.Metrics;
using DCA.DotNet.Extensions.CloudEvents.Diagnostics.Aggregators;
using Microsoft.Extensions.Logging;

namespace DCA.DotNet.Extensions.CloudEvents;

internal static partial class CloudEventInstruments
{
    internal static Meter s_meter = new("DCA.CloudEvents", "0.0.1");
    internal static ActivitySource s_source = new("DCA.CloudEvents");

    public static readonly CounterAggregatorGroup MessageChannelRead = new(s_meter, "dca_cloudevents_message_channel_read");

    private static readonly CounterAggregatorGroup s_cloudEventsPublished = new(s_meter, "dca_cloudevents_published");
    public static void OnCloudEventPublished(CloudEventMetadata metadata)
    {
        var aggregator = s_cloudEventsPublished.FindOrCreate(new("pubsub", metadata.PubSubName, "topic", metadata.Topic, "type", metadata.Type));
        aggregator.Add(1);
    }

    private static readonly HistogramAggregatorGroup s_cloudEventsDeliveryLatency =
        new(
            new HistogramAggregatorOptions(new long[] { 5, 20, 50, 100, 200, 500, 1_000, 2_000, 5_000, 10_000, 20_000, 60_000, 2 * 60_000, 5 * 60_000, 10 * 60_000 }),
            s_meter, "dca_cloudevents_delivery_latency", "ms"
        );

    public static void OnCloudEventProcessed(CloudEventMetadata metadata, TimeSpan latency)
    {
        var aggregator = s_cloudEventsDeliveryLatency.FindOrCreate(new("pubsub", metadata.PubSubName, "topic", metadata.Topic, "type", metadata.Type));
        aggregator.Record((long)latency.TotalMilliseconds);
    }


    public static Activity? OnCloudEventPublishing<TData>(CloudEventMetadata metadata, CloudEvent<TData> cloudEvent)
    {
        var activity = s_source.StartActivity($"CloudEvents Create  {cloudEvent.Type}", ActivityKind.Producer);
        if (activity is not null)
        {
            activity.SetTag("messaging.system", metadata.PubSubName);
            activity.SetTag("messaging.destination", metadata.Topic);
            activity.SetTag("messaging.destination_kind", "topic");

            activity.SetTag("cloudevents.event_id", cloudEvent.Id.ToString());
            activity.SetTag("cloudevents.event_source", cloudEvent.Source);
            activity.SetTag("cloudevents.event_type", cloudEvent.Type);
            if (cloudEvent.Subject is not null)
            {
                activity.SetTag("cloudevents.event_subject", cloudEvent.Subject);
            }
            if (cloudEvent.Extensions is null)
            {
                cloudEvent.Extensions = new();
            }
            cloudEvent.Extensions["traceparent"] = activity.Id;
            cloudEvent.Extensions["tracestate"] = activity.TraceStateString;
        }
        return activity;
    }


    public static Activity? OnProcess(CloudEventMetadata metadata, CloudEvent cloudEvent)
    {
        ActivityContext parentContext = default;
        if (cloudEvent.Extensions is not null && cloudEvent.Extensions.Count != 0)
        {
            cloudEvent.Extensions.TryGetValue("traceparent", out var traceparent);
            cloudEvent.Extensions.TryGetValue("tracestate", out var tracestate);

            ActivityContext.TryParse(traceparent.GetString(), tracestate.GetString(), out parentContext);
        }
        var activity = s_source.StartActivity($"CloudEvents Process  {cloudEvent.Type}", ActivityKind.Consumer, parentContext);
        if (activity is not null)
        {
            activity.SetTag("messaging.system", metadata.PubSubName);
            activity.SetTag("messaging.destination", metadata.Topic);
            activity.SetTag("messaging.destination_kind", "topic");

            activity.SetTag("cloudevents.event_id", cloudEvent.Id.ToString());
            activity.SetTag("cloudevents.event_source", cloudEvent.Source);
            activity.SetTag("cloudevents.event_type", cloudEvent.Type);
            if (cloudEvent.Subject is not null)
            {
                activity.SetTag("cloudevents.event_subject", cloudEvent.Subject);
            }
        }
        return activity;
    }


    [LoggerMessage(
        Level = LogLevel.Debug,
        Message = "Failed to process CloudEvent {Type}:{Id}"
    )]
    public static partial void OnProcessingCloudEventFailed(ILogger logger, Exception ex, string type, string id);
}
