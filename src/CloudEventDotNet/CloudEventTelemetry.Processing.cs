
using System.Diagnostics;
using CloudEventDotNet.Diagnostics.Aggregators;
using Microsoft.Extensions.Logging;

namespace CloudEventDotNet;

internal partial class CloudEventProcessingTelemetry
{
    private readonly ILogger _logger;
    private readonly CloudEventMetadata _metadata;
    private readonly HistogramAggregator _cloudEventsDeliveryLatency;
    private static readonly HistogramAggregatorGroup s_cloudEventsDeliveryLatency;
    static CloudEventProcessingTelemetry()
    {
        s_cloudEventsDeliveryLatency =
            new(
                new HistogramAggregatorOptions(new long[] { 5, 20, 50, 100, 200, 500, 1_000, 2_000, 5_000, 10_000, 20_000, 60_000, 2 * 60_000, 5 * 60_000, 10 * 60_000 }),
                CloudEventTelemetry.Meter, "dca_cloudevents_delivery_latency", "ms"
            );
    }


    [LoggerMessage(
        Level = LogLevel.Debug,
        Message = "No handler for {metadata}, ignored"
    )]
    public static partial void OnHandlerNotFound(ILogger logger, CloudEventMetadata metadata);

    public CloudEventProcessingTelemetry(
        ILoggerFactory loggerFactory,
        CloudEventMetadata metadata)
    {
        _logger = loggerFactory.CreateLogger($"CloudEventDotNet.CloudEventProcessing:{metadata.PubSubName}:{metadata.Topic}");
        _metadata = metadata;

        _cloudEventsDeliveryLatency = s_cloudEventsDeliveryLatency.FindOrCreate(new("pubsub", metadata.PubSubName, "topic", metadata.Topic, "type", metadata.Type));
    }


    public Activity? OnProcessing(CloudEvent cloudEvent)
    {
        ActivityContext parentContext = default;
        if (cloudEvent.Extensions is not null && cloudEvent.Extensions.Count != 0)
        {
            cloudEvent.Extensions.TryGetValue("traceparent", out var traceparent);
            cloudEvent.Extensions.TryGetValue("tracestate", out var tracestate);

            ActivityContext.TryParse(traceparent.GetString(), tracestate.GetString(), out parentContext);
        }
        var activity = CloudEventTelemetry.Source.StartActivity($"CloudEvents Process {cloudEvent.Type}", ActivityKind.Consumer, parentContext);
        if (activity is not null)
        {
            activity.SetTag("messaging.system", _metadata.PubSubName);
            activity.SetTag("messaging.destination", _metadata.Topic);
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
        Level = LogLevel.Information,
        Message = "Process CloudEvent {Id}"
    )]
    public partial void LogOnCloudEventProcessed(string id);

    public void OnCloudEventProcessed(CloudEvent @event)
    {
        LogOnCloudEventProcessed(@event.Id);
        var latency = DateTimeOffset.UtcNow.Subtract(@event.Time);
        _cloudEventsDeliveryLatency.Record((long)latency.TotalMilliseconds);
    }

    [LoggerMessage(
        Level = LogLevel.Error,
        Message = "Failed to process CloudEvent {Id}"
    )]
    public partial void OnProcessingCloudEventFailed(Exception ex, string id);
}

