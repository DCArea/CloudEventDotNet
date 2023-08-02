
using System.Diagnostics;
using CloudEventDotNet.Diagnostics.Aggregators;
using Microsoft.Extensions.Logging;

namespace CloudEventDotNet;

internal partial class CloudEventProcessingTelemetry
{
    private readonly ILogger _logger;
    private readonly CloudEventMetadata _metadata;
    private readonly HistogramAggregator _deliveryLatency;
    private readonly HistogramAggregator _dispatchLatency;
    private readonly HistogramAggregator _processLatency;
    internal static readonly HistogramAggregatorGroup s_DeliveryLatency;
    internal static readonly HistogramAggregatorGroup s_DispatchLatency;
    internal static readonly HistogramAggregatorGroup s_ProcessLatency;

    static CloudEventProcessingTelemetry()
    {
        var buckets = new long[] { 5, 20, 50, 100, 200, 500, 1_000, 2_000, 5_000, 10_000, 20_000, 60_000, 2 * 60_000, 5 * 60_000, 10 * 60_000 };
        s_DeliveryLatency =
            new(
                new HistogramAggregatorOptions(buckets),
                CloudEventTelemetry.Meter, "dca_cloudevents_delivery_latency", "ms"
            );
        s_DispatchLatency =
            new(
                new HistogramAggregatorOptions(buckets),
                CloudEventTelemetry.Meter, "dca_cloudevents_dispatch_latency", "ms"
            );
        s_ProcessLatency =
            new(
                new HistogramAggregatorOptions(buckets),
                CloudEventTelemetry.Meter, "dca_cloudevents_process_latency", "ms"
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

        _deliveryLatency = s_DeliveryLatency.FindOrCreate(new("pubsub", metadata.PubSubName, "topic", metadata.Topic));
        _dispatchLatency = s_DispatchLatency.FindOrCreate(new("pubsub", metadata.PubSubName, "topic", metadata.Topic));
        _processLatency = s_ProcessLatency.FindOrCreate(new("pubsub", metadata.PubSubName, "topic", metadata.Topic, "type", metadata.Type));
    }


    [LoggerMessage(
        Level = LogLevel.Information,
        Message = "Processing CloudEvent {id}, received at {receivedAt:o}"
    )]
    public partial void LogOnProcessing(string id, DateTime receivedAt);
    public Activity? OnProcessing(CloudEvent cloudEvent, DateTime receivedAt)
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

            activity.SetTag("cloudevents.event_id", cloudEvent.Id);
            activity.SetTag("cloudevents.event_source", cloudEvent.Source);
            activity.SetTag("cloudevents.event_type", cloudEvent.Type);
            if (cloudEvent.Subject is not null)
            {
                activity.SetTag("cloudevents.event_subject", cloudEvent.Subject);
            }
        }

        var processingAt = DateTime.UtcNow;
        var deliveryLatency = receivedAt - cloudEvent.Time;
        var dispatchLatency = processingAt - receivedAt;
        _deliveryLatency.Record((long)deliveryLatency.TotalMilliseconds);
        _dispatchLatency.Record((long)dispatchLatency.TotalMilliseconds);
        LogOnProcessing(cloudEvent.Id, receivedAt);

        return activity;
    }


    [LoggerMessage(
        Level = LogLevel.Information,
        Message = "Processed CloudEvent {Id}"
    )]
    public partial void LogOnCloudEventProcessed(string id);

    public void OnCloudEventProcessed(CloudEvent @event, TimeSpan processLatency)
    {
        LogOnCloudEventProcessed(@event.Id);
        _processLatency.Record((long)processLatency.TotalMilliseconds);
    }

    public void OnFinish(DateTime producedAt, DateTime deliveredAt, DateTime dispatchedAt)
    {
        var deliveryLatency = deliveredAt - producedAt;
        var dispatchLatency = dispatchedAt - deliveredAt;
        _deliveryLatency.Record((long)deliveryLatency.TotalMilliseconds);
        _dispatchLatency.Record((long)dispatchLatency.TotalMilliseconds);
    }

    [LoggerMessage(
        Level = LogLevel.Error,
        Message = "Failed to process CloudEvent {Id}"
    )]
    public partial void OnProcessingCloudEventFailed(Exception ex, string id);

}

