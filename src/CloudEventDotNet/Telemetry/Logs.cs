using Microsoft.Extensions.Logging;

namespace CloudEventDotNet.Telemetry;

internal static partial class Logs
{
    [LoggerMessage(
        Level = LogLevel.Information,
        Message = "[{pubsub}/{topic}/{type}] Published CloudEvent {id}"
    )]
    public static partial void CloudEventPublished(ILogger logger, string pubsub, string topic, string type, string id);

    [LoggerMessage(
        Level = LogLevel.Debug,
        Message = "[{channel}] No handler for {metadata}, ignored"
    )]
    public static partial void CloudEventHandlerNotFound(ILogger logger, string channel, CloudEventMetadata metadata);


    [LoggerMessage(
        Level = LogLevel.Information,
        Message = "Processed CloudEvent {Id}"
    )]
    public static partial void CloudEventProcessed(ILogger logger, string id);

    [LoggerMessage(
        Level = LogLevel.Error,
        Message = "[{pubsub}/{topic}/{type}] Failed to process CloudEvent {Id}"
    )]
    public static partial void CloudEventProcessFailed(ILogger logger, string pubsub, string topic, string type, Exception ex, string id);

    [LoggerMessage(
        Level = LogLevel.Information,
        Message = "[{pubsub}/{topic}/{type}] Sent dead letter {id}"
    )]
    public static partial void DeadLetterSent(ILogger logger, string pubsub, string topic, string type, string id);
}

internal sealed partial class CloudEventLogger
{
    private readonly ILogger _logger;
    private readonly CloudEventMetadata _metadata;

    public CloudEventLogger(ILogger logger, CloudEventMetadata metadata)
    {
        _logger = logger;
        _metadata = metadata;
    }

    [LoggerMessage(
        Level = LogLevel.Debug,
        Message = "[{pubsub}/{topic}/{type}] Processing CloudEvent {id}"
    )]
    private partial void CloudEventProcessing(string pubsub, string topic, string type, string id);

    [LoggerMessage(
        Level = LogLevel.Warning,
        Message = "[{pubsub}/{topic}/{type}] CloudEvent {id} were not delivered in time, tardiness: {tardiness} "
    )]
    private partial void CloudEventNotDeliveredInTime(string pubsub, string topic, string type, string id, TimeSpan tardiness);

    public void CloudEventProcessing(string id, TimeSpan tardiness)
    {
        if (tardiness.TotalMilliseconds > 60_000)
        {
            CloudEventNotDeliveredInTime(_metadata.PubSubName, _metadata.Topic, _metadata.Type, id, tardiness);
        }
        else
        {
            CloudEventProcessing(_metadata.PubSubName, _metadata.Topic, _metadata.Type, id);
        }
    }

    [LoggerMessage(
        Level = LogLevel.Information,
        Message = "[{pubsub}/{topic}/{type}] Processed CloudEvent {id}"
    )]
    private partial void CloudEventProcessed(string pubsub, string topic, string type, string id);
    public void CloudEventProcessed(string id)
        => CloudEventProcessed(_metadata.PubSubName, _metadata.Topic, _metadata.Type, id);

    [LoggerMessage(
        Level = LogLevel.Error,
        Message = "[{pubsub}/{topic}/{type}] Failed to process CloudEvent {id}"
    )]
    private partial void CloudEventProcessFailed(string pubsub, string topic, string type, Exception ex, string id);
    public void CloudEventProcessFailed(Exception ex, string id)
        => CloudEventProcessFailed(_metadata.PubSubName, _metadata.Topic, _metadata.Type, ex, id);
}
