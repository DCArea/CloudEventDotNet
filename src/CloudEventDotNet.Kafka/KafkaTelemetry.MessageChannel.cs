using CloudEventDotNet.Diagnostics.Aggregators;
using Microsoft.Extensions.Logging;

namespace CloudEventDotNet.Kafka;

internal partial class KafkaMessageChannelTelemetry
{

    private readonly ILogger _logger;
    private readonly HistogramAggregator _deliveryLatency;
    private readonly HistogramAggregator _dispatchLatency;

    public KafkaMessageChannelTelemetry(
        ILoggerFactory loggerFactory,
        KafkaMessageChannelContext context)
    {
        _logger = loggerFactory.CreateLogger($"{nameof(KafkaMessageChannelTelemetry)}:{context.PubSubName}:{context.TopicPartition.Topic}:{context.TopicPartition.Partition.Value}");

        _deliveryLatency = CloudEventProcessingTelemetry.s_DeliveryLatency.FindOrCreate(new("pubsub", context.PubSubName, "topic", context.TopicPartition.Topic));
        _dispatchLatency = CloudEventProcessingTelemetry.s_DispatchLatency.FindOrCreate(new("pubsub", context.PubSubName, "topic", context.TopicPartition.Topic));
    }

    public ILogger Logger => _logger;

    // reader
    [LoggerMessage(
        Level = LogLevel.Debug,
        Message = "Polling started")]
    public partial void OnMessageChannelReaderStarted();

    [LoggerMessage(
        Level = LogLevel.Trace,
        Message = "Work item not started, starting it")]
    public partial void OnWorkItemStarting();

    [LoggerMessage(
        Level = LogLevel.Trace,
        Message = "Work item started")]
    public partial void OnWorkItemStarted();

    [LoggerMessage(
        Level = LogLevel.Trace,
        Message = "Work item not completed, waiting")]
    public partial void OnWaitingWorkItemComplete();

    [LoggerMessage(
        Level = LogLevel.Trace,
        Message = "Work item completed")]
    public partial void OnWorkItemCompleted();

    [LoggerMessage(
        Level = LogLevel.Trace,
        Message = "Reader cancelled")]
    public partial void MessageChannelReaderCancelled();

    [LoggerMessage(
        Level = LogLevel.Debug,
        Message = "Reader stopped")]
    public partial void MessageChannelReaderStopped();

    [LoggerMessage(
        Level = LogLevel.Trace,
        Message = "Waiting for next work item")]
    public partial void WaitingForNextWorkItem();

    [LoggerMessage(
        EventId = 10700,
        Level = LogLevel.Error,
        Message = "Exception on polling")]
    public partial void ExceptionOnReadingWorkItems(Exception exception);

    [LoggerMessage(
        Level = LogLevel.Trace,
        Message = "Checked offset {offset}")]
    public partial void OnOffsetChecked(long offset);
}
