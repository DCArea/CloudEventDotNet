using System.Diagnostics;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;

namespace CloudEventDotNet.Kafka;

internal sealed partial class KafkaConsumerTelemetry
{
    private readonly ILogger _logger;
    public KafkaConsumerTelemetry(string pubSubName, ILoggerFactory loggerFactory)
    {
        _logger = loggerFactory.CreateLogger($"{nameof(CloudEventDotNet)}.{nameof(KafkaConsumerTelemetry)}:{pubSubName}");
    }
    public ILogger Logger => _logger;

    [LoggerMessage(
        Level = LogLevel.Error,
        Message = "Consumer error: {error}"
    )]
    public partial void OnConsumerError(Error error);

    [LoggerMessage(
        Level = LogLevel.Debug,
        Message = "Partitions assgined: {partitions}"
    )]
    public partial void OnPartitionsAssigned(List<TopicPartition> partitions);

    [LoggerMessage(
        Level = LogLevel.Debug,
        Message = "Partitions lost: {partitions}"
    )]
    public partial void OnPartitionsLost(List<TopicPartitionOffset> partitions);

    [LoggerMessage(
        Level = LogLevel.Debug,
        Message = "Partitions revoked: {partitions}"
    )]
    public partial void OnPartitionsRevoked(List<TopicPartitionOffset> partitions);

    [LoggerMessage(
        Message = "Consumer log: {message}"
    )]
    partial void OnConsumerLog(LogLevel logLevel, string message);
    public void OnConsumerLog(LogMessage log)
    {
        int level = log.LevelAs(LogLevelType.MicrosoftExtensionsLogging);
        OnConsumerLog((LogLevel)level, log.Message);
    }


    [LoggerMessage(
        Level = LogLevel.Trace,
        Message = "Committed offsets: {offsets}"
    )]
    partial void LogOnConsumerOffsetsCommited(IList<TopicPartitionOffsetError> offsets);

    [LoggerMessage(
        Level = LogLevel.Error,
        Message = "Error on committed offsets: {error}, detail : {offsets}"
    )]
    partial void LogOnConsumerOffsetsCommitedFailed(Error error, IList<TopicPartitionOffsetError> offsets);

    public void OnConsumerOffsetsCommited(CommittedOffsets offsets)
    {
        if (offsets.Error.IsError)
        {
            LogOnConsumerOffsetsCommitedFailed(offsets.Error, offsets.Offsets);
        }
        else
        {
            LogOnConsumerOffsetsCommited(offsets.Offsets);
        }
    }

    [LoggerMessage(
        Level = LogLevel.Debug,
        Message = "Consume loop started"
    )]
    public partial void OnConsumeLoopStarted();

    [LoggerMessage(
        Level = LogLevel.Debug,
        Message = "Consume loop stopped"
    )]
    public partial void OnConsumeLoopStopped();

    [LoggerMessage(
        Level = LogLevel.Trace,
        Message = "Fetched message {offset}"
    )]
    public partial void OnMessageFetched(TopicPartitionOffset offset);

    [LoggerMessage(
        Level = LogLevel.Error,
        Message = "Error on consuming"
    )]
    public partial void OnConsumeFailed(Exception exception);

    [LoggerMessage(
        Level = LogLevel.Debug,
        Message = "Committed offsets: {offsets}"
    )]
    public partial void OnOffsetsCommited(TopicPartitionOffset[] offsets);


    [LoggerMessage(
        Level = LogLevel.Error,
        Message = "Producer error: {error}"
    )]
    public partial void OnProducerError(Error error);

    [LoggerMessage(
        Message = "Producer log: {message}"
    )]
    partial void OnProducerLog(LogLevel logLevel, string message);
    public void OnProducerLog(LogMessage log)
    {
        int level = log.LevelAs(LogLevelType.MicrosoftExtensionsLogging);
        OnProducerLog((LogLevel)level, log.Message);
    }


    [LoggerMessage(
        Level = LogLevel.Debug,
        Message = "Commit loop started"
    )]
    public partial void OnCommitLoopStarted();

    [LoggerMessage(
        Level = LogLevel.Debug,
        Message = "Commit loop stopped"
    )]
    public partial void OnCommitLoopStopped();

    [LoggerMessage(
        Level = LogLevel.Error,
        Message = "Error in commit loop"
    )]
    public partial void OnCommitLoopError(Exception exception);

    public static void OnConsumed(
        string consumerName,
        string consumerGroup)
    {
        var activity = Activity.Current;
        if (activity is not null)
        {
            // activity.SetTag("messaging.kafka.message_key", message.Key);
            activity.SetTag("messaging.kafka.client_id", consumerName);
            activity.SetTag("messaging.kafka.consumer_group", consumerGroup);
        }
    }
}
