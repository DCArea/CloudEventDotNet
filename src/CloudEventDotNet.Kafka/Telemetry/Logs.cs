using Confluent.Kafka;
using Microsoft.Extensions.Logging;

namespace CloudEventDotNet.Kafka.Telemetry;

internal static partial class Logs
{
    [LoggerMessage(
        Level = LogLevel.Error,
        Message = "[{pubsub}] Consumer error: {error}"
    )]
    public static partial void ConsumerError(ILogger logger, string pubsub, Error error);

    [LoggerMessage(
        Level = LogLevel.Information,
        Message = "[{pubsub}] Partitions assgined: {partitions}"
    )]
    public static partial void PartitionsAssigned(ILogger logger, string pubsub, List<TopicPartition> partitions);

    [LoggerMessage(
        Level = LogLevel.Information,
        Message = "[{pubsub}] Partitions revoked: {partitions}"
    )]
    public static partial void PartitionsRevoked(ILogger logger, string pubsub, List<TopicPartitionOffset> partitions);

    [LoggerMessage(
        Level = LogLevel.Warning,
        Message = "[{pubsub}] Partitions lost: {partitions}"
    )]
    public static partial void PartitionsLost(ILogger logger, string pubsub, List<TopicPartitionOffset> partitions);

    [LoggerMessage(
        Message = "[{pubsub}] Consumer log: {message}"
    )]
    private static partial void ConsumerLog(ILogger logger, string pubsub, LogLevel logLevel, string message);
    public static void OnConsumerLog(ILogger logger, string pubsub, LogMessage log)
    {
        int level = log.LevelAs(LogLevelType.MicrosoftExtensionsLogging);
        ConsumerLog(logger, pubsub, (LogLevel)level, log.Message);
    }

    [LoggerMessage(
        Level = LogLevel.Debug,
        Message = "[{pubsub}] Committed offsets: {offsets}"
    )]
    public static partial void CommittedOffsets(ILogger logger, string pubsub, TopicPartitionOffset[] offsets);

    [LoggerMessage(
        Level = LogLevel.Trace,
        Message = "[{pubsub}] Fetched message {offset}"
    )]
    public static partial void FetchedMessage(ILogger logger, string pubsub, TopicPartitionOffset offset);

    [LoggerMessage(
        Level = LogLevel.Error,
        Message = "[{pubsub}] Error on consuming"
    )]
    public static partial void ConsumeFailed(ILogger logger, Exception exception, string pubsub);

    [LoggerMessage(
        Level = LogLevel.Error,
        Message = "Producer error: {error}"
    )]
    public static partial void ProducerError(ILogger logger, Error error);

    [LoggerMessage(
        Message = "Producer log: {message}"
    )]
    private static partial void ProducerLog(ILogger logger, LogLevel logLevel, string message);
    public static void OnProducerLog(ILogger logger, LogMessage log)
    {
        int level = log.LevelAs(LogLevelType.MicrosoftExtensionsLogging);
        ProducerLog(logger, (LogLevel)level, log.Message);
    }

    [LoggerMessage(
        Level = LogLevel.Debug,
        Message = "[{pubsub}] Produced message {topic}:{partition}:{offset}"
    )]
    public static partial void MessageProduced(ILogger logger, string pubsub, string topic, int partition, long offset);

}
