using System.Diagnostics;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;

namespace CloudEventDotNet.Kafka;

internal sealed partial class KafkaProducerTelemetry
{
    private readonly ILogger _logger;

    public KafkaProducerTelemetry(string pubSubName, ILoggerFactory loggerFactory)
    {
        _logger = loggerFactory.CreateLogger($"{nameof(KafkaProducerTelemetry)}:{pubSubName}");
    }

    // produce
    [LoggerMessage(
        Level = LogLevel.Debug,
        Message = "Produced message {topic}:{partition}:{offset}"
    )]
    partial void LogOnMessageProduced(string topic, int partition, long offset);
    public void OnMessageProduced(DeliveryResult<byte[], byte[]> result, string clientId)
    {
        var activity = Activity.Current;
        if (activity is not null)
        {
            activity.SetTag("messaging.kafka.client_id", clientId);
            activity.SetTag("messaging.kafka.partition", result.Partition.Value);
        }
        LogOnMessageProduced(result.Topic, result.Partition.Value, result.Offset.Value);
    }


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

}
