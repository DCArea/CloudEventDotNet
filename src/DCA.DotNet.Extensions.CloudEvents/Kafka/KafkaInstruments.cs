using System.Diagnostics;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;

namespace DCA.DotNet.Extensions.CloudEvents.Kafka;
public static partial class KafkaInstruments
{
    [LoggerMessage(
        Level = LogLevel.Debug,
        Message = "Produced message to topic {topic}"
    )]
    static partial void OnProduced(ILogger logger, string topic);

    public static void OnProduced(
        ILogger logger,
        IProducer<byte[], byte[]> producer,
        DeliveryResult<byte[], byte[]> result)
    {
        var activity = Activity.Current;
        if (activity is not null)
        {
            // activity.SetTag("messaging.kafka.message_key", message.Key);
            activity.SetTag("messaging.kafka.client_id", producer.Name);
            activity.SetTag("messaging.kafka.partition", result.Partition.Value);
        }
        OnProduced(logger, result.Topic);
    }

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
