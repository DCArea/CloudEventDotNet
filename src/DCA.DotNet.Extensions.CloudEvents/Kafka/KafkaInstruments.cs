using System.Diagnostics;
using System.Reflection.Metadata.Ecma335;
using Confluent.Kafka;
using DCA.DotNet.Extensions.CloudEvents.Diagnostics.Aggregators;
using Microsoft.Extensions.Logging;

namespace DCA.DotNet.Extensions.CloudEvents.Kafka;

public static partial class KafkaInstruments
{


    [LoggerMessage(
        EventId = 40001,
        Level = LogLevel.Debug,
        Message = "Produced message to topic {topic}"
    )]
    static partial void OnProduced(ILogger logger, string topic);

    public static void OnProduced(
        ILogger logger,
        IProducer<Ignore, byte[]> producer,
        DeliveryResult<Ignore, byte[]> result)
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
        Activity? activity,
        ConsumerContext consumer
    )
    {
        if (activity is not null)
        {
            // activity.SetTag("messaging.kafka.message_key", message.Key);
            activity.SetTag("messaging.kafka.client_id", consumer.Name);
            activity.SetTag("messaging.kafka.consumer_group", consumer.Group);
        }
    }
}
