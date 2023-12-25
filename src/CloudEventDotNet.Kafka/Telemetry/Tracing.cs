using System.Diagnostics;
using Confluent.Kafka;

namespace CloudEventDotNet.Kafka.Telemetry;

internal static class Tracing
{
    public static void OnMessageProcessing(
        Activity activity,
        string consumerGroup,
        TopicPartitionOffset offset)
    {
        activity.SetTag("messaging.system", "kafka");
        activity.SetTag("messaging.kafka.consumer.group", consumerGroup);
        activity.SetTag("messaging.kafka.destination.partition", offset.Partition.Value);
        activity.SetTag("messaging.kafka.message.offset", offset.Offset.Value);
    }

    public static void OnMessageProduced(DeliveryResult<byte[], byte[]> result)
    {
        var activity = Activity.Current;
        if (activity is not null)
        {
            activity.SetTag("messaging.system", "kafka");
            activity.SetTag("messaging.kafka.destination.partition", result.Partition.Value);
            activity.SetTag("messaging.kafka.message.offset", result.Offset.Value);
        }
    }

}
