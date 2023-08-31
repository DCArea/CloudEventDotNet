using System.Diagnostics;
using Confluent.Kafka;

namespace CloudEventDotNet.Kafka.Telemetry;

internal static class Tracing
{
    public static void OnMessageProcessing(
        Activity activity,
        string consumerName,
        string consumerGroup)
    {
        activity.SetTag("messaging.kafka.client_id", consumerName);
        activity.SetTag("messaging.kafka.consumer_group", consumerGroup);
    }

    public static void OnMessageProduced(DeliveryResult<byte[], byte[]> result, string clientId)
    {
        var activity = Activity.Current;
        if (activity is not null)
        {
            activity.SetTag("messaging.kafka.client_id", clientId);
            activity.SetTag("messaging.kafka.partition", result.Partition.Value);
        }
    }

}
