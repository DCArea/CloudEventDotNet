using CloudEventDotNet;
using Confluent.Kafka;

namespace CloudEventKafkaTester
{
    internal static class KafkaEnv
    {
        public static string broker { get; set; } = Environment.GetEnvironmentVariable("KAFKA_BROKER") ?? "localhost:9092";

        public static string topic { get; set; } = Environment.GetEnvironmentVariable("KAFKA_TOPIC") ?? "devperftest";

        public static string consumerGroup { get; set; } = Environment.GetEnvironmentVariable("KAFKA_CONSUMER_GROUP") ?? "devperftest";

        public static int runningWorkItemLimit { get; set; } = int.Parse(Environment.GetEnvironmentVariable("KAFKA_RUNNING_WORK_ITEM_LIMIT") ?? "128");

        public static  AutoOffsetReset autoOffsetReset { get; set; } = Environment.GetEnvironmentVariable("KAFKA_AUTO_OFFSET_RESET") switch
        {
            "latest" => AutoOffsetReset.Latest,
            "earliest" => AutoOffsetReset.Earliest,
            _ => AutoOffsetReset.Latest
        };

        public static DeliveryGuarantee deliveryGuarantee { get; set; } = Environment.GetEnvironmentVariable("KAFKA_DELIVERY_GUARANTEE") switch
        {
            "at_least_once" => DeliveryGuarantee.AtLeastOnce,
            "at_most_once" => DeliveryGuarantee.AtMostOnce,
            _ => DeliveryGuarantee.AtLeastOnce
        };
    }
}
