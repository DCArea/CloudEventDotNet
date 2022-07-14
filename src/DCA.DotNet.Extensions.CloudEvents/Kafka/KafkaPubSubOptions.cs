using Confluent.Kafka;

namespace DCA.DotNet.Extensions.CloudEvents;

public class KafkaPublishOptions
{
    public ProducerConfig ProducerConfig { get; set; } = new ProducerConfig();
}

public class KafkaSubscribeOptions
{
    public ConsumerConfig ConsumerConfig { get; set; } = new ConsumerConfig();
    public DeliveryGuarantee DeliveryGuarantee { get; set; } = DeliveryGuarantee.AtMostOnce;
    public int RunningWorkItemLimit { get; set; } = 1024;
}

public enum DeliveryGuarantee
{
    AtMostOnce, AtLeastOnce
}
