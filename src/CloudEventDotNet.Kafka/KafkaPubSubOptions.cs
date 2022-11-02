using Confluent.Kafka;

namespace CloudEventDotNet;

/// <summary>
/// Options for Kafka publisher
/// </summary>
public class KafkaPublishOptions
{
    /// <summary>
    /// Config for Kafka producer.
    /// </summary>
    public ProducerConfig ProducerConfig { get; set; } = new ProducerConfig();
}

/// <summary>
/// Options for Kafka subscriber
/// </summary>
public class KafkaSubscribeOptions
{
    /// <summary>
    /// Config for Kafka consumer.
    /// </summary>
    public ConsumerConfig ConsumerConfig { get; set; } = new ConsumerConfig();

    /// <summary>
    /// Delivery guarantee, defaults to AtMostOnce delivery.
    /// </summary>
    public DeliveryGuarantee DeliveryGuarantee { get; set; } = DeliveryGuarantee.AtMostOnce;

    /// <summary>
    /// The limit of unprocessed CloudEvents in local process queue.
    /// </summary>
    public int RunningWorkItemLimit { get; set; } = 1024;

    /// <summary>
    /// Max retries for cloud events failed to processing, set 0 to disable retries, default to int.MaxValue.
    /// </summary>
    public int MaxRetries { get; set; } = int.MaxValue;
}

public enum DeliveryGuarantee
{
    AtMostOnce, AtLeastOnce
}
