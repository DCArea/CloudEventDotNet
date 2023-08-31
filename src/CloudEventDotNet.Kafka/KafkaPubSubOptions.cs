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
    /// The limit of unprocessed CloudEvents in local process queue. Defaults to 1024.
    /// </summary>
    public int RunningWorkItemLimit { get; set; } = 1024;
}
