using Confluent.Kafka;

namespace CloudEventDotNet.Kafka;

internal record KafkaWorkItemContext(
    Registry Registry,
    KafkaRedeliverProducer Producer);
