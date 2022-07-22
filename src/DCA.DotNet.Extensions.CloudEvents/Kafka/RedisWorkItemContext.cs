using Confluent.Kafka;

namespace DCA.DotNet.Extensions.CloudEvents.Kafka;

internal record KafkaWorkItemContext(
    Registry Registry,
    KafkaRedeliverProducer Producer);
