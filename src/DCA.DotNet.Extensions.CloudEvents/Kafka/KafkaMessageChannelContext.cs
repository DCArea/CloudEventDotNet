using Confluent.Kafka;

namespace DCA.DotNet.Extensions.CloudEvents.Kafka;

internal record KafkaMessageChannelContext(
    string PubSubName,
    string ConsumerName,
    string ConsumerGroup,
    TopicPartition TopicPartition
);
