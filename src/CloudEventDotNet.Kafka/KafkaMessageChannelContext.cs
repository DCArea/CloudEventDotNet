using Confluent.Kafka;

namespace CloudEventDotNet.Kafka;

internal record struct KafkaMessageChannelContext(
    string PubSubName,
    string ConsumerName,
    string ConsumerGroup,
    TopicPartition TopicPartition
);
