using Confluent.Kafka;

namespace CloudEventDotNet.Kafka;

internal record KafkaMessageChannelContext(
    string PubSubName,
    string ConsumerName,
    string ConsumerGroup,
    TopicPartition TopicPartition
);
