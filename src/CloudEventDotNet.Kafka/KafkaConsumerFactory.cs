using Confluent.Kafka;

namespace CloudEventDotNet.Kafka;

internal sealed class KafkaConsumerFactory : IKafkaConsumerFactory
{
    public IConsumer<TKey, TValue> Create<TKey, TValue>(
        ConsumerConfig consumerConfig,
        Action<IConsumer<TKey, TValue>, Error>? errorHandler = null,
        Action<IConsumer<TKey, TValue>, List<TopicPartition>>? partitionAssignmentHandler = null,
        Action<IConsumer<TKey, TValue>, List<TopicPartitionOffset>>? partitionsLostHandler = null,
        Action<IConsumer<TKey, TValue>, List<TopicPartitionOffset>>? partitionsRevokedHandler = null,
        Action<IConsumer<TKey, TValue>, LogMessage>? logHandler = null
    )
    {
        var builder = new ConsumerBuilder<TKey, TValue>(consumerConfig);
        if (errorHandler is not null)
        {
            builder.SetErrorHandler(errorHandler);
        }
        if (partitionAssignmentHandler is not null)
        {
            builder.SetPartitionsAssignedHandler(partitionAssignmentHandler);
        }
        if (partitionsLostHandler is not null)
        {
            builder.SetPartitionsLostHandler(partitionsLostHandler);
        }
        if (partitionsRevokedHandler is not null)
        {
            builder.SetPartitionsRevokedHandler(partitionsRevokedHandler);
        }
        if (logHandler is not null)
        {
            builder.SetLogHandler(logHandler);
        }
        return builder.Build();
    }
}

internal interface IKafkaConsumerFactory
{
    IConsumer<TKey, TValue> Create<TKey, TValue>(
        ConsumerConfig consumerConfig,
        Action<IConsumer<TKey, TValue>, Error>? errorHandler = null,
        Action<IConsumer<TKey, TValue>, List<TopicPartition>>? partitionAssignmentHandler = null,
        Action<IConsumer<TKey, TValue>, List<TopicPartitionOffset>>? partitionsLostHandler = null,
        Action<IConsumer<TKey, TValue>, List<TopicPartitionOffset>>? partitionsRevokedHandler = null,
        Action<IConsumer<TKey, TValue>, LogMessage>? logHandler = null
    );
}
