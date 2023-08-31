using Confluent.Kafka;

namespace CloudEventDotNet.Kafka;

internal sealed class KafkaProducerFactory : IKafkaProducerFactory
{
    public IProducer<TKey, TValue> Create<TKey, TValue>(
        ProducerConfig producerConfig,
        Action<IProducer<TKey, TValue>, Error>? errorHandler = null,
        Action<IProducer<TKey, TValue>, LogMessage>? logHandler = null
    )
    {
        var builder = new ProducerBuilder<TKey, TValue>(producerConfig);

        if (errorHandler is not null)
        {
            builder.SetErrorHandler(errorHandler);
        }
        if (logHandler is not null)
        {
            builder.SetLogHandler(logHandler);
        }

        return builder.Build();
    }
}

internal interface IKafkaProducerFactory
{
    IProducer<TKey, TValue> Create<TKey, TValue>(
        ProducerConfig producerConfig,
        Action<IProducer<TKey, TValue>, Error>? errorHandler = null,
        Action<IProducer<TKey, TValue>, LogMessage>? logHandler = null);
}
