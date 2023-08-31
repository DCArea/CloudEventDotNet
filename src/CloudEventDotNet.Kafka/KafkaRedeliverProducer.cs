
using CloudEventDotNet.Kafka.Telemetry;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;

namespace CloudEventDotNet.Kafka;

internal sealed class KafkaRedeliverProducer
{
    private readonly IProducer<byte[], byte[]> _producer;
    private readonly ILogger<KafkaRedeliverProducer> _logger;

    public KafkaRedeliverProducer(
        KafkaSubscribeOptions options,
        ILogger<KafkaRedeliverProducer> logger,
        IKafkaProducerFactory producerFactory
    )
    {
        _logger = logger;

        var producerConfig = new ProducerConfig()
        {
            BootstrapServers = options.ConsumerConfig.BootstrapServers,
            Acks = Acks.Leader,
            LingerMs = 10
        };

        _producer = producerFactory.Create<byte[], byte[]>(producerConfig,
            errorHandler: (_, e) => Logs.ProducerError(_logger, e),
            logHandler: (_, log) => Logs.OnProducerLog(_logger, log));
    }

    public Task ReproduceAsync(ConsumeResult<byte[], byte[]> consumeResult)
        => _producer.ProduceAsync(consumeResult.Topic, consumeResult.Message);
}
