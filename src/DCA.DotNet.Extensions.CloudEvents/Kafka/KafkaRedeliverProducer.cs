
using Confluent.Kafka;

namespace DCA.DotNet.Extensions.CloudEvents.Kafka;

internal class KafkaRedeliverProducer
{
    private readonly IProducer<byte[], byte[]> _producer;

    public KafkaRedeliverProducer(
        KafkaSubscribeOptions options,
        KafkaConsumerTelemetry telemetry
    )
    {
        var producerConfig = new ProducerConfig()
        {
            BootstrapServers = options.ConsumerConfig.BootstrapServers,
            Acks = Acks.Leader,
            LingerMs = 10
        };

        _producer = new ProducerBuilder<byte[], byte[]>(producerConfig)
            .SetErrorHandler((_, e) => telemetry.OnProducerError(e))
            .SetLogHandler((_, log) => telemetry.OnProducerLog(log))
            .Build();
    }

    public Task ReproduceAsync(ConsumeResult<byte[], byte[]> consumeResult)
    {
        return _producer.ProduceAsync(consumeResult.Topic, consumeResult.Message);
    }

}
