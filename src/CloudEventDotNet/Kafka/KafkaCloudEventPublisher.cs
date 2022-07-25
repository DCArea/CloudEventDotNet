using System.Text.Json;
using Confluent.Kafka;
using CloudEventDotNet.Kafka;
using Microsoft.Extensions.Logging;

namespace CloudEventDotNet;

internal sealed class KafkaCloudEventPublisher : ICloudEventPublisher
{
    private readonly IProducer<byte[], byte[]> _producer;
    private readonly KafkaProducerTelemetry _telemetry;

    public KafkaCloudEventPublisher(
        string pubSubName,
        KafkaPublishOptions options,
        ILoggerFactory loggerFactory)
    {
        _telemetry = new KafkaProducerTelemetry(pubSubName, loggerFactory);
        _producer = new ProducerBuilder<byte[], byte[]>(options.ProducerConfig)
            .SetErrorHandler((_, e) => _telemetry.OnProducerError(e))
            .SetLogHandler((_, log) => _telemetry.OnProducerLog(log))
            .Build();
    }

    public async Task PublishAsync<TData>(string topic, CloudEvent<TData> cloudEvent)
    {
        var message = new Message<byte[], byte[]>
        {
            Value = JsonSerializer.SerializeToUtf8Bytes(cloudEvent)
        };

        DeliveryResult<byte[], byte[]> result = await _producer.ProduceAsync(topic, message).ConfigureAwait(false);
        _telemetry.OnMessageProduced(result, _producer.Name);
    }
}
