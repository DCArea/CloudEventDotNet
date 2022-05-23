
using System.Text.Json;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;

namespace DCA.DotNet.Extensions.CloudEvents;

internal class KafkaCloudEventPublisher : ICloudEventPublisher
{
    private readonly IProducer<string, byte[]> _producer;
    private readonly ILogger<KafkaCloudEventPublisher> _logger;

    public KafkaCloudEventPublisher(
        KafkaPublishOptions options,
        ILogger<KafkaCloudEventPublisher> logger)
    {
        _producer = new ProducerBuilder<string, byte[]>(options.ProducerConfig)
            .Build();
        _logger = logger;
    }

    public async Task PublishAsync<TData>(string topic, CloudEvent<TData> cloudEvent)
    {
        var message = new Message<string, byte[]>
        {
            Value = JsonSerializer.SerializeToUtf8Bytes(cloudEvent)
        };
        await _producer.ProduceAsync(topic, message);
        _logger.LogDebug("Published message to topic {Topic}", topic);
    }
}
