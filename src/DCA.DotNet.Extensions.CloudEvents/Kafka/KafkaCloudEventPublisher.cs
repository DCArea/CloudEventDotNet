using System.Diagnostics;
using System.Runtime.CompilerServices;
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

        DeliveryResult<string, byte[]> result = await _producer.ProduceAsync(topic, message);
        var activity = Activity.Current;
        if (activity is not null)
        {
            // activity.SetTag("messaging.kafka.message_key", message.Key);
            activity.SetTag("messaging.kafka.client_id", _producer.Name);
            activity.SetTag("messaging.kafka.partition", result.Partition.Value);
        }
    }
}
