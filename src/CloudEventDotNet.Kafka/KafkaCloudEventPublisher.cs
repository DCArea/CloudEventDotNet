using CloudEventDotNet.Kafka;
using CloudEventDotNet.Kafka.Telemetry;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;

namespace CloudEventDotNet;

internal sealed class KafkaCloudEventPublisher : ICloudEventPublisher
{
    private readonly IProducer<byte[], byte[]> _producer;
    private readonly string _pubSubName;
    private readonly ILogger<KafkaCloudEventPublisher> _logger;

    public KafkaCloudEventPublisher(
        string pubSubName,
        KafkaPublishOptions options,
        ILogger<KafkaCloudEventPublisher> logger,
        IKafkaProducerFactory producerFactory)
    {
        _pubSubName = pubSubName;
        _logger = logger;
        _producer = producerFactory.Create<byte[], byte[]>(options.ProducerConfig,
            (_, e) => Logs.ProducerError(_logger, e),
            (_, log) => Logs.OnProducerLog(_logger, log));
    }

    public async Task PublishAsync<TData>(string topic, CloudEvent<TData> cloudEvent)
    {
        var message = new Message<byte[], byte[]>
        {
            Value = JSON.SerializeToUtf8Bytes(cloudEvent)
        };

        var result = await _producer
            .ProduceAsync(topic, message)
            .ConfigureAwait(false);

        Logs.MessageProduced(_logger, _pubSubName, topic, result.Partition, result.Offset);
        Tracing.OnMessageProduced(result, _producer.Name);
    }
}
