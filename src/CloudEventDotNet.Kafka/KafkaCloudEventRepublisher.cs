
using Confluent.Kafka;
using Microsoft.Extensions.Logging;

namespace CloudEventDotNet.Kafka;

internal class KafkaCloudEventRepublisher : ICloudEventRepublisher
{
    private readonly IProducer<byte[], byte[]> _producer;
    private readonly KafkaSubscribeOptions _options;
    private readonly KafkaConsumerTelemetry _telemetry;

    public KafkaCloudEventRepublisher(
        KafkaSubscribeOptions options,
        KafkaConsumerTelemetry telemetry
    )
    {
        var producerConfig = new ProducerConfig()
        {
            BootstrapServers = options.ConsumerConfig.BootstrapServers,
            Acks = Acks.Leader,
            LingerMs = 10,

            // producer retries sending messages by default
        };

        _producer = new ProducerBuilder<byte[], byte[]>(producerConfig)
            .SetErrorHandler((_, e) => telemetry.OnProducerError(e))
            .SetLogHandler((_, log) => telemetry.OnProducerLog(log))
            .Build();
        _options = options;
        _telemetry = telemetry;
    }

    public async Task RepublishAsync(string topic, CloudEvent cloudEvent)
    {
        int retry = cloudEvent.Retry;

        if (retry >= _options.MaxRetries)
        {
            CloudEventProcessingTelemetry.LogOnCloudEventDropped(_telemetry.Logger, cloudEvent.Id);
            return;
        }

        cloudEvent.Retry = retry + 1;

        var message = new Message<byte[], byte[]>
        {
            Value = JSON.SerializeToUtf8Bytes(cloudEvent)
        };
        DeliveryResult<byte[], byte[]> result;
        try
        {
            result = await _producer.ProduceAsync(topic, message).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _telemetry.Logger.LogError(ex, "Failed to republish cloud event {id}", cloudEvent.Id);
            CloudEventProcessingTelemetry.LogOnCloudEventDropped(_telemetry.Logger, cloudEvent.Id);
            return;
        }
        _telemetry.Logger.LogInformation("Republished cloud event {id} at {offset}", cloudEvent.Id, result.TopicPartitionOffset);

    }
}
