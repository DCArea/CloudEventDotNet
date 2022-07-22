using System.Text.Json;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;

namespace DCA.DotNet.Extensions.CloudEvents.Kafka;

internal sealed class KafkaMessageWorkItem : IThreadPoolWorkItem
{
    private readonly KafkaMessageChannelContext _channelContext;
    private readonly KafkaWorkItemContext _context;
    private readonly KafkaMessageChannelTelemetry _telemetry;
    private readonly ConsumeResult<byte[], byte[]> _message;
    private readonly CancellationTokenSource _cancellationTokenSource = new();

    internal KafkaMessageWorkItem(
        KafkaMessageChannelContext channelContext,
        KafkaWorkItemContext context,
        KafkaMessageChannelTelemetry telemetry,
        ConsumeResult<byte[], byte[]> message)
    {
        _channelContext = channelContext;
        _context = context;
        _telemetry = telemetry;
        _message = message;
    }

    public TopicPartitionOffset TopicPartitionOffset => _message.TopicPartitionOffset;

    public bool Started => _started == 1;
    private int _started = 0;

    public void Execute()
    {
        if (Interlocked.CompareExchange(ref _started, 1, 0) == 0)
        {
            _ = ExecuteAsync();
        }
        else
        {
            return;
        }
    }

    private readonly WorkItemWaiter _waiter = new();
    public ValueTask WaitToCompleteAsync()
    {
        return _waiter.Task;
    }

    internal async Task ExecuteAsync()
    {
        try
        {
            var cloudEvent = JsonSerializer.Deserialize<CloudEvent>(_message.Message.Value)!;
            var metadata = new CloudEventMetadata(_channelContext.PubSubName, _message.Topic, cloudEvent.Type, cloudEvent.Source);
            if (!_context.Registry.TryGetHandler(metadata, out var handler))
            {
                CloudEventProcessingTelemetry.OnHandlerNotFound(_telemetry.Logger, metadata);
                return;
            }
            bool succeed = await handler.ProcessAsync(cloudEvent, _cancellationTokenSource.Token).ConfigureAwait(false);
            KafkaInstruments.OnConsumed(_channelContext.ConsumerName, _channelContext.ConsumerGroup);

            if (!succeed)
            {
                await _context.Producer.ReproduceAsync(_message).ConfigureAwait(false);
            }
        }
        catch (Exception ex)
        {
            _telemetry.Logger.LogError(ex, "Error handling Kafka message");
        }
        finally
        {
            _waiter.SetResult();
        }
    }
}

