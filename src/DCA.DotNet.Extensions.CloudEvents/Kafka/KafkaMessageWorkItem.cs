using System.Text.Json;
using Confluent.Kafka;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace DCA.DotNet.Extensions.CloudEvents.Kafka;

internal class KafkaMessageWorkItem : IThreadPoolWorkItem
{
    private readonly ConsumerContext _consumer;
    private readonly ConsumeResult<Ignore, byte[]> _message;
    private readonly KafkaWorkItemLifetime _lifetime;
    private readonly Registry _registry;
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly ILogger _logger;
    private readonly CancellationTokenSource _cancellationTokenSource = new();

    internal KafkaMessageWorkItem(
        ConsumerContext consumer,
        ConsumeResult<Ignore, byte[]> message,
        KafkaWorkItemLifetime lifetime,
        Registry registry,
        IServiceScopeFactory scopeFactory,
        ILogger logger)
    {
        _consumer = consumer;
        _message = message;
        _lifetime = lifetime;
        _registry = registry;
        _scopeFactory = scopeFactory;
        _logger = logger;
    }

    public TopicPartition TopicPartition => _message.TopicPartition;
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
            var metadata = new CloudEventMetadata(_consumer.PubSubName, _message.Topic, cloudEvent.Type, cloudEvent.Source);
            using var activity = CloudEventInstruments.OnProcess(metadata, cloudEvent);
            if (_registry.TryGetHandler(metadata, out var handler))
            {
                using var scope = _scopeFactory.CreateScope();
                await handler
                    .Invoke(scope.ServiceProvider, cloudEvent!, _cancellationTokenSource.Token)
                    .ConfigureAwait(false);
                CloudEventInstruments.OnCloudEventProcessed(metadata, DateTimeOffset.UtcNow.Subtract(cloudEvent.Time));
                KafkaInstruments.OnConsumed(activity, _consumer);
            }
            else
            {
                _logger.LogWarning("No handler found for {Metadata}", metadata);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error handling Kafka message");
        }
        finally
        {
            _waiter.SetResult();
            var vt = _lifetime.OnFinished(this);
            if (!vt.IsCompletedSuccessfully) await vt;
        }
    }
}

