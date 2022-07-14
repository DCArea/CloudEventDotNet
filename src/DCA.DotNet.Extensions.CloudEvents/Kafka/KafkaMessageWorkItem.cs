using System.Diagnostics.CodeAnalysis;
using System.Text.Json;
using System.Threading.Tasks.Sources;
using Confluent.Kafka;
using DCA.DotNet.Extensions.CloudEvents.Diagnostics;
using DCA.DotNet.Extensions.CloudEvents.Kafka;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace DCA.DotNet.Extensions.CloudEvents;

internal class KafkaMessageWorkItem : IThreadPoolWorkItem
{
    private readonly ConsumerContext _consumer;
    private readonly ConsumeResult<Ignore, byte[]> _message;
    private readonly IWorkItemLifetime _lifetime;
    private readonly Registry _registry;
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly ILogger _logger;
    private readonly object _lockable = new();
    private readonly CancellationTokenSource _cancellationTokenSource = new();

    internal KafkaMessageWorkItem(
        ConsumerContext consumer,
        ConsumeResult<Ignore, byte[]> message,
        IWorkItemLifetime lifetime,
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

    private readonly TaskCompletionSource _taskCompletionSource = new();
    public Task Task => _taskCompletionSource.Task;
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

    internal async Task ExecuteAsync()
    {
        try
        {
            var cloudEvent = JsonSerializer.Deserialize<CloudEvent>(_message.Message.Value)!;
            var metadata = new CloudEventMetadata(_consumer.PubSubName, _message.Topic, cloudEvent.Type, cloudEvent.Source);
            using var activity = Activities.OnProcess(metadata, cloudEvent);
            if (_registry.TryGetHandler(metadata, out var handler))
            {
                using var scope = _scopeFactory.CreateScope();
                await handler.Invoke(scope.ServiceProvider, cloudEvent!, _cancellationTokenSource.Token);
            }
            Metrics.OnCloudEventProcessed(metadata, DateTimeOffset.UtcNow.Subtract(cloudEvent.Time));
            KafkaInstruments.OnConsumed(activity, _consumer);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error handling Kafka message");
            throw;
        }
        finally
        {
            _taskCompletionSource.SetResult();
            var vt = _lifetime.OnFinished(this);
            if (!vt.IsCompletedSuccessfully) await vt;
        }
    }
}
