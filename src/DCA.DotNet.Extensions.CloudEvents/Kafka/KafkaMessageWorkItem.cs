using System.Diagnostics.CodeAnalysis;
using System.Text.Json;
using System.Threading.Tasks.Sources;
using Confluent.Kafka;
using DCA.DotNet.Extensions.CloudEvents.Diagnostics;
using DCA.DotNet.Extensions.CloudEvents.Kafka;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace DCA.DotNet.Extensions.CloudEvents;

internal class KafkaMessageWorkItem : IThreadPoolWorkItem//, IValueTaskSource// IAsyncDisposable
{
    private readonly ConsumerContext _consumer;
    private readonly ConsumeResult<Ignore, byte[]> _message;
    private readonly IWorkItemLifetime _lifetime;
    private readonly Registry _registry;
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly ILogger _logger;
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
            using var activity = Activities.OnProcess(metadata, cloudEvent);
            if (_registry.TryGetHandler(metadata, out var handler))
            {
                using (var scope = _scopeFactory.CreateScope())
                {
                    await handler
                        .Invoke(scope.ServiceProvider, cloudEvent!, _cancellationTokenSource.Token)
                        .ConfigureAwait(false);
                }
            }
            Metrics.OnCloudEventProcessed(metadata, DateTimeOffset.UtcNow.Subtract(cloudEvent.Time));
            KafkaInstruments.OnConsumed(activity, _consumer);
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

internal class WorkItemWaiter : IValueTaskSource
{
    private ManualResetValueTaskSourceCore<bool> _tcs;
    public void GetResult(short token) => _tcs.GetResult(token);
    public ValueTaskSourceStatus GetStatus(short token) => _tcs.GetStatus(token);
    public void OnCompleted(
        Action<object?> continuation,
        object? state,
        short token,
        ValueTaskSourceOnCompletedFlags flags)
        => _tcs.OnCompleted(continuation, state, token, flags);

    public void SetResult() => _tcs.SetResult(true);
    public ValueTask Task => new ValueTask(this, _tcs.Version);
}
