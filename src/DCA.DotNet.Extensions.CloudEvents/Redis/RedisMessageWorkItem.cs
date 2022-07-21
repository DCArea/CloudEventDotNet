using System.Text.Json;
using DCA.DotNet.Extensions.CloudEvents.Redis.Instruments;
using StackExchange.Redis;

namespace DCA.DotNet.Extensions.CloudEvents.Redis;

internal sealed class RedisMessageWorkItem : IThreadPoolWorkItem
{
    private readonly CancellationTokenSource _cancellationTokenSource = new();
    private readonly WorkItemWaiter _waiter = new();
    private readonly RedisWorkItemContext _context;

    internal RedisMessageWorkItem(
        RedisMessageChannelContext channelContext,
        RedisWorkItemContext context,
        StreamEntry message)
    {
        _context = context;
        ChannelContext = channelContext;
        Message = message;
    }

    public RedisMessageChannelContext ChannelContext { get; }
    public StreamEntry Message { get; }

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

    public ValueTask WaitToCompleteAsync()
    {
        return _waiter.Task;
    }

    internal async Task ExecuteAsync()
    {
        try
        {
            var cloudEvent = JsonSerializer.Deserialize<CloudEvent>((byte[])Message["data"]!)!;
            var metadata = new CloudEventMetadata(ChannelContext.PubSubName, ChannelContext.Topic, cloudEvent.Type, cloudEvent.Source);
            if (!_context.Registry.TryGetHandler(metadata, out var handler))
            {
                return;
            }

            bool succeed = false;
            using var activity = CloudEventInstruments.OnProcess(metadata, cloudEvent);
            using (var scope = _context.ScopeFactory.CreateScope())
            {
                try
                {
                    await handler
                        .Invoke(scope.ServiceProvider, cloudEvent!, _cancellationTokenSource.Token)
                        .ConfigureAwait(false);
                    CloudEventInstruments.OnCloudEventProcessed(metadata, DateTimeOffset.UtcNow.Subtract(cloudEvent.Time));
                    succeed = true;
                }
                catch (Exception ex)
                {
                    CloudEventInstruments.OnProcessingCloudEventFailed(_context.Logger, ex, metadata.Type, cloudEvent.Id);
                    throw;
                }
            }
            if (succeed)
            {
                await _context.Redis.StreamAcknowledgeAsync(
                    ChannelContext.Topic,
                    ChannelContext.ConsumerGroup,
                    Message.Id);
                _context.RedisTelemetry.OnMessageAcknowledged(Message.Id.ToString());
            }
            RedisTelemetry.OnMessageProcessed(activity, ChannelContext.ConsumerGroup, ChannelContext.ConsumerName);
        }
        catch (Exception ex)
        {
            _context.RedisTelemetry.OnProcessMessageFailed(Message.Id.ToString(), ex);
        }
        finally
        {
            _waiter.SetResult();
        }
    }
}
