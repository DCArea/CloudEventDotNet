using System.Diagnostics;
using CloudEventDotNet.Redis.Instruments;
using StackExchange.Redis;

namespace CloudEventDotNet.Redis;

internal sealed class RedisMessageWorkItem : IThreadPoolWorkItem
{
    private readonly CancellationTokenSource _cancellationTokenSource = new();
    private readonly WorkItemWaiter _waiter = new();
    private readonly RedisWorkItemContext _context;
    private readonly DateTime _receivedAt;

    internal RedisMessageWorkItem(
        RedisMessageChannelContext channelContext,
        RedisWorkItemContext context,
        StreamEntry message,
        DateTime receivedAt)
    {
        _context = context;
        ChannelContext = channelContext;
        Message = message;
        _receivedAt = receivedAt;
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
        => _waiter.Task;

    internal async Task ExecuteAsync()
    {
        Activity? activity = null;
        try
        {
            var cloudEvent = JSON.Deserialize<CloudEvent>((byte[])Message["data"]!)!;
            var metadata = new CloudEventMetadata(ChannelContext.PubSubName, ChannelContext.Topic, cloudEvent.Type, cloudEvent.Source);
            if (!_context.Registry.TryGetHandler(metadata, out var handler))
            {
                return;
            }

            activity = handler.Telemetry.OnProcessing(cloudEvent, _receivedAt);
            var succeed = await handler.ProcessAsync(cloudEvent, _cancellationTokenSource.Token).ConfigureAwait(false);
            if (succeed)
            {
                await _context.Redis.StreamAcknowledgeAsync(
                    ChannelContext.Topic,
                    ChannelContext.ConsumerGroup,
                    Message.Id).ConfigureAwait(false);
                _context.RedisTelemetry.OnMessageAcknowledged(Message.Id.ToString());
            }

            if (activity is not null)
            {
                RedisTelemetry.OnMessageProcessed(ChannelContext.ConsumerGroup, ChannelContext.ConsumerName);
            }

        }
        catch (Exception ex)
        {
            _context.RedisTelemetry.OnProcessMessageFailed(Message.Id.ToString(), ex);
        }
        finally
        {
            activity?.Dispose();
            _waiter.SetResult();
        }
    }

}
