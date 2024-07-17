using System.Diagnostics;
using CloudEventDotNet.Redis.Telemetry;
using DCA.Extensions.BackgroundTask;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;

namespace CloudEventDotNet.Redis;

internal sealed class RedisCloudEventMessage(
    StreamEntry message,
    ILogger<RedisCloudEventMessage> logger,
    RedisMessageChannelContext channelContext,
    MetricsContext metrics,
    Registry2 registry,
    IDatabase redis
    ) : IBackgroundTask
{
    private readonly WorkItemWaiter _waiter = new();
    private int _started = 0;
    public bool Started => _started == 1;
    void IThreadPoolWorkItem.Execute() => Start();
    public void Start()
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
    public ValueTask WaitToCompleteAsync() => _waiter.Task;

    private readonly CancellationTokenSource _cancellationTokenSource = new();

    internal async Task ExecuteAsync()
    {
        Activity? activity = null;
        try
        {
            var cloudEvent = JSON.Deserialize<CloudEvent>((byte[])message["data"]!)!;
            var metadata = new CloudEventMetadata(channelContext.PubSubName, channelContext.Topic, cloudEvent.Type, cloudEvent.Source);

            if (!registry.TryGetSubscription(metadata, out var sub))
            {
                CloudEventDotNet.Telemetry.Logs.CloudEventHandlerNotFound(logger, channelContext.Key, metadata);
                return;
            }

            activity = CloudEventDotNet.Telemetry.Tracing.OnProcessing(channelContext.PubSubName, channelContext.Topic, cloudEvent);
            if (activity is not null)
            {
                Tracing.OnMessageProcessing(channelContext.ConsumerGroup, message.Id.ToString());
            }

            var result = await sub.Handler.ProcessAsync(cloudEvent, sub.Options, _cancellationTokenSource.Token)
                .ConfigureAwait(false);

            if (result is ProcessingResult.Success or ProcessingResult.SentToDeadLetter)
            {
                await redis.StreamAcknowledgeAsync(
                    channelContext.Topic,
                    channelContext.ConsumerGroup,
                    message.Id)
                    .ConfigureAwait(false);
                Logs.MessageAcknowledged(logger, channelContext.PubSubName, channelContext.Topic, message.Id.ToString());
                metrics.OnMessageAcknowledged();
            }
        }
        catch (Exception ex)
        {
            activity?.SetStatus(ActivityStatusCode.Error, $"Exception: {ex.GetType().Name}");
            Logs.MessageProcessFailed(logger, channelContext.PubSubName, channelContext.Topic, message.Id.ToString(), ex);
        }
        finally
        {
            activity?.Dispose();
            _waiter.SetResult();
        }
    }

}
