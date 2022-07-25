using System.Threading.Channels;
using StackExchange.Redis;

namespace CloudEventDotNet.Redis;

internal sealed class RedisMessageChannelWriter
{
    private readonly RedisSubscribeOptions _options;
    private readonly IDatabase _database;
    private readonly RedisMessageChannelContext _channelContext;
    private readonly ChannelWriter<RedisMessageWorkItem> _channelWriter;
    private readonly RedisWorkItemContext _workItemContext;
    private readonly RedisMessageTelemetry _telemetry;
    private readonly CancellationToken _stopToken;
    private readonly Task _pollNewMessagesLoop;
    private readonly Task _claimPendingMessagesLoop;

    public RedisMessageChannelWriter(
        RedisSubscribeOptions options,
        IDatabase database,
        RedisMessageChannelContext channelContext,
        ChannelWriter<RedisMessageWorkItem> channelWriter,
        RedisWorkItemContext workItemContext,
        RedisMessageTelemetry telemetry,
        CancellationToken stopToken)
    {
        _options = options;
        _database = database;
        _channelContext = channelContext;
        _channelWriter = channelWriter;
        _workItemContext = workItemContext;
        _telemetry = telemetry;
        _stopToken = stopToken;
        _pollNewMessagesLoop = Task.Run(PollNewMessagesLoop, default);
        _claimPendingMessagesLoop = Task.Run(ClaimPendingMessagesLoop, default);
    }

    public async Task StopAsync()
    {
        await _pollNewMessagesLoop;
        await _claimPendingMessagesLoop;
    }

    private async Task PollNewMessagesLoop()
    {
        try
        {
            _telemetry.OnFetchMessagesLoopStarted();
            try
            {
                await _database.StreamCreateConsumerGroupAsync(
                    _channelContext.Topic,
                    _channelContext.ConsumerGroup,
                    StreamPosition.NewMessages);
            }
            catch (RedisServerException ex)
            {
                if (ex.Message != "BUSYGROUP Consumer Group name already exists") throw;
            }

            while (!_stopToken.IsCancellationRequested)
            {
                var messages = await _database.StreamReadGroupAsync(
                    _channelContext.Topic,
                    _channelContext.ConsumerGroup,
                    _channelContext.ConsumerGroup,
                    StreamPosition.NewMessages,
                    _options.PollBatchSize).ConfigureAwait(false);

                if (messages.Length > 0)
                {
                    _telemetry.OnMessagesFetched(messages.Length);
                    await DispatchMessages(messages).ConfigureAwait(false);
                }
                else
                {
                    _telemetry.OnNoMessagesFetched();
                    await Task.Delay(_options.PollInterval, default);
                }
            }
            _telemetry.OnFetchNewMessagesLoopStopped();
        }
        catch (Exception ex)
        {
            _telemetry.OnFetchNewMessagesLoopError(ex);
            throw;
        }
    }


    private async Task ClaimPendingMessagesLoop()
    {
        try
        {
            _telemetry.OnClaimMessagesLoopStarted();
            while (!_stopToken.IsCancellationRequested)
            {
                await ClaimPendingMessages().ConfigureAwait(false);
                await Task.Delay(_options.PollInterval, default);
            }
            _telemetry.OnClaimMessagesLoopStopped();

            async Task ClaimPendingMessages()
            {
                while (!_stopToken.IsCancellationRequested)
                {
                    var pendingMessages = await _database.StreamPendingMessagesAsync(
                        _channelContext.Topic,
                        _channelContext.ConsumerGroup,
                        _options.PollBatchSize,
                        RedisValue.Null).ConfigureAwait(false);
                    _telemetry.OnPendingMessagesInformationFetched(pendingMessages.Length);

                    if (pendingMessages.Length == 0)
                    {
                        _telemetry.OnNoMessagesToClaim();
                        return;
                    }

                    var messagesToClaim = pendingMessages
                        .Where(msg => msg.IdleTimeInMilliseconds >= _options.ProcessingTimeout.TotalMilliseconds)
                        .Select(msg => msg.MessageId)
                        .ToArray();

                    if (messagesToClaim.Length == 0)
                    {
                        var first = pendingMessages[0];
                        _telemetry.OnNoTimeoutedMessagesToClaim(first.MessageId.ToString(), first.IdleTimeInMilliseconds, first.DeliveryCount);
                        return;
                    }

                    var claimedMessages = await _database.StreamClaimAsync(
                        _channelContext.Topic,
                        _channelContext.ConsumerGroup,
                        _channelContext.ConsumerGroup,
                        (long)_options.ProcessingTimeout.TotalMilliseconds,
                        messagesToClaim
                    ).ConfigureAwait(false);

                    _telemetry.OnMessagesClaimed(claimedMessages.Length);
                    await DispatchMessages(claimedMessages).ConfigureAwait(false);
                }
            }
        }
        catch (Exception ex)
        {
            _telemetry.OnClaimMessagesLoopError(ex);
            throw;
        }
    }

    private async ValueTask DispatchMessages(StreamEntry[] messages)
    {
        foreach (var message in messages)
        {
            var workItem = new RedisMessageWorkItem(
                _channelContext,
                _workItemContext,
                message
            );
            if (!_channelWriter.TryWrite(workItem))
            {
                await _channelWriter.WriteAsync(workItem).ConfigureAwait(false);
            }
            ThreadPool.UnsafeQueueUserWorkItem(workItem, false);
            _telemetry.OnMessageDispatched(message.Id.ToString());
        }
        _telemetry.OnMessagesDispatched(messages.Length);
    }

}
