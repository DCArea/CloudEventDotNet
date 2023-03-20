using System.Runtime.CompilerServices;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;
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
    private Task? _pollNewMessagesLoop;
    private Task? _claimPendingMessagesLoop;

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
    }

    public async Task StartAsync()
    {
        try
        {
            _ = await _database.StreamCreateConsumerGroupAsync(
                _channelContext.Topic,
                _channelContext.ConsumerGroup,
                StreamPosition.NewMessages);
        }
        catch (RedisServerException ex)
        {
            if (ex.Message != "BUSYGROUP Consumer Group name already exists")
            {
                throw;
            }
        }

        _pollNewMessagesLoop = Task.Run(PollNewMessagesLoop, default);
        _claimPendingMessagesLoop = Task.Run(ClaimPendingMessagesLoop, default);
    }

    public async Task StopAsync()
    {
        if (_pollNewMessagesLoop is null || _claimPendingMessagesLoop is null)
        {
            throw new InvalidOperationException("Redis message channel has not started");
        }
        await _pollNewMessagesLoop;
        await _claimPendingMessagesLoop;
        _telemetry.Logger.LogDebug("Writer stopped");
    }

    private async Task PollNewMessagesLoop()
    {
        _telemetry.OnFetchMessagesLoopStarted();
        while (!_stopToken.IsCancellationRequested)
        {
            try
            {
                StreamEntry[] messages = await _database.StreamReadGroupAsync(
                    _channelContext.Topic,
                    _channelContext.ConsumerGroup,
                    _channelContext.ConsumerGroup,
                    StreamPosition.NewMessages,
                    _options.PollBatchSize).ConfigureAwait(false);

                if (messages.Length > 0)
                {
                    _telemetry.OnMessagesFetched(messages.Length);
                    await DispatchMessages(messages).ConfigureAwait(false);
                    continue;
                }
                _telemetry.OnNoMessagesFetched();
                await Task.Delay(_options.PollInterval, _stopToken);
            }
            catch (Exception ex)
            {
                if (ex is TaskCanceledException cancel && cancel.CancellationToken == _stopToken)
                {
                    break;
                }
                _telemetry.OnFetchNewMessagesError(ex);
                try
                {
                    await Task.Delay(_options.PollInterval, _stopToken);
                }
                catch { }
            }
        }
        _telemetry.OnFetchNewMessagesLoopStopped();
    }


    private async Task ClaimPendingMessagesLoop()
    {
        _telemetry.OnClaimMessagesLoopStarted();
        while (!_stopToken.IsCancellationRequested)
        {
            try
            {
                await ClaimPendingMessages().ConfigureAwait(false);
                await Task.Delay(_options.PollInterval, _stopToken);
            }
            catch (Exception ex)
            {
                if (ex is TaskCanceledException cancel && cancel.CancellationToken == _stopToken)
                {
                    break;
                }
                _telemetry.OnClaimMessagesError(ex);
                try
                {
                    await Task.Delay(_options.PollInterval, _stopToken);
                }
                catch { }
            }
        }
        _telemetry.OnClaimMessagesLoopStopped();

        async Task ClaimPendingMessages()
        {
            while (!_stopToken.IsCancellationRequested)
            {
                StreamPendingMessageInfo[] pendingMessages = await _database.StreamPendingMessagesAsync(
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

                RedisValue[] messagesToClaim = pendingMessages
                    .Where(msg => msg.IdleTimeInMilliseconds >= _options.ProcessingTimeout.TotalMilliseconds)
                    .Select(msg => msg.MessageId)
                    .ToArray();

                if (messagesToClaim.Length == 0)
                {
                    StreamPendingMessageInfo first = pendingMessages[0];
                    _telemetry.OnNoTimeoutedMessagesToClaim(first.MessageId.ToString(), first.IdleTimeInMilliseconds, first.DeliveryCount);
                    return;
                }

                StreamEntry[] claimedMessages = await _database.StreamClaimAsync(
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

    private async ValueTask DispatchMessages(StreamEntry[] messages, [CallerMemberName]string caller = "")
    {
        foreach (StreamEntry message in messages)
        {
            if (message.IsNull)
            {
                _telemetry.OnNullMessageFetched(caller);
            }

            var workItem = new RedisMessageWorkItem(
                _channelContext,
                _workItemContext,
                message
            );
            if (!_channelWriter.TryWrite(workItem))
            {
                await _channelWriter.WriteAsync(workItem).ConfigureAwait(false);
            }
            _ = ThreadPool.UnsafeQueueUserWorkItem(workItem, false);
            _telemetry.OnMessageDispatched(message.Id.ToString());
        }
        _telemetry.OnMessagesDispatched(messages.Length);
    }

}
