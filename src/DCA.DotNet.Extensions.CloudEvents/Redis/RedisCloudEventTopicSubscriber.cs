using Microsoft.Extensions.Logging;
using StackExchange.Redis;

namespace DCA.DotNet.Extensions.CloudEvents.Redis;

internal class RedisCloudEventTopicSubscriber
{
    private readonly RedisSubscribeOptions _options;
    private readonly IDatabase _database;
    private readonly RedisMessageChannel _channel;
    private readonly RedisMessageChannelContext _channelContext;
    private readonly RedisWorkItemContext _workItemContext;
    private readonly RedisMessageTelemetry _telemetry;

    public RedisCloudEventTopicSubscriber(
        RedisSubscribeOptions options,
        IDatabase database,
        RedisMessageChannelContext channelContext,
        RedisMessageChannel channel,
        RedisWorkItemContext workItemContext)
    {
        _options = options;
        _database = database;
        _channelContext = channelContext;
        _workItemContext = workItemContext;
        _channel = channel;
        _telemetry = workItemContext.RedisTelemetry;
    }

    public async Task SubscribeAsync(CancellationToken token)
    {
        try
        {
            var task1 = PollNewMessagesLoop(token);
            var task2 = ClaimPendingMessagesLoop(token);
            await task1;
            await task2;
        }
        catch (Exception ex)
        {
            _telemetry.Logger.LogError(ex, "Error in subscription loop");
            throw;
        }
    }

    public Task Shutdown()
    {
        _channel.Deactivate();
        return _channel.PollTask;
    }

    private async Task PollNewMessagesLoop(CancellationToken token)
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

        while (!token.IsCancellationRequested)
        {
            var messages = await _database.StreamReadGroupAsync(
                _channelContext.Topic,
                _channelContext.ConsumerGroup,
                _channelContext.ConsumerGroup,
                StreamPosition.NewMessages,
                _options.PollBatchSize);

            if (messages.Length > 0)
            {
                _telemetry.OnMessagesFetched(messages.Length);
                await DispatchMessages(messages);
            }
            else
            {
                _telemetry.OnNoMessagesFetched();
                await Task.Delay(_options.PollInterval, default);
            }
        }

        _telemetry.OnFetchNewMessagesLoopStopped();
    }

    private async Task ClaimPendingMessagesLoop(CancellationToken token)
    {
        _telemetry.OnClaimMessagesLoopStarted();
        while (!token.IsCancellationRequested)
        {
            await ClaimPendingMessages(token);
            await Task.Delay(_options.PollInterval, default);
        }
        _telemetry.OnClaimMessagesLoopStopped();

        async Task ClaimPendingMessages(CancellationToken token)
        {
            while (!token.IsCancellationRequested)
            {
                var pendingMessages = await _database.StreamPendingMessagesAsync(
                    _channelContext.Topic,
                    _channelContext.ConsumerGroup,
                    _options.PollBatchSize,
                    RedisValue.Null);
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
                    _telemetry.OnNoTimeoutedMessagesToClaim(pendingMessages.First().IdleTimeInMilliseconds);
                    return;
                }

                var claimedMessages = await _database.StreamClaimAsync(
                    _channelContext.Topic,
                    _channelContext.ConsumerGroup,
                    _channelContext.ConsumerGroup,
                    (long)_options.ProcessingTimeout.TotalMilliseconds,
                    messagesToClaim
                );

                _telemetry.OnMessagesClaimed(claimedMessages.Length);
                await DispatchMessages(claimedMessages);
            }
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
            await _channel.WriteAsync(workItem);
            _telemetry.OnMessageDispatched(message.Id.ToString());
        }
        _telemetry.OnMessagesDispatched(messages.Length);
    }

}
