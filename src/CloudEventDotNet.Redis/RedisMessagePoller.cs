using CloudEventDotNet.Redis.Telemetry;
using DCA.Extensions.BackgroundTask;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;

namespace CloudEventDotNet.Redis;

internal class RedisMessagePoller
{
    private readonly string _pubSubName;
    private readonly string _topic;
    private readonly RedisSubscribeOptions _options;
    private readonly IDatabase _redis;
    private readonly ILogger<RedisMessagePoller> _logger;
    private readonly ILogger<RedisCloudEventMessage> _messageLogger;
    private readonly Registry2 _registry;
    private readonly BackgroundTaskChannel _channel;
    private readonly RedisMessageChannelContext _channelContext;
    private readonly MetricsContext _metrics;
    private readonly CancellationTokenSource _stopTokenSource = new();

    public RedisMessagePoller(
        string pubSubName,
        string topic,
        RedisSubscribeOptions options,
        IDatabase redis,
        ILogger<RedisMessagePoller> logger,
        ILogger<BackgroundTaskChannel> channelLogger,
        ILogger<RedisCloudEventMessage> messageLogger,
        Registry2 registry)
    {
        _pubSubName = pubSubName;
        _topic = topic;
        _options = options;
        _redis = redis;
        _logger = logger;
        _messageLogger = messageLogger;
        _registry = registry;
        var channelKey = $"{_pubSubName}/{_topic}";
        _channel = new BackgroundTaskChannel(channelLogger, new BackgroundTaskChannelOptions
        {
            Key = $"{_pubSubName}/{_topic}",
            Capacity = options.RunningWorkItemLimit
        });
        _channelContext = new RedisMessageChannelContext(
            _channel.Key,
            _pubSubName,
            _redis.Multiplexer.ClientName,
            _options.ConsumerGroup,
            _topic);
        _metrics = new MetricsContext(_pubSubName, topic);

    }

    private Task? _pollNewMessagesLoop;
    private Task? _claimPendingMessagesLoop;
    public async Task StartAsync()
    {
        try
        {
            await _redis.StreamCreateConsumerGroupAsync(
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

        await _channel.StartAsync();
        _pollNewMessagesLoop = Task.Run(PollNewMessagesLoop, default);
        _claimPendingMessagesLoop = Task.Run(ClaimPendingMessagesLoop, default);
        _logger.LogDebug("[{pubsub}/{topic}] Started", _pubSubName, _topic);
    }

    public async Task StopAsync()
    {
        if (_pollNewMessagesLoop is null || _claimPendingMessagesLoop is null)
        {
            throw new InvalidOperationException("Redis message channel has not started");
        }
        _stopTokenSource.Cancel();
        await _pollNewMessagesLoop;
        await _claimPendingMessagesLoop;
        await _channel.StopAsync();
        _logger.LogDebug("[{pubsub}/{topic}] Stopped", _pubSubName, _topic);
    }

    private async Task PollNewMessagesLoop()
    {
        _logger.LogDebug("[{pubsub}/{topic}] Poll loop started", _pubSubName, _topic);
        while (!_stopTokenSource.Token.IsCancellationRequested)
        {
            try
            {
                StreamEntry[] messages = await _redis.StreamReadGroupAsync(
                    _topic,
                    _options.ConsumerGroup,
                    _options.ConsumerGroup,
                    StreamPosition.NewMessages,
                    _options.PollBatchSize)
                    .ConfigureAwait(false);

                if (messages.Length > 0)
                {
                    Logs.MessageBatchFetched(_logger, _pubSubName, _topic, messages.Length);
                    _metrics.OnMessagesFetched(messages.Length);
                    await DispatchMessages(messages)
                        .ConfigureAwait(false);
                    continue;
                }
                else
                {
                    Logs.MessagesFetched(_logger, _pubSubName, _topic);
                    await Task.Delay(_options.PollInterval, _stopTokenSource.Token);
                }
            }
            catch (Exception ex)
            {
                if (ex is TaskCanceledException cancel && cancel.CancellationToken == _stopTokenSource.Token)
                {
                    break;
                }
                Logs.MessageBatchFetchFailed(_logger, _pubSubName, _topic, ex);
                try
                {
                    await Task.Delay(_options.PollInterval, _stopTokenSource.Token);
                }
                catch (TaskCanceledException) { }
            }
        }
        _logger.LogDebug("[{pubsub}/{topic}] Poll loop stopped", _pubSubName, _topic);
    }


    private async Task ClaimPendingMessagesLoop()
    {
        _logger.LogDebug("[{pubsub}/{topic}] Reclaim loop started", _pubSubName, _topic);
        while (!_stopTokenSource.IsCancellationRequested)
        {
            try
            {
                await ClaimPendingMessages().ConfigureAwait(false);
                await Task.Delay(_options.PollInterval, _stopTokenSource.Token);
            }
            catch (Exception ex)
            {
                if (ex is TaskCanceledException cancel && cancel.CancellationToken == _stopTokenSource.Token)
                {
                    break;
                }
                Logs.PendingMessagesClaimFailed(_logger, _pubSubName, _topic, ex);
                try
                {
                    await Task.Delay(_options.PollInterval, _stopTokenSource.Token);
                }
                catch { }
            }
        }
        _logger.LogDebug("[{pubsub}/{topic}] Reclaim loop stopped", _pubSubName, _topic);
    }

    async Task ClaimPendingMessages()
    {
        while (!_stopTokenSource.IsCancellationRequested)
        {
            StreamPendingMessageInfo[] pendingMessages = await _redis.StreamPendingMessagesAsync(
                _channelContext.Topic,
                _channelContext.ConsumerGroup,
                _options.PollBatchSize,
                RedisValue.Null).ConfigureAwait(false);
            Logs.PendingMessagesFetched(_logger, _pubSubName, _topic, pendingMessages.Length);

            if (pendingMessages.Length == 0)
            {
                Logs.MessagesToClaim(_logger, _pubSubName, _topic);
                return;
            }

            RedisValue[] messagesToClaim = pendingMessages
                .Where(msg => msg.IdleTimeInMilliseconds >= _options.ProcessingTimeout.TotalMilliseconds)
                .Select(msg => msg.MessageId)
                .ToArray();

            if (messagesToClaim.Length == 0)
            {
                StreamPendingMessageInfo first = pendingMessages[0];
                Logs.TimeoutedMessagesToClaim(_logger, _pubSubName, _topic, first.MessageId.ToString(), first.IdleTimeInMilliseconds, first.DeliveryCount);
                return;
            }

            StreamEntry[] claimedMessages = await _redis.StreamClaimAsync(
                _channelContext.Topic,
                _channelContext.ConsumerGroup,
                _channelContext.ConsumerGroup,
                (long)_options.ProcessingTimeout.TotalMilliseconds,
                messagesToClaim
            ).ConfigureAwait(false);
            Logs.MessagesClaimed(_logger, _pubSubName, _topic, claimedMessages.Length);
            _metrics.MessagesClaimed(claimedMessages.Length);
            await DispatchMessages(claimedMessages).ConfigureAwait(false);

            var messageToRemoveIds = messagesToClaim
                .Except(claimedMessages.Where(msg => !msg.IsNull).Select(msg => msg.Id));
            await RemoveMessagesThatNoLongerExistFromPending(messageToRemoveIds);
        }
    }

    private async Task RemoveMessagesThatNoLongerExistFromPending(IEnumerable<RedisValue> messageIds)
    {
        foreach (var messageId in messageIds)
        {
            _logger.LogInformation("Claiming nil message {messageId}", messageId);
            Logs.OrphanMessageClaiming(_logger, _pubSubName, _topic, messageId.ToString());
            StreamEntry[] claimedMessages = await _redis.StreamClaimAsync(
                _channelContext.Topic,
                _channelContext.ConsumerGroup,
                _channelContext.ConsumerGroup,
                (long)_options.ProcessingTimeout.TotalMilliseconds,
                [messageId]
            ).ConfigureAwait(false);
            if (claimedMessages.Length == 0 || claimedMessages[0].IsNull)
            {
                await _redis.StreamAcknowledgeAsync(
                    _channelContext.Topic,
                    _channelContext.ConsumerGroup,
                    messageId).ConfigureAwait(false);
                Logs.OrphanMessageAcked(_logger, _pubSubName, _topic, messageId.ToString());
            }
            else
            {
                await DispatchMessages(claimedMessages);
            }
        }
    }

    private async ValueTask DispatchMessages(
        StreamEntry[] messages)
    {
        foreach (StreamEntry message in messages)
        {
            if (message.IsNull)
            {
                continue;
            }

            var workItem = new RedisCloudEventMessage(
            message,
                _messageLogger,
                _channelContext,
                _metrics,
                _registry,
                _redis
            );
            await _channel.DispatchAsync(workItem, true);
        }
    }
}
