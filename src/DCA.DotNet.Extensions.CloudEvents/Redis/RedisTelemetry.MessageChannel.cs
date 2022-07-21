using DCA.DotNet.Extensions.CloudEvents.Diagnostics.Aggregators;
using DCA.DotNet.Extensions.CloudEvents.Redis.Instruments;
using Microsoft.Extensions.Logging;

namespace DCA.DotNet.Extensions.CloudEvents.Redis;

#pragma warning disable IDE0032

internal sealed partial class RedisMessageTelemetry
{
    private static readonly CounterAggregatorGroup s_newMessageFetchedCounterGroup
        = new(RedisTelemetry.Meter, "dca_cloudevents_redis_message_fetched");
    private static readonly CounterAggregatorGroup s_messageClaimedCounterGroup
        = new(RedisTelemetry.Meter, "dca_cloudevents_redis_message_claimed");
    private static readonly CounterAggregatorGroup s_workItemDispatchedCounterGroup
        = new(RedisTelemetry.Meter, "dca_cloudevents_redis_message_workitem_dispatched");

    private static readonly CounterAggregatorGroup s_messageAckedCounterGroup
        = new(RedisTelemetry.Meter, "dca_cloudevents_redis_message_acked");

    private readonly ILogger _logger;
    public ILogger Logger => _logger;
    private readonly CounterAggregator _fetchCounter;
    private readonly CounterAggregator _claimCounter;
    private readonly CounterAggregator _dispatchedCounter;
    private readonly CounterAggregator _ackCounter;

    public RedisMessageTelemetry(
        ILogger logger,
        RedisMessageChannelContext channelContext)
    {
        _logger = logger;
        var tagList = new TagList("pubsub", channelContext.PubSubName, "topic", channelContext.Topic);
        _fetchCounter = s_newMessageFetchedCounterGroup.FindOrCreate(tagList);
        _claimCounter = s_messageClaimedCounterGroup.FindOrCreate(tagList);
        _dispatchedCounter = s_workItemDispatchedCounterGroup.FindOrCreate(tagList);
        _ackCounter = s_messageAckedCounterGroup.FindOrCreate(tagList);
    }

    [LoggerMessage(
        Level = LogLevel.Debug,
        Message = "Started fetch new messages loop"
    )]
    public partial void OnFetchMessagesLoopStarted();

    [LoggerMessage(
        Level = LogLevel.Debug,
        Message = "Stopped fetch new messages loop"
    )]
    public partial void OnFetchNewMessagesLoopStopped();

    // ..

    [LoggerMessage(
        Level = LogLevel.Debug,
        Message = "Fetched {count} new messages"
    )]
    private partial void LogOnNewMessagesFetched(int count);
    public void OnMessagesFetched(int count)
    {
        LogOnNewMessagesFetched(count);
        _fetchCounter.Add(count);
    }

    // ..

    [LoggerMessage(
        Level = LogLevel.Trace,
        Message = "Message {id} dispatched to process"
    )]
    public partial void OnMessageDispatched(string id);

    [LoggerMessage(
        Level = LogLevel.Debug,
        Message = "Dispatched {count} messages to process"
    )]
    private partial void LogOnMessagesDispatched(int count);
    public void OnMessagesDispatched(int count)
    {
        LogOnMessagesDispatched(count);
        _dispatchedCounter.Add(count);
    }

    [LoggerMessage(
        Level = LogLevel.Debug,
        Message = "No new messages, waiting for next loop"
    )]
    public partial void OnNoMessagesFetched();


    // claim
    [LoggerMessage(
        Level = LogLevel.Debug,
        Message = "Started claim messages loop"
    )]
    public partial void OnClaimMessagesLoopStarted();

    [LoggerMessage(
        Level = LogLevel.Debug,
        Message = "Stopped claim messages loop"
    )]
    public partial void OnClaimMessagesLoopStopped();

    [LoggerMessage(
        Level = LogLevel.Debug,
        Message = "Fetched {count} pending messages"
    )]
    public partial void OnPendingMessagesInformationFetched(int count);

    [LoggerMessage(
        Level = LogLevel.Debug,
        Message = "No messages to claim, waiting for next loop"
    )]
    public partial void OnNoMessagesToClaim();

    [LoggerMessage(
        Level = LogLevel.Debug,
        Message = "No timeouted messages to claim, max idle: {maxIdle}ms, waiting for next loop"
    )]
    public partial void OnNoTimeoutedMessagesToClaim(long maxIdle);

    [LoggerMessage(
        Level = LogLevel.Debug,
        Message = "Claimed {count} messages"
    )]
    private partial void LogOnMessagesClaimed(int count);
    public void OnMessagesClaimed(int count)
    {
        LogOnMessagesClaimed(count);
        _claimCounter.Add(count);
    }

    // ack
    [LoggerMessage(
        Level = LogLevel.Trace,
        Message = "Message {id} acknowledged"
    )]
    private partial void LogOnMessageAcknowledged(string id);

    public void OnMessageAcknowledged(string id)
    {
        LogOnMessageAcknowledged(id);
        _ackCounter.Add(1);
    }

    [LoggerMessage(
        Level = LogLevel.Error,
        Message = "Failed to process message {id}"
    )]
    public partial void OnProcessMessageFailed(string id, Exception exception);

}
