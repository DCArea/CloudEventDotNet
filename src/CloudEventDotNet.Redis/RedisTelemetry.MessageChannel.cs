using CloudEventDotNet.Diagnostics.Aggregators;
using CloudEventDotNet.Redis.Instruments;
using Microsoft.Extensions.Logging;

namespace CloudEventDotNet.Redis;

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
        ILoggerFactory loggerFactory,
        RedisMessageChannelContext channelContext)
    {
        _logger = loggerFactory.CreateLogger($"{nameof(RedisMessageTelemetry)}:{channelContext.PubSubName}:{channelContext.Topic}");
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

    [LoggerMessage(
        Level = LogLevel.Error,
        Message = "Error on fetching new messages"
    )]
    public partial void OnFetchNewMessagesError(Exception exception);

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
        Level = LogLevel.Error,
        Message = "Error on claiming pending messages"
    )]
    public partial void OnClaimMessagesError(Exception exception);

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

    // [LoggerMessage(
    //     Level = LogLevel.Debug,
    //     Message = "No timeouted messages to claim, max idle: {maxIdle}ms, waiting for next loop"
    // )]
    // public partial void OnNoTimeoutedMessagesToClaim(long maxIdle);

    [LoggerMessage(
        Level = LogLevel.Debug,
        Message = "No timeouted messages to claim, waiting for next loop, earliest: id: {id}, idle {idle}ms , dc {dc}"
    )]
    public partial void OnNoTimeoutedMessagesToClaim(string id, long idle, int dc);

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

    // reader
    [LoggerMessage(
        Level = LogLevel.Debug,
        Message = "Message channel reader started")]
    public partial void OnMessageChannelReaderStarted();

    [LoggerMessage(
        Level = LogLevel.Trace,
        Message = "Work item not started, starting it")]
    public partial void OnWorkItemStarting();

    [LoggerMessage(
        Level = LogLevel.Trace,
        Message = "Work item started")]
    public partial void OnWorkItemStarted();

    [LoggerMessage(
        Level = LogLevel.Trace,
        Message = "Work item not completed, waiting")]
    public partial void OnWaitingWorkItemComplete();

    [LoggerMessage(
        Level = LogLevel.Trace,
        Message = "Work item completed")]
    public partial void OnWorkItemCompleted();

    [LoggerMessage(
        Level = LogLevel.Trace,
        Message = "Reader cancelled")]
    public partial void MessageChannelReaderCancelled();

    [LoggerMessage(
        Level = LogLevel.Debug,
        Message = "Reader stopped")]
    public partial void MessageChannelReaderStopped();

    [LoggerMessage(
        Level = LogLevel.Trace,
        Message = "Waiting for next work item")]
    public partial void WaitingForNextWorkItem();

    [LoggerMessage(
        EventId = 10700,
        Level = LogLevel.Error,
        Message = "Exception on polling")]
    public partial void ExceptionOnReadingWorkItems(Exception exception);

}
