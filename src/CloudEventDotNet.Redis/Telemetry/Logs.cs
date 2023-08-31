using Microsoft.Extensions.Logging;

namespace CloudEventDotNet.Redis.Telemetry;

internal static partial class Logs
{
    [LoggerMessage(
         Level = LogLevel.Debug,
         Message = "[{pubsub}] Produced message {messageId} to {topic}"
     )]
    public static partial void MessageProduced(ILogger logger, string pubsub, string topic, string messageId);

    [LoggerMessage(
        Level = LogLevel.Debug,
        Message = "[{pubsub}/{topic}] Fetched {count} messages"
    )]
    public static partial void MessageBatchFetched(ILogger logger, string pubsub, string topic, int count);

    [LoggerMessage(
        Level = LogLevel.Debug,
        Message = "[{pubsub}/{topic}] No new messages, waiting for next loop"
    )]
    public static partial void MessagesFetched(ILogger logger, string pubsub, string topic);

    [LoggerMessage(
        Level = LogLevel.Error,
        Message = "[{pubsub}/{topic}] Failed to fetch new messages"
    )]
    public static partial void MessageBatchFetchFailed(ILogger logger, string pubsub, string topic, Exception exception);

    [LoggerMessage(
        Level = LogLevel.Trace,
        Message = "[{pubsub}/{topic}] Acknowledged message {id} "
    )]
    public static partial void MessageAcknowledged(ILogger logger, string pubsub, string topic, string id);

    [LoggerMessage(
        Level = LogLevel.Error,
        Message = "[{pubsub}/{topic}] Failed to process message {id}"
    )]
    public static partial void MessageProcessFailed(ILogger logger, string pubsub, string topic, string id, Exception exception);

    [LoggerMessage(
        Level = LogLevel.Error,
        Message = "[{pubsub}/{topic}] Failed to claim pending messages"
    )]
    public static partial void PendingMessagesClaimFailed(ILogger logger, string pubsub, string topic, Exception exception);

    [LoggerMessage(
        Level = LogLevel.Debug,
        Message = "[{pubsub}/{topic}] Fetched {count} pending messages"
    )]
    public static partial void PendingMessagesFetched(ILogger logger, string pubsub, string topic, int count);

    [LoggerMessage(
        Level = LogLevel.Debug,
        Message = "[{pubsub}/{topic}] No messages to claim, waiting for next loop"
    )]
    public static partial void MessagesToClaim(ILogger logger, string pubsub, string topic);

    [LoggerMessage(
        Level = LogLevel.Debug,
        Message = "[{pubsub}/{topic}] No timeouted messages to claim, waiting for next loop, earliest: id: {id}, idle {idle}ms , dc {dc}"
    )]
    public static partial void TimeoutedMessagesToClaim(ILogger logger, string pubsub, string topic, string id, long idle, int dc);


    [LoggerMessage(
        Level = LogLevel.Information,
        Message = "[{pubsub}/{topic}] Claimed {count} messages"
    )]
    public static partial void MessagesClaimed(ILogger logger, string pubsub, string topic, int count);

    [LoggerMessage(
        Level = LogLevel.Debug,
        Message = "[{pubsub}/{topic}] Claiming orphan message {messageId}"
    )]
    public static partial void OrphanMessageClaiming(ILogger logger, string pubsub, string topic, string messageId);

    [LoggerMessage(
        Level = LogLevel.Information,
        Message = "[{pubsub}/{topic}] Acked orphan message {messageId}"
    )]
    public static partial void OrphanMessageAcked(ILogger logger, string pubsub, string topic, string messageId);
}
