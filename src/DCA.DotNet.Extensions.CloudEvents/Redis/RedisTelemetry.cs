
using System.Diagnostics;
using System.Diagnostics.Metrics;
using DCA.DotNet.Extensions.CloudEvents.Diagnostics.Aggregators;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;

namespace DCA.DotNet.Extensions.CloudEvents.Redis.Instruments;

internal static partial class RedisTelemetry
{
    static RedisTelemetry()
    {
        Meter = new("DCA.CloudEvents.Redis", "0.0.1");
    }

    [LoggerMessage(
        Level = LogLevel.Debug,
        Message = "Produced message {messageId} to {topic}"
    )]
    public static partial void LogOnMessageProduced(ILogger logger, string topic, string messageId);
    public static void OnMessageProduced(ILogger logger, IConnectionMultiplexer multiplexer, string topic, string messageId)
    {
        LogOnMessageProduced(logger, topic, messageId);
        var activity = Activity.Current;
        if (activity is not null)
        {
            activity.SetTag("messaging.redis.client_name", multiplexer.ClientName);
        }
    }

    public static void OnMessageProcessed(
        Activity? activity,
        string consumerGroup,
        string consumerName
    )
    {
        if (activity is not null)
        {
            activity.SetTag("messaging.redis.client_id", consumerName);
            activity.SetTag("messaging.redis.consumer_group", consumerGroup);
        }
    }

    public static Meter Meter { get; }


    [LoggerMessage(
        Level = LogLevel.Debug,
        Message = "Subscribe starting"
    )]
    public static partial void OnSubscriberStarting(ILogger logger);

    [LoggerMessage(
        Level = LogLevel.Information,
        Message = "Subscriber started"
    )]
    public static partial void OnSubscriberStarted(ILogger logger);

}
