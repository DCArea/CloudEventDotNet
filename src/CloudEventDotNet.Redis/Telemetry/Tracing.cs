using System.Diagnostics;

namespace CloudEventDotNet.Redis.Telemetry;

internal static class Tracing
{
    public static void OnMessageProduced(string messageId)
    {
        var activity = Activity.Current;
        activity?.SetTag("messaging.redis.message.id", messageId);
    }

    public static void OnMessageProcessing(
        string consumerGroup,
        string messageId
    )
    {
        Activity? activity = Activity.Current;
        if (activity is not null)
        {
            activity.SetTag("messaging.redis.consumer.group", consumerGroup);
            activity?.SetTag("messaging.redis.message.id", messageId);
        }
    }

}
