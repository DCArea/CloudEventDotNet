using System.Diagnostics;

namespace CloudEventDotNet.Redis.Telemetry;

internal static class Tracing
{
    public static void OnMessageProduced(string clientName)
    {
        var activity = Activity.Current;
        activity?.SetTag("messaging.redis.client_name", clientName);
    }

    public static void OnMessageProcessing(
        string consumerGroup,
        string consumerName
    )
    {
        Activity? activity = Activity.Current;
        if (activity is not null)
        {
            activity.SetTag("messaging.redis.client_id", consumerName);
            activity.SetTag("messaging.redis.consumer_group", consumerGroup);
        }
    }

}
