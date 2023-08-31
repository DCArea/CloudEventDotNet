using CloudEventDotNet.Diagnostics.Aggregators;

namespace CloudEventDotNet.Redis.Telemetry;

internal static class Metrics
{
    internal static readonly CounterAggregatorGroup s_newMessageFetchedCounterGroup
        = new(CloudEventDotNet.Telemetry.Metrics.Meter, "dca_cloudevents_redis_message_fetched");
    internal static readonly CounterAggregatorGroup s_messageClaimedCounterGroup
        = new(CloudEventDotNet.Telemetry.Metrics.Meter, "dca_cloudevents_redis_message_claimed");
    internal static readonly CounterAggregatorGroup s_messageAckedCounterGroup
        = new(CloudEventDotNet.Telemetry.Metrics.Meter, "dca_cloudevents_redis_message_acked");
}

internal class MetricsContext
{
    private readonly CounterAggregator _fetchCounter;
    private readonly CounterAggregator _claimCounter;
    private readonly CounterAggregator _ackCounter;

    public MetricsContext(string pubSubName, string topic)
    {
        var tagList = new TagList("pubsub", pubSubName, "topic", topic);
        _fetchCounter = Metrics.s_newMessageFetchedCounterGroup.FindOrCreate(tagList);
        _claimCounter = Metrics.s_messageClaimedCounterGroup.FindOrCreate(tagList);
        _ackCounter = Metrics.s_messageAckedCounterGroup.FindOrCreate(tagList);
    }

    public void OnMessagesFetched(int count)
    {
        _fetchCounter.Add(count);
    }

    public void MessagesClaimed(int count)
    {
        _claimCounter.Add(count);
    }

    public void OnMessageAcknowledged()
    {
        _ackCounter.Add(1);
    }
}
