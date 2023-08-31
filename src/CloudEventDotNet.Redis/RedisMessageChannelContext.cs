namespace CloudEventDotNet.Redis;

internal sealed record RedisMessageChannelContext(
    string Key,
    string PubSubName,
    string ConsumerName,
    string ConsumerGroup,
    string Topic
);
