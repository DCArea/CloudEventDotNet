namespace CloudEventDotNet.Redis;

internal sealed record RedisMessageChannelContext(
    string PubSubName,
    string ConsumerName,
    string ConsumerGroup,
    string Topic
);
