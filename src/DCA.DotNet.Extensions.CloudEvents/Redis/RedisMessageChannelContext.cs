namespace DCA.DotNet.Extensions.CloudEvents.Redis;

internal record RedisMessageChannelContext(
    string PubSubName,
    string ConsumerName,
    string ConsumerGroup,
    string Topic
);
