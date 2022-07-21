using StackExchange.Redis;

namespace DCA.DotNet.Extensions.CloudEvents.Redis;

internal sealed class RedisMessageWorkItemLifetime : IWorkItemLifetime<RedisMessageWorkItem>
{
    private readonly IDatabase _redis;

    public RedisMessageWorkItemLifetime(IDatabase redis)
    {
        _redis = redis;
    }

    public ValueTask OnProcessed(RedisMessageWorkItem workItem)
    {
        return new(_redis.StreamAcknowledgeAsync(
            workItem.ChannelContext.Topic,
            workItem.ChannelContext.ConsumerGroup,
            workItem.Message.Id));
    }
    public ValueTask OnFinished(RedisMessageWorkItem workItem) => ValueTask.CompletedTask;

}
