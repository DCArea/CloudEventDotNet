using DCA.DotNet.Extensions.CloudEvents.Redis.Instruments;
using Microsoft.Extensions.Logging;

namespace DCA.DotNet.Extensions.CloudEvents.Redis;

internal class RedisCloudEventSubscriber : ICloudEventSubscriber
{
    private readonly ILogger _logger;
    private readonly RedisCloudEventTopicSubscriber[] _subscribers;

    public RedisCloudEventSubscriber(
        string pubSubName,
        ILogger<RedisCloudEventSubscriber> logger,
        RedisProcessChannelFactory channelFactory)
    {
        _logger = logger;
        _subscribers = channelFactory.Create(pubSubName);
    }

    public void Subscribe(CancellationToken token)
    {
        RedisTelemetry.OnSubscriberStarting(_logger);
        SubscribeAsync(token).GetAwaiter().GetResult();
    }

    private async Task SubscribeAsync(CancellationToken token)
    {
        try
        {
            await Task.WhenAll(_subscribers.Select(s => s.SubscribeAsync(token)));
            await Task.WhenAll(_subscribers.Select(s => s.Shutdown()));
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error in subscription loop");
            throw;
        }
    }

}
