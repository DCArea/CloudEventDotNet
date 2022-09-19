using CloudEventDotNet.Redis.Instruments;
using Microsoft.Extensions.Logging;

namespace CloudEventDotNet.Redis;

internal class RedisCloudEventSubscriber : ICloudEventSubscriber
{
    private readonly string _pubSubName;
    private readonly ILogger _logger;
    private readonly RedisMessageChannelFactory _channelFactory;
    private RedisMessageChannel[]? _subscribers;

    public RedisCloudEventSubscriber(
        string pubSubName,
        ILogger<RedisCloudEventSubscriber> logger,
        RedisMessageChannelFactory channelFactory)
    {
        _pubSubName = pubSubName;
        _logger = logger;
        _channelFactory = channelFactory;
    }

    public Task StartAsync()
    {
        RedisTelemetry.OnSubscriberStarting(_logger);
        _subscribers = _channelFactory.Create(_pubSubName);
        return Task.CompletedTask;
    }

    public async Task StopAsync()
    {
        await Task.WhenAll(_subscribers!.Select(s => s.StopAsync()));
    }
}
