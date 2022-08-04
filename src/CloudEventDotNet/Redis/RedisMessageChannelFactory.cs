using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using StackExchange.Redis;

namespace CloudEventDotNet.Redis;

internal class RedisMessageChannelFactory
{
    private readonly IOptionsFactory<RedisSubscribeOptions> _optionsFactory;
    private readonly Registry _registry;
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly ILoggerFactory _loggerFactory;

    public RedisMessageChannelFactory(
        IOptionsFactory<RedisSubscribeOptions> optionsFactory,
        Registry registry,
        IServiceScopeFactory scopeFactory,
        ILoggerFactory loggerFactory)
    {
        _optionsFactory = optionsFactory;
        _registry = registry;
        _scopeFactory = scopeFactory;
        _loggerFactory = loggerFactory;
    }

    public RedisMessageChannel[] Create(string pubSubName)
    {
        var options = _optionsFactory.Create(pubSubName);
        var multiplexer = options.ConnectionMultiplexerFactory();
        var redis = multiplexer.GetDatabase(options.Database);

        return _registry.GetSubscribedTopics(pubSubName)
            .Select(topic => Create(pubSubName, topic, options, redis)).ToArray();
    }

    private RedisMessageChannel Create(
        string pubSubName,
        string topic,
        RedisSubscribeOptions options,
        IDatabase redis)
    {
        var logger = _loggerFactory.CreateLogger($"{nameof(RedisCloudEventSubscriber)}[{pubSubName}:{topic}]");
        var channelContext = new RedisMessageChannelContext(
            pubSubName,
            redis.Multiplexer.ClientName,
            options.ConsumerGroup,
            topic
        );
        var redisTelemetry = new RedisMessageTelemetry(_loggerFactory, channelContext);
        var workItemContext = new RedisWorkItemContext(
            _registry,
            _scopeFactory,
            redis,
            redisTelemetry
        );
        var channel = new RedisMessageChannel(
            options,
            redis,
            channelContext,
            workItemContext,
            redisTelemetry);
        return channel;
    }

}
