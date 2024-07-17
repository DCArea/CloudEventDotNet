using DCA.Extensions.BackgroundTask;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;

namespace CloudEventDotNet.Redis;

internal class RedisCloudEventSubscriber : ICloudEventSubscriber
{
    private readonly string _pubSubName;
    private readonly RedisSubscribeOptions _options;
    private readonly ILogger _logger;
    private readonly Registry2 _registry;
    private readonly ILoggerFactory _loggerFactory;
    private readonly IDatabase _database;
    private readonly RedisMessagePoller[] _pollers;

    public RedisCloudEventSubscriber(
        string pubSubName,
        RedisSubscribeOptions options,
        ILogger<RedisCloudEventSubscriber> logger,
        Registry2 registry,
        ILoggerFactory loggerFactory)
    {
        _pubSubName = pubSubName;
        _options = options;
        _logger = logger;
        _registry = registry;
        _loggerFactory = loggerFactory;
        _database = options.ConnectionMultiplexerFactory().GetDatabase(options.Database);

        _pollers = _registry.GetSubscribedTopics(_pubSubName)
            .Select(topic => new RedisMessagePoller(
                _pubSubName,
                topic,
                _options,
                _database,
                _loggerFactory.CreateLogger<RedisMessagePoller>(),
                _loggerFactory.CreateLogger<BackgroundTaskChannel>(),
                _loggerFactory.CreateLogger<RedisCloudEventMessage>(),
                _registry
                ))
            .ToArray();
    }

    public async Task StartAsync()
    {
        await Task.WhenAll(_pollers.Select(sub => sub.StartAsync()));
        _logger.LogInformation("[{pubsub}] Started", _pubSubName);
    }

    public async Task StopAsync()
    {
        await Task.WhenAll(_pollers.Select(s => s.StopAsync()));
        _logger.LogInformation("[{pubsub}] Stopped", _pubSubName);
    }

}
