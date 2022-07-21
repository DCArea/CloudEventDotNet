using System.Text.Json;
using DCA.DotNet.Extensions.CloudEvents.Redis.Instruments;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;

namespace DCA.DotNet.Extensions.CloudEvents.Redis;

internal class RedisCloudEventPublisher : ICloudEventPublisher
{
    private readonly ILogger<RedisCloudEventPublisher> _logger;
    private readonly RedisPublishOptions _options;
    private readonly IConnectionMultiplexer _multiplexer;
    private readonly IDatabase _database;

    public RedisCloudEventPublisher(ILogger<RedisCloudEventPublisher> logger, RedisPublishOptions options)
    {
        _logger = logger;
        _options = options;
        _multiplexer = _options.ConnectionMultiplexerFactory();
        _database = _multiplexer.GetDatabase(_options.Database);
    }


    public async Task PublishAsync<TData>(string topic, CloudEvent<TData> cloudEvent)
    {
        byte[] data = JsonSerializer.SerializeToUtf8Bytes(cloudEvent);
        var id = await _database.StreamAddAsync(topic, "data", data, maxLength: _options.MaxLength, useApproximateMaxLength: true);
        RedisTelemetry.OnMessageProduced(_logger, _multiplexer, topic, id.ToString());
    }
}
