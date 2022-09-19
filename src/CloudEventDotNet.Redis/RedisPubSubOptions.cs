using System.ComponentModel.DataAnnotations;
using StackExchange.Redis;

namespace CloudEventDotNet.Redis;

public abstract class RedisPubSubOptions
{
    [Required]
    public Func<IConnectionMultiplexer> ConnectionMultiplexerFactory { get; set; } = default!;

    public int Database { get; set; }
}

public class RedisPublishOptions : RedisPubSubOptions
{
    public int? MaxLength { get; set; }
}

public class RedisSubscribeOptions : RedisPubSubOptions
{
    [Required]
    public string ConsumerGroup { get; set; } = default!;

    public int PollBatchSize { get; set; } = 100;

    public TimeSpan PollInterval { get; set; } = TimeSpan.FromSeconds(15);

    public int RunningWorkItemLimit { get; set; } = 128;

    public TimeSpan ProcessingTimeout { get; set; } = TimeSpan.FromSeconds(60);
}
