using System.ComponentModel.DataAnnotations;
using StackExchange.Redis;

namespace CloudEventDotNet.Redis;

public abstract class RedisPubSubOptions
{
    /// <summary>
    /// A factory to resolve redis IConnectionMultiplexer used by pubsub.
    /// </summary>
    [Required]
    public Func<IConnectionMultiplexer> ConnectionMultiplexerFactory { get; set; } = default!;

    /// <summary>
    /// Database selected after connecting to redis.
    /// </summary>
    public int Database { get; set; } = -1;
}

/// <summary>
/// Options for redis publisher.
/// </summary>
public class RedisPublishOptions : RedisPubSubOptions
{
    /// <summary>
    /// Maximum number of items inside a stream.The old entries are automatically evicted when the specified length is reached, so that the stream is left at a constant size. Defaults to unlimited.
    /// </summary>
    public int? MaxLength { get; set; }
}

/// <summary>
/// Options for redis subscriber.
/// </summary>
public class RedisSubscribeOptions : RedisPubSubOptions
{
    /// <summary>
    /// The consumer group of the subsriber.
    /// </summary>
    [Required]
    public string ConsumerGroup { get; set; } = default!;

    /// <summary>
    /// The max count of CloudEvents for each poll.
    /// </summary>
    public int PollBatchSize { get; set; } = 100;

    /// <summary>
    /// The interval between polling for new CloudEvents
    /// </summary>
    public TimeSpan PollInterval { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// The limit of unprocessed CloudEvents in local process queue.
    /// </summary>
    public int RunningWorkItemLimit { get; set; } = 128;

    /// <summary>
    /// The amount time a CloudEvent must be pending before attempting to redeliver it. Defaults to "60s".
    /// </summary>
    public TimeSpan ProcessingTimeout { get; set; } = TimeSpan.FromSeconds(60);
}
