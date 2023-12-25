using CloudEventDotNet;
using Confluent.Kafka;
using StackExchange.Redis;

namespace CloudEventTester;

public abstract class Tester
{
    public IServiceCollection Services { get; }

    public Tester()
    {
        Services = new ServiceCollection()
            .AddLogging(logging => logging.AddConsole()
                // .AddFilter("DCA.Extensions.BackgroundTask", LogLevel.Debug)
                .SetMinimumLevel(LogLevel.None)
            );

        string providerName = Environment.GetEnvironmentVariable("PROVIDER")!.ToLowerInvariant();
        if (providerName == "kafka")
        {
            ConfigureKafka();
        }
        else if (providerName == "redis")
        {
            ConfigureRedis();
        }
    }

    public abstract Task RunAsync(string[] args);

    protected virtual PubSubBuilder ConfigureKafka()
    {
        PubSubBuilder builder = Services.AddCloudEvents(defaultPubSubName: "kafka", defaultTopic: KafkaEnv.topic)
            .Load(typeof(Ping).Assembly);
        return builder.AddKafkaPubSub("kafka", options =>
        {
            options.ProducerConfig = new ProducerConfig
            {
                BootstrapServers = KafkaEnv.broker,
                Acks = Acks.Leader,
                LingerMs = 10
            };
        }, options =>
        {
            options.ConsumerConfig = new ConsumerConfig
            {
                BootstrapServers = KafkaEnv.broker,
                GroupId = KafkaEnv.consumerGroup,
                AutoOffsetReset = KafkaEnv.autoOffsetReset,

                QueuedMinMessages = 300_000,
                FetchWaitMaxMs = 1_000
            };
            options.RunningWorkItemLimit = KafkaEnv.runningWorkItemLimit;
        });
    }

    protected virtual PubSubBuilder ConfigureRedis()
    {
        var redis = ConnectionMultiplexer.Connect(RedisEnv.redisConnectionString);
        return Services.AddCloudEvents(defaultPubSubName: "redis", defaultTopic: RedisEnv.topic)
            .Load(typeof(Ping).Assembly)
            .AddRedisPubSub("redis", options =>
            {
                options.ConnectionMultiplexerFactory = () => redis;
                options.MaxLength = RedisEnv.maxLength;
            }, options =>
            {
                options.ConnectionMultiplexerFactory = () => redis;
                options.ConsumerGroup = RedisEnv.consumerGroup;
                options.RunningWorkItemLimit = RedisEnv.runningWorkItemLimit;
            });
    }
}
