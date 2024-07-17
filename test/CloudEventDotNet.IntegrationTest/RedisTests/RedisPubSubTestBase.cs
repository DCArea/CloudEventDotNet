using System.Collections.Concurrent;
using System.Threading.Channels;
using FakeItEasy;
using Microsoft.Extensions.DependencyInjection;
using StackExchange.Redis;

namespace CloudEventDotNet.IntegrationTest.RedisTests;

public class RedisPubSubTestBase : PubSubTestBase
{
    public const string PubsubName = "redis";
    public const string Topic = "test_topic";
    public const string Source = "test.source";

    public IDatabase redisDb = A.Fake<IDatabase>();
    public ConcurrentDictionary<string, Channel<StreamEntry>> Streams { get; set; } = [];
    internal ConcurrentBag<CloudEvent> PublishedCloudEvents { get; } = [];
    internal List<CloudEvent> AckedCloudEvents { get; } = [];

    protected override void ConfigurePubSub(IServiceCollection services)
    {
        var streamIndex = 0;
        A.CallTo(() => redisDb.StreamAddAsync(A<RedisKey>.Ignored, "data", A<RedisValue>.Ignored, null, A<int?>.Ignored, A<bool>.Ignored, CommandFlags.None))
            .ReturnsLazily(async call =>
            {
                var data = (RedisValue)call.Arguments[2]!;
                var cloudEvent = JSON.Deserialize<CloudEvent>((byte[])data!)!;
                PublishedCloudEvents.Add(cloudEvent);
                var id = Interlocked.Increment(ref streamIndex);

                var topic = (string)((RedisKey)call.Arguments[0]!)!;
                var stream = Streams.GetOrAdd(topic, t => Channel.CreateUnbounded<StreamEntry>(new UnboundedChannelOptions { AllowSynchronousContinuations = false }));
                await stream.Writer.WriteAsync(new StreamEntry(id, [new NameValueEntry("data", data)]));
                return (RedisValue)id;
            });

        var redisConn = A.Fake<IConnectionMultiplexer>();
        A.CallTo(() => redisConn.GetDatabase(A<int>.Ignored, A<object?>.Ignored)).Returns(redisDb);
        services.AddCloudEvents(PubsubName, Topic, Source)
            .AddRedisPubSub(PubsubName, opts =>
            {
                opts.ConnectionMultiplexerFactory = () => redisConn;
            }, opts =>
            {
                opts.ConnectionMultiplexerFactory = () => redisConn;
                opts.ConsumerGroup = "Test";
                opts.RunningWorkItemLimit = 8;
                opts.PollInterval = TimeSpan.FromSeconds(1);
            })
            .EnableDeadLetter(defaultDeadLetterTopic: "Test_DL")
            .Load(GetType().Assembly)
            .Build();

        //var registry = ServiceProvider.GetRequiredService<Registry>();
        //Streams = registry.GetSubscribedTopics(PubsubName).ToDictionary(t => t, t => Channel.CreateUnbounded<StreamEntry>(new UnboundedChannelOptions { AllowSynchronousContinuations = false }));

    }

    protected override Task StartAsync()
    {
        A.CallTo(() => redisDb.StreamReadGroupAsync(A<RedisKey>.Ignored, A<RedisValue>.Ignored, A<RedisValue>.Ignored, A<RedisValue>.Ignored, A<int?>.Ignored, false, CommandFlags.None))
            .ReturnsLazily(async call =>
            {
                var topic = (string)((RedisKey)call.Arguments[0]!)!;
                var stream = Streams.GetOrAdd(topic, t => Channel.CreateUnbounded<StreamEntry>(new UnboundedChannelOptions { AllowSynchronousContinuations = false }));
                var items = new List<StreamEntry>();
                if (await stream.Reader.WaitToReadAsync())
                {
                    while (stream.Reader.TryRead(out var item))
                    {
                        items.Add(item);
                    }
                }
                return [.. items];
            });

        A.CallTo(() => redisDb.StreamAcknowledgeAsync(A<RedisKey>.Ignored, A<RedisValue>.Ignored, A<RedisValue>.Ignored, CommandFlags.None))
            .ReturnsLazily(call =>
            {
                return Task.FromResult(1L);
            });

        return base.StartAsync();
    }

    protected override async Task StopAsync()
    {
        foreach (var (_, stream) in Streams)
        {
            stream.Writer.Complete();
        }
        await base.StopAsync();
    }

}
