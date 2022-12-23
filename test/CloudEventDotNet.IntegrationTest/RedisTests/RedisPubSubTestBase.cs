using System.Diagnostics;
using System.Threading.Channels;
using FakeItEasy;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;
using Xunit.Abstractions;
using Xunit.Sdk;

namespace CloudEventDotNet.IntegrationTest.RedisTests;

public class RedisPubSubTestBase
{
    public const string PubsubName = "redis";
    public RedisPubSubTestBase(ITestOutputHelper output)
    {
        var services = new ServiceCollection();
        services.AddSingleton(DeliveredCloudEvents);
        services.AddLogging(log =>
        {
            //log.AddXUnit(output).SetMinimumLevel(LogLevel.Trace);
        });
        var redisConn = A.Fake<IConnectionMultiplexer>();
        var redisDb = A.Fake<IDatabase>();
        A.CallTo(() => redisConn.GetDatabase(A<int>.Ignored, A<object?>.Ignored)).Returns(redisDb);
        services.AddCloudEvents()
            .Load(GetType().Assembly)
            .AddRedisPubSub(PubsubName, opts =>
            {
                opts.ConnectionMultiplexerFactory = () => redisConn;
            }, opts =>
            {
                opts.ConnectionMultiplexerFactory = () => redisConn;
                opts.ConsumerGroup = "Test";
            })
            .AddPubSubDeadLetterSender(opts =>
            {
                opts.PubSubName = PubsubName;
                opts.Topic = "Test_DL";
            });


        ServiceProvider = services.BuildServiceProvider();
        var registry = ConfigureRegistry();
        Subscriber = (SubscribeHostedService)ServiceProvider.GetRequiredService<IHostedService>();
        Streams = registry.GetSubscribedTopics(PubsubName).ToDictionary(t => t, t => Channel.CreateUnbounded<StreamEntry>(new UnboundedChannelOptions { AllowSynchronousContinuations = false }));

        var streamIndex = 0;

        A.CallTo(() => redisDb.StreamReadGroupAsync(A<RedisKey>.Ignored, A<RedisValue>.Ignored, A<RedisValue>.Ignored, A<RedisValue>.Ignored, A<int?>.Ignored, false, CommandFlags.None))
            .ReturnsLazily(async call =>
            {
                var topic = (RedisKey)call.Arguments[0]!;
                var stream = Streams[(string)topic!];
                var items = new List<StreamEntry>();
                if (await stream.Reader.WaitToReadAsync())
                {
                    while (stream.Reader.TryRead(out var item))
                    {
                        items.Add(item);
                    }
                }
                return items.ToArray();
            });

        A.CallTo(() => redisDb.StreamAddAsync(A<RedisKey>.Ignored, "data", A<RedisValue>.Ignored, null, A<int?>.Ignored, A<bool>.Ignored, CommandFlags.None))
            .ReturnsLazily(async call =>
            {
                var data = (RedisValue)call.Arguments[2]!;
                var cloudEvent = JSON.Deserialize<CloudEvent>((byte[])data!)!;
                PublishedCloudEvents.Add(cloudEvent);
                var id = Interlocked.Increment(ref streamIndex);

                var topic = (RedisKey)call.Arguments[0]!;
                var stream = Streams[(string)topic!];
                await stream.Writer.WriteAsync(new StreamEntry(id, new NameValueEntry[] { new NameValueEntry("data", data) }));
                return (RedisValue)id;
            });

        A.CallTo(() => redisDb.StreamAcknowledgeAsync(A<RedisKey>.Ignored, A<RedisValue>.Ignored, A<RedisValue>.Ignored, CommandFlags.None))
            .ReturnsLazily(call =>
            {
                var messageId = (int)(RedisValue)call.Arguments[2]!;
                var cloudEvent = PublishedCloudEvents[messageId - 1];
                AckedCloudEvents.Add(cloudEvent);
                return Task.FromResult(1L);
            });


        Pubsub = ServiceProvider.GetRequiredService<ICloudEventPubSub>();
    }

    public ServiceProvider ServiceProvider { get; }
    public SubscribeHostedService Subscriber { get; }
    public Dictionary<string, Channel<StreamEntry>> Streams { get; }
    public ICloudEventPubSub Pubsub { get; }
    internal List<CloudEvent> PublishedCloudEvents { get; } = new();
    internal List<CloudEvent> DeliveredCloudEvents { get; } = new();
    internal List<CloudEvent> AckedCloudEvents { get; } = new();

    private Registry ConfigureRegistry()
    {
        var registry = ServiceProvider.GetRequiredService<Registry>();
        foreach (var (metadata, dele) in registry._handlerDelegates)
        {
            var handler = registry._handlers[metadata];
            HandleCloudEventDelegate newDelegate = (IServiceProvider serviceProvider, CloudEvent @event, CancellationToken token) =>
            {
                DeliveredCloudEvents.Add(@event);
                return dele(serviceProvider, @event, token);
            };
            var newHandler = ActivatorUtilities.CreateInstance<CloudEventHandler>(ServiceProvider, metadata, newDelegate);
            registry._handlers[metadata] = newHandler;
        }
        return registry;
    }

    protected async Task StopAsync()
    {
        foreach (var (_, stream) in Streams)
        {
            stream.Writer.Complete();
        }
        await Subscriber.StopAsync(default).WaitAsync(TimeSpan.FromSeconds(10));
    }

    [StackTraceHidden]
    [DebuggerHidden]
    protected void WaitUntillDelivered<TData>(CloudEvent<TData> cloudEvent, int timeoutSeconds = 5) => WaitUntill(() => DeliveredCloudEvents.Any(e => e.Id == cloudEvent.Id), timeoutSeconds * 1000);

    [StackTraceHidden]
    [DebuggerHidden]
    protected static void WaitUntill(Func<bool> condition, int timeoutSeconds = 5)
    {
        if (!SpinWait.SpinUntil(condition, timeoutSeconds * 1000))
        {
            throw new XunitException($"{condition} not satisfied within {timeoutSeconds}s");
        }
    }
}
