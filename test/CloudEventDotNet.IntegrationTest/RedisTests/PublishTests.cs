using System.Collections.Concurrent;
using System.Diagnostics;
using System.Text.Json;
using CloudEventDotNet.Redis;
using FakeItEasy;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using StackExchange.Redis;
using Xunit;

namespace CloudEventDotNet.IntegrationTest.RedisTests;

[CloudEvent(PubSubName = "redis")]
public record Ping(string FA)
{
    public class Handler(SubscriptionMonitor<Ping> monitor) : ICloudEventHandler<Ping>
    {
        public Task HandleAsync(CloudEvent<Ping> cloudEvent, CancellationToken token)
        {
            monitor.DeliveredEvents.Add(cloudEvent);
            return Task.CompletedTask;
        }
    }
}

public class PublishTests
{
    [Fact]
    public async Task PublishCloudEvent()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddCloudEvents()
            .Load(typeof(Ping).Assembly)
            .AddRedisPubSub("redis", opts => { }, null)
            .Build();
        var redisConn = A.Fake<IConnectionMultiplexer>();
        var redisDb = A.Fake<IDatabase>();
        A.CallTo(() => redisConn.GetDatabase(A<int>.Ignored, A<object?>.Ignored)).Returns(redisDb);
        services.Configure<RedisPublishOptions>("redis", opt => opt.ConnectionMultiplexerFactory = () => redisConn);

        var publishedCloudEvents = new List<CloudEvent>();
        A.CallTo(() => redisDb.StreamAddAsync(A<RedisKey>.Ignored, "data", A<RedisValue>.Ignored, null, A<int?>.Ignored, true, CommandFlags.None))
            .Invokes(call =>
            {
                var data = (RedisValue)call.Arguments[2]!;
                var cloudEvent = JSON.Deserialize<CloudEvent>((byte[])data!)!;
                publishedCloudEvents.Add(cloudEvent);
            });

        var sp = services.BuildServiceProvider();

        var ping = new Ping(Guid.NewGuid().ToString());
        var pubsub = sp.GetRequiredService<ICloudEventPubSub>();
        await pubsub.PublishAsync(ping);

        Assert.NotEmpty(publishedCloudEvents);
        var published = publishedCloudEvents[0];
        published.Data.Deserialize<Ping>(JSON.DefaultJsonSerializerOptions).Should().Be(ping);
    }
}
