using System.Text.Json;
using CloudEventDotNet.Diagnostics.Aggregators;
using CloudEventDotNet.Telemetry;
using FluentAssertions;
using Xunit;

namespace CloudEventDotNet.IntegrationTest.RedisTests;

public class ProcessTests : RedisPubSubTestBase
{
    [Fact]
    public async Task Subscribe()
    {
        await Subscriber.StartAsync(default);

        var ping = new Ping(Guid.NewGuid().ToString());
        var pe = await Pubsub.PublishAsync(ping);
        WaitUntillDelivered(pe, 3);
        await StopAsync();
        
        DeliveredCloudEvents.Count.Should().Be(1);
    }

    [CloudEvent(PubSubName = "redis")]
    public record TestEventForRepublish(string FA)
    {
        public class Handler : ICloudEventHandler<TestEventForRepublish>
        {
            public Task HandleAsync(CloudEvent<TestEventForRepublish> cloudEvent, CancellationToken token) => throw new NotImplementedException();
        }
    };

    [CloudEvent(PubSubName = "redis", Topic = "Test_DL", Type = $"dl:{nameof(TestEventForRepublish)}")]
    public record TestEventForRepublishDeadLetter() : DeadLetter<TestEventForRepublish>
    {
        public class Handler : ICloudEventHandler<TestEventForRepublishDeadLetter>
        {
            public Task HandleAsync(CloudEvent<TestEventForRepublishDeadLetter> cloudEvent, CancellationToken token)
            {
                //cloudEvent.Data.DeadEvent.Should().NotBeNull();
                return Task.CompletedTask;
            }
        }
    }


    [Fact]
    public async Task ShouldSendDeadLetter()
    {
        await Subscriber.StartAsync(default);
        var ping = new TestEventForRepublish(Guid.NewGuid().ToString());

        var pe = await Pubsub.PublishAsync(ping);
        WaitUntillDelivered(pe, 10);
        WaitUntill(() =>
        {
            return PublishedCloudEvents.Count == 2;
        }, 10);
        await StopAsync();

        var de = PublishedCloudEvents.Single(e=>e.Type.StartsWith("dl:"));
        de.Type.Should().Be($"dl:{nameof(TestEventForRepublish)}");
        var deadLetter = de.Data.Deserialize<TestEventForRepublishDeadLetter>(JSON.DefaultJsonSerializerOptions)!;
        deadLetter.DeadEvent.Should().BeEquivalentTo(pe);
    }


    [CloudEvent(PubSubName = "redis")]
    public record TestEventForRepublish2(string FA)
    {
        public class Handler : ICloudEventHandler<TestEventForRepublish2>
        {
            public Task HandleAsync(CloudEvent<TestEventForRepublish2> cloudEvent, CancellationToken token) => throw new NotImplementedException();
        }
    };
    [CloudEvent(PubSubName = "redis", Topic = "Test_DL", Type = $"dl:{nameof(TestEventForRepublish2)}")]
    public record TestEventForRepublish2DeadLetter() : DeadLetter<TestEventForRepublish2>
    {
        public class Handler : ICloudEventHandler<TestEventForRepublish2DeadLetter>
        {
            public Task HandleAsync(CloudEvent<TestEventForRepublish2DeadLetter> cloudEvent, CancellationToken token) => throw new NotImplementedException();
        }
    }

    [Fact]
    public async Task ShouldNotSendDeadLetterForDeadLetter()
    {
        await Subscriber.StartAsync(default);
        var ping = new TestEventForRepublish2(Guid.NewGuid().ToString());

        var pe = await Pubsub.PublishAsync(ping);
        WaitUntillDelivered(pe, 10);
        WaitUntill(() =>
        {
            return DeliveredCloudEvents.Count == 2;
        }, 10);
        await StopAsync();

        PublishedCloudEvents.Should().HaveCount(2);
        var de = PublishedCloudEvents.Single(e=>e.Type.StartsWith("dl:"));
        de.Type.Should().Be($"dl:{nameof(TestEventForRepublish2)}");
        var deadLetter = de.Data.Deserialize<TestEventForRepublish2DeadLetter>(JSON.DefaultJsonSerializerOptions)!;
        deadLetter.DeadEvent.Should().BeEquivalentTo(pe);
    }

    [Fact]
    public async Task DequeueTest()
    {
        await Subscriber.StartAsync(default);
        for (int i = 0; i < 100; i++)
        {
            var ping = new Ping(Guid.NewGuid().ToString());
            await Pubsub.PublishAsync(ping);
        }
        WaitUntill(() =>
        {
            return DeliveredCloudEvents.Count == 100;
        }, 10);
        await StopAsync();
    }
}
