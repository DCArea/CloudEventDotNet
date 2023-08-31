using System.Text.Json;
using CloudEventDotNet.Diagnostics.Aggregators;
using CloudEventDotNet.Telemetry;
using FluentAssertions;
using Xunit;

namespace CloudEventDotNet.IntegrationTest.KafkaTests;

public class ProcessTests : KafkaPubSubTestBase
{
    [Fact]
    public async Task Subscribe()
    {
        var tags = new TagList("pubsub", "kafka",
            "topic", "Test",
            "type", nameof(Ping));

        await StartAsync();

        var ping = new Ping(Guid.NewGuid().ToString());
        var pe = await Pubsub.PublishAsync(ping);
        WaitUntillDelivered(pe, 3);
        await StopAsync();
        Collector.Processed.Count.Should().Be(1);
    }

    [CloudEvent]
    public record TestEventForRepublish(string FA)
    {
        public class Handler : ICloudEventHandler<TestEventForRepublish>
        {
            public Task HandleAsync(CloudEvent<TestEventForRepublish> cloudEvent, CancellationToken token) => throw new NotImplementedException();
        }
    };

    [CloudEvent(Topic = "Test_DL", Type = $"dl:{nameof(TestEventForRepublish)}")]
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
        await StartAsync();
        var ping = new TestEventForRepublish(Guid.NewGuid().ToString());

        var pe = await Pubsub.PublishAsync(ping);
        WaitUntillDelivered(pe, 10);
        WaitUntill(() =>
        {
            return Kafka.ProducedMessages.Count == 2;
        }, 10);
        await StopAsync();

        var de = JSON.Deserialize<CloudEvent<TestEventForRepublishDeadLetter>>(Kafka.ProducedMessages.Single(m=>m.Topic == "Test_DL").Value)!;
        de.Type.Should().Be($"dl:{nameof(TestEventForRepublish)}");
        de.Data.DeadEvent.Should().BeEquivalentTo(pe);
    }


    [CloudEvent]
    public record TestEventForRepublish2(string FA)
    {
        public class Handler : ICloudEventHandler<TestEventForRepublish2>
        {
            public Task HandleAsync(CloudEvent<TestEventForRepublish2> cloudEvent, CancellationToken token) => throw new NotImplementedException();
        }
    };
    [CloudEvent(Topic = "Test_DL", Type = $"dl:{nameof(TestEventForRepublish2)}")]
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
        await StartAsync();
        var ping = new TestEventForRepublish2(Guid.NewGuid().ToString());

        var pe = await Pubsub.PublishAsync(ping);
        WaitUntillDelivered(pe, 10);
        WaitUntill(() =>
        {
            return Kafka.ProducedMessages.Count == 2;
        }, 10);
        await StopAsync();

        var de = JSON.Deserialize<CloudEvent<TestEventForRepublishDeadLetter>>(Kafka.ProducedMessages.Single(m=>m.Topic == "Test_DL").Value)!;
        de.Type.Should().Be($"dl:{nameof(TestEventForRepublish2)}");
        de.Data.DeadEvent.Should().BeEquivalentTo(pe);
    }

    [Fact]
    public async Task DequeueTest()
    {
        await StartAsync();
        for (int i = 0; i < 100; i++)
        {
            var ping = new Ping(Guid.NewGuid().ToString());
            await Pubsub.PublishAsync(ping);
        }
        WaitUntill(() =>
        {
            return Collector.Processed.Count == 100;
        }, 5);
        await StopAsync();
    }
}
