using FluentAssertions;
using Xunit;

namespace CloudEventDotNet.IntegrationTest.KafkaTests;

public class ProcessTests : KafkaPubSubTestBase
{
    [Fact]
    public async Task Subscribe()
    {
        await StartAsync();

        var ping = new Ping(Guid.NewGuid().ToString());
        var pe = await Pubsub.PublishAsync(ping);
        GetMonitor<Ping>().WaitUntillDelivered(pe, 3);
        await StopAsync();

        GetMonitor<Ping>().WaitUntillCount(1);
    }

    [CloudEvent]
    public record TestEventForRepublish(string FA)
    {
        public class Handler(SubscriptionMonitor<TestEventForRepublish> monitor) : MonitoredHandler<TestEventForRepublish>(monitor)
        {
            public override Task HandleInternalAsync(CloudEvent<TestEventForRepublish> cloudEvent, CancellationToken token) => throw new NotImplementedException();
        }
    };

    [CloudEvent(Topic = "Test_DL", Type = $"dl:{nameof(TestEventForRepublish)}")]
    public record TestEventForRepublishDeadLetter() : DeadLetter<TestEventForRepublish>
    {
        public class Handler(SubscriptionMonitor<TestEventForRepublishDeadLetter> monitor)
            : MonitoredHandler<TestEventForRepublishDeadLetter>(monitor)
        {
            public override Task HandleInternalAsync(CloudEvent<TestEventForRepublishDeadLetter> cloudEvent, CancellationToken token)
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
        GetMonitor<TestEventForRepublish>().WaitUntillDelivered(pe, 10);
        WaitHelper.WaitUntill(() => Kafka.ProducedMessages.Count == 2);
        await StopAsync();

        var de = JSON.Deserialize<CloudEvent<TestEventForRepublishDeadLetter>>(Kafka.ProducedMessages.Single(m => m.Topic == "Test_DL").Value)!;
        de.Type.Should().Be($"dl:{nameof(TestEventForRepublish)}");
        de.Data.DeadEvent.Should().BeEquivalentTo(pe);
    }


    [CloudEvent]
    public record TestEventForRepublish2(string FA)
    {
        public class Handler(SubscriptionMonitor<TestEventForRepublish2> monitor)
            : MonitoredHandler<TestEventForRepublish2>(monitor)
        {
            public override Task HandleInternalAsync(CloudEvent<TestEventForRepublish2> cloudEvent, CancellationToken token) => throw new NotImplementedException();
        }
    };
    [CloudEvent(Topic = "Test_DL", Type = $"dl:{nameof(TestEventForRepublish2)}")]
    public record TestEventForRepublish2DeadLetter() : DeadLetter<TestEventForRepublish2>
    {
        public class Handler(SubscriptionMonitor<TestEventForRepublish2DeadLetter> monitor)
            : MonitoredHandler<TestEventForRepublish2DeadLetter>(monitor)
        {
            public override Task HandleInternalAsync(CloudEvent<TestEventForRepublish2DeadLetter> cloudEvent, CancellationToken token) => throw new NotImplementedException();
        }
    }

    [Fact]
    public async Task ShouldNotSendDeadLetterForDeadLetter()
    {
        await StartAsync();
        var ping = new TestEventForRepublish2(Guid.NewGuid().ToString());

        var pe = await Pubsub.PublishAsync(ping);
        GetMonitor<TestEventForRepublish2>().WaitUntillDelivered(pe);
        WaitHelper.WaitUntill(() => Kafka.ProducedMessages.Count == 2);
        await StopAsync();

        var de = JSON.Deserialize<CloudEvent<TestEventForRepublishDeadLetter>>(Kafka.ProducedMessages.Single(m => m.Topic == "Test_DL").Value)!;
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
        GetMonitor<Ping>().WaitUntillCount(100);
        await StopAsync();
    }
}
