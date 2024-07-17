using CloudEventDotNet.IntegrationTest.Events;
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
        var pe = await PubSub.PublishAsync(ping);
        GetMonitor<Ping>().WaitUntillDelivered(pe);
        await StopAsync();

        GetMonitor<Ping>().WaitUntillCount(1);
    }

    [Fact]
    public async Task ShouldSendDeadLetter()
    {
        await StartAsync();
        var ping = new TestEventForRepublish(Guid.NewGuid().ToString());

        var pe = await PubSub.PublishAsync(ping);
        GetMonitor<TestEventForRepublish>().WaitUntillDelivered(pe);
        WaitHelper.WaitUntill(() => Kafka.ProducedMessages.Count == 2);
        await StopAsync();

        var de = JSON.Deserialize<CloudEvent<TestEventForRepublishDeadLetter>>(Kafka.ProducedMessages.Single(m => m.Topic == "Test_DL").Value)!;
        de.Type.Should().Be($"dl:{nameof(TestEventForRepublish)}");
        de.Data.DeadEvent.Should().BeEquivalentTo(pe);
    }

    [Fact]
    public async Task ShouldNotSendDeadLetterForDeadLetter()
    {
        await StartAsync();
        var ping = new TestEventForRepublish2(Guid.NewGuid().ToString());

        var pe = await PubSub.PublishAsync(ping);
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
            await PubSub.PublishAsync(ping);
        }
        GetMonitor<Ping>().WaitUntillCount(100);
        await StopAsync();
    }
}
