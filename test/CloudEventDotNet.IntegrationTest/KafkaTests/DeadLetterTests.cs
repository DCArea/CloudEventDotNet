using CloudEventDotNet.IntegrationTest.Events;
using FluentAssertions;
using Xunit;

namespace CloudEventDotNet.IntegrationTest.KafkaTests;

public class DeadLetterTests : KafkaPubSubTestBase
{
    [Fact]
    public async Task ShouldSendDeadLetterBySettings()
    {
        await StartAsync();
        var eventData = new TestEvent_ShouldSendDeadLetterBySettings();

        var pe = await PubSub.PublishAsync(eventData);
        GetMonitor<TestEvent_ShouldSendDeadLetterBySettings>().WaitUntillDelivered(pe);
        var dl = GetMonitor<TestEvent_ShouldSendDeadLetterBySettings_DeadLetter>().WaitUntillDelivered($"dl:{nameof(TestEvent_ShouldSendDeadLetterBySettings)}");
        await StopAsync();

        dl.Source.Should().Be("test.source2");
        dl.Data.DeadEvent.Should().BeEquivalentTo(pe);
    }

}
