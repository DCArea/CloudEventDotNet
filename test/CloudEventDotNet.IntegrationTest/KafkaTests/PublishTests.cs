using CloudEventDotNet.IntegrationTest.Events;
using FluentAssertions;
using Xunit;

namespace CloudEventDotNet.IntegrationTest.KafkaTests;

public class PublishTests : KafkaPubSubTestBase
{
    [Fact]
    public async Task PublishCloudEvent()
    {
        var ping = new Ping(Guid.NewGuid().ToString());
        await PubSub.PublishAsync(ping);

        Assert.NotEmpty(Kafka.ProducedMessages);
        var produced = Kafka.ProducedMessages.Single();
        JSON.Deserialize<CloudEvent<Ping>>(produced.Message.Value)!.Data.Should().Be(ping);
    }
}
