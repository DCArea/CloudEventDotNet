using System.Text.Json;
using Confluent.Kafka;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace CloudEventDotNet.IntegrationTest.KafkaTests;

[CloudEvent(PubSubName = "kafka")]
public record Ping(string FA)
{
    public class Handler : ICloudEventHandler<Ping>
    {
        public Task HandleAsync(CloudEvent<Ping> cloudEvent, CancellationToken token) => Task.CompletedTask;
    }
}

public class PublishTests : KafkaPubSubTestBase
{
    [Fact]
    public async Task PublishCloudEvent()
    {
        var ping = new Ping(Guid.NewGuid().ToString());
        await Pubsub.PublishAsync(ping);

        Assert.NotEmpty(Kafka.ProducedMessages);
        var produced = Kafka.ProducedMessages.Single();
        JSON.Deserialize<CloudEvent<Ping>>(produced.Message.Value)!.Data.Should().Be(ping);
    }
}
